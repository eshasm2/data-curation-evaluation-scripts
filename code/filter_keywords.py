# filter_keywords - Stage 1 — Query CommonCrawl for local news articles matching keyword lists.
#
# This is the first stage and the only one that talks to CommonCrawl directly.
# It queries the CDX index for each source domain and downloads matching article text.
#
# Usage (via cli.py)
#   python cli.py --resource filter_keywords --years 2018-2024
#
# Usage (direct)
#   python code/filter_keywords.py -y 2018-2024
#
# Outputs (data/data_filter_keywords_<years>/)
#   matched_articles.txt   — full article text, one block per article
#   keyword_matches.csv    — matched URLs with source/keyword metadata
#   report.csv             — timestamped log of crawl progress

import argparse
import csv
import io
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator
import ahocorasick

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from code.config import (
    parse_range, validate_years, load_terms, log_report,
    keywords_dir, sources_dir, cc_collections,
    max_articles_per_source, min_article_chars,
    cc_request_delay, cc_retry_attempts,
    get_output_dir,
)

csv.field_size_limit(2**31 - 1)


def build_automaton(words):
    # builds an Aho-Corasick automaton from a list of keyword strings
    ac = ahocorasick.Automaton()
    for idx, word in enumerate(words):
        ac.add_word(word.lower(), (idx, word))
    ac.make_automaton()
    return ac


def url_matches(url, ac):
    # returns all keywords found in a URL string
    return list({match for _, (_, match) in ac.iter(url.lower())})


def load_sources(path):
    # loads the sources CSV and keeps only the columns we need
    df = pd.read_csv(path)
    return df[["name", "state", "city", "URL"]].dropna()


def domain_variants(domain):
    # try common subdomains in case the CDX index uses a different prefix
    domain = domain.lower().strip()
    variants = [domain]
    if not domain.startswith("www."):
        variants.append(f"www.{domain}")
    for sub in ["news", "blog", "m", "mobile", "editorial"]:
        variants.append(f"{sub}.{domain}")
    return variants


def query_cc(domain, year):
    # query CommonCrawl CDX index for all pages under a domain in a given year.
    # tries each configured collection with exponential backoff on failure.
    # returns a flat list of CDX records (dicts with url, filename, offset, length).
    collections = cc_collections.get(year, [])
    records = []
    for col in collections:
        index_url = f"https://index.commoncrawl.org/{col}-index"
        for attempt in range(1, cc_retry_attempts + 1):
            try:
                resp = requests.get(
                    index_url,
                    params={"url": f"{domain}/*", "matchType": "host", "output": "json"},
                    timeout=60,
                )
                resp.raise_for_status()
                time.sleep(cc_request_delay)
                lines = [json.loads(l) for l in resp.text.splitlines()]
                if lines:
                    records.extend(lines)
                    break
            except requests.RequestException as e:
                backoff = 3 ** attempt
                print(f"Attempt {attempt} failed for {domain} ({year}, {col}): {e}. Retry in {backoff}s.")
                time.sleep(backoff)
    return records


def extract_text(html):
    # pull readable article body from raw HTML.
    # priority: <article> tag → known publisher div classes → <body>.
    # scripts, styles, and noscript blocks are stripped first.
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    article = soup.find("article")
    if article and len(article.get_text(strip=True)) > min_article_chars:
        return article.get_text(separator="\n", strip=True)
    for cls in ["content__article-body", "article-body", "l-container",
                "zn-body-text", "pg-rail-tall__body"]:
        div = soup.find("div", class_=cls)
        if div and len(div.get_text(strip=True)) > min_article_chars:
            return div.get_text(separator="\n", strip=True)
    body = soup.find("body")
    if body and len(body.get_text(strip=True)) > min_article_chars:
        return body.get_text(separator="\n", strip=True)
    return ""


def fetch_warc(record):
    # fetch one WARC record from CommonCrawl using a byte-range request.
    # each CDX record points to an exact byte offset in a WARC file on S3,
    # so we download only that article rather than the whole file.
    # returns extracted text if long enough, otherwise None.
    url = f"https://data.commoncrawl.org/{record['filename']}"
    start, length = int(record["offset"]), int(record["length"])
    try:
        resp = requests.get(
            url, headers={"Range": f"bytes={start}-{start+length-1}"}, timeout=20
        )
        resp.raise_for_status()
        for rec in ArchiveIterator(io.BytesIO(resp.content)):
            if rec.rec_type == "response":
                html = rec.content_stream().read().decode("utf-8", errors="ignore")
                text = extract_text(html)
                if len(text.strip()) > min_article_chars:
                    return text.strip()
    except Exception as e:
        print(f"WARC read error: {e}")
    return None


def read_last_processed(path):
    # parse the output txt to find the most recent year collected per domain.
    # this allows resuming an interrupted crawl without re-processing completed sources.
    last = {}
    if not Path(path).exists():
        return last
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if line.strip() == "---" and i + 3 < len(lines):
            try:
                url  = lines[i+1].strip().replace("url:", "").strip()
                year = int(lines[i+2].strip().replace("year:", "").strip())
                dom  = urlparse(url).netloc or url
                if dom not in last or year > last[dom]:
                    last[dom] = year
            except Exception:
                continue
    return last


def get_args(argv=None):
    parser = argparse.ArgumentParser(description="Stage 1 — crawl CommonCrawl and collect matching articles.")
    parser.add_argument("-y", "--years",  required=True)
    parser.add_argument("-t", "--type",   default="articles")
    parser.add_argument("-i", "--input",  type=str)
    parser.add_argument("-o", "--output", type=str)
    args = parser.parse_args(argv)
    validate_years(args.years, parser)
    return args


def main(argv=None):
    args = get_args(argv)
    years = parse_range(args.years)

    out_dir = Path(args.output) if args.output else get_output_dir("filter_keywords", args.years)
    out_dir.mkdir(parents=True, exist_ok=True)

    # set up output files and logging
    report_path = str(out_dir / "report.csv")
    txt_path    = out_dir / "matched_articles.txt"
    csv_path    = out_dir / "keyword_matches.csv"

    log_report(report_path, f"Starting filter_keywords | years={args.years}")

    # load keyword lists and source inventory
    keywords_t1 = load_terms(str(keywords_dir / "keywords_tier1.txt"))
    keywords_t2 = load_terms(str(keywords_dir / "keywords_tier2.txt"))
    automaton   = build_automaton(keywords_t1 + keywords_t2)
    sources     = load_sources(str(sources_dir / "sources.csv"))

    # check what's already been collected so we can resume if interrupted
    last_years  = read_last_processed(txt_path)

    log_report(report_path, f"Loaded {len(keywords_t1)+len(keywords_t2)} keywords | {len(sources)} sources")

    with open(csv_path, "a", newline="", encoding="utf-8") as csvfile, \
         open(txt_path, "a", encoding="utf-8") as txtfile:

        writer = csv.writer(csvfile)
        # only write header if the file is new — avoid duplicate headers on resume
        if csv_path.stat().st_size == 0:
            writer.writerow(["source", "state", "url", "year", "matched_keywords"])

        for _, row in tqdm(sources.iterrows(), total=len(sources), desc="Sources"):
            domain      = urlparse(row["URL"]).netloc or row["URL"]
            source_name = row["name"]
            state       = row["state"]

            for year in years:
                # skip if we already collected this domain through this year
                if domain in last_years and year <= last_years[domain]:
                    continue

                matched = 0
                for variant in domain_variants(domain):
                    for rec in query_cc(variant, year):
                        url = rec.get("url")
                        if not url:
                            continue
                        matches = url_matches(url, automaton)
                        if not matches:
                            continue
                        text = fetch_warc(rec)
                        if not text:
                            continue

                        writer.writerow([source_name, state, url, year, ";".join(matches)])
                        csvfile.flush()

                        txtfile.write("---\n")
                        txtfile.write(f"url: {url}\n")
                        txtfile.write(f"year: {year}\n")
                        txtfile.write(f"source: {source_name}\n")
                        txtfile.write(f"state: {state}\n")
                        txtfile.write(f"text:\n{text}\n\n")
                        txtfile.flush()

                        matched += 1
                        if matched >= max_articles_per_source:
                            break
                    if matched >= max_articles_per_source:
                        break

    log_report(report_path, "filter_keywords complete")


if __name__ == "__main__":
    main()
