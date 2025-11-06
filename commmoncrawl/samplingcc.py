import os, io, csv, time, json
from urllib.parse import urlparse

import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import ahocorasick

# Increase CSV field limit for large content
csv.field_size_limit(2**31 - 1)

drive_dir = "/content/drive/MyDrive/ccrawl_results"
os.makedirs(drive_dir, exist_ok=True)

txt_file = os.path.join(drive_dir, "matched_articles.txt") # full article text
csv_file = os.path.join(drive_dir, "keyword_matches.csv") # matched URLs and keywords

max_per_source = 80   # max articles per source per year
years = [2024]        # years to process
cc_2024 = [           # Common Crawl collections for 2024
    "CC-MAIN-2024-10", "CC-MAIN-2024-18", "CC-MAIN-2024-22",
    "CC-MAIN-2024-26", "CC-MAIN-2024-30", "CC-MAIN-2024-33",
    "CC-MAIN-2024-38", "CC-MAIN-2024-42", "CC-MAIN-2024-46",
    "CC-MAIN-2024-51"
]

def load_keywords(path):
    # read keywords from file, strip empty lines
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def build_automaton(words):
    # build Aho-Corasick automaton for keyword matching
    ac = ahocorasick.Automaton()
    for idx, word in enumerate(words):
        ac.add_word(word.lower(), (idx, word))
    ac.make_automaton()
    return ac


def load_sources(path):
    # read source CSV, keep only relevant columns
    df = pd.read_csv(path)
    return df[['name', 'state', 'city', 'URL']].dropna()


def query_cc(domain, year, retries=3, delay=4):
    # query Common Crawl index for domain/year, return list of records
    if year == 2024:
        collections = cc_2024
    else:
        year_map = {2018: "CC-MAIN-2017-51", 2019: "CC-MAIN-2018-51",
                    2020: "CC-MAIN-2019-51", 2023: "CC-MAIN-2023-14"}
        collections = [year_map.get(year)] if year in year_map else []

    records = []

    # loop through collections
    for col in collections:
        url = f"https://index.commoncrawl.org/{col}-index"
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(
                    url,
                    params={"url": f"{domain}/*", "matchType": "host", "output": "json"},
                    timeout=60
                )
                resp.raise_for_status()
                time.sleep(delay)

                # parse JSON lines
                lines = [json.loads(l) for l in resp.text.splitlines()]
                if lines:
                    records.extend(lines)
                    break
            except requests.RequestException as e:
                backoff = 3 ** attempt
                print(f"Attempt {attempt} failed for {domain} ({year}, {col}): {e}. Retrying in {backoff}s.")
                time.sleep(backoff)

    return records


def domain_variants(domain):
    # generate common subdomain variants for crawling
    domain = domain.lower().strip()
    variants = [domain]
    if not domain.startswith("www."):
        variants.append(f"www.{domain}")
    subs = ["news", "blog", "m", "mobile", "editorial"]
    variants.extend([f"{s}.{domain}" for s in subs])
    return variants


def url_matches(url, ac):
    # return matched keywords in URL
    return list({m for _, (_, m) in ac.iter(url.lower())})


def extract_text(html):
    # extract readable article text from HTML
    soup = BeautifulSoup(html, "html.parser")

    # remove non-content elements
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # try <article> tag first
    article = soup.find("article")
    if article and len(article.get_text(strip=True)) > 1000:
        return article.get_text(separator="\n", strip=True)

    # then check common div classes
    div_classes = ["content__article-body", "article-body", "l-container",
                   "zn-body-text", "pg-rail-tall__body"]
    for cls in div_classes:
        div = soup.find("div", class_=cls)
        if div and len(div.get_text(strip=True)) > 1000:
            return div.get_text(separator="\n", strip=True)

    # fallback to <body>
    body = soup.find("body")
    if body and len(body.get_text(strip=True)) > 1000:
        return body.get_text(separator="\n", strip=True)

    return ""


def extract_warc(record):
    # download WARC snippet and extract text if >1000 chars
    url = f"https://data.commoncrawl.org/{record['filename']}"
    start = int(record["offset"])
    length = int(record["length"])
    headers = {"Range": f"bytes={start}-{start + length - 1}"}

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        stream = io.BytesIO(resp.content)

        # iterate through WARC records
        for rec in ArchiveIterator(stream):
            if rec.rec_type == "response":
                html = rec.content_stream().read().decode("utf-8", errors="ignore")
                text = extract_text(html)
                if len(text.strip()) > 1000:
                    return text.strip()
    except Exception as e:
        print(f"WARC read error: {e}")

    return None


def read_last_years(path):
    # read previously processed URLs and last processed year
    last = {}
    if not os.path.exists(path):
        return last

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # parse lines with '---' separator to find URL/year
    for i, line in enumerate(lines):
        if line.strip() == "---" and i + 3 < len(lines):
            try:
                url_line = lines[i + 1].strip()
                year_line = lines[i + 2].strip()
                url = url_line.replace("url:", "").strip()
                year = int(year_line.replace("year:", "").strip())
                dom = urlparse(url).netloc or url
                if dom not in last or year > last[dom]:
                    last[dom] = year
            except Exception:
                continue
    return last
