import requests
import json
import time
import ahocorasick
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import io
import os
import csv
import shutil
from urllib.parse import urlparse

from google.colab import drive
drive.mount('/content/drive')

DRIVE_DIR = "/content/drive/MyDrive/ccrawl_results"
os.makedirs(DRIVE_DIR, exist_ok=True)

ARTICLE_TEXT_PATH = f"{DRIVE_DIR}/matched_articles_50.txt"
OUTPUT_CSV_PATH = f"{DRIVE_DIR}/keyword_matches_per_source_per_year_50.csv"
BACKUP_ARTICLE = f"{DRIVE_DIR}/matched_articles_new_50.txt"
BACKUP_CSV = f"{DRIVE_DIR}/keyword_matches_new_50.csv"

MAX_MATCHED_URLS_PER_SOURCE_PER_YEAR = 80
YEARS_TO_PROCESS = [2024]

COLLECTIONS_2024 = ["CC-MAIN-2024-10", "CC-MAIN-2024-18", "CC-MAIN-2024-22",  "CC-MAIN-2024-26",  "CC-MAIN-2024-30",
                    "CC-MAIN-2024-33",  "CC-MAIN-2024-38",  "CC-MAIN-2024-42",  "CC-MAIN-2024-46",  "CC-MAIN-2024-51"]



def backup_to_drive():
    try:
        shutil.copy(ARTICLE_TEXT_PATH, BACKUP_ARTICLE)
        shutil.copy(OUTPUT_CSV_PATH, BACKUP_CSV)
        print("📦 Backup completed to Google Drive.")
    except Exception as e:
        print(f"⚠️ Backup failed: {e}")

def load_keywords(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def build_automaton(keywords):
    A = ahocorasick.Automaton()
    for idx, keyword in enumerate(keywords):
        A.add_word(keyword.lower(), (idx, keyword))
    A.make_automaton()
    return A

def load_sources(csv_path):
    df = pd.read_csv(csv_path)
    return df[['name', 'state', 'city', 'URL']].dropna()

def query_commoncrawl(domain, year, base_delay=4, retries=3):
    if year != 2024:
        collection = {
            2018: "CC-MAIN-2017-51",
            2019: "CC-MAIN-2018-51",
            2020: "CC-MAIN-2019-51",
            2023: "CC-MAIN-2023-14"
        }.get(year)
        collections_to_try = [collection] if collection else []
    else:
        collections_to_try = COLLECTIONS_2024

    all_records = []
    for collection in collections_to_try:
        url = f"https://index.commoncrawl.org/{collection}-index"
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    url,
                    params={"url": f"{domain}/*", "matchType": "host", "output": "json"},
                    timeout=60,
                )
                response.raise_for_status()
                time.sleep(base_delay)
                records = [json.loads(line) for line in response.text.splitlines()]
                if records:
                    all_records.extend(records)
                    break
            except requests.RequestException as e:
                print(f" Failed to query {domain} ({year}, {collection}), attempt {attempt}/{retries}: {e}")
                backoff = 3 ** attempt
                time.sleep(backoff)
    return all_records

def domain_variants(domain):
    """
    Return domain variants including common subdomains.
    """
    domain = domain.lower().strip()
    variants = [domain]
    if not domain.startswith("www."):
        variants.append(f"www.{domain}")

    subdomains = ["news", "blog", "m", "mobile", "editorial"]
    for sub in subdomains:
        variants.append(f"{sub}.{domain}")

    return variants

def find_matches(url, automaton):
    return list({match for _, (_, match) in automaton.iter(url.lower())})

def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    article = soup.find("article")
    if article and len(article.get_text(strip=True)) > 1000:
        return article.get_text(separator="\n", strip=True)

    candidates = ["content__article-body", "article-body", "l-container", "zn-body-text", "pg-rail-tall__body"]
    for class_name in candidates:
        div = soup.find("div", class_=class_name)
        if div and len(div.get_text(strip=True)) > 1000:
            return div.get_text(separator="\n", strip=True)

    body = soup.find("body")
    if body and len(body.get_text(strip=True)) > 1000:
        return body.get_text(separator="\n", strip=True)

    return ""

def extract_body_from_warc(record):
    warc_path = f"https://data.commoncrawl.org/{record['filename']}"
    offset = int(record["offset"])
    length = int(record["length"])

    headers = {"Range": f"bytes={offset}-{offset+length-1}"}
    try:
        response = requests.get(warc_path, headers=headers, timeout=20)
        response.raise_for_status()
        stream = io.BytesIO(response.content)
        for rec in ArchiveIterator(stream):
            if rec.rec_type == "response":
                raw_html = rec.content_stream().read().decode("utf-8", errors="ignore")
                body_text = extract_text(raw_html)
                if len(body_text.strip()) > 1000:
                    return body_text.strip()
    except Exception as e:
        print(f"Error reading WARC: {e}")
    return None

def read_last_processed_years(article_path):
    last_processed = {}
    if not os.path.exists(article_path):
        return last_processed

    with open(article_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for i in range(len(lines)):
        if lines[i].strip() == "---" and i + 3 < len(lines):
            url_line = lines[i+1].strip()
            year_line = lines[i+2].strip()

            try:
                url = url_line.replace("url:", "").strip()
                year = int(year_line.replace("year:", "").strip())
                domain = urlparse(url).netloc or url

                if domain not in last_processed or year > last_processed[domain]:
                    last_processed[domain] = year
            except:
                continue

    return last_processed


tier1_keywords = load_keywords("all_tier1_final.txt")
tier2_keywords = load_keywords("all_tier2_final.txt")
sources = load_sources("Sheet_NE - Sheet1.csv")

automaton_tier1 = build_automaton(tier1_keywords)
automaton_tier2 = build_automaton(tier2_keywords)

seen_urls = set()
if os.path.exists(ARTICLE_TEXT_PATH):
    with open(ARTICLE_TEXT_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("url: "):
                seen_urls.add(line.replace("url:", "").strip())

last_processed_years = read_last_processed_years(ARTICLE_TEXT_PATH)

csv_header = [
    "source_name", "state", "city", "domain", "year", "url", "timestamp",
    "status", "digest", "matched_keywords", "tier", "article_line_start"
]
write_csv_header = not os.path.exists(OUTPUT_CSV_PATH)
csv_file = open(OUTPUT_CSV_PATH, "a", encoding="utf-8", newline="")
csv_writer = csv.DictWriter(csv_file, fieldnames=csv_header)
if write_csv_header:
    csv_writer.writeheader()

line_offset = 0
start_index = 0
sources_to_process = sources.iloc[start_index:].reset_index(drop=True)

with open(ARTICLE_TEXT_PATH, "a", encoding="utf-8") as f_text:
    for i, row in tqdm(sources_to_process.iterrows(), total=len(sources_to_process), desc="Processing sources (starting from 0)"):
        name, state, city, domain = row["name"], row["state"], row["city"], row["URL"]

        if i > 0 and i % 5 == 0:
            backup_to_drive()

        for year in YEARS_TO_PROCESS:
            print(f"\n Year: {year} | Source: {domain}")

            records = []
            for variant in domain_variants(domain):
                records = query_commoncrawl(variant, year)
                if records:
                    print(f" Found {len(records)} records for {variant}")
                    break
            if not records:
                print(f" No records found for {domain} in year {year}")
                continue

            matches_for_source = 0

            for tier, automaton in [("tier1", automaton_tier1), ("tier2", automaton_tier2)]:
                for rec in records:
                    if matches_for_source >= MAX_MATCHED_URLS_PER_SOURCE_PER_YEAR:
                        break
                    if str(rec.get("status")) != "200":
                        continue
                    url = rec.get("url", "")
                    if url in seen_urls:
                        continue

                    matched = find_matches(url, automaton)
                    if matched:
                        article_text = extract_body_from_warc(rec)
                        time.sleep(0.5)

                        if article_text:
                            start_line = line_offset + 1
                            f_text.write(f"---\nurl: {url}\nyear: {year}\nstate: {state}\nsource: {name}\n---\n")
                            f_text.write(article_text.strip() + "\n\n---\n")
                            line_offset += article_text.count("\n") + 4

                            row = {
                                "source_name": name,
                                "state": state,
                                "city": city,
                                "domain": domain,
                                "year": year,
                                "url": url,
                                "timestamp": rec.get("timestamp"),
                                "status": rec.get("status"),
                                "digest": rec.get("digest"),
                                "matched_keywords": ", ".join(matched),
                                "tier": tier,
                                "article_line_start": start_line
                            }
                            csv_writer.writerow(row)
                            csv_file.flush()

                            seen_urls.add(url)
                            matches_for_source += 1

                if matches_for_source >= MAX_MATCHED_URLS_PER_SOURCE_PER_YEAR:
                    break

            print(f" {matches_for_source} matches found for {domain} in {year}")

csv_file.close()
backup_to_drive()

print(f"\n Finished. Articles saved to: {ARTICLE_TEXT_PATH}")
print(f"  CSV saved to: {OUTPUT_CSV_PATH}")
