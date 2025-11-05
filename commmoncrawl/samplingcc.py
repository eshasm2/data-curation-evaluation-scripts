"""
Common Crawl Article Extractor
------------------------------

This script queries Common Crawl indexes for articles from a list of sources,
searches for keyword matches in URLs, downloads and extracts article text from WARC files,
and saves the results in both a text file and a CSV file.

Features:
- Supports multiple keyword tiers
- Handles domain variants (www, mobile, blog, etc.)
- Limits max matches per source per year
- Resumable: skips already processed URLs
- Backup to Google Drive
"""

import os
import io
import csv
import time
import json
import shutil
from urllib.parse import urlparse

import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import ahocorasick

# -----------------------------
# Configuration
# -----------------------------
# Google Drive mount path
DRIVE_DIR = "/content/drive/MyDrive/ccrawl_results"
os.makedirs(DRIVE_DIR, exist_ok=True)

ARTICLE_TEXT_PATH = os.path.join(DRIVE_DIR, "matched_articles.txt")
CSV_OUTPUT_PATH = os.path.join(DRIVE_DIR, "keyword_matches.csv")
BACKUP_ARTICLE_PATH = os.path.join(DRIVE_DIR, "matched_articles_backup.txt")
BACKUP_CSV_PATH = os.path.join(DRIVE_DIR, "keyword_matches_backup.csv")

# Query settings
MAX_MATCHES_PER_SOURCE_PER_YEAR = 80
YEARS_TO_PROCESS = [2024]

# Common Crawl collections for 2024
CC_2024_COLLECTIONS = [
    "CC-MAIN-2024-10", "CC-MAIN-2024-18", "CC-MAIN-2024-22",
    "CC-MAIN-2024-26", "CC-MAIN-2024-30", "CC-MAIN-2024-33",
    "CC-MAIN-2024-38", "CC-MAIN-2024-42", "CC-MAIN-2024-46",
    "CC-MAIN-2024-51"
]

# -----------------------------
# Utility Functions
# -----------------------------
def backup_files():
    """Copy current text and CSV files to backup locations."""
    try:
        shutil.copy(ARTICLE_TEXT_PATH, BACKUP_ARTICLE_PATH)
        shutil.copy(CSV_OUTPUT_PATH, BACKUP_CSV_PATH)
        print("Backup completed.")
    except Exception as e:
        print(f"Backup failed: {e}")


def load_keywords(file_path):
    """Load keywords from a text file (one per line)."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def build_aho_automaton(keywords):
    """
    Build an Aho-Corasick automaton for efficient multi-keyword matching.
    Returns the automaton object.
    """
    automaton = ahocorasick.Automaton()
    for idx, kw in enumerate(keywords):
        automaton.add_word(kw.lower(), (idx, kw))
    automaton.make_automaton()
    return automaton


def load_sources(csv_path):
    """Load sources from a CSV file with columns: name, state, city, URL."""
    df = pd.read_csv(csv_path)
    return df[['name', 'state', 'city', 'URL']].dropna()


def query_commoncrawl(domain, year, retries=3, delay=4):
    """
    Query the Common Crawl index for a given domain and year.
    Returns a list of records.
    """
    if year == 2024:
        collections = CC_2024_COLLECTIONS
    else:
        year_to_collection = {
            2018: "CC-MAIN-2017-51",
            2019: "CC-MAIN-2018-51",
            2020: "CC-MAIN-2019-51",
            2023: "CC-MAIN-2023-14"
        }
        collections = [year_to_collection.get(year)] if year in year_to_collection else []

    all_records = []
    for collection in collections:
        url = f"https://index.commoncrawl.org/{collection}-index"
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    url,
                    params={"url": f"{domain}/*", "matchType": "host", "output": "json"},
                    timeout=60
                )
                response.raise_for_status()
                time.sleep(delay)
                records = [json.loads(line) for line in response.text.splitlines()]
                if records:
                    all_records.extend(records)
                    break
            except requests.RequestException as e:
                backoff = 3 ** attempt
                print(f"Attempt {attempt} failed for {domain} ({year}, {collection}): {e}. Retrying in {backoff}s.")
                time.sleep(backoff)
    return all_records


def get_domain_variants(domain):
    """
    Generate common domain variants for broader crawling.
    Example: news.example.com, www.example.com, m.example.com
    """
    domain = domain.lower().strip()
    variants = [domain]
    if not domain.startswith("www."):
        variants.append(f"www.{domain}")
    subdomains = ["news", "blog", "m", "mobile", "editorial"]
    variants.extend([f"{sub}.{domain}" for sub in subdomains])
    return variants


def find_keyword_matches(url, automaton):
    """Return list of matched keywords in a URL."""
    return list({match for _, (_, match) in automaton.iter(url.lower())})


def extract_article_text(html):
    """
    Extract readable article text from HTML content.
    Prioritize <article>, common article divs, then fallback to <body>.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content elements
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Check <article> tag first
    article_tag = soup.find("article")
    if article_tag and len(article_tag.get_text(strip=True)) > 1000:
        return article_tag.get_text(separator="\n", strip=True)

    # Check common div classes for article content
    candidate_classes = ["content__article-body", "article-body", "l-container", "zn-body-text", "pg-rail-tall__body"]
    for cls in candidate_classes:
        div = soup.find("div", class_=cls)
        if div and len(div.get_text(strip=True)) > 1000:
            return div.get_text(separator="\n", strip=True)

    # Fallback to full body
    body = soup.find("body")
    if body and len(body.get_text(strip=True)) > 1000:
        return body.get_text(separator="\n", strip=True)

    return ""


def extract_text_from_warc(record):
    """
    Fetch WARC file content for a record and extract readable text.
    Returns None if extraction fails.
    """
    warc_url = f"https://data.commoncrawl.org/{record['filename']}"
    offset = int(record["offset"])
    length = int(record["length"])
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

    try:
        response = requests.get(warc_url, headers=headers, timeout=20)
        response.raise_for_status()
        stream = io.BytesIO(response.content)
        for rec in ArchiveIterator(stream):
            if rec.rec_type == "response":
                html_content = rec.content_stream().read().decode("utf-8", errors="ignore")
                body_text = extract_article_text(html_content)
                if len(body_text.strip()) > 1000:
                    return body_text.strip()
    except Exception as e:
        print(f"Error reading WARC: {e}")
    return None


def read_last_processed_years(article_path):
    """
    Read previously processed URLs and their latest processed year
    to avoid duplicate processing.
    """
    last_processed = {}
    if not os.path.exists(article_path):
        return last_processed

    with open(article_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for i, line in enumerate(lines):
        if line.strip() == "---" and i + 3 < len(lines):
            try:
                url_line = lines[i + 1].strip()
                year_line = lines[i + 2].strip()
                url = url_line.replace("url:", "").strip()
                year = int(year_line.replace("year:", "").strip())
                domain = urlparse(url).netloc or url
                if domain not in last_processed or year > last_processed[domain]:
                    last_processed[domain] = year
            except Exception:
                continue
    return last_processed
