# filter_curate - Stage 2 — Filter raw crawled articles by date presence and keyword body match.
#
# Reads matched_articles.txt from filter_keywords and applies:
#   1. Date check   — articles without a parseable publication date are dropped
#   2. Tier-1 check — article body must contain at least one Tier-1 keyword
#   Tier-2 keywords are recorded as metadata but do not affect inclusion.
#
# Usage (via cli.py)
#   python cli.py --resource filter_curate --years 2018-2024
#
# Usage (direct) — auto-detects the most recent filter_keywords output folder
#   python code/filter_curate.py -y 2018-2024
#
# Outputs (data/data_filter_curate_<years>/)
#   filtered_articles.txt   — retained article text + metadata
#   filtered_metadata.csv   — one row per retained article
#   report.csv              — timestamped log of what passed/failed each filter

import argparse
import csv
import re
import sys
from pathlib import Path

import ahocorasick

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from code.config import (
    validate_years, load_terms, log_report,
    keywords_dir, get_output_dir, get_input_dir,
)

date_patterns = [
    r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b",
    r"\b\w+ \d{1,2},? \d{4}\b",
    r"\b\d{1,2} \w+ \d{4}\b",
]


def build_automaton(path):
    # builds an Aho-Corasick automaton from a keyword file
    words = load_terms(str(path))
    ac = ahocorasick.Automaton()
    for idx, word in enumerate(words):
        ac.add_word(word.lower(), (idx, word))
    ac.make_automaton()
    return ac


def find_matches(text, ac):
    # returns a deduplicated list of keywords found in text
    return list({match for _, (_, match) in ac.iter(text.lower())})


def has_date(text):
    # returns True if the text contains at least one recognizable publication date
    return any(re.search(p, text) for p in date_patterns)


def parse_articles(path):
    # generator — yield one dict per article block from matched_articles.txt.
    # blocks are delimited by '\n---\n' and contain url, year, source, state, and text fields.
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    for block in raw.split("\n---\n"):
        if not block.strip():
            continue
        article = {}
        text_start = block.find("text:\n")
        if text_start != -1:
            article["text"] = block[text_start + 6:].strip()
            header = block[:text_start]
        else:
            article["text"] = ""
            header = block
        for line in header.splitlines():
            for field in ["url", "year", "source", "state"]:
                if line.startswith(f"{field}:"):
                    article[field] = line[len(field)+1:].strip()
        yield article


def get_args(argv=None):
    parser = argparse.ArgumentParser(description="Stage 2 — filter crawled articles by date and tier-1 keyword.")
    parser.add_argument("-y", "--years",  required=True)
    parser.add_argument("-t", "--type",   default="articles")
    parser.add_argument("-i", "--input",  type=str)
    parser.add_argument("-o", "--output", type=str)
    args = parser.parse_args(argv)
    validate_years(args.years, parser)
    return args


def main(argv=None):
    args = get_args(argv)

    in_dir  = Path(args.input)  if args.input  else get_input_dir("filter_curate", args.years)
    out_dir = Path(args.output) if args.output else get_output_dir("filter_curate", args.years)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = str(out_dir / "report.csv")
    log_report(report_path, f"Starting filter_curate | years={args.years}")

    # build keyword automatons for both tiers
    # tier-1 is required for inclusion; tier-2 is recorded as metadata only
    tier1_ac = build_automaton(keywords_dir / "keywords_tier1.txt")
    tier2_ac = build_automaton(keywords_dir / "keywords_tier2.txt")

    txt_in  = in_dir  / "matched_articles.txt"
    txt_out = out_dir / "filtered_articles.txt"
    csv_out = out_dir / "filtered_metadata.csv"

    total = kept = no_date = no_keyword = 0

    with open(txt_out, "w", encoding="utf-8") as out_txt, \
         open(csv_out, "w", newline="", encoding="utf-8") as out_csv:

        writer = csv.DictWriter(out_csv, fieldnames=[
            "url", "year", "source", "state", "tier1_keywords", "tier2_keywords"
        ])
        writer.writeheader()

        for article in parse_articles(txt_in):
            total += 1
            text = article.get("text", "")

            if not has_date(text):
                no_date += 1
                continue

            tier1_hits = find_matches(text, tier1_ac)
            if not tier1_hits:
                no_keyword += 1
                continue

            tier2_hits = find_matches(text, tier2_ac)
            kept += 1

            out_txt.write("---\n")
            for field in ["url", "year", "source", "state"]:
                out_txt.write(f"{field}: {article.get(field, '')}\n")
            out_txt.write(f"tier1_keywords: {';'.join(tier1_hits)}\n")
            out_txt.write(f"tier2_keywords: {';'.join(tier2_hits)}\n")
            out_txt.write(f"text:\n{text}\n\n")

            writer.writerow({
                "url":            article.get("url", ""),
                "year":           article.get("year", ""),
                "source":         article.get("source", ""),
                "state":          article.get("state", ""),
                "tier1_keywords": ";".join(tier1_hits),
                "tier2_keywords": ";".join(tier2_hits),
            })

    log_report(report_path, (
        f"filter_curate complete | total={total} kept={kept} "
        f"dropped(no date)={no_date} dropped(no keyword)={no_keyword}"
    ))


if __name__ == "__main__":
    main()
