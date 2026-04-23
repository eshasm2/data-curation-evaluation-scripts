import argparse
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional


# project root paths
project_root = Path(__file__).resolve().parent.parent
code_dir     = project_root / "code"
data_dir     = project_root / "data"
keywords_dir = project_root / "keywords"
results_dir  = project_root / "results"

sources_dir  = data_dir / "data_sources"
ratings_dir  = data_dir / "data_relevance_ratings"
figures_dir  = results_dir / "figures"
tables_dir   = results_dir / "tables"


# corpus constants
# column headers for LUCID output files
headers = ["url", "year", "source", "state", "tier1_keywords", "tier2_keywords", "text"]

# default resource processing order, used for auto path resolution
default_resource_order = [
    "filter_keywords",
    "filter_curate",
    "label_agreement",
    "visualize",
]

# CommonCrawl index collections by year
cc_collections = {
    2018: ["CC-MAIN-2018-43"],
    2019: ["CC-MAIN-2019-09"],
    2020: ["CC-MAIN-2020-10"],
    2023: ["CC-MAIN-2023-06"],
    2024: [
        "CC-MAIN-2024-10", "CC-MAIN-2024-18", "CC-MAIN-2024-22",
        "CC-MAIN-2024-26", "CC-MAIN-2024-30", "CC-MAIN-2024-33",
        "CC-MAIN-2024-38", "CC-MAIN-2024-42", "CC-MAIN-2024-46",
        "CC-MAIN-2024-51",
    ],
}

# cap per source to avoid over-representing any single outlet
max_articles_per_source = 80
# minimum character threshold to filter stub pages and navigation dumps
min_article_chars       = 1000
# seconds between CommonCrawl requests to stay within rate limits
cc_request_delay        = 4
cc_retry_attempts       = 3

# ideology binning for visualization
ideology_bins   = [-1, -0.1, 0.1, 1]
ideology_labels = ["Left-leaning (< -0.1)", "Center (-0.1 to 0.1)", "Right-leaning (> 0.1)"]
figure_dpi      = 150

# U.S. state abbreviations
us_states = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY",
}

# U.S. state to Census region mapping
state_to_region = {
    "CT":"Northeast","ME":"Northeast","MA":"Northeast","NH":"Northeast",
    "RI":"Northeast","VT":"Northeast","NJ":"Northeast","NY":"Northeast","PA":"Northeast",
    "IL":"Midwest","IN":"Midwest","MI":"Midwest","OH":"Midwest","WI":"Midwest",
    "IA":"Midwest","KS":"Midwest","MN":"Midwest","MO":"Midwest","NE":"Midwest",
    "ND":"Midwest","SD":"Midwest",
    "DE":"South","FL":"South","GA":"South","MD":"South","NC":"South","SC":"South",
    "VA":"South","WV":"South","AL":"South","KY":"South","MS":"South","TN":"South",
    "AR":"South","LA":"South","OK":"South","TX":"South",
    "AZ":"West","CO":"West","ID":"West","MT":"West","NV":"West","NM":"West",
    "UT":"West","WY":"West","AK":"West","CA":"West","HI":"West","OR":"West","WA":"West",
}

# state-level racial population proportions (Census estimates)
state_pop_data = [
    {"state":"AL","white_prop":0.6536,"black_prop":0.2609},
    {"state":"AK","white_prop":0.6070,"black_prop":0.0310},
    {"state":"AZ","white_prop":0.6320,"black_prop":0.0464},
    {"state":"AR","white_prop":0.7086,"black_prop":0.1491},
    {"state":"CA","white_prop":0.4395,"black_prop":0.0554},
    {"state":"CO","white_prop":0.7346,"black_prop":0.0401},
    {"state":"CT","white_prop":0.6757,"black_prop":0.1069},
    {"state":"DE","white_prop":0.6182,"black_prop":0.2194},
    {"state":"DC","white_prop":0.3907,"black_prop":0.4326},
    {"state":"FL","white_prop":0.5991,"black_prop":0.1534},
    {"state":"GA","white_prop":0.6050,"black_prop":0.3240},
    {"state":"HI","white_prop":0.2220,"black_prop":0.0220},
    {"state":"ID","white_prop":0.7900,"black_prop":0.0090},
    {"state":"IL","white_prop":0.6100,"black_prop":0.1460},
    {"state":"IN","white_prop":0.8510,"black_prop":0.0980},
    {"state":"IA","white_prop":0.9070,"black_prop":0.0400},
    {"state":"KS","white_prop":0.7560,"black_prop":0.0580},
    {"state":"KY","white_prop":0.8760,"black_prop":0.0840},
    {"state":"LA","white_prop":0.6290,"black_prop":0.3270},
    {"state":"ME","white_prop":0.9460,"black_prop":0.0160},
    {"state":"MD","white_prop":0.5880,"black_prop":0.3090},
    {"state":"MA","white_prop":0.8080,"black_prop":0.0890},
    {"state":"MI","white_prop":0.7930,"black_prop":0.1410},
    {"state":"MN","white_prop":0.8410,"black_prop":0.0680},
    {"state":"MS","white_prop":0.5910,"black_prop":0.3780},
    {"state":"MO","white_prop":0.8300,"black_prop":0.1100},
    {"state":"MT","white_prop":0.8310,"black_prop":0.0200},
    {"state":"NE","white_prop":0.8800,"black_prop":0.0500},
    {"state":"NV","white_prop":0.6200,"black_prop":0.0700},
    {"state":"NH","white_prop":0.8980,"black_prop":0.0200},
    {"state":"NJ","white_prop":0.8080,"black_prop":0.1600},
    {"state":"NM","white_prop":0.5100,"black_prop":0.0210},
    {"state":"NY","white_prop":0.6150,"black_prop":0.1800},
    {"state":"NC","white_prop":0.7350,"black_prop":0.2200},
    {"state":"ND","white_prop":0.8700,"black_prop":0.0200},
    {"state":"OH","white_prop":0.7930,"black_prop":0.1400},
    {"state":"OK","white_prop":0.6900,"black_prop":0.0700},
    {"state":"OR","white_prop":0.8200,"black_prop":0.0200},
    {"state":"PA","white_prop":0.7200,"black_prop":0.1200},
    {"state":"RI","white_prop":0.8000,"black_prop":0.1000},
    {"state":"SC","white_prop":0.5900,"black_prop":0.2600},
    {"state":"SD","white_prop":0.8070,"black_prop":0.0200},
    {"state":"TN","white_prop":0.7900,"black_prop":0.1490},
    {"state":"TX","white_prop":0.5000,"black_prop":0.1400},
    {"state":"UT","white_prop":0.8300,"black_prop":0.0200},
    {"state":"VT","white_prop":0.9010,"black_prop":0.0100},
    {"state":"VA","white_prop":0.7400,"black_prop":0.2000},
    {"state":"WA","white_prop":0.7800,"black_prop":0.0400},
    {"state":"WV","white_prop":0.9000,"black_prop":0.0200},
    {"state":"WI","white_prop":0.7900,"black_prop":0.0700},
    {"state":"WY","white_prop":0.8300,"black_prop":0.0160},
]


# year validation
valid_year_range = (2018, 2024)


def validate_years(years_str: str, parser: argparse.ArgumentParser) -> None:
    # takes a single year like "2020" or a range like "2018-2020" and errors if out of bounds
    match = re.fullmatch(r"(\d{4})(?:-(\d{4}))?", years_str)
    if not match:
        parser.error("--years must be a 4-digit year or a range like 2018-2020.")
    start = int(match.group(1))
    end   = int(match.group(2)) if match.group(2) else start
    lo, hi = valid_year_range
    if not (lo <= start <= hi and lo <= end <= hi):
        parser.error(f"Years must be between {lo} and {hi}.")
    if start > end:
        parser.error("Start year must be <= end year.")


def parse_range(value: str) -> List[int]:
    # converts "2018" → [2018] or "2018-2020" → [2018, 2019, 2020]
    try:
        if "-" in value:
            start, end = map(int, value.split("-", 1))
        else:
            start = end = int(value)
        return list(range(start, end + 1))
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid value '{value}': must be a year or range (e.g. 2018 or 2018-2020)."
        )


# keyword helpers


def load_terms(file_path: str) -> List[str]:
    # reads a keyword file and returns lowercase, stripped terms
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.lower().rstrip("\r\n") for line in f if line.strip()]


# logging helpers


def log_report(report_file_path: Optional[str] = None, message: Optional[str] = None) -> None:
    # appends a timestamped message to the report CSV and prints it
    if message is None:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if report_file_path:
        os.makedirs(os.path.dirname(report_file_path), exist_ok=True)
        with open(report_file_path, "a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([timestamp, message])
    print(f"{timestamp} - {message}")
    sys.stdout.flush()


def log_error(
    function_name: str,
    file: str,
    line_number: int,
    line_content: str,
    error: Exception,
    report_file_path: Optional[str] = None,
    output_path: Optional[str] = None,
) -> None:
    # writes a per-error file for debugging and logs a pointer to it in the report
    try:
        error_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_filename = f"error_{os.path.basename(file)}_{line_number}_{error_time}.txt"
        if output_path:
            os.makedirs(output_path, exist_ok=True)
            with open(os.path.join(output_path, error_filename), "w", encoding="utf-8") as ef:
                ef.write(f"Error in {function_name} at line {line_number}: {error}\n")
                ef.write(f"Line content: {line_content}\n")
        if report_file_path:
            log_report(report_file_path, f"Logged error in {error_filename}")
    except Exception:
        pass


# path helpers


def get_output_dir(resource: str, years: Optional[str] = None) -> Path:
    # builds the standard output path for a resource run, e.g. data/data_filter_curate_2018_2020
    parts = [f"data_{resource}"]
    if years:
        parts.append(years.replace("-", "_"))
    return data_dir / "_".join(parts)


def get_input_dir(resource: str, years: Optional[str] = None) -> Path:
    # the input for each stage is the output directory of the previous stage
    idx = default_resource_order.index(resource)
    if idx == 0:
        return sources_dir
    prev_resource = default_resource_order[idx - 1]
    return get_output_dir(prev_resource, years)
