# cli.py - entry point for the LUCID pipeline
#
# Usage
#   python cli.py --resource filter_keywords --years 2018-2024
#   python cli.py --resource filter_curate   --years 2018-2024
#   python cli.py --resource label_agreement
#   python cli.py --resource visualize

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from code.config import validate_years

code_dir = Path(__file__).resolve().parent / "code"

# resources that require --years
needs_years = ["filter_keywords", "filter_curate"]


def get_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--resource", type=str, required=True,
                        choices=["filter_keywords", "filter_curate", "label_agreement", "visualize"])
    parser.add_argument("-y", "--years",  type=str)
    parser.add_argument("-t", "--type",   type=str, choices=["articles", "all"], default="articles")
    parser.add_argument("-i", "--input",  type=str)
    parser.add_argument("-o", "--output", type=str)

    args = parser.parse_args(argv)

    if args.resource in needs_years:
        if not args.years:
            parser.error(f"--years is required for {args.resource}")
        validate_years(args.years, parser)

    return args


if __name__ == "__main__":
    args = get_args()

    # delegate to the resource script as a subprocess so each stage runs in isolation
    resource_script = code_dir / f"{args.resource}.py"
    cmd_parts = [sys.executable, str(resource_script)]

    if args.years:  cmd_parts.extend(["-y", args.years])
    if args.type:   cmd_parts.extend(["-t", args.type])
    if args.input:  cmd_parts.extend(["-i", args.input])
    if args.output: cmd_parts.extend(["-o", args.output])

    print("[cli] running:", subprocess.list2cmdline([str(p) for p in cmd_parts]))
    subprocess.run(cmd_parts, check=True)
