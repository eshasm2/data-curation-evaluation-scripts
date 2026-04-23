# visualize - Stage 4 — Generate all figures.
#
# Pulls from the curated article metadata (stage 2 output) and the
# LLM annotation files to produce all figures. Requires filter_curate to have run first.
#
# Figures:
#   1. ideology_distribution.png      — histogram of ideology scores across sources
#   2. sources_per_state.png          — article counts by U.S. state
#   3. articles_per_region.png        — article counts by U.S. Census region
#   4. perp_vs_victim_race.png        — perp/victim race proportions by ideology
#   5. population_adjusted_race.png   — observed/expected race representation ratios
#
# Usage (via cli.py)
#   python cli.py --resource visualize
#
# Usage (direct) — auto-detects the most recent filter_curate output folder
#   python code/visualize.py
#
# Outputs (results/figures/)
#   ideology_distribution.png, sources_per_state.png, articles_per_region.png
#   perp_vs_victim_race.png, population_adjusted_race.png
#   report.csv — timestamped run log

import argparse
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.modules.pop("code", None)
from code.config import (
    log_report,
    data_dir, ratings_dir, sources_dir, figures_dir,
    ideology_bins, ideology_labels, figure_dpi,
    us_states, state_to_region, state_pop_data,
)

# maps race category names to the corresponding population proportion column
race_to_prop = {"white": "white_prop", "black": "black_prop", "other": "other_prop"}


def extract_domain(url_series):
    # strips protocol and www prefix from a URL series to get bare domain names
    return (
        url_series.str.lower()
        .str.replace(r"^https?://", "", regex=True)
        .str.replace(r"^www\.", "", regex=True)
        .str.split("/").str[0]
    )


def save_fig(fig, name, report_path):
    # saves a figure to the figures directory and logs the path
    figures_dir.mkdir(parents=True, exist_ok=True)
    path = figures_dir / name
    fig.savefig(path, dpi=figure_dpi, bbox_inches="tight")
    plt.close(fig)
    log_report(report_path, f"Saved: {path}")


def plot_ideology_distribution(metadata_csv, sources_csv, report_path):
    # join filtered metadata to the master source list to get ideology scores,
    # then bin into left/center/right for the summary log
    master  = pd.read_csv(sources_csv)
    sources = pd.read_csv(metadata_csv)
    master["domain"]  = extract_domain(master["URL"])
    sources["domain"] = extract_domain(sources["url"])
    merged = sources.merge(master[["domain", "ideology"]], on="domain", how="left")
    merged = merged.dropna(subset=["ideology"])
    merged["category"] = pd.cut(merged["ideology"], bins=ideology_bins, labels=ideology_labels)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(merged["ideology"], bins=np.linspace(-1, 1, 20), edgecolor="black")
    ax.axvline(0, linestyle="--", color="gray")
    ax.set_title("Distribution of Ideology Scores Across US Newspaper Sources")
    ax.set_xlabel("Political Leaning Score")
    ax.set_ylabel("Number of Articles")
    fig.tight_layout()
    save_fig(fig, "ideology_distribution.png", report_path)

    counts = merged["category"].value_counts().reindex(ideology_labels).fillna(0).astype(int)
    log_report(report_path, f"Ideology counts: {counts.to_dict()}")
    log_report(report_path, f"Mean={merged['ideology'].mean():.3f} Median={merged['ideology'].median():.3f}")


def plot_sources_per_state(metadata_csv, report_path):
    # filter to valid U.S. state codes before counting to drop any malformed rows
    df = pd.read_csv(metadata_csv)
    df.columns = df.columns.str.strip().str.lower()
    df = df[df["state"].isin(us_states)]
    counts = df["state"].value_counts().sort_values(ascending=False)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(counts.index, counts.values)
    ax.set_xticklabels(counts.index, rotation=90)
    ax.set_xlabel("State")
    ax.set_ylabel("Number of Sources")
    ax.set_title("Number of Sources per U.S. State")
    fig.tight_layout()
    save_fig(fig, "sources_per_state.png", report_path)


def plot_articles_per_region(metadata_csv, report_path):
    df = pd.read_csv(metadata_csv)
    df["region"] = df["state"].map(state_to_region)
    counts = df["region"].value_counts()

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(counts.index, counts.values)
    ax.set_xlabel("Region")
    ax.set_ylabel("Number of Articles")
    ax.set_title("Total Articles per U.S. Census Region")
    fig.tight_layout()
    save_fig(fig, "articles_per_region.png", report_path)

    for region in counts.index:
        n_states = df[df["region"] == region]["state"].nunique()
        log_report(report_path, f"{region} ({n_states} states): {counts[region]}")


def plot_perp_victim_race(race_counts_csv, report_path):
    # expects a CSV with columns: role, race, ideology, count.
    # an "all" ideology panel is added by summing across negative and positive.
    df = pd.read_csv(race_counts_csv)
    df["race"] = df["race"].astype(str).str.strip().str.lower()
    df = df[~df["race"].isin(["not provided", "not_provided", "none", ""])]
    df = df[df["race"].notna()]

    all_df = df.groupby(["role", "race"], as_index=False).agg({"count": "sum"})
    all_df["ideology"] = "all"
    plot_df = pd.concat([df, all_df], ignore_index=True)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=True)
    for ax, ideology in zip(axes, ["negative", "positive", "all"]):
        subset = plot_df[plot_df["ideology"] == ideology].copy()
        subset["proportion"] = subset["count"] / subset["count"].sum()
        sns.barplot(data=subset, x="race", y="proportion", hue="role", errorbar=None, ax=ax)
        ax.set_title(f"{ideology.capitalize()}")
        ax.set_xlabel("Race")
        ax.set_ylabel("Proportion of Total" if ideology == "negative" else "")

    fig.suptitle("Perpetrator vs Victim Race Proportions by Ideology")
    fig.tight_layout()
    save_fig(fig, "perp_vs_victim_race.png", report_path)


def plot_population_adjusted_race(people_csv, sources_csv, report_path):
    # compute observed/expected race ratios for perps and victims by ideology.
    # expected counts are derived from state-level Census population proportions,
    # weighted by how many articles came from each state.
    # error bars use Poisson 95% CIs on the log scale: exp(log(O/E) ± 1.96/sqrt(O)).
    state_pop = pd.DataFrame(state_pop_data)
    state_pop["other_prop"] = 1 - (state_pop["white_prop"] + state_pop["black_prop"])

    people  = pd.read_csv(people_csv)
    sources = pd.read_csv(sources_csv)
    sources["url_norm"] = sources["URL"].str.strip().str.lower()

    def _dom(url):
        try:
            d = urlparse(str(url).strip().lower()).netloc
            return d[4:] if d.startswith("www.") else d
        except Exception:
            return None

    people["url_norm"] = people["url"].apply(_dom)
    merged = people.merge(
        sources[["url_norm", "ideology", "state"]], on="url_norm", how="left"
    )
    merged = merged[merged["ideology"].notna()].copy()
    merged["ideology_label"] = merged["ideology"].apply(
        lambda s: "negative" if s < 0 else ("positive" if s > 0 else None)
    )
    merged = merged[merged["ideology_label"].notna()]

    def soft_majority(vals):
        clean = [str(v).strip().lower() for v in vals
                 if pd.notna(v) and str(v).strip().lower() not in ("", "not_provided")]
        if not clean: return None
        counts = Counter(clean)
        top = counts.most_common()
        return top[0][0] if len(top) == 1 or top[0][1] > top[1][1] else None

    def norm_race(r):
        if pd.isna(r): return None
        r = str(r).strip().lower()
        if r in ("", "not_provided", "unknown"): return None
        return r if r in ("white", "black") else "other"

    # take a soft majority vote across the three LLMs for each role
    for role, prefix in [("perp", "perp"), ("victim", "victim")]:
        merged[f"{role}_race_soft"] = merged.apply(
            lambda r: soft_majority([
                r.get(f"gpt_{prefix}_race"),
                r.get(f"llama_{prefix}_race"),
                r.get(f"qwen_{prefix}_race"),
            ]), axis=1
        )
        merged[f"{role}_race_norm"] = merged[f"{role}_race_soft"].apply(norm_race)

    merged = merged.merge(state_pop, on="state", how="left")
    merged = merged[merged["white_prop"].notna()]

    # build observed/expected records for each ideology × role × race combination
    records = []
    for ideology in ["negative", "positive"]:
        sub = merged[merged["ideology_label"] == ideology]
        for role, col in [("perp", "perp_race_norm"), ("victim", "victim_race_norm")]:
            role_df = sub[sub[col].notna()]
            for race in ["white", "black", "other"]:
                obs = (role_df[col] == race).sum()
                exp = role_df[race_to_prop[race]].sum()
                records.append({
                    "ideology": ideology, "role": role, "race": race,
                    "observed": obs, "expected": exp,
                    "ratio": obs / exp if exp > 0 else None,
                })

    adj_df = pd.DataFrame(records)
    adj_df["ci_lower"] = None
    adj_df["ci_upper"] = None
    for i, row in adj_df.iterrows():
        O, E = row["observed"], row["expected"]
        if O > 0 and E > 0:
            se = 1 / np.sqrt(O)
            adj_df.at[i, "ci_lower"] = np.exp(np.log(O / E) - 1.96 * se)
            adj_df.at[i, "ci_upper"] = np.exp(np.log(O / E) + 1.96 * se)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    races = ["white", "black", "other"]
    x = np.arange(len(races))
    width = 0.35
    for ax, ideology in zip(axes, ["negative", "positive"]):
        subset = adj_df[adj_df["ideology"] == ideology]
        for i, role in enumerate(["perp", "victim"]):
            role_sub = subset[subset["role"] == role].set_index("race").reindex(races)
            ratios = role_sub["ratio"].values.astype(float)
            lowers = (role_sub["ratio"] - role_sub["ci_lower"]).clip(lower=0).values.astype(float)
            uppers = (role_sub["ci_upper"] - role_sub["ratio"]).clip(lower=0).values.astype(float)
            ax.bar(x + i * width, ratios, width, yerr=[lowers, uppers],
                   capsize=4, label=role.capitalize(), error_kw={"elinewidth": 1})
        ax.axhline(1, linestyle="--", color="gray", linewidth=0.8)
        ax.set_yscale("log")
        ax.set_xticks(x + width / 2)
        ax.set_xticklabels([r.capitalize() for r in races])
        ax.set_title(ideology.capitalize())
        ax.set_xlabel("Race")
        if ideology == "negative":
            ax.set_ylabel("Observed / Expected (log scale)")
        ax.legend()

    fig.suptitle("Population-Adjusted Race Representation by Ideology")
    fig.tight_layout()
    save_fig(fig, "population_adjusted_race.png", report_path)


def _build_race_counts(people_csv, sources_csv, out_dir):
    # aggregate LLM soft-majority race labels into role/race/ideology counts
    # for use by plot_perp_victim_race, which expects a pre-aggregated CSV
    def _dom(url):
        try:
            d = urlparse(str(url).strip().lower()).netloc
            return d[4:] if d.startswith("www.") else d
        except Exception:
            return None

    def _norm(r):
        if pd.isna(r):
            return None
        r = str(r).strip().lower()
        if r in ("", "not_provided", "unknown", "not provided", "multiple"):
            return None
        return r if r in ("white", "black") else "other"

    def _soft_vote(vals):
        # return majority value if two or more LLMs agree, otherwise None
        clean = [v for v in (_norm(x) for x in vals) if v]
        if not clean:
            return None
        counts = Counter(clean)
        top = counts.most_common(2)
        return top[0][0] if (len(top) == 1 or top[0][1] > top[1][1]) else None

    people  = pd.read_csv(people_csv)
    sources = pd.read_csv(sources_csv)
    sources["url_norm"] = sources["URL"].str.strip().str.lower()
    people["url_norm"]  = people["url"].apply(_dom)
    merged = people.merge(sources[["url_norm", "ideology"]], on="url_norm", how="left")
    merged = merged[merged["ideology"].notna()].copy()
    merged["ideology_label"] = merged["ideology"].apply(
        lambda s: "negative" if s < 0 else ("positive" if s > 0 else None)
    )
    merged = merged[merged["ideology_label"].notna()]

    records = []
    for role, prefix in [("perp", "perp"), ("victim", "victim")]:
        merged["_race"] = merged.apply(
            lambda r: _soft_vote([
                r.get(f"gpt_{prefix}_race"),
                r.get(f"llama_{prefix}_race"),
                r.get(f"qwen_{prefix}_race"),
            ]), axis=1
        )
        for ideology in ["negative", "positive"]:
            vc = merged[merged["ideology_label"] == ideology]["_race"].value_counts()
            for race, cnt in vc.items():
                if pd.notna(race):
                    records.append({"role": role, "race": race, "ideology": ideology, "count": int(cnt)})

    out_df = pd.DataFrame(records)
    path = out_dir / "race_counts.csv"
    out_df.to_csv(path, index=False)
    return path


def get_args(argv=None):
    parser = argparse.ArgumentParser(description="Stage 4 — generate all paper figures.")
    parser.add_argument("-t", "--type",   default="articles")
    parser.add_argument("-i", "--input",  type=str,
                        help="Directory containing filtered_metadata.csv (filter_curate output).")
    parser.add_argument("-o", "--output", type=str,
                        help="Override output directory for figures.")
    return parser.parse_args(argv)


def main(argv=None):
    args = get_args(argv)

    out_dir = Path(args.output) if args.output else figures_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = str(out_dir / "report.csv")
    log_report(report_path, "Starting visualize")

    sources_csv = sources_dir / "sources.csv"
    people_csv  = ratings_dir / "people_info_cleaned_latest.csv"

    # find the filter_curate output directory to get filtered_metadata.csv.
    # if --input is not passed, use the most recently created one for this group.
    if args.input:
        in_dir = Path(args.input)
    else:
        candidates = sorted(
            [d for d in data_dir.iterdir()
             if d.is_dir() and d.name.startswith("data_filter_curate_")],
            key=lambda p: p.name,
        ) if data_dir.exists() else []
        if not candidates:
            raise FileNotFoundError(
                "No filter_curate output found. Run filter_curate first or pass --input."
            )
        in_dir = candidates[-1]

    metadata_csv = in_dir / "filtered_metadata.csv"
    log_report(report_path, f"Using metadata: {metadata_csv}")

    plot_ideology_distribution(metadata_csv, sources_csv, report_path)
    plot_sources_per_state(metadata_csv, report_path)
    plot_articles_per_region(metadata_csv, report_path)

    # build the race counts CSV on the fly from people data, then plot
    race_counts_csv = _build_race_counts(people_csv, sources_csv, out_dir)
    plot_perp_victim_race(race_counts_csv, report_path)
    plot_population_adjusted_race(people_csv, sources_csv, report_path)

    log_report(report_path, "visualize complete")


if __name__ == "__main__":
    main()
