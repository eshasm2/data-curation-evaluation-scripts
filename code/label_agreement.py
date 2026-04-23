# label_agreement - Stage 3 — Compute inter-rater reliability between human coders and LLMs.
#
# Runs independently of stages 1 and 2 — reads from pre-existing annotation files only.
#
# Metrics:
#   - Cohen's κ         for binary variables  (crime_present, violent_crime)
#   - Krippendorff's α  for nominal variables  (race, gender, crime_label)
#
# Raters: Eleanor, Sarah (human) + GPT, LLaMA, Qwen (LLM).
#
# Usage (via cli.py)
#   python cli.py --resource label_agreement
#
# Usage (direct)
#   python code/label_agreement.py
#
# Outputs (results/tables/)
#   irr_human_only.csv            — pairwise kappa/alpha between Eleanor and Sarah
#   irr_human_vs_llm.csv          — human vs each LLM
#   crime_label_disagreements.csv — articles where raters disagreed on crime type
#   report.csv                    — timestamped run log

import argparse
import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from code.config import log_report, ratings_dir, tables_dir

binary_vars  = {"crime_present", "violent_crime"}
nominal_vars = {"perp_race", "perp_gender", "victim_race", "victim_gender", "crime_label"}

# maps each variable to the column names used by each rater.
# llm crime label columns use a different naming convention (gpt_crime vs gpt_crime_label).
variables = {
    "crime_present": {
        "eleanor": "eleanor_crime_present", "sarah": "sarah_crime_present",
        "gpt": "gpt_crime_present", "llama": "llama_crime_present", "qwen": "qwen_crime_present",
    },
    "violent_crime": {
        "eleanor": "eleanor_violent_crime", "sarah": "sarah_violent_crime",
        "gpt": "gpt_violent_crime", "llama": "llama_violent_crime", "qwen": "qwen_violent_crime",
    },
    "crime_label": {
        "eleanor": "eleanor_crime_label", "sarah": "sarah_crime_label",
        "gpt": "gpt_crime", "llama": "llama_crime", "qwen": "qwen_crime",
    },
    "perp_race": {
        "eleanor": "eleanor_perp_race", "sarah": "sarah_perp_race",
        "gpt": "gpt_perp_race", "llama": "llama_perp_race", "qwen": "qwen_perp_race",
    },
    "perp_gender": {
        "eleanor": "eleanor_perp_gender", "sarah": "sarah_perp_gender",
        "gpt": "gpt_perp_gender", "llama": "llama_perp_gender", "qwen": "qwen_perp_gender",
    },
    "victim_race": {
        "eleanor": "eleanor_victim_race", "sarah": "sarah_victim_race",
        "gpt": "gpt_victim_race", "llama": "llama_victim_race", "qwen": "qwen_victim_race",
    },
    "victim_gender": {
        "eleanor": "eleanor_victim_gender", "sarah": "sarah_victim_gender",
        "gpt": "gpt_victim_gender", "llama": "llama_victim_gender", "qwen": "qwen_victim_gender",
    },
}

var_labels = {
    "crime_present": "Crime Present",       "violent_crime":  "Violent Crime",
    "crime_label":   "Crime Label",         "perp_race":      "Perpetrator Race",
    "perp_gender":   "Perpetrator Gender",  "victim_race":    "Victim Race",
    "victim_gender": "Victim Gender",
}

raters     = ["eleanor", "sarah", "gpt", "llama", "qwen"]
llm_raters = ["gpt", "llama", "qwen"]

# ambiguous or multi-person race values collapse to NaN rather than a category
race_collapse = {
    "hispanic": "other", "asian": "other", "middle eastern": "other",
    "pacific islander": "other", "native american": "other", "native": "other",
    "no name provided": np.nan, "multiple": np.nan, "unknown": np.nan,
    "not_provided": np.nan, "not provided": np.nan,
}
gender_collapse = {
    "no name provided": np.nan, "multiple": np.nan,
    "not_provided": np.nan, "not provided": np.nan, "nonbinary/other": np.nan,
}


# normalization helpers


def normalize(val, collapse=None):
    # lowercases and strips a value, applying an optional collapse mapping
    if pd.isna(val): return np.nan
    v = str(val).strip().lower()
    if v in ("", "nan", "none"): return np.nan
    if collapse and v in collapse: return collapse[v]
    return v

def norm_yn(val):
    # standardizes yes/no fields to 'yes', 'no', or NaN
    v = normalize(val)
    if v in ("yes", "true", "1"):  return "yes"
    if v in ("no", "false", "0"): return "no"
    return np.nan

norm_race   = lambda v: normalize(v, race_collapse)
norm_gender = lambda v: normalize(v, gender_collapse)


# agreement metrics


def cohens_kappa(s1, s2):
    # cohen's kappa for two raters on a binary variable.
    # only uses rows where both raters gave a non-null answer.
    #   1.0  = perfect agreement
    #   0.0  = agreement no better than chance
    #  <0.0  = systematic disagreement
    # returns (kappa, n_valid_pairs).
    mask = s1.notna() & s2.notna()
    n = mask.sum()
    if n < 2: return np.nan, n
    try:
        kappa = cohen_kappa_score(s1[mask], s2[mask])
    except Exception:
        # can fail if only one unique label is present in the subset
        kappa = np.nan
    return round(kappa, 3), n


def kripp_alpha(df):
    # krippendorff's alpha for nominal data across any number of raters.
    # uses the coincidence matrix approach; skips rows with fewer than 2 valid values.
    #
    # alpha measures inter-rater reliability:
    #   1.0  = perfect agreement
    #   0.0  = agreement no better than chance
    #  <0.0  = systematic disagreement
    #
    # formula: alpha = 1 - (Do / De)
    #   Do = observed disagreement (proportion of differing pairs)
    #   De = expected disagreement under chance (based on overall value distribution)
    data = df.values.astype(object)
    coinc, n_c = {}, 0
    for row in data:
        valid = [str(v) for v in row
                 if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if len(valid) < 2: continue
        for v1, v2 in combinations(valid, 2):
            for k in [(v1, v2), (v2, v1)]:
                coinc[k] = coinc.get(k, 0) + 1
                n_c += 1
    if n_c == 0: return np.nan
    Do = sum(v for (k1, k2), v in coinc.items() if k1 != k2) / n_c
    vc = {}
    for (k1, _), cnt in coinc.items():
        vc[k1] = vc.get(k1, 0) + cnt
    tot = sum(vc.values())
    De = 1 - sum((c / tot) ** 2 for c in vc.values())
    return round(1 - Do / De, 3) if De != 0 else np.nan


def majority_vote(vals):
    # return the value that appears more than once across LLMs; None if no majority.
    clean = [v for v in vals if pd.notna(v)]
    if not clean: return None
    counts = Counter(clean)
    top_val, top_count = counts.most_common(1)[0]
    return top_val if top_count > 1 else None


# data loading


def load_llm_data():
    # collapse to one row per article — the raw file has one row per person mentioned,
    # but article-level labels (crime_present, etc.) are consistent across rows
    llm_raw = pd.read_csv(ratings_dir / "people_info_cleaned_latest.csv")
    llm_raw["art_num"] = llm_raw["article_id"].astype(int)
    llm_cols = [c for c in llm_raw.columns
                if any(c.startswith(p) for p in ["gpt_", "llama_", "qwen_"])]
    llm = llm_raw.groupby("art_num")[llm_cols].first().reset_index()
    for c in [x for x in llm_cols if "crime_present" in x or "violent_crime" in x]:
        llm[c] = llm[c].apply(norm_yn)
    for c in [x for x in llm_cols if "_race" in x]:
        llm[c] = llm[c].apply(norm_race)
    for c in [x for x in llm_cols if "_gender" in x]:
        llm[c] = llm[c].apply(norm_gender)
    for c in [x for x in llm_cols
              if "_crime" in x and "_present" not in x and "violent" not in x]:
        llm[c] = llm[c].apply(normalize)
    return llm


def load_human_data():
    # qualtrics exports one row per rater with one column per article×question.
    # we melt to long format, then pivot so each row is one rater × one article.
    import re
    human_raw = pd.read_csv(ratings_dir / "data_for_qualtrics.csv", header=0, skiprows=[1])
    art_q1 = "ART0001_Q1_CRIME_COMMITTED"

    # drop qualtrics header/preview rows (they contain template strings like "{...}")
    human_data = human_raw[~human_raw[art_q1].astype(str).str.startswith("{")].copy()
    human_data = human_data[human_data[art_q1].notna()].reset_index(drop=True)
    human_data["rater"] = ["Eleanor", "Sarah"][:len(human_data)]

    art_cols = [c for c in human_data.columns if re.match(r"ART\d{4}_Q", c)]

    # reshape from wide to long so each row is one (rater, column) pair,
    # then pull article number and question name out of the column string
    long = human_data[["rater"] + art_cols].melt(
        id_vars=["rater"], var_name="col", value_name="value"
    )
    long["art_num"]  = long["col"].str.extract(r"ART(\d+)").astype(int)
    long["question"] = long["col"].str.replace(r"ART\d+_", "", regex=True)

    human_long = long.pivot_table(
        index=["rater", "art_num"], columns="question",
        values="value", aggfunc="first"
    ).reset_index()
    human_long.columns.name = None

    def find_col(df, pattern):
        m = [c for c in df.columns if pattern.lower() in c.lower()]
        return m[0] if m else None

    q_map = {
        "crime_present": find_col(human_long, "Q1_CRIME_COMMITTED"),
        "violent_crime": find_col(human_long, "Q2_VIOLENT"),
        "perp_race":     find_col(human_long, "Q4_PERP_RACE"),
        "perp_gender":   find_col(human_long, "Q6_PERP_GENDER"),
        "victim_race":   find_col(human_long, "Q9_VICTIM_RACE"),
        "victim_gender": find_col(human_long, "Q11_VICTIM_GENDER"),
        "crime_label":   find_col(human_long, "Q13_CRIME_LABEL"),
    }
    norm_fns = {
        "crime_present": norm_yn,     "violent_crime": norm_yn,
        "perp_race":     norm_race,   "victim_race":   norm_race,
        "perp_gender":   norm_gender, "victim_gender": norm_gender,
        "crime_label":   normalize,
    }
    for var, col in q_map.items():
        if col:
            human_long[var] = human_long[col].apply(norm_fns[var])

    norm_vars = list(norm_fns.keys())

    def pivot_rater(name):
        sub = human_long[human_long["rater"] == name][["art_num"] + norm_vars].copy()
        sub.columns = ["art_num"] + [f"{name.lower()}_{v}" for v in norm_vars]
        return sub

    return pivot_rater("Eleanor").merge(pivot_rater("Sarah"), on="art_num", how="inner")


def get_args(argv=None):
    parser = argparse.ArgumentParser(description="Stage 3 — compute inter-rater reliability between humans and LLMs.")
    parser.add_argument("-t", "--type",   default="articles")
    parser.add_argument("-i", "--input",  type=str)
    parser.add_argument("-o", "--output", type=str)
    return parser.parse_args(argv)


def main(argv=None):
    args = get_args(argv)
    out_dir = Path(args.output) if args.output else tables_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = str(out_dir / "report.csv")
    log_report(report_path, "Starting label_agreement")

    # load and merge human and llm annotations on article number
    llm    = load_llm_data()
    human  = load_human_data()
    merged = llm.merge(human, on="art_num", how="inner")
    log_report(report_path, f"Merged articles: {len(merged)}")

    # add llm majority vote columns before computing agreement
    for var, cols in variables.items():
        cols_to_use = [cols[r] for r in llm_raters if cols.get(r) in merged.columns]
        if len(cols_to_use) >= 2:
            merged[f"{var}_llm_majority"] = merged[cols_to_use].apply(
                lambda row: majority_vote(row.values), axis=1
            )

    # compute pairwise agreement for every rater combination
    results = []
    for var, cols in variables.items():
        valid_cols = {r: c for r, c in cols.items() if c in merged.columns}
        for r1, r2 in combinations(raters, 2):
            if r1 not in valid_cols or r2 not in valid_cols: continue
            if var in binary_vars:
                val, n = cohens_kappa(merged[valid_cols[r1]], merged[valid_cols[r2]])
                metric = "cohens_kappa"
            else:
                val = kripp_alpha(merged[[valid_cols[r1], valid_cols[r2]]])
                n   = merged[[valid_cols[r1], valid_cols[r2]]].notna().all(axis=1).sum()
                metric = "krippendorff_alpha"
            results.append({"variable": var, "rater_1": r1, "rater_2": r2,
                             "metric": metric, "value": val, "n": n})

    res = pd.DataFrame(results)

    # save human-only IRR table
    irr = res[(res["rater_1"] == "eleanor") & (res["rater_2"] == "sarah")].copy()
    irr["variable"] = irr["variable"].map(var_labels)
    irr.to_csv(out_dir / "irr_human_only.csv", index=False)
    log_report(report_path,
               f"Human IRR:\n{irr[['variable','metric','value','n']].to_string(index=False)}")

    # save human vs llm IRR table
    hvl = res[res["rater_1"].isin(["eleanor","sarah"]) & res["rater_2"].isin(llm_raters)].copy()
    hvl["variable"] = hvl["variable"].map(var_labels)
    hvl.to_csv(out_dir / "irr_human_vs_llm.csv", index=False)

    # save articles where raters disagreed on the crime label for qualitative review
    lc = variables["crime_label"]
    dis_cols = ["art_num"] + [lc[r] for r in raters if lc.get(r) in merged.columns]
    dis = merged[dis_cols].copy()
    dis.columns = ["art_num"] + [r for r in raters if lc.get(r) in merged.columns]
    dis["all_agree"] = dis.drop(columns="art_num").apply(
        lambda row: len({v for v in row if pd.notna(v)}) <= 1, axis=1
    )
    dis[~dis["all_agree"]].to_csv(out_dir / "crime_label_disagreements.csv", index=False)
    log_report(report_path, "label_agreement complete")


if __name__ == "__main__":
    main()
