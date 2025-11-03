# data-curation-evaluation-scripts
This repository contains tools and documentation for the development and evaluation of the Local News Crime Article Dataset (LNCAD) — a curated dataset of 33,506 local U.S. news articles covering crime and justice topics. The dataset includes content from 551 local news sources across all 50 U.S. states, spanning a broad range of ideological perspectives (left-leaning, right-leaning, and centrist) ([CITE; add GitHub link]).


Dataset Features:
Each record in the dataset includes:
Local source domain
Home state of the outlet
Scraping date
Matched keywords and keyword tier
Extracted article text
Publication year


Data Composition
Geographic coverage: All U.S. states
Sources per state (avg): ~700 per year
Ideological balance: Left, Right, and Centrist outlets represented
Regional coverage: Even distribution across Northeast, Midwest, South, and West
Figures 1–3 in the manuscript visualize source count by state, regional representation, and ideological distribution.

How to Run:
git clone https://github.com/yourusername/data-curation-evaluation-scripts.git
cd data-curation-evaluation-scripts
Run the pipeline:
python samplingcc.py \
  --keywords keywords_tier1.csv keywords_tier2.csv \
  --output output_folder

This command extracts articles from the specified Common Crawl snapshots that match your keyword sets.
