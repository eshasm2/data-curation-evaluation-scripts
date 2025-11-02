# data-curation-evaluation-scripts
Local News Crime Article Dataset (U.S.)

This dataset contains 33,506 local news articles from 551 U.S. sources covering crime and justice topics. Sources represent all 50 states and a range of ideological perspectives (left, right, centrist) ([CITE; add GitHub link]).

Data Collection
Articles were retrieved from Common Crawl snapshots (2018–2020, 2023–2024).
BeautifulSoup was used to extract article text.
Only pages with clear publication dates were included (2000–2024).

Keyword Selection
Tier 1 (specific): sexual abuse, child abuse, perpetrator, offender
Tier 2 (broader): defendant, misconduct, witness
Articles were selected if they contained Tier 1 term, or two Tier 2 matches.

Dataset Features
Local source domain and home state
Scraping date
Matched keywords and keyword tier

How to Run
Download the keyword files from this repository.
Run the samplingcc.py script to extract articles from the Common Crawl snapshots:
python samplingcc.py --keywords keywords_tier1.csv keywords_tier2.csv --output output_folder
