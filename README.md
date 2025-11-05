# data-curation-evaluation-scripts (LUCIC???)
This repository contains tools for the development and evaluation of the Local U.S. Crime and Ideology Corpus (LUCIC), a large-scale dataset of local news articles about crime and justice systems across the United States. LUCIC was created using a curated list of local news websites from all 50 states representing different ideological perspectives, including left-leaning, right-leaning, and centrist outlets ([CITE]; [GitHub link]).

Articles were identified within selected Common Crawl collections (CC-MAIN-2018-43, CC-MAIN-2019-09, CC-MAIN-2020-10, CC-MAIN-2023-06, and CC-MAIN-2024-14) using a purpose-built Python pipeline. Text was extracted with BeautifulSoup, and articles without reliable publication dates were excluded.

Content was filtered through a hierarchical keyword framework to capture discussions of crime and justice, with two tiers of specificity (e.g., sexual abuse, offender, defendant, witness). The final dataset includes 33,506 articles from 551 local U.S. news sources, spanning all 50 states and representing diverse ideological and geographic coverage. Scripts for data collection and evaluation are provided in this repository.

**total corpus size:** 33,506 articles


## The Current Repository

## How To Use
**Repository Setup**

Install Git on your computer. When finished, open a command line terminal, navigate to where you would like to place the repository, then enter git clone https://github.com/BabakHemmatian/Illinois_Social_Attitudes.git. Note that the raw and processed data files for the full 2007-2023 take several terabytes of space. Choose the repository location according to your use case's storage needs.

Download this folder into the newly created Illinois_Social_Attitudes folder.

The raw Reddit data that the filter_keywords resource requires can be found and downloaded here. The functions currently assume Reddit Comments as the type of data, with the relevant .zst files for a given timeframe to be placed in data/data_reddit_raw/reddit_comments/.

**Virtual Environment Setup**

Follow the steps here to install the desired version of Anaconda.

Once finished, navigate to Illinois_Social_attitudes on the command line and enter conda create --name ISAAC python=3.12 pip. Answer 'y' to the question. When finished, run conda activate ISAAC. Once the environment is activated, run the following command to install the necessary packages: pip install -r req.txt.


**Commands**

git clone https://github.com/yourusername/data-curation-evaluation-scripts.git

cd data-curation-evaluation-scripts

Run the pipeline:

python samplingcc.py \
  --keywords keywords_tier1.csv keywords_tier2.csv \
  --output output_folder

This command extracts articles from the specified Common Crawl snapshots that match your keyword sets.
