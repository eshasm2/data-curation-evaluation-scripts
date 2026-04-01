import pandas as pd
import matplotlib.pyplot as plt

# --- Load correctly as CSV (comma-separated) ---
df = pd.read_csv('matched_articles_50_metadata.csv')  # or .tsv if extension differs, but use comma

# --- Clean column names ---
df.columns = df.columns.str.strip().str.lower()

# --- Define valid U.S. state abbreviations ---
us_state_abbrevs = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS',
    'KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY',
    'NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
    'WI','WY'
}

# --- Filter out non-U.S. states ---
df = df[df['state'].isin(us_state_abbrevs)]

# --- Count and plot ---
state_counts = df['state'].value_counts().sort_values(ascending=False)

plt.figure(figsize=(12,6))
plt.bar(state_counts.index, state_counts.values)
plt.xticks(rotation=90)
plt.xlabel('State')
plt.ylabel('Number of Sources')
plt.title('Number of Sources per U.S. State')
plt.tight_layout()
plt.show()
