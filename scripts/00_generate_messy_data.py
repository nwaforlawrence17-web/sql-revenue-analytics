import pandas as pd
import numpy as np
import random
import os

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# File Paths
repo_dir = r"c:\Users\User\Desktop\SQL_Revenue_Analytics_Repo"
source_path = os.path.join(repo_dir, "data", "revenue_data.xlsx")
output_path = os.path.join(repo_dir, "data", "01_raw_messy_sales_data.csv")

if not os.path.exists(source_path):
    print(f"Error: Source file {source_path} not found.")
    exit(1)

# 1. Load the actual clean revenue data
print(f"Loading {source_path}...")
df_clean = pd.read_excel(source_path)

# 2. Sample 200 rows for the portfolio version
n_rows = 200
if len(df_clean) < n_rows:
    df = df_clean.copy()
    print(f"Warning: Source only has {len(df_clean)} rows. Using all of them.")
else:
    df = df_clean.sample(n=n_rows, random_state=42).reset_index(drop=True)

# 3. INTRODUCE CHAOS (The "Messy" Part)

# A. Nulls and Missing Values
# Target transaction_id and customer_full_name for some missingness
df.loc[np.random.choice(df.index, 10), 'transaction_id'] = np.nan
df.loc[np.random.choice(df.index, 15), 'customer_full_name'] = '   '

# B. Invalid Dates (transaction_timestamp)
# Convert some timestamps to messy strings
messy_dates = []
for d in df['transaction_timestamp']:
    r = random.random()
    if r < 0.05:
        messy_dates.append(pd.to_datetime(d).strftime('%d/%m/%Y')) # EU format
    elif r < 0.05:
        messy_dates.append(pd.to_datetime(d).strftime('%b %d %Y')) # Text format
    elif r < 0.02:
        messy_dates.append('N/A')
    else:
        messy_dates.append(str(d))
df['transaction_timestamp'] = messy_dates

# C. Messy String Cases and Spaces
# Target product_name and customer_country
df['product_name'] = df['product_name'].apply(lambda x: str(x).lower() if random.random() < 0.1 else (str(x).upper() if random.random() < 0.1 else x))
df['customer_country'] = df['customer_country'].apply(lambda x: f"  {x}  " if random.random() < 0.1 else x)

# D. Bad Numeric Values (revenue and unit_price)
df['revenue'] = df['revenue'].astype(object)
df.loc[np.random.choice(df.index, 8), 'revenue'] = '#N/A'
df.loc[np.random.choice(df.index, 5), 'revenue'] = 'FREE'

df['unit_price'] = df['unit_price'].astype(object)
df.loc[np.random.choice(df.index, 5), 'unit_price'] = '?'

# 4. Save the Messy Dataset
os.makedirs(os.path.join(repo_dir, "data"), exist_ok=True)
df.to_csv(output_path, index=False)

print(f"Successfully created messy dataset at: {output_path}")
print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
print("Consistency check: Column names match source revenue_data.xlsx.")
