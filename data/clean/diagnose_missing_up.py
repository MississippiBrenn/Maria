# save as audit_up_master_sources.py; run: python3 audit_up_master_sources.py
import pandas as pd
df = pd.read_csv("UP_master.csv", dtype=str, keep_default_na=False)
print("Total rows:", len(df))
if "source_file" in df.columns:
    print("\nCounts by source_file:")
    print(df["source_file"].value_counts(dropna=False).to_string())
else:
    print("\nNo 'source_file' column found in UP_master.csv")
