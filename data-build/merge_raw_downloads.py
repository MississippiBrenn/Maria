#!/usr/bin/env python3
"""
Merge multiple partial NamUs CSV downloads into single compiled files.
NamUs limits downloads to 10k records, so we need to merge multiple files.
"""

import os
import glob
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
COMPILED_DIR = os.path.join(ROOT, "data", "compiled")
os.makedirs(COMPILED_DIR, exist_ok=True)

print("=== Merging NamUs CSV Downloads ===\n")

# Find all Missing Persons CSV files
print("Finding Missing Persons files...")
mp_pattern = os.path.join(RAW_DIR, "Missing", "*.csv")
mp_files = sorted(glob.glob(mp_pattern))

if not mp_files:
    print(f"  ERROR: No CSV files found in {os.path.join(RAW_DIR, 'Missing')}")
    print(f"  Pattern: {mp_pattern}")
else:
    print(f"  Found {len(mp_files)} file(s):")
    for f in mp_files:
        print(f"    - {os.path.basename(f)}")

    # Merge Missing Persons files
    print("\n  Merging Missing Persons files...")
    mp_dfs = []
    total_rows = 0
    for f in mp_files:
        df = pd.read_csv(f)
        mp_dfs.append(df)
        total_rows += len(df)
        print(f"    {os.path.basename(f)}: {len(df)} rows")

    mp_merged = pd.concat(mp_dfs, ignore_index=True)

    # Remove duplicates based on Case Number
    before_dedup = len(mp_merged)
    mp_merged = mp_merged.drop_duplicates(subset=['Case Number'], keep='first')
    after_dedup = len(mp_merged)

    print(f"\n  Total rows: {total_rows}")
    print(f"  After deduplication: {after_dedup} unique cases")
    if before_dedup > after_dedup:
        print(f"  Removed {before_dedup - after_dedup} duplicates")

    # Save merged file
    mp_output = os.path.join(COMPILED_DIR, "missing_persons.csv")
    mp_merged.to_csv(mp_output, index=False)
    print(f"  ✓ Wrote: {mp_output}")

print("\n" + "="*50 + "\n")

# Find all Unidentified Persons CSV files
print("Finding Unidentified Persons files...")
up_pattern = os.path.join(RAW_DIR, "Unidentified", "*.csv")
up_files = sorted(glob.glob(up_pattern))

if not up_files:
    print(f"  ERROR: No CSV files found in {os.path.join(RAW_DIR, 'Unidentified')}")
    print(f"  Pattern: {up_pattern}")
else:
    print(f"  Found {len(up_files)} file(s):")
    for f in up_files:
        print(f"    - {os.path.basename(f)}")

    # Merge Unidentified Persons files
    print("\n  Merging Unidentified Persons files...")
    up_dfs = []
    total_rows = 0
    for f in up_files:
        df = pd.read_csv(f)
        up_dfs.append(df)
        total_rows += len(df)
        print(f"    {os.path.basename(f)}: {len(df)} rows")

    up_merged = pd.concat(up_dfs, ignore_index=True)

    # Remove duplicates based on Case
    before_dedup = len(up_merged)
    up_merged = up_merged.drop_duplicates(subset=['Case'], keep='first')
    after_dedup = len(up_merged)

    print(f"\n  Total rows: {total_rows}")
    print(f"  After deduplication: {after_dedup} unique cases")
    if before_dedup > after_dedup:
        print(f"  Removed {before_dedup - after_dedup} duplicates")

    # Save merged file
    up_output = os.path.join(COMPILED_DIR, "unidentified_persons.csv")
    up_merged.to_csv(up_output, index=False)
    print(f"  ✓ Wrote: {up_output}")

print("\n" + "="*50)
print("\n✓ Merging complete!")
print(f"\nCompiled files are ready in: {COMPILED_DIR}")
print("\nNext step:")
print(f"  python3 data-build/process_namus_downloads.py \\")
print(f"    --mp data/compiled/missing_persons.csv \\")
print(f"    --up data/compiled/unidentified_persons.csv")
