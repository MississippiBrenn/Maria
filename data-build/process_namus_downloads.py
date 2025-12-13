#!/usr/bin/env python3
"""
Process NamUs CSV downloads into master files for matching.

Usage:
    python3 data-build/process_namus_downloads.py \\
        --mp data/raw/missing_persons.csv \\
        --up data/raw/unidentified_persons.csv
"""

import os
import argparse
import pandas as pd
from dateutil import parser as date_parser

ROOT = os.path.dirname(os.path.dirname(__file__))
CLEAN_DIR = os.path.join(ROOT, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)


def parse_date(value):
    """Parse various date formats to ISO format."""
    if pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return date_parser.parse(str(value), fuzzy=True).date().isoformat()
    except Exception:
        return None


def normalize_sex(value):
    """Normalize sex values to M/F/Unknown."""
    if pd.isna(value):
        return "Unknown"

    s = str(value).strip().upper()

    # Male
    if s in ["M", "MALE"]:
        return "M"
    # Female
    elif s in ["F", "FEMALE"]:
        return "F"
    # Everything else (Unsure, Other, Not Provided, etc.)
    else:
        return "Unknown"


def process_missing_persons(mp_file):
    """Process Missing Persons CSV into MP_master.csv."""
    print(f"Reading {mp_file}...")
    df = pd.read_csv(mp_file)

    # Expected columns from NamUs MP download
    mp_master = pd.DataFrame({
        "id": df["Case Number"].astype(str).str.strip(),
        "first_name": df.get("Legal First Name", "").fillna(""),
        "last_name": df.get("Legal Last Name", "").fillna(""),
        "sex": df["Biological Sex"].apply(normalize_sex),
        "race": df.get("Race / Ethnicity", "").fillna(""),
        "age_min": pd.to_numeric(df.get("Missing Age"), errors="coerce"),
        "age_max": pd.to_numeric(df.get("Missing Age"), errors="coerce"),
        "last_seen_date": df.get("DLC", "").apply(parse_date),
        "city": df.get("City", "").fillna(""),
        "county": df.get("County", "").fillna(""),
        "state": df.get("State", "").fillna(""),
        "date_modified": df.get("Date Modified", "").apply(parse_date),
    })

    # Expand age range (±2 years to account for estimation)
    mp_master["age_min"] = (mp_master["age_min"] - 2).clip(lower=0)
    mp_master["age_max"] = (mp_master["age_max"] + 2).clip(upper=120)

    out_path = os.path.join(CLEAN_DIR, "MP_master.csv")
    mp_master.to_csv(out_path, index=False)
    print(f"✓ Wrote {len(mp_master)} missing persons to {out_path}")
    return mp_master


def process_unidentified_persons(up_file):
    """Process Unidentified Persons CSV into UP_master.csv."""
    print(f"Reading {up_file}...")
    df = pd.read_csv(up_file)

    # Expected columns from NamUs UP download
    up_master = pd.DataFrame({
        "id": df["Case"].astype(str).str.strip(),
        "mec_case": df.get("ME/C Case", "").fillna(""),
        "sex": df["Biological Sex"].apply(normalize_sex),
        "race": df.get("Race / Ethnicity", "").fillna(""),
        "age_min": pd.to_numeric(df.get("Age From"), errors="coerce"),
        "age_max": pd.to_numeric(df.get("Age To"), errors="coerce"),
        "found_date": df.get("DBF", "").apply(parse_date),
        "city": df.get("City", "").fillna(""),
        "county": df.get("County", "").fillna(""),
        "state": df.get("State", "").fillna(""),
        "date_modified": df.get("Date Modified", "").apply(parse_date),
    })

    out_path = os.path.join(CLEAN_DIR, "UP_master.csv")
    up_master.to_csv(out_path, index=False)
    print(f"✓ Wrote {len(up_master)} unidentified persons to {out_path}")
    return up_master


def main():
    parser = argparse.ArgumentParser(description="Process NamUs CSV downloads")
    parser.add_argument("--mp", required=True, help="Missing Persons CSV file")
    parser.add_argument("--up", required=True, help="Unidentified Persons CSV file")
    args = parser.parse_args()

    mp_df = process_missing_persons(args.mp)
    up_df = process_unidentified_persons(args.up)

    print(f"\n✓ Processing complete!")
    print(f"  MP: {len(mp_df)} cases")
    print(f"  UP: {len(up_df)} cases")
    print(f"  Potential matches: {len(mp_df) * len(up_df):,}")


if __name__ == "__main__":
    main()
