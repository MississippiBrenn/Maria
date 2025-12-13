#!/usr/bin/env python3
"""
Open ALL NamUs cases for top matches at once (no batching).
"""

import pandas as pd
import webbrowser
import time
import sys
import os

# Load from all_matches_scored.csv (updated with infant filter)
if os.path.exists('out/all_matches_scored.csv'):
    tracker = pd.read_csv('out/all_matches_scored.csv')
    # Add ID column
    tracker['ID'] = range(1, len(tracker) + 1)
    # Rename columns to match old format
    tracker = tracker.rename(columns={
        'final_score': 'Score',
        'mp_match_count': 'MP_Cnt',
        'up_match_count': 'UP_Cnt',
        'mp_id': 'MP',
        'up_id': 'UP',
        'mp_name': 'Name'
    })
elif os.path.exists('out/match_investigation_tracker.csv'):
    tracker = pd.read_csv('out/match_investigation_tracker.csv')
else:
    print("Error: No match files found!")
    sys.exit(1)

def extract_case_number(case_id):
    """Extract numeric case number from ID like MP89129 or UP11054"""
    return case_id.replace('MP', '').replace('UP', '')

def open_match(row):
    """Open both MP and UP cases in browser"""
    mp_num = extract_case_number(row['MP'])
    up_num = extract_case_number(row['UP'])

    mp_url = f"https://www.namus.gov/MissingPersons/Case#/{mp_num}"
    up_url = f"https://www.namus.gov/UnidentifiedPersons/Case#/{up_num}/details?nav"

    print(f"  Match #{row['ID']}: {row['Name']} (Score: {row['Score']}, MP:{row['MP_Cnt']}, UP:{row['UP_Cnt']})")

    webbrowser.open(mp_url)
    time.sleep(0.3)  # Small delay between tabs
    webbrowser.open(up_url)
    time.sleep(0.5)  # Small delay between matches

def main():
    count = 20  # Default: all 20

    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            print(f"Invalid count: {sys.argv[1]}, using default of 20")

    matches_to_open = tracker.head(count)

    print("="*60)
    print("Opening NamUs Cases")
    print("="*60)
    print(f"Opening top {len(matches_to_open)} matches ({len(matches_to_open) * 2} tabs total)")
    print()

    for idx, row in matches_to_open.iterrows():
        open_match(row)

    print()
    print("="*60)
    print(f"✓ Opened {len(matches_to_open)} matches!")
    print("="*60)

if __name__ == '__main__':
    main()
