#!/usr/bin/env python3
"""
Open NamUs cases for top matches in batches of 10.
"""

import pandas as pd
import webbrowser
import time
import sys

# Load tracker
tracker = pd.read_csv('out/match_investigation_tracker.csv')

def extract_case_number(case_id):
    """Extract numeric case number from ID like MP89129 or UP11054"""
    return case_id.replace('MP', '').replace('UP', '')

def open_match(row):
    """Open both MP and UP cases in browser"""
    mp_num = extract_case_number(row['MP'])
    up_num = extract_case_number(row['UP'])

    mp_url = f"https://www.namus.gov/MissingPersons/Case#/{mp_num}"
    up_url = f"https://www.namus.gov/UnidentifiedPersons/Case#/{up_num}/details?nav"

    print(f"\nMatch #{row['ID']}: {row['Name']}")
    print(f"  Score: {row['Score']} | MP matches: {row['MP_Cnt']} | UP matches: {row['UP_Cnt']}")
    print(f"  Opening: {row['MP']} and {row['UP']}")

    webbrowser.open(mp_url)
    time.sleep(0.5)  # Small delay between tabs
    webbrowser.open(up_url)

def main():
    batch_size = 10

    if len(sys.argv) > 1:
        try:
            batch_size = int(sys.argv[1])
        except ValueError:
            print(f"Invalid batch size: {sys.argv[1]}, using default of 10")

    total_matches = len(tracker)
    batches = (total_matches + batch_size - 1) // batch_size

    print("="*60)
    print("NamUs Match Case Opener")
    print("="*60)
    print(f"Total matches: {total_matches}")
    print(f"Batch size: {batch_size}")
    print(f"Number of batches: {batches}")
    print()

    for batch_num in range(batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_matches)
        batch = tracker.iloc[start_idx:end_idx]

        print(f"\n{'='*60}")
        print(f"BATCH {batch_num + 1}/{batches} (Matches {start_idx + 1}-{end_idx})")
        print(f"{'='*60}")

        for idx, row in batch.iterrows():
            open_match(row)
            time.sleep(1)  # Delay between matches to not overwhelm browser

        if batch_num < batches - 1:
            input(f"\nPress Enter to open next batch ({end_idx + 1}-{min(end_idx + batch_size, total_matches)})...")
        else:
            print(f"\n{'='*60}")
            print("All matches opened!")
            print(f"{'='*60}")

if __name__ == '__main__':
    main()
