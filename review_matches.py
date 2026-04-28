#!/usr/bin/env python3
"""
Interactive match review helper.

Improved version that:
1. Works with prioritized output files
2. Shows detailed info before opening cases
3. Allows marking matches as reviewed/excluded
4. Tracks review progress
"""

import pandas as pd
import webbrowser
import time
import sys
import os
from datetime import date

OUT_DIR = 'out'
DATA_DIR = 'data/clean'
EXCLUSIONS_PATH = 'data/exclusions.csv'
REVIEW_LOG_PATH = 'out/review_log.csv'


def load_data():
    """Load all necessary data files."""
    # Try prioritized file first, fall back to regular
    prioritized_path = os.path.join(OUT_DIR, 'best_match_per_up.csv')
    if os.path.exists(prioritized_path):
        pairs = pd.read_csv(prioritized_path)
        print(f"Loaded: {prioritized_path} ({len(pairs)} pairs)")
    else:
        pairs_path = os.path.join(OUT_DIR, 'candidate_pairs_tier1.csv')
        if os.path.exists(pairs_path):
            pairs = pd.read_csv(pairs_path)
            print(f"Loaded: {pairs_path} ({len(pairs)} pairs)")
        else:
            print("ERROR: No prioritized match file found. Run prioritize_matches.py first.")
            sys.exit(1)

    mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    # Load review log if exists
    if os.path.exists(REVIEW_LOG_PATH):
        review_log = pd.read_csv(REVIEW_LOG_PATH)
    else:
        review_log = pd.DataFrame(columns=['mp_id', 'up_id', 'date', 'action', 'notes'])

    return pairs, mp, up, review_log


def get_case_details(case_id, mp_df, up_df):
    """Get detailed info for a case."""
    if case_id.startswith('MP'):
        row = mp_df[mp_df['id'] == case_id]
        if len(row) == 0:
            return None
        row = row.iloc[0]
        return {
            'type': 'MP',
            'id': case_id,
            'name': f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
            'sex': row.get('sex', 'Unknown'),
            'race': row.get('race', 'Unknown'),
            'age': f"{row.get('age_min', '?')}-{row.get('age_max', '?')}",
            'date': row.get('last_seen_date', 'Unknown'),
            'city': row.get('city', ''),
            'county': row.get('county', ''),
            'state': row.get('state', ''),
        }
    else:  # UP
        row = up_df[up_df['id'] == case_id]
        if len(row) == 0:
            return None
        row = row.iloc[0]
        return {
            'type': 'UP',
            'id': case_id,
            'mec': row.get('mec_case', ''),
            'sex': row.get('sex', 'Unknown'),
            'race': row.get('race', 'Unknown'),
            'age': f"{row.get('age_min', '?')}-{row.get('age_max', '?')}",
            'date': row.get('found_date', 'Unknown'),
            'city': row.get('city', ''),
            'county': row.get('county', ''),
            'state': row.get('state', ''),
        }


def print_match_details(mp_id, up_id, pair_row, mp_df, up_df):
    """Print detailed comparison of MP and UP."""
    mp_info = get_case_details(mp_id, mp_df, up_df)
    up_info = get_case_details(up_id, mp_df, up_df)

    if not mp_info or not up_info:
        print("  ERROR: Could not load case details")
        return

    print("\n" + "=" * 70)
    print(f"{'MISSING PERSON':<35} | {'UNIDENTIFIED PERSON':<35}")
    print("=" * 70)
    print(f"{mp_info['id']:<35} | {up_info['id']:<35}")
    if mp_info.get('name'):
        print(f"{mp_info['name']:<35} | {up_info.get('mec', ''):<35}")
    print("-" * 70)
    print(f"Sex: {mp_info['sex']:<29} | Sex: {up_info['sex']:<29}")
    print(f"Race: {mp_info['race']:<28} | Race: {up_info['race']:<28}")
    print(f"Age: {mp_info['age']:<29} | Age: {up_info['age']:<29}")
    print(f"Date: {str(mp_info['date']):<28} | Date: {str(up_info['date']):<28}")
    print(f"City: {mp_info['city']:<28} | City: {up_info['city']:<28}")
    print(f"County: {mp_info['county']:<26} | County: {up_info['county']:<26}")
    print(f"State: {mp_info['state']:<27} | State: {up_info['state']:<27}")
    print("-" * 70)

    # Show match metrics if available
    if 'priority_score' in pair_row:
        print(f"Priority Score: {pair_row.get('priority_score', 'N/A'):.3f}")
    if 'priority_tier' in pair_row:
        print(f"Priority Tier: {pair_row.get('priority_tier', 'N/A')} ({pair_row.get('tier_reason', '')})")
    if 'up_match_count' in pair_row:
        print(f"UP has {pair_row.get('up_match_count', '?')} total candidate matches")
    if 'days_gap' in pair_row and pd.notna(pair_row['days_gap']):
        days = int(pair_row['days_gap'])
        years = days / 365.25
        print(f"Time gap: {days} days ({years:.1f} years)")


def open_cases(mp_id, up_id):
    """Open both cases in browser."""
    mp_num = mp_id.replace('MP', '')
    up_num = up_id.replace('UP', '')

    mp_url = f"https://www.namus.gov/MissingPersons/Case#/{mp_num}"
    up_url = f"https://www.namus.gov/UnidentifiedPersons/Case#/{up_num}/details?nav"

    print(f"\nOpening {mp_id} and {up_id} in browser...")
    webbrowser.open(mp_url)
    time.sleep(0.3)
    webbrowser.open(up_url)


def add_exclusion(up_id, reason, notes):
    """Add UP to exclusions file."""
    today = date.today().isoformat()

    # Load existing exclusions
    if os.path.exists(EXCLUSIONS_PATH):
        exclusions = pd.read_csv(EXCLUSIONS_PATH)
    else:
        exclusions = pd.DataFrame(columns=['id', 'type', 'reason', 'date_added', 'notes'])

    # Check if already excluded
    if up_id in exclusions['id'].values:
        print(f"  {up_id} is already excluded")
        return

    # Add new exclusion
    new_row = pd.DataFrame([{
        'id': up_id,
        'type': 'UP',
        'reason': reason,
        'date_added': today,
        'notes': notes
    }])
    exclusions = pd.concat([exclusions, new_row], ignore_index=True)
    exclusions.to_csv(EXCLUSIONS_PATH, index=False)
    print(f"  Added {up_id} to exclusions ({reason})")


def log_review(mp_id, up_id, action, notes, review_log):
    """Log a review action."""
    today = date.today().isoformat()
    new_row = pd.DataFrame([{
        'mp_id': mp_id,
        'up_id': up_id,
        'date': today,
        'action': action,
        'notes': notes
    }])
    review_log = pd.concat([review_log, new_row], ignore_index=True)
    review_log.to_csv(REVIEW_LOG_PATH, index=False)
    return review_log


def main():
    print("=" * 70)
    print("Interactive Match Review Helper")
    print("=" * 70)

    pairs, mp_df, up_df, review_log = load_data()

    # Filter out already reviewed pairs
    reviewed_pairs = set(zip(review_log['mp_id'], review_log['up_id']))
    pairs['reviewed'] = pairs.apply(
        lambda r: (r['mp_id'], r['up_id']) in reviewed_pairs, axis=1
    )
    unreviewed = pairs[~pairs['reviewed']]

    print(f"Total pairs: {len(pairs)}")
    print(f"Already reviewed: {len(pairs) - len(unreviewed)}")
    print(f"Remaining to review: {len(unreviewed)}")

    if len(unreviewed) == 0:
        print("\nAll pairs have been reviewed!")
        return

    print("\nCommands during review:")
    print("  [Enter] - Open cases in browser")
    print("  s - Skip this match")
    print("  x - Exclude UP (will prompt for reason)")
    print("  v - Mark as viable candidate")
    print("  r - Mark as ruled out")
    print("  q - Quit review")

    for idx, (_, row) in enumerate(unreviewed.iterrows()):
        mp_id = row['mp_id']
        up_id = row['up_id']

        print(f"\n\n>>> Match {idx + 1}/{len(unreviewed)} <<<")
        print_match_details(mp_id, up_id, row, mp_df, up_df)

        while True:
            cmd = input("\nAction ([Enter]=open, s=skip, x=exclude, v=viable, r=ruled out, q=quit): ").strip().lower()

            if cmd == '':
                open_cases(mp_id, up_id)
                continue  # Stay on same match after opening

            elif cmd == 's':
                print("  Skipped")
                break

            elif cmd == 'x':
                print("\nExclusion reasons:")
                print("  1. INFANT - Confirmed infant/fetus")
                print("  2. PARTIAL_REMAINS - Partial remains only")
                print("  3. HISTORICAL - Pre-dates MP database")
                print("  4. OTHER - Other reason")
                reason_choice = input("Choose reason (1-4): ").strip()

                reasons = {'1': 'INFANT', '2': 'PARTIAL_REMAINS', '3': 'HISTORICAL', '4': 'OTHER'}
                reason = reasons.get(reason_choice, 'OTHER')

                notes = input("Notes (optional): ").strip()
                add_exclusion(up_id, reason, notes)
                review_log = log_review(mp_id, up_id, 'excluded', f"{reason}: {notes}", review_log)
                break

            elif cmd == 'v':
                notes = input("Notes about viable match: ").strip()
                review_log = log_review(mp_id, up_id, 'viable', notes, review_log)
                print(f"  Marked as VIABLE candidate")
                break

            elif cmd == 'r':
                notes = input("Reason for ruling out: ").strip()
                review_log = log_review(mp_id, up_id, 'ruled_out', notes, review_log)
                print(f"  Marked as RULED OUT")
                break

            elif cmd == 'q':
                print("\nReview session ended.")
                print(f"Progress saved to {REVIEW_LOG_PATH}")
                return

            else:
                print("  Unknown command. Try again.")

    print("\n" + "=" * 70)
    print("Review session complete!")
    print(f"Progress saved to {REVIEW_LOG_PATH}")


if __name__ == '__main__':
    main()
