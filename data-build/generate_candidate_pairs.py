#!/usr/bin/env python3
"""
OPTIMIZED version: Build case JSON files and match candidates from NamUs data.
Uses vectorized operations and smart indexing to avoid nested loops.
"""

import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from state_normalizer import normalize_state


def parse_races(race_str):
    """Parse a race string into a set of normalized race categories."""
    if pd.isna(race_str) or str(race_str).strip() == '':
        return set()
    # Split by comma and normalize each part
    races = set()
    for part in str(race_str).split(','):
        normalized = part.strip().upper()
        if normalized and normalized not in ('UNCERTAIN', 'UNKNOWN', 'OTHER'):
            races.add(normalized)
    return races


def races_overlap(race_mp, race_up):
    """Check if two race strings have any overlapping categories.

    Returns True if:
    - Either race is empty/unknown (can't rule out match)
    - There's at least one common race category
    """
    races_mp = parse_races(race_mp)
    races_up = parse_races(race_up)

    # If either is empty/unknown, allow the match (can't rule it out)
    if not races_mp or not races_up:
        return True

    # Check for any overlap
    return bool(races_mp & races_up)

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))
os.makedirs(OUT_DIR, exist_ok=True)

print("="*60)
print("OPTIMIZED Candidate Pair Generation")
print("="*60)

# Load master CSVs
print("\n1. Loading data...")
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

# Normalize dates → ordinal days for temporal calculations
mp['last_seen_days'] = pd.to_datetime(mp['last_seen_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)
up['found_days'] = pd.to_datetime(up['found_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)

print(f"   Loaded {len(mp):,} missing persons and {len(up):,} unidentified persons")
print(f"   Potential comparisons: {len(mp) * len(up):,}")

# OPTIMIZATION 1: Group by sex first to reduce candidate pairs
print("\n2. Preparing for sex-based matching...")
print("   NOTE: Matching by sex groups first, then cross-state")

# Group by sex
sex_groups = ['M', 'F', 'Unknown']
mp_by_sex = {sex: mp[mp['sex'] == sex].copy() for sex in sex_groups}
up_by_sex = {sex: up[up['sex'] == sex].copy() for sex in sex_groups}

print(f"   MP by sex: M={len(mp_by_sex['M'])}, F={len(mp_by_sex['F'])}, Unknown={len(mp_by_sex['Unknown'])}")
print(f"   UP by sex: M={len(up_by_sex['M'])}, F={len(up_by_sex['F'])}, Unknown={len(up_by_sex['Unknown'])}")

# OPTIMIZATION 2: Process by sex groups in chunks (to avoid memory issues)
print("\n3. Generating candidate pairs by sex groups...")
all_pairs = []
CHUNK_SIZE = 200  # Reduced from 1000 to manage memory better

# Define which sex groups should match
sex_matching_rules = [
    ('M', ['M', 'Unknown']),      # Male can match Male or Unknown
    ('F', ['F', 'Unknown']),      # Female can match Female or Unknown
    ('Unknown', ['M', 'F', 'Unknown'])  # Unknown can match anyone
]

for mp_sex, up_sexes in sex_matching_rules:
    mp_sex_group = mp_by_sex[mp_sex]

    if len(mp_sex_group) == 0:
        continue

    print(f"\n   Processing {mp_sex} MPs ({len(mp_sex_group):,} cases)...")

    # Collect all matching UPs for this sex group
    up_matches = pd.concat([up_by_sex[s] for s in up_sexes if len(up_by_sex[s]) > 0], ignore_index=True)

    if len(up_matches) == 0:
        continue

    print(f"   Matching against {len(up_matches):,} UPs")

    # Process this sex group in chunks
    for chunk_start in tqdm(range(0, len(mp_sex_group), CHUNK_SIZE), desc=f"   {mp_sex} chunks"):
        chunk_end = min(chunk_start + CHUNK_SIZE, len(mp_sex_group))
        mp_chunk = mp_sex_group.iloc[chunk_start:chunk_end].copy()

        if len(mp_chunk) == 0:
            continue

        # OPTIMIZATION 3: Use cross merge for Cartesian product (faster than nested loops)
        mp_chunk['_merge_key'] = 1
        up_temp = up_matches.copy()
        up_temp['_merge_key'] = 1

        # Cross join this MP chunk with matching UPs
        pairs_chunk = mp_chunk[['id', 'sex', 'age_min', 'age_max', 'last_seen_days',
                                 'county', 'city', 'state', 'race', '_merge_key']].merge(
            up_temp[['id', 'sex', 'age_min', 'age_max', 'found_days',
                    'county', 'city', 'state', 'race', '_merge_key']],
            on='_merge_key',
            suffixes=('_mp', '_up')
        )

        # OPTIMIZATION 4: Vectorized filtering (much faster than row-by-row)

        if len(pairs_chunk) == 0:
            continue

        # Filter 2: Age range overlap (allow NaN ages - will be scored lower)
        age_overlap = (
            pairs_chunk['age_min_mp'].isna() |
            pairs_chunk['age_max_up'].isna() |
            pairs_chunk['age_min_up'].isna() |
            pairs_chunk['age_max_mp'].isna() |
            ((pairs_chunk['age_min_mp'] <= pairs_chunk['age_max_up']) &
             (pairs_chunk['age_min_up'] <= pairs_chunk['age_max_mp']))
        )
        pairs_chunk = pairs_chunk[age_overlap]

        if len(pairs_chunk) == 0:
            continue

        # Filter 3: Temporal (found >= last seen, with 7 day tolerance)
        temporal_valid = (
            pairs_chunk['found_days'].isna() |
            pairs_chunk['last_seen_days'].isna() |
            (pairs_chunk['found_days'] >= pairs_chunk['last_seen_days'] - 7)
        )
        pairs_chunk = pairs_chunk[temporal_valid]

        if len(pairs_chunk) == 0:
            continue

        # Filter 4: Same state (critical for reducing pairs to manageable size)
        pairs_chunk['state_mp_norm'] = pairs_chunk['state_mp'].str.strip().str.upper()
        pairs_chunk['state_up_norm'] = pairs_chunk['state_up'].str.strip().str.upper()
        same_state_filter = (
            (pairs_chunk['state_mp_norm'] == pairs_chunk['state_up_norm']) &
            (pairs_chunk['state_mp_norm'] != '') &
            (pairs_chunk['state_mp_norm'].notna())
        )
        pairs_chunk = pairs_chunk[same_state_filter]

        if len(pairs_chunk) == 0:
            continue

        # Filter 5: Race overlap (at least one common race category, or unknown)
        race_overlap_mask = pairs_chunk.apply(
            lambda row: races_overlap(row['race_mp'], row['race_up']), axis=1
        )
        pairs_chunk = pairs_chunk[race_overlap_mask]

        if len(pairs_chunk) == 0:
            continue

        # Calculate days_gap
        pairs_chunk['days_gap'] = pairs_chunk['found_days'] - pairs_chunk['last_seen_days']

        # Calculate geographic matches (same state, county, city)
        pairs_chunk['same_state'] = (
            (pairs_chunk['state_mp'].str.strip().str.upper() ==
             pairs_chunk['state_up'].str.strip().str.upper()) &
            (pairs_chunk['state_mp'].notna()) &
            (pairs_chunk['state_mp'] != '')
        )

        pairs_chunk['same_county'] = (
            (pairs_chunk['county_mp'].str.strip().str.upper() ==
             pairs_chunk['county_up'].str.strip().str.upper()) &
            (pairs_chunk['county_mp'].notna()) &
            (pairs_chunk['county_mp'] != '')
        )

        pairs_chunk['same_city'] = (
            (pairs_chunk['city_mp'].str.strip().str.upper() ==
             pairs_chunk['city_up'].str.strip().str.upper()) &
            (pairs_chunk['city_mp'].notna()) &
            (pairs_chunk['city_mp'] != '')
        )

        # Keep only needed columns
        pairs_final = pairs_chunk[[
            'id_mp', 'id_up', 'days_gap', 'same_state', 'same_county', 'same_city'
        ]].rename(columns={'id_mp': 'mp_id', 'id_up': 'up_id'})

        all_pairs.append(pairs_final)

# Combine all chunks
print("\n4. Combining results...")
if all_pairs:
    pairs_df = pd.concat(all_pairs, ignore_index=True)
    print(f"   Generated {len(pairs_df):,} candidate matches")
    print(f"   Reduction: {100 * (1 - len(pairs_df)/(len(mp)*len(up))):.1f}% filtered out")
else:
    print("   ERROR: No candidate pairs generated!")
    exit(1)

# Save pairs for build_candidates.py
print("\n5. Saving output files...")
pairs_path = os.path.join(OUT_DIR, 'candidate_pairs.csv')
pairs_df.to_csv(pairs_path, index=False)
print(f"   ✓ {pairs_path}")

# Export case JSONs for frontend/display
mp_cases = mp[[
    'id', 'first_name', 'last_name', 'sex', 'race',
    'age_min', 'age_max', 'last_seen_date',
    'city', 'county', 'state'
]].to_dict(orient='records')

up_cases = up[[
    'id', 'mec_case', 'sex', 'race',
    'age_min', 'age_max', 'found_date',
    'city', 'county', 'state'
]].to_dict(orient='records')

mp_json_path = os.path.join(OUT_DIR, 'cases_mp.json')
up_json_path = os.path.join(OUT_DIR, 'cases_up.json')

with open(mp_json_path, 'w', encoding='utf-8') as f:
    json.dump(mp_cases, f, indent=2)
print(f"   ✓ {mp_json_path}")

with open(up_json_path, 'w', encoding='utf-8') as f:
    json.dump(up_cases, f, indent=2)
print(f"   ✓ {up_json_path}")

# Statistics
print("\n" + "="*60)
print("Statistics:")
print(f"  Candidate pairs: {len(pairs_df):,}")
print(f"  Same state: {pairs_df['same_state'].sum():,}")
print(f"  Same county: {pairs_df['same_county'].sum():,}")
print(f"  Same city: {pairs_df['same_city'].sum():,}")
print(f"  Cross-state: {(~pairs_df['same_state']).sum():,}")
print(f"  Avg matches per MP: {len(pairs_df) / len(mp):.1f}")
print(f"  Avg matches per UP: {len(pairs_df) / len(up):.1f}")
print("="*60)

print("\n✓ Done! Next step: python3 data-build/build_candidates_simple.py")
