#!/usr/bin/env python3
"""
OPTIMIZED version: Build case JSON files and match candidates from NamUs data.
Uses vectorized operations and smart indexing to avoid nested loops.
"""

import os
import json
import pandas as pd
from tqdm import tqdm
from state_normalizer import normalize_state

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

# OPTIMIZATION 1: Group by state first (most selective filter)
print("\n2. Grouping by state (hard filter #1)...")
mp['state_clean'] = mp['state'].apply(normalize_state)
up['state_clean'] = up['state'].apply(normalize_state)

# Get unique states that exist in BOTH datasets
common_states = set(mp['state_clean'].unique()).intersection(set(up['state_clean'].unique()))
common_states = [s for s in common_states if s and s != '']

print(f"   Found {len(common_states)} states with both MPs and UPs")

# OPTIMIZATION 2: Process state-by-state (much smaller chunks)
print("\n3. Generating candidate pairs by state...")
all_pairs = []

for state in tqdm(sorted(common_states), desc="Processing states"):
    # Filter to just this state
    mp_state = mp[mp['state_clean'] == state].copy()
    up_state = up[up['state_clean'] == state].copy()

    if len(mp_state) == 0 or len(up_state) == 0:
        continue

    # OPTIMIZATION 3: Use cross merge for Cartesian product (faster than nested loops)
    mp_state['_merge_key'] = 1
    up_state['_merge_key'] = 1

    # Cross join
    pairs_state = mp_state[['id', 'sex', 'age_min', 'age_max', 'last_seen_days',
                             'county', 'city', '_merge_key']].merge(
        up_state[['id', 'sex', 'age_min', 'age_max', 'found_days',
                  'county', 'city', '_merge_key']],
        on='_merge_key',
        suffixes=('_mp', '_up')
    )

    # OPTIMIZATION 4: Vectorized filtering (much faster than row-by-row)

    # Filter 1: Sex match (or one Unknown)
    sex_match = (
        (pairs_state['sex_mp'] == pairs_state['sex_up']) |
        (pairs_state['sex_mp'] == 'Unknown') |
        (pairs_state['sex_up'] == 'Unknown')
    )
    pairs_state = pairs_state[sex_match]

    if len(pairs_state) == 0:
        continue

    # Filter 2: Age range overlap (allow NaN ages - will be scored lower)
    age_overlap = (
        pairs_state['age_min_mp'].isna() |
        pairs_state['age_max_up'].isna() |
        pairs_state['age_min_up'].isna() |
        pairs_state['age_max_mp'].isna() |
        ((pairs_state['age_min_mp'] <= pairs_state['age_max_up']) &
         (pairs_state['age_min_up'] <= pairs_state['age_max_mp']))
    )
    pairs_state = pairs_state[age_overlap]

    if len(pairs_state) == 0:
        continue

    # Filter 3: Temporal (found >= last seen, with 7 day tolerance)
    temporal_valid = (
        pairs_state['found_days'].isna() |
        pairs_state['last_seen_days'].isna() |
        (pairs_state['found_days'] >= pairs_state['last_seen_days'] - 7)
    )
    pairs_state = pairs_state[temporal_valid]

    if len(pairs_state) == 0:
        continue

    # Calculate days_gap
    pairs_state['days_gap'] = pairs_state['found_days'] - pairs_state['last_seen_days']

    # Calculate geographic matches
    pairs_state['same_county'] = (
        (pairs_state['county_mp'].str.strip().str.upper() ==
         pairs_state['county_up'].str.strip().str.upper()) &
        (pairs_state['county_mp'].notna()) &
        (pairs_state['county_mp'] != '')
    )

    pairs_state['same_city'] = (
        (pairs_state['city_mp'].str.strip().str.upper() ==
         pairs_state['city_up'].str.strip().str.upper()) &
        (pairs_state['city_mp'].notna()) &
        (pairs_state['city_mp'] != '')
    )

    # Keep only needed columns
    pairs_final = pairs_state[[
        'id_mp', 'id_up', 'days_gap', 'same_county', 'same_city'
    ]].rename(columns={'id_mp': 'mp_id', 'id_up': 'up_id'})

    all_pairs.append(pairs_final)

# Combine all states
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
print(f"  States processed: {len(common_states)}")
print(f"  Candidate pairs: {len(pairs_df):,}")
print(f"  Same county: {pairs_df['same_county'].sum():,}")
print(f"  Same city: {pairs_df['same_city'].sum():,}")
print(f"  Avg matches per MP: {len(pairs_df) / len(mp):.1f}")
print(f"  Avg matches per UP: {len(pairs_df) / len(up):.1f}")
print("="*60)

print("\n✓ Done! Next step: python3 data-build/build_candidates_simple.py")
