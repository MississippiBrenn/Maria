#!/usr/bin/env python3
"""
Build case JSON files and match candidates from simplified NamUs data.
Works with limited fields: sex, age, race, location (city/county/state), dates.
"""

import os
import json
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))
os.makedirs(OUT_DIR, exist_ok=True)

# Load master CSVs
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

# Normalize dates → ordinal days for temporal calculations
mp['last_seen_days'] = pd.to_datetime(mp['last_seen_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)
up['found_days'] = pd.to_datetime(up['found_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)

print(f"Loaded {len(mp)} missing persons and {len(up)} unidentified persons")

# Generate candidate pairs with basic blocking filters
print("Generating candidate matches...")
pairs = []
for _, m in mp.iterrows():
    mp_sex = str(m['sex']).strip()
    mp_state = str(m['state']).strip().upper()

    for _, u in up.iterrows():
        up_sex = str(u['sex']).strip()
        up_state = str(u['state']).strip().upper()

        # Hard filter 1: Sex must match (or one is Unknown)
        if mp_sex != up_sex and mp_sex != "Unknown" and up_sex != "Unknown":
            continue

        # Hard filter 2: Must be in same state
        if mp_state != up_state:
            continue

        # Hard filter 3: Age ranges must overlap
        if pd.notna(m['age_min']) and pd.notna(m['age_max']) and \
           pd.notna(u['age_min']) and pd.notna(u['age_max']):
            if not (m['age_min'] <= u['age_max'] and u['age_min'] <= m['age_max']):
                continue

        # Hard filter 4: Found date must be >= last seen date (if both exist)
        if pd.notna(m['last_seen_days']) and pd.notna(u['found_days']):
            if u['found_days'] < m['last_seen_days']:
                continue

        # Calculate days between last seen and found
        days_gap = None
        if pd.notna(m['last_seen_days']) and pd.notna(u['found_days']):
            days_gap = int(u['found_days'] - m['last_seen_days'])

        # Geographic match score
        same_county = (str(m.get('county', '')).strip().upper() ==
                       str(u.get('county', '')).strip().upper() and
                       str(m.get('county', '')).strip() != '')
        same_city = (str(m.get('city', '')).strip().upper() ==
                     str(u.get('city', '')).strip().upper() and
                     str(m.get('city', '')).strip() != '')

        pairs.append({
            'mp_id': m['id'],
            'up_id': u['id'],
            'days_gap': days_gap,
            'same_county': same_county,
            'same_city': same_city,
        })

print(f"Generated {len(pairs):,} candidate matches")

# Save pairs for build_candidates.py
pairs_df = pd.DataFrame(pairs)
pairs_path = os.path.join(OUT_DIR, 'candidate_pairs.csv')
pairs_df.to_csv(pairs_path, index=False)
print(f"Wrote: {pairs_path}")

# Export case JSONs for frontend/display
print("Exporting case data...")
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
print(f"Wrote: {mp_json_path}")

with open(up_json_path, 'w', encoding='utf-8') as f:
    json.dump(up_cases, f, indent=2)
print(f"Wrote: {up_json_path}")

print("\n✓ Done! Next step: python3 data-build/build_candidates.py")
