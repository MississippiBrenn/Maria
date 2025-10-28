#!/usr/bin/env python3
"""
Simple Neptune bulk-load generator that works with available data.
Focuses on state + sex + age matching since geo/temporal data is sparse.
"""

import os
import csv
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.path.join(ROOT, 'out', 'neptune_load')
os.makedirs(OUT_DIR, exist_ok=True)

# State name normalization
STATE_ABBREV = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH',
    'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA',
    'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN',
    'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC'
}

def normalize_state(s):
    """Convert state name to 2-letter abbrev."""
    if pd.isna(s):
        return ''
    s = str(s).upper().strip()
    return STATE_ABBREV.get(s, s)  # Return abbrev if found, else original

print("Loading master CSV files...")
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'), dtype=str, low_memory=False)
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'), dtype=str, low_memory=False)

print(f"Loaded {len(mp)} missing persons and {len(up)} unidentified persons")

# Normalize states
mp['state_norm'] = mp['state'].apply(normalize_state)
up['state_norm'] = up['state'].apply(normalize_state)

# Normalize types
for df in [mp, up]:
    for c in ['age_min', 'age_max', 'height_in', 'weight_lb']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

# Write nodes CSV
nodes_path = os.path.join(OUT_DIR, 'nodes.csv')
print(f"Writing nodes to {nodes_path}...")

with open(nodes_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow([
        '~id', '~label', 'namus_number', 'sex', 'race',
        'age_min:int', 'age_max:int',
        'height_in:int', 'weight_lb:int',
        'city', 'state', 'county'
    ])

    for _, r in mp.iterrows():
        w.writerow([
            f"MP-{r['namus_number']}",
            'MissingPerson',
            r.get('namus_number', ''),
            r.get('sex', ''),
            r.get('race', ''),
            int(r['age_min']) if pd.notna(r.get('age_min')) else '',
            int(r['age_max']) if pd.notna(r.get('age_max')) else '',
            int(r['height_in']) if pd.notna(r.get('height_in')) else '',
            int(r['weight_lb']) if pd.notna(r.get('weight_lb')) else '',
            r.get('city', ''),
            r.get('state_norm', ''),
            r.get('county', '')
        ])

    for _, r in up.iterrows():
        w.writerow([
            f"UID-{r['namus_number']}",
            'UnidentifiedPerson',
            r.get('namus_number', ''),
            r.get('sex', ''),
            r.get('race', ''),
            int(r['age_min']) if pd.notna(r.get('age_min')) else '',
            int(r['age_max']) if pd.notna(r.get('age_max')) else '',
            int(r['height_in']) if pd.notna(r.get('height_in')) else '',
            int(r['weight_lb']) if pd.notna(r.get('weight_lb')) else '',
            r.get('city', ''),
            r.get('state_norm', ''),
            r.get('county', '')
        ])

print(f"Wrote {len(mp) + len(up)} nodes")

# Generate edges - simplified matching
edges_path = os.path.join(OUT_DIR, 'edges.csv')
print(f"Generating edges (state + sex + age matching)...")

# Build index by state/sex
from collections import defaultdict
uid_index = defaultdict(list)
for idx, r in up.iterrows():
    state = r['state_norm']
    sex = str(r.get('sex', '')).upper().strip()[:1] if pd.notna(r.get('sex')) else ''
    uid_index[(state, sex)].append(idx)

edge_count = 0
with open(edges_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['~id', '~from', '~to', '~label', 'match_type'])

    for i, mp_r in mp.iterrows():
        if i % 1000 == 0:
            print(f"  Processing MP {i}/{len(mp)}... ({edge_count} edges)")

        mp_state = mp_r['state_norm']
        mp_sex = str(mp_r.get('sex', '')).upper().strip()[:1] if pd.notna(mp_r.get('sex')) else ''

        candidates = uid_index.get((mp_state, mp_sex), [])
        if not candidates:
            continue

        for uid_idx in candidates:
            up_r = up.iloc[uid_idx]

            # Age overlap check (if both have age data)
            has_age_data = (pd.notna(mp_r.get('age_min')) or pd.notna(mp_r.get('age_max'))) and \
                          (pd.notna(up_r.get('age_min')) or pd.notna(up_r.get('age_max')))

            if has_age_data:
                mp_min = float(mp_r['age_min']) if pd.notna(mp_r.get('age_min')) else 0
                mp_max = float(mp_r['age_max']) if pd.notna(mp_r.get('age_max')) else 150
                up_min = float(up_r['age_min']) if pd.notna(up_r.get('age_min')) else 0
                up_max = float(up_r['age_max']) if pd.notna(up_r.get('age_max')) else 150

                # Check if ranges overlap
                if mp_max < up_min or up_max < mp_min:
                    continue  # No overlap

            mp_id = f"MP-{mp_r['namus_number']}"
            up_id = f"UID-{up_r['namus_number']}"
            edge_id = f"edge-{mp_r['namus_number']}-{up_r['namus_number']}"
            w.writerow([edge_id, mp_id, up_id, 'CANDIDATE_MATCH', 'state_sex_age'])
            edge_count += 1

print(f"\nWrote {edge_count:,} edges")
print(f"\nDone! Neptune bulk-load files generated:")
print(f"  - {nodes_path}")
print(f"  - {edges_path}")
print(f"\nStatistics:")
print(f"  - Nodes: {len(mp) + len(up):,}")
print(f"  - Edges: {edge_count:,}")
print(f"  - Avg edges per MP: {edge_count/len(mp):.1f}")
print(f"\nNext: Run neptune_bulkload.py to upload to AWS")
