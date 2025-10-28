#!/usr/bin/env python3
"""
Generate Neptune bulk-load CSVs from MP_master.csv and UP_master.csv.
Creates nodes and edges in Neptune bulk-loader format.
"""

import os
import csv
import pandas as pd
from math import radians, sin, cos, asin, sqrt

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.path.join(ROOT, 'out', 'neptune_load')
os.makedirs(OUT_DIR, exist_ok=True)

# Parameters
MAX_DISTANCE_KM = 322  # ~200 miles
DATE_WINDOW_DAYS = 730  # 2 years

def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two lat/lon points."""
    if any(pd.isna([lat1, lon1, lat2, lon2])):
        return None
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371.0088 * c

print("Loading master CSV files...")
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'), dtype=str, low_memory=False)
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'), dtype=str, low_memory=False)

print(f"Loaded {len(mp)} missing persons and {len(up)} unidentified persons")

# Normalize types
for df in [mp, up]:
    if 'date_missing' in df.columns:
        df['date_missing'] = pd.to_datetime(df['date_missing'], errors='coerce')
    if 'date_found' in df.columns:
        df['date_found'] = pd.to_datetime(df['date_found'], errors='coerce')
    for c in ['age_min', 'age_max', 'latitude', 'longitude', 'height_in', 'weight_lb']:
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
        'date:string', 'lat:double', 'lon:double',
        'city', 'state', 'country'
    ])

    # Missing persons
    for _, r in mp.iterrows():
        w.writerow([
            f"MP-{r['namus_number']}",  # ~id
            'MissingPerson',  # ~label
            r.get('namus_number', ''),
            r.get('sex', ''),
            r.get('race', ''),
            int(r['age_min']) if pd.notna(r.get('age_min')) else '',
            int(r['age_max']) if pd.notna(r.get('age_max')) else '',
            int(r['height_in']) if pd.notna(r.get('height_in')) else '',
            int(r['weight_lb']) if pd.notna(r.get('weight_lb')) else '',
            r['date_missing'].strftime('%Y-%m-%d') if pd.notna(r.get('date_missing')) else '',
            float(r['latitude']) if pd.notna(r.get('latitude')) else '',
            float(r['longitude']) if pd.notna(r.get('longitude')) else '',
            r.get('city', ''),
            r.get('state', ''),
            r.get('country', '')
        ])

    # Unidentified persons
    for _, r in up.iterrows():
        w.writerow([
            f"UID-{r['namus_number']}",  # ~id
            'UnidentifiedPerson',  # ~label
            r.get('namus_number', ''),
            r.get('sex', ''),
            r.get('race', ''),
            int(r['age_min']) if pd.notna(r.get('age_min')) else '',
            int(r['age_max']) if pd.notna(r.get('age_max')) else '',
            int(r['height_in']) if pd.notna(r.get('height_in')) else '',
            int(r['weight_lb']) if pd.notna(r.get('weight_lb')) else '',
            r['date_found'].strftime('%Y-%m-%d') if pd.notna(r.get('date_found')) else '',
            float(r['latitude']) if pd.notna(r.get('latitude')) else '',
            float(r['longitude']) if pd.notna(r.get('longitude')) else '',
            r.get('city', ''),
            r.get('state', ''),
            r.get('country', '')
        ])

print(f"Wrote {len(mp) + len(up)} nodes")

# Generate edges (candidate pairs)
edges_path = os.path.join(OUT_DIR, 'edges.csv')
print(f"Generating edges to {edges_path}...")
print("This may take a while for large datasets...")

edge_count = 0
with open(edges_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['~from', '~to', '~label', 'km:double', 'days_gap:int'])

    for i, mp_r in mp.iterrows():
        if i % 1000 == 0:
            print(f"  Processing MP {i}/{len(mp)}... ({edge_count} edges so far)")

        for _, up_r in up.iterrows():
            # State filter (hard requirement)
            if pd.notna(mp_r.get('state')) and pd.notna(up_r.get('state')):
                if str(mp_r['state']).upper().strip() != str(up_r['state']).upper().strip():
                    continue

            # Sex filter (if both known)
            if pd.notna(mp_r.get('sex')) and pd.notna(up_r.get('sex')):
                mp_sex = str(mp_r['sex']).upper().strip()[:1]
                up_sex = str(up_r['sex']).upper().strip()[:1]
                if mp_sex and up_sex and mp_sex != up_sex:
                    continue

            # Age overlap check
            if pd.notna(mp_r.get('age_min')) and pd.notna(up_r.get('age_max')):
                if float(mp_r['age_min']) > float(up_r['age_max']):
                    continue
            if pd.notna(mp_r.get('age_max')) and pd.notna(up_r.get('age_min')):
                if float(mp_r['age_max']) < float(up_r['age_min']):
                    continue

            # Geographic distance check
            km = haversine_km(
                mp_r.get('latitude'), mp_r.get('longitude'),
                up_r.get('latitude'), up_r.get('longitude')
            )
            if km and km > MAX_DISTANCE_KM:
                continue

            # Temporal check (found date should be after or near missing date)
            days_gap = None
            if pd.notna(mp_r.get('date_missing')) and pd.notna(up_r.get('date_found')):
                days_gap = (up_r['date_found'] - mp_r['date_missing']).days
                # Skip if found before missing (with small tolerance)
                if days_gap < -7:
                    continue
                # Skip if too far in the future
                if days_gap > DATE_WINDOW_DAYS:
                    continue

            # Write NEAR edge
            mp_id = f"MP-{mp_r['namus_number']}"
            up_id = f"UID-{up_r['namus_number']}"
            w.writerow([
                mp_id,
                up_id,
                'NEAR',
                f"{km:.2f}" if km else '',
                int(days_gap) if days_gap is not None else ''
            ])
            edge_count += 1

print(f"Wrote {edge_count} edges")
print(f"\nDone! Neptune bulk-load files generated:")
print(f"  - {nodes_path}")
print(f"  - {edges_path}")
print(f"\nNext step: Run neptune_bulkload.py to upload to AWS")
