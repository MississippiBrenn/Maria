#!/usr/bin/env python3
"""
VECTORIZED scoring: Score all candidate matches using pandas vectorization.
Much faster than row-by-row iteration.
"""

import os
import json
import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))

print("="*60)
print("VECTORIZED Candidate Scoring")
print("="*60)

# Load data
print("\n1. Loading data...")
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))
pairs = pd.read_csv(os.path.join(OUT_DIR, 'candidate_pairs.csv'))

print(f"   {len(mp):,} MPs, {len(up):,} UPs")
print(f"   {len(pairs):,} candidate pairs to score")

# Convert dates
mp['last_seen_days'] = pd.to_datetime(mp['last_seen_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)
up['found_days'] = pd.to_datetime(up['found_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)

# Merge MP and UP data into pairs
print("\n2. Merging case data with pairs...")
pairs = pairs.merge(
    mp[['id', 'sex', 'age_min', 'race', 'state', 'county', 'city', 'last_seen_days', 'first_name', 'last_name']].rename(
        columns={'sex': 'sex_mp', 'age_min': 'age_min_mp', 'race': 'race_mp',
                 'state': 'state_mp', 'county': 'county_mp', 'city': 'city_mp'}
    ),
    left_on='mp_id', right_on='id'
).drop(columns=['id'])

pairs = pairs.merge(
    up[['id', 'sex', 'age_min', 'age_max', 'race', 'state', 'county', 'city', 'found_days', 'mec_case']].rename(
        columns={'sex': 'sex_up', 'age_min': 'age_min_up', 'age_max': 'age_max_up', 'race': 'race_up',
                 'state': 'state_up', 'county': 'county_up', 'city': 'city_up'}
    ),
    left_on='up_id', right_on='id'
).drop(columns=['id'])

print(f"   Merged data ready: {len(pairs):,} rows")

# VECTORIZED SCORING
print("\n3. Computing scores (vectorized)...")

# Calculate temporal gap in days and years
pairs['days_gap'] = pairs['found_days'] - pairs['last_seen_days']
pairs['years_between'] = pairs['days_gap'] / 365.25

# Component 1: Sex Match Score
def vec_sex_score(mp_sex, up_sex):
    result = np.where(mp_sex == up_sex, 1.0, 0.0)
    result = np.where((mp_sex == 'Unknown') | (up_sex == 'Unknown'), 0.5, result)
    return result

pairs['sex_score'] = vec_sex_score(pairs['sex_mp'], pairs['sex_up'])

# Component 2: Age Similarity (with projection)
def vec_age_score(mp_age, up_min, up_max, years_between):
    # Project MP age forward
    mp_projected = np.where(
        pd.notna(years_between) & (years_between >= 0),
        mp_age + years_between,
        mp_age
    )

    # MP range (±2 years)
    mp_min = mp_projected - 2
    mp_max = mp_projected + 2

    # Calculate overlap
    overlap_start = np.maximum(mp_min, up_min)
    overlap_end = np.minimum(mp_max, up_max)
    overlap = np.maximum(0, overlap_end - overlap_start + 1)

    # Normalize by range size
    mp_range = mp_max - mp_min + 1
    up_range = up_max - up_min + 1
    avg_range = (mp_range + up_range) / 2.0

    score = np.minimum(1.0, overlap / avg_range)

    # Handle no overlap with exponential decay
    gap = np.minimum(np.abs(mp_min - up_max), np.abs(up_min - mp_max))
    no_overlap_score = np.maximum(0, 0.5 * np.exp(-gap / 5.0))

    result = np.where(overlap > 0, score, no_overlap_score)

    # Default to 0.5 if data missing
    result = np.where(pd.isna(mp_age) | pd.isna(up_min) | pd.isna(up_max), 0.5, result)

    return result

pairs['age_score'] = vec_age_score(
    pairs['age_min_mp'], pairs['age_min_up'], pairs['age_max_up'], pairs['years_between']
)

# Component 3: Geographic Score
def vec_geo_score(mp_state, mp_county, mp_city, up_state, up_county, up_city):
    mp_state = mp_state.str.strip().str.upper()
    up_state = up_state.str.strip().str.upper()
    mp_county = mp_county.str.strip().str.upper()
    up_county = up_county.str.strip().str.upper()
    mp_city = mp_city.str.strip().str.upper()
    up_city = up_city.str.strip().str.upper()

    # Same city + county
    same_city_county = (mp_state == up_state) & (mp_county == up_county) & (mp_city == up_city) & (mp_city != '')

    # Same county
    same_county = (mp_state == up_state) & (mp_county == up_county) & (mp_county != '')

    # Same state
    same_state = (mp_state == up_state) & (mp_state != '')

    score = np.where(same_city_county, 1.0,
            np.where(same_county, 0.85,
            np.where(same_state, 0.3, 0.0)))

    return score

pairs['geo_score'] = vec_geo_score(
    pairs['state_mp'], pairs['county_mp'], pairs['city_mp'],
    pairs['state_up'], pairs['county_up'], pairs['city_up']
)

# Component 4: Race Match
def vec_race_score(mp_race, up_race):
    mp_race = mp_race.str.strip().str.upper()
    up_race = up_race.str.strip().str.upper()

    result = np.where(mp_race == up_race, 1.0, 0.3)
    result = np.where((mp_race == '') | (up_race == ''), 0.5, result)

    return result

pairs['race_score'] = vec_race_score(pairs['race_mp'], pairs['race_up'])

# Component 5: Temporal Score
def vec_temporal_score(days_gap):
    result = np.where(days_gap <= 30, 1.0,
            np.where(days_gap <= 180, 0.8,
            np.where(days_gap <= 365, 0.6,
            np.where(days_gap <= 1825, 0.4,
            0.4 * np.exp(-(days_gap - 1825) / 3650)))))

    result = np.where(pd.isna(days_gap), 0.5, result)

    return result

pairs['temporal_score'] = vec_temporal_score(pairs['days_gap'])

# Weighted average
print("\n4. Computing weighted scores...")
weights = {'sex': 2.0, 'age': 1.5, 'geography': 2.0, 'race': 0.8, 'temporal': 1.2}

pairs['weighted_sum'] = (
    pairs['sex_score'] * weights['sex'] +
    pairs['age_score'] * weights['age'] +
    pairs['geo_score'] * weights['geography'] +
    pairs['race_score'] * weights['race'] +
    pairs['temporal_score'] * weights['temporal']
)

total_weight = sum(weights.values())
pairs['base_score'] = (pairs['weighted_sum'] / total_weight).clip(0, 1)

# Uniqueness boost
print("\n5. Calculating uniqueness boost...")
mp_match_counts = pairs.groupby('mp_id').size()
up_match_counts = pairs.groupby('up_id').size()

pairs['mp_match_count'] = pairs['mp_id'].map(mp_match_counts)
pairs['up_match_count'] = pairs['up_id'].map(up_match_counts)

# HARD FILTER: Only keep matches where BOTH sides have ≤10 candidates
print(f"   Before uniqueness filter: {len(pairs):,} pairs")
pairs = pairs[(pairs['mp_match_count'] <= 10) & (pairs['up_match_count'] <= 10)]
print(f"   After uniqueness filter: {len(pairs):,} pairs (both sides ≤10 matches)")

def vec_uniqueness_boost(mp_count, up_count):
    # Much more aggressive boost for truly unique matches
    boost = np.zeros(len(mp_count))
    # MP side: 1-2 matches = 0.25, 3-5 = 0.15, 6-10 = 0.08
    boost += np.where(mp_count <= 2, 0.25, np.where(mp_count <= 5, 0.15, 0.08))
    # UP side: 1-2 matches = 0.25, 3-5 = 0.15, 6-10 = 0.08
    boost += np.where(up_count <= 2, 0.25, np.where(up_count <= 5, 0.15, 0.08))
    return boost

pairs['uniqueness_boost'] = vec_uniqueness_boost(pairs['mp_match_count'], pairs['up_match_count'])

# Rarity boost
print("\n6. Calculating rarity boost...")

from rarity_scoring import calculate_rarity_scores

mp_rarity = calculate_rarity_scores(mp, 'MP')
up_rarity = calculate_rarity_scores(up, 'UP')

pairs['mp_rarity'] = pairs['mp_id'].map(mp_rarity).fillna(0)
pairs['up_rarity'] = pairs['up_id'].map(up_rarity).fillna(0)

def vec_rarity_boost(mp_rarity, up_rarity):
    # Both rare
    both_rare = (mp_rarity > 0.5) & (up_rarity > 0.5)
    one_rare = ((mp_rarity > 0.5) | (up_rarity > 0.5)) & ~both_rare
    somewhat_rare = ((mp_rarity > 0.3) & (up_rarity > 0.3)) & ~both_rare & ~one_rare

    boost = np.where(both_rare, 0.20,
            np.where(one_rare, 0.10,
            np.where(somewhat_rare, 0.05, 0.0)))

    return boost

pairs['rarity_boost'] = vec_rarity_boost(pairs['mp_rarity'], pairs['up_rarity'])

# Final score
pairs['final_score'] = (pairs['base_score'] + pairs['uniqueness_boost'] + pairs['rarity_boost']).clip(0, 1)

# Sort by score
pairs = pairs.sort_values('final_score', ascending=False)

print(f"\n" + "="*60)
print("Top 10 Matches:")
print("="*60)

pairs['mp_name'] = pairs['first_name'] + ' ' + pairs['last_name']

for i, row in pairs.head(10).iterrows():
    print(f"\n{row['mp_id']} ({row['mp_name'].strip()}) <-> {row['up_id']} ({row['mec_case']})")
    print(f"  Final Score: {row['final_score']:.3f}")
    print(f"    Base: {row['base_score']:.3f}")
    print(f"    Uniqueness: +{row['uniqueness_boost']:.3f} (MP: {row['mp_match_count']} matches, UP: {row['up_match_count']} matches)")
    print(f"    Rarity: +{row['rarity_boost']:.3f} (MP: {row['mp_rarity']:.2f}, UP: {row['up_rarity']:.2f})")

# Save results
print(f"\n" + "="*60)
print("Saving results...")
print("="*60)

full_output = os.path.join(OUT_DIR, 'all_matches_scored.csv')
pairs.to_csv(full_output, index=False)
print(f"✓ {full_output}")

# High priority matches: both sides have ≤5 matches AND final score ≥ 0.7
high_priority = pairs[
    (pairs['mp_match_count'] <= 5) &
    (pairs['up_match_count'] <= 5) &
    (pairs['final_score'] >= 0.7)
].copy()
high_priority_output = os.path.join(OUT_DIR, 'high_priority_matches.csv')
high_priority.to_csv(high_priority_output, index=False)
print(f"✓ {high_priority_output} ({len(high_priority):,} matches)")

# Top 20 per MP
print("\nGenerating top 20 per MP...")
top_per_mp = {}
for mp_id in tqdm(pairs['mp_id'].unique(), desc="Processing MPs"):
    matches = pairs[pairs['mp_id'] == mp_id].head(20)
    top_per_mp[mp_id] = matches[[
        'up_id', 'final_score', 'base_score', 'uniqueness_boost', 'rarity_boost',
        'mp_match_count', 'up_match_count', 'days_gap'
    ]].to_dict(orient='records')

jsonl_output = os.path.join(OUT_DIR, 'candidates.jsonl')
with open(jsonl_output, 'w', encoding='utf-8') as f:
    for mp_id, candidates in top_per_mp.items():
        f.write(json.dumps({'mp_id': mp_id, 'candidates': candidates}) + '\n')

print(f"✓ {jsonl_output}")

print(f"\n" + "="*60)
print("✓ COMPLETE!")
print(f"Review high_priority_matches.csv for best leads!")
print("="*60)
