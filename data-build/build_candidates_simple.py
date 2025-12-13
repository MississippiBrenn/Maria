#!/usr/bin/env python3
"""
Score all candidate matches and rank by uniqueness + score.

Key insight: If an MP only matches a few UPs (or vice versa),
those matches are much more likely to be correct.
"""

import os
import json
import pandas as pd
from scoring_simple import score_match
from rarity_scoring import add_rarity_scores_to_matches
from county_adjacency import add_adjacency_to_pairs

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))

print("Loading data...")
mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))
pairs = pd.read_csv(os.path.join(OUT_DIR, 'candidate_pairs.csv'))

# Convert dates to ordinal days
mp['last_seen_days'] = pd.to_datetime(mp['last_seen_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)
up['found_days'] = pd.to_datetime(up['found_date'], errors='coerce').dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)

print(f"Scoring {len(pairs):,} candidate matches...")

# Score each pair
scored = []
for _, pair in pairs.iterrows():
    mp_row = mp[mp['id'] == pair['mp_id']].iloc[0]
    up_row = up[up['id'] == pair['up_id']].iloc[0]

    mp_dict = {
        'sex': mp_row['sex'],
        'age_min': mp_row['age_min'],
        'age_max': mp_row['age_max'],
        'state': mp_row['state'],
        'county': mp_row.get('county'),
        'city': mp_row.get('city'),
        'race': mp_row.get('race'),
        'last_seen_days': mp_row['last_seen_days'],
    }

    up_dict = {
        'sex': up_row['sex'],
        'age_min': up_row['age_min'],
        'age_max': up_row['age_max'],
        'state': up_row['state'],
        'county': up_row.get('county'),
        'city': up_row.get('city'),
        'race': up_row.get('race'),
        'found_days': up_row['found_days'],
    }

    score, components = score_match(mp_dict, up_dict)

    if score is None:
        continue  # Rejected

    scored.append({
        'mp_id': pair['mp_id'],
        'up_id': pair['up_id'],
        'base_score': score,
        'components': components,
        'mp_name': f"{mp_row.get('first_name', '')} {mp_row.get('last_name', '')}".strip(),
        'up_mec': up_row.get('mec_case', ''),
        'mp_state': mp_row['state'],
        'up_state': up_row['state'],
        'days_gap': pair.get('days_gap'),
    })

scored_df = pd.DataFrame(scored)
print(f"Valid matches after scoring: {len(scored_df):,}")

# UNIQUENESS SCORING - Your key insight!
print("\nCalculating uniqueness scores...")

# Count matches per MP and UP
mp_match_counts = scored_df.groupby('mp_id').size().to_dict()
up_match_counts = scored_df.groupby('up_id').size().to_dict()

print(f"  MP match counts - min: {min(mp_match_counts.values())}, max: {max(mp_match_counts.values())}, avg: {sum(mp_match_counts.values())/len(mp_match_counts):.1f}")
print(f"  UP match counts - min: {min(up_match_counts.values())}, max: {max(up_match_counts.values())}, avg: {sum(up_match_counts.values())/len(up_match_counts):.1f}")

# Add uniqueness boost
def calculate_uniqueness_boost(mp_count, up_count):
    """
    Boost score if either MP or UP has very few matches.

    - If MP only matches 1-3 UPs: +0.15
    - If UP only matches 1-3 MPs: +0.15
    - If both are unique: +0.25 (cumulative)
    """
    boost = 0.0

    if mp_count <= 3:
        boost += 0.15
    elif mp_count <= 10:
        boost += 0.08

    if up_count <= 3:
        boost += 0.15
    elif up_count <= 10:
        boost += 0.08

    return boost

scored_df['mp_match_count'] = scored_df['mp_id'].map(mp_match_counts)
scored_df['up_match_count'] = scored_df['up_id'].map(up_match_counts)
scored_df['uniqueness_boost'] = scored_df.apply(
    lambda r: calculate_uniqueness_boost(r['mp_match_count'], r['up_match_count']),
    axis=1
)

# Add rarity scoring
print("\n" + "="*50)
scored_df = add_rarity_scores_to_matches(scored_df, mp, up)

# Final score = base_score + uniqueness_boost + rarity_boost (capped at 1.0)
scored_df['final_score'] = (scored_df['base_score'] + scored_df['uniqueness_boost'] + scored_df['rarity_boost']).clip(upper=1.0)

# Sort by final score
scored_df = scored_df.sort_values('final_score', ascending=False)

print(f"\n" + "="*50)
print(f"Top 10 matches:")
for i, row in scored_df.head(10).iterrows():
    print(f"  {row['mp_id']} ({row['mp_name']}) <-> {row['up_id']} ({row['up_mec']})")
    print(f"    Final Score: {row['final_score']:.3f}")
    print(f"      Base: {row['base_score']:.3f}")
    print(f"      Uniqueness: +{row['uniqueness_boost']:.3f} (MP: {row['mp_match_count']} matches, UP: {row['up_match_count']} matches)")
    print(f"      Rarity: +{row['rarity_boost']:.3f} (MP rarity: {row['mp_rarity']:.2f}, UP rarity: {row['up_rarity']:.2f})")
    print()

# Save full results
full_output = os.path.join(OUT_DIR, 'all_matches_scored.csv')
scored_df.to_csv(full_output, index=False)
print(f"Wrote all matches to: {full_output}")

# Save top matches per MP (for review)
top_per_mp = {}
for mp_id in scored_df['mp_id'].unique():
    matches = scored_df[scored_df['mp_id'] == mp_id].head(20)
    top_per_mp[mp_id] = matches[[
        'up_id', 'final_score', 'base_score', 'uniqueness_boost',
        'mp_match_count', 'up_match_count', 'days_gap'
    ]].to_dict(orient='records')

jsonl_output = os.path.join(OUT_DIR, 'candidates.jsonl')
with open(jsonl_output, 'w', encoding='utf-8') as f:
    for mp_id, candidates in top_per_mp.items():
        f.write(json.dumps({'mp_id': mp_id, 'candidates': candidates}) + '\n')

print(f"Wrote top 20 per MP to: {jsonl_output}")

# Save HIGH PRIORITY matches (uniqueness boost >= 0.15)
high_priority = scored_df[scored_df['uniqueness_boost'] >= 0.15].copy()
high_priority_output = os.path.join(OUT_DIR, 'high_priority_matches.csv')
high_priority.to_csv(high_priority_output, index=False)
print(f"\nHIGH PRIORITY: {len(high_priority)} matches with uniqueness boost >= 0.15")
print(f"Wrote to: {high_priority_output}")

print("\n✓ Done!")
