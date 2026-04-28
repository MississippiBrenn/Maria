#!/usr/bin/env python3
"""
Prioritize candidate matches for manual review.

Combines multiple scoring factors to produce a ranked list optimized for
manual review efficiency - putting the most promising matches first.

Scoring factors:
1. Priority tier (geography - same city deprioritized)
2. Match count (fewer matches = more specific = higher priority)
3. Rarity score (rare demographics matching = higher confidence)
4. Temporal proximity (closer in time = higher priority)
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))


def calculate_rarity_score(row, state_counts, race_counts, total_count):
    """Calculate how rare this case's demographics are."""
    rarity = 0.0
    components = 0

    # State rarity
    state = row.get('state', '')
    if state and state in state_counts:
        state_freq = state_counts[state] / total_count
        rarity += (1 - state_freq)
        components += 1

    # Race rarity
    race = row.get('race', '')
    if race and race in race_counts:
        race_freq = race_counts[race] / total_count
        rarity += (1 - race_freq)
        components += 1

    return rarity / components if components > 0 else 0.5


def calculate_temporal_score(days_gap):
    """Score based on time between last seen and found."""
    if pd.isna(days_gap) or days_gap is None:
        return 0.5

    days = float(days_gap)
    if days < 0:
        return 0.1  # Found before last seen (unlikely match)
    if days <= 30:
        return 1.0
    if days <= 180:
        return 0.8
    if days <= 365:
        return 0.6
    if days <= 1825:  # 5 years
        return 0.4
    return 0.2


def main():
    print("=" * 60)
    print("Match Prioritization")
    print("=" * 60)

    # Load data
    print("\n1. Loading data...")
    pairs_path = os.path.join(OUT_DIR, 'candidate_pairs.csv')
    if not os.path.exists(pairs_path):
        print(f"   ERROR: {pairs_path} not found. Run generate_candidate_pairs_v2.py first.")
        return

    pairs_df = pd.read_csv(pairs_path)
    mp_df = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up_df = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    print(f"   Loaded {len(pairs_df):,} candidate pairs")

    # Calculate match counts (fewer = more specific = higher priority)
    print("\n2. Calculating match counts...")
    up_match_counts = pairs_df['up_id'].value_counts().to_dict()
    mp_match_counts = pairs_df['mp_id'].value_counts().to_dict()

    pairs_df['up_match_count'] = pairs_df['up_id'].map(up_match_counts)
    pairs_df['mp_match_count'] = pairs_df['mp_id'].map(mp_match_counts)

    # Calculate rarity scores
    print("\n3. Calculating rarity scores...")
    mp_state_counts = mp_df['state'].value_counts().to_dict()
    mp_race_counts = mp_df['race'].value_counts().to_dict()
    up_state_counts = up_df['state'].value_counts().to_dict()
    up_race_counts = up_df['race'].value_counts().to_dict()

    # Create lookups
    mp_lookup = mp_df.set_index('id').to_dict('index')
    up_lookup = up_df.set_index('id').to_dict('index')

    mp_rarity = {}
    for mp_id, data in mp_lookup.items():
        mp_rarity[mp_id] = calculate_rarity_score(data, mp_state_counts, mp_race_counts, len(mp_df))

    up_rarity = {}
    for up_id, data in up_lookup.items():
        up_rarity[up_id] = calculate_rarity_score(data, up_state_counts, up_race_counts, len(up_df))

    pairs_df['mp_rarity'] = pairs_df['mp_id'].map(mp_rarity).fillna(0.5)
    pairs_df['up_rarity'] = pairs_df['up_id'].map(up_rarity).fillna(0.5)
    pairs_df['combined_rarity'] = (pairs_df['mp_rarity'] + pairs_df['up_rarity']) / 2

    # Calculate temporal score
    print("\n4. Calculating temporal scores...")
    pairs_df['temporal_score'] = pairs_df['days_gap'].apply(calculate_temporal_score)

    # Calculate composite priority score
    print("\n5. Calculating composite priority scores...")

    # Normalize match counts (inverse - fewer matches = higher score)
    max_up_matches = pairs_df['up_match_count'].max()
    max_mp_matches = pairs_df['mp_match_count'].max()

    pairs_df['match_specificity'] = 1 - (
        (pairs_df['up_match_count'] / max_up_matches) +
        (pairs_df['mp_match_count'] / max_mp_matches)
    ) / 2

    # Convert priority tier to score (tier 1 = best)
    tier_scores = {1: 1.0, 2: 0.7, 3: 0.4, 4: 0.1}
    pairs_df['tier_score'] = pairs_df['priority_tier'].map(tier_scores).fillna(0.5)

    # Composite score (weighted)
    # Weights based on what we learned from manual review
    pairs_df['priority_score'] = (
        pairs_df['tier_score'] * 0.30 +           # Geography tier (30%)
        pairs_df['match_specificity'] * 0.30 +    # Few matches = specific (30%)
        pairs_df['combined_rarity'] * 0.20 +      # Rare demographics (20%)
        pairs_df['temporal_score'] * 0.20         # Time proximity (20%)
    )

    # Sort by priority score
    pairs_df = pairs_df.sort_values('priority_score', ascending=False)

    # Save prioritized output
    print("\n6. Saving prioritized outputs...")

    # Full prioritized list
    output_cols = [
        'mp_id', 'up_id', 'priority_score', 'priority_tier', 'tier_reason',
        'up_match_count', 'mp_match_count', 'match_specificity',
        'combined_rarity', 'temporal_score', 'days_gap',
        'same_state', 'same_county', 'same_city'
    ]

    prioritized_path = os.path.join(OUT_DIR, 'candidate_pairs_prioritized.csv')
    pairs_df[output_cols].to_csv(prioritized_path, index=False)
    print(f"   Full prioritized: {prioritized_path}")

    # Top matches for review (top 1000 by priority score)
    top_matches = pairs_df.head(1000)
    top_path = os.path.join(OUT_DIR, 'top_matches_for_review.csv')
    top_matches[output_cols].to_csv(top_path, index=False)
    print(f"   Top 1000 matches: {top_path}")

    # Group by UP for structured review (UPs with few, high-quality matches)
    print("\n7. Creating review-friendly grouped output...")

    # Find UPs with 1-10 matches and good priority scores
    good_ups = pairs_df[
        (pairs_df['up_match_count'] <= 10) &
        (pairs_df['priority_tier'] <= 2)
    ].copy()

    if len(good_ups) > 0:
        # Get best match per UP
        best_per_up = good_ups.loc[good_ups.groupby('up_id')['priority_score'].idxmax()]
        best_per_up = best_per_up.sort_values('priority_score', ascending=False)

        best_path = os.path.join(OUT_DIR, 'best_match_per_up.csv')
        best_per_up[output_cols].to_csv(best_path, index=False)
        print(f"   Best match per UP (1-10 matches, Tier 1-2): {best_path} ({len(best_per_up)} UPs)")

    # Statistics
    print("\n" + "=" * 60)
    print("Priority Score Distribution:")
    print(f"  Mean: {pairs_df['priority_score'].mean():.3f}")
    print(f"  Median: {pairs_df['priority_score'].median():.3f}")
    print(f"  Top 10%: >= {pairs_df['priority_score'].quantile(0.9):.3f}")
    print(f"  Top 1%: >= {pairs_df['priority_score'].quantile(0.99):.3f}")

    print("\nMatch Count Distribution:")
    print(f"  UPs with 1 match: {(pairs_df.groupby('up_id').size() == 1).sum():,}")
    print(f"  UPs with 2-5 matches: {((pairs_df.groupby('up_id').size() >= 2) & (pairs_df.groupby('up_id').size() <= 5)).sum():,}")
    print(f"  UPs with 6-10 matches: {((pairs_df.groupby('up_id').size() >= 6) & (pairs_df.groupby('up_id').size() <= 10)).sum():,}")
    print(f"  UPs with 10+ matches: {(pairs_df.groupby('up_id').size() > 10).sum():,}")

    print("\nTier Distribution:")
    for tier in sorted(pairs_df['priority_tier'].unique()):
        count = (pairs_df['priority_tier'] == tier).sum()
        pct = 100 * count / len(pairs_df)
        print(f"  Tier {tier}: {count:,} ({pct:.1f}%)")

    print("=" * 60)
    print("\nRecommended review order:")
    print("  1. best_match_per_up.csv - UPs with few matches, high priority")
    print("  2. top_matches_for_review.csv - Top 1000 pairs overall")
    print("  3. candidate_pairs_tier1.csv - All Tier 1 matches")


if __name__ == '__main__':
    main()
