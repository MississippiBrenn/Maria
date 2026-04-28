#!/usr/bin/env python3
"""
IMPROVED Candidate Pair Generation v2

Key improvements based on manual review learnings:
1. Applies exclusions at generation time (infants, partial remains, historical)
2. Creates priority tiers (same-city matches deprioritized)
3. Supports cross-state matching for adjacent states
4. Better scoring integration
"""

import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from state_normalizer import normalize_state
from adjacent_states import are_states_adjacent, get_adjacent_states

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data', 'clean')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))
EXCLUSIONS_PATH = os.path.join(ROOT, 'data', 'exclusions.csv')
os.makedirs(OUT_DIR, exist_ok=True)


def parse_races(race_str):
    """Parse a race string into a set of normalized race categories."""
    if pd.isna(race_str) or str(race_str).strip() == '':
        return set()
    races = set()
    for part in str(race_str).split(','):
        normalized = part.strip().upper()
        if normalized and normalized not in ('UNCERTAIN', 'UNKNOWN', 'OTHER'):
            races.add(normalized)
    return races


def races_overlap(race_mp, race_up):
    """Check if two race strings have any overlapping categories."""
    races_mp = parse_races(race_mp)
    races_up = parse_races(race_up)
    if not races_mp or not races_up:
        return True  # Can't rule it out
    return bool(races_mp & races_up)


def load_exclusions():
    """Load exclusions file and return set of excluded UP IDs."""
    if not os.path.exists(EXCLUSIONS_PATH):
        print(f"   Warning: No exclusions file found at {EXCLUSIONS_PATH}")
        return set()

    try:
        exclusions_df = pd.read_csv(EXCLUSIONS_PATH)
        excluded_ids = set(exclusions_df['id'].astype(str))
        return excluded_ids
    except Exception as e:
        print(f"   Warning: Error loading exclusions: {e}")
        return set()


def calculate_priority_tier(same_state, same_county, same_city, adjacent_state=False):
    """
    Calculate priority tier based on geographic relationship.

    Tier 1 (HIGHEST): Different county within same state - likely NOT already checked
    Tier 2: Adjacent county (heuristic) or adjacent state
    Tier 3: Same county, different city
    Tier 4 (LOWEST): Same city - authorities likely already checked

    Returns: (tier, reason)
    """
    if same_city:
        return 4, "same_city"
    elif same_county:
        return 3, "same_county_diff_city"
    elif adjacent_state:
        return 2, "adjacent_state"
    elif same_state:
        return 1, "same_state_diff_county"
    else:
        return 2, "adjacent_state"  # Cross-state matches


def process_state_pair(mp_group, up_group, is_adjacent_state=False):
    """Process matching between MP and UP groups, returning candidate pairs."""
    if len(mp_group) == 0 or len(up_group) == 0:
        return pd.DataFrame()

    CHUNK_SIZE = 200
    all_pairs = []

    for chunk_start in range(0, len(mp_group), CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, len(mp_group))
        mp_chunk = mp_group.iloc[chunk_start:chunk_end].copy()

        if len(mp_chunk) == 0:
            continue

        # Cross join
        mp_chunk['_merge_key'] = 1
        up_temp = up_group.copy()
        up_temp['_merge_key'] = 1

        pairs_chunk = mp_chunk[['id', 'sex', 'age_min', 'age_max', 'last_seen_days',
                                'county', 'city', 'state', 'race', '_merge_key']].merge(
            up_temp[['id', 'sex', 'age_min', 'age_max', 'found_days',
                    'county', 'city', 'state', 'race', '_merge_key']],
            on='_merge_key',
            suffixes=('_mp', '_up')
        )

        if len(pairs_chunk) == 0:
            continue

        # Age filter
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

        # Temporal filter (found >= last seen - 7 days tolerance)
        temporal_valid = (
            pairs_chunk['found_days'].isna() |
            pairs_chunk['last_seen_days'].isna() |
            (pairs_chunk['found_days'] >= pairs_chunk['last_seen_days'] - 7)
        )
        pairs_chunk = pairs_chunk[temporal_valid]

        if len(pairs_chunk) == 0:
            continue

        # Race filter
        race_overlap_mask = pairs_chunk.apply(
            lambda row: races_overlap(row['race_mp'], row['race_up']), axis=1
        )
        pairs_chunk = pairs_chunk[race_overlap_mask]

        if len(pairs_chunk) == 0:
            continue

        # Calculate metrics
        pairs_chunk['days_gap'] = pairs_chunk['found_days'] - pairs_chunk['last_seen_days']

        pairs_chunk['same_state'] = (
            pairs_chunk['state_mp'].str.strip().str.upper() ==
            pairs_chunk['state_up'].str.strip().str.upper()
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

        # Mark adjacent state matches
        pairs_chunk['adjacent_state'] = is_adjacent_state and not pairs_chunk['same_state'].any()

        # Calculate priority tier
        pairs_chunk[['priority_tier', 'tier_reason']] = pairs_chunk.apply(
            lambda r: pd.Series(calculate_priority_tier(
                r['same_state'], r['same_county'], r['same_city'], is_adjacent_state
            )), axis=1
        )

        pairs_final = pairs_chunk[[
            'id_mp', 'id_up', 'days_gap', 'same_state', 'same_county', 'same_city',
            'priority_tier', 'tier_reason'
        ]].rename(columns={'id_mp': 'mp_id', 'id_up': 'up_id'})

        all_pairs.append(pairs_final)

    if all_pairs:
        return pd.concat(all_pairs, ignore_index=True)
    return pd.DataFrame()


def main():
    print("=" * 60)
    print("IMPROVED Candidate Pair Generation v2")
    print("=" * 60)

    # Load exclusions
    print("\n1. Loading exclusions...")
    excluded_ups = load_exclusions()
    print(f"   Loaded {len(excluded_ups)} excluded UP cases")

    # Load data
    print("\n2. Loading data...")
    mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    # Apply exclusions
    original_up_count = len(up)
    up = up[~up['id'].astype(str).isin(excluded_ups)]
    print(f"   Loaded {len(mp):,} MPs and {len(up):,} UPs")
    print(f"   Excluded {original_up_count - len(up)} UP cases")

    # Convert dates
    mp['last_seen_days'] = pd.to_datetime(mp['last_seen_date'], errors='coerce').dt.date.map(
        lambda d: d.toordinal() if pd.notna(d) else None
    )
    up['found_days'] = pd.to_datetime(up['found_date'], errors='coerce').dt.date.map(
        lambda d: d.toordinal() if pd.notna(d) else None
    )

    # Normalize states
    mp['state_norm'] = mp['state'].apply(normalize_state)
    up['state_norm'] = up['state'].apply(normalize_state)

    # Group by sex
    sex_groups = ['M', 'F', 'Unknown']
    mp_by_sex = {sex: mp[mp['sex'] == sex].copy() for sex in sex_groups}
    up_by_sex = {sex: up[up['sex'] == sex].copy() for sex in sex_groups}

    print(f"\n3. Processing matches...")
    print(f"   MP by sex: M={len(mp_by_sex['M'])}, F={len(mp_by_sex['F'])}, Unknown={len(mp_by_sex['Unknown'])}")
    print(f"   UP by sex: M={len(up_by_sex['M'])}, F={len(up_by_sex['F'])}, Unknown={len(up_by_sex['Unknown'])}")

    sex_matching_rules = [
        ('M', ['M', 'Unknown']),
        ('F', ['F', 'Unknown']),
        ('Unknown', ['M', 'F', 'Unknown'])
    ]

    all_pairs = []

    # Get all unique states
    all_states = set(mp['state_norm'].dropna().unique()) | set(up['state_norm'].dropna().unique())
    all_states = {s for s in all_states if s and len(s) == 2}

    print(f"\n   Found {len(all_states)} states to process")

    for mp_sex, up_sexes in sex_matching_rules:
        mp_sex_group = mp_by_sex[mp_sex]
        if len(mp_sex_group) == 0:
            continue

        up_matches = pd.concat([up_by_sex[s] for s in up_sexes if len(up_by_sex[s]) > 0], ignore_index=True)
        if len(up_matches) == 0:
            continue

        print(f"\n   Processing {mp_sex} MPs ({len(mp_sex_group):,} cases)...")

        # Process same-state matches
        for state in tqdm(all_states, desc=f"   {mp_sex} same-state"):
            mp_state = mp_sex_group[mp_sex_group['state_norm'] == state]
            up_state = up_matches[up_matches['state_norm'] == state]

            if len(mp_state) > 0 and len(up_state) > 0:
                pairs = process_state_pair(mp_state, up_state, is_adjacent_state=False)
                if len(pairs) > 0:
                    all_pairs.append(pairs)

        # Process adjacent-state matches
        print(f"   Processing {mp_sex} cross-state matches...")
        for state in tqdm(all_states, desc=f"   {mp_sex} cross-state"):
            mp_state = mp_sex_group[mp_sex_group['state_norm'] == state]
            if len(mp_state) == 0:
                continue

            adjacent = get_adjacent_states(state)
            for adj_state in adjacent:
                up_adj = up_matches[up_matches['state_norm'] == adj_state]
                if len(up_adj) > 0:
                    pairs = process_state_pair(mp_state, up_adj, is_adjacent_state=True)
                    if len(pairs) > 0:
                        all_pairs.append(pairs)

    # Combine all
    print("\n4. Combining results...")
    if all_pairs:
        pairs_df = pd.concat(all_pairs, ignore_index=True)
        # Remove duplicates (same MP-UP pair from different processing paths)
        pairs_df = pairs_df.drop_duplicates(subset=['mp_id', 'up_id'])
        print(f"   Generated {len(pairs_df):,} candidate matches")
    else:
        print("   ERROR: No candidate pairs generated!")
        return

    # Statistics by tier
    print("\n5. Priority tier breakdown:")
    tier_counts = pairs_df['priority_tier'].value_counts().sort_index()
    tier_names = {1: 'Tier 1 (HIGH - diff county)', 2: 'Tier 2 (adjacent)',
                  3: 'Tier 3 (same county)', 4: 'Tier 4 (LOW - same city)'}
    for tier, count in tier_counts.items():
        pct = 100 * count / len(pairs_df)
        print(f"   {tier_names.get(tier, f'Tier {tier}')}: {count:,} ({pct:.1f}%)")

    # Save outputs
    print("\n6. Saving output files...")

    # Full pairs file (all tiers)
    pairs_path = os.path.join(OUT_DIR, 'candidate_pairs.csv')
    pairs_df.to_csv(pairs_path, index=False)
    print(f"   All pairs: {pairs_path}")

    # High priority only (Tier 1-2)
    high_priority = pairs_df[pairs_df['priority_tier'] <= 2]
    hp_path = os.path.join(OUT_DIR, 'candidate_pairs_high_priority.csv')
    high_priority.to_csv(hp_path, index=False)
    print(f"   High priority (Tier 1-2): {hp_path} ({len(high_priority):,} pairs)")

    # Tier 1 only (best for manual review)
    tier1 = pairs_df[pairs_df['priority_tier'] == 1]
    t1_path = os.path.join(OUT_DIR, 'candidate_pairs_tier1.csv')
    tier1.to_csv(t1_path, index=False)
    print(f"   Tier 1 only: {t1_path} ({len(tier1):,} pairs)")

    # Export case JSONs
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
    print(f"   {mp_json_path}")

    with open(up_json_path, 'w', encoding='utf-8') as f:
        json.dump(up_cases, f, indent=2)
    print(f"   {up_json_path}")

    # Final statistics
    print("\n" + "=" * 60)
    print("Summary Statistics:")
    print(f"  Total candidate pairs: {len(pairs_df):,}")
    print(f"  High priority (Tier 1-2): {len(high_priority):,}")
    print(f"  Same state: {pairs_df['same_state'].sum():,}")
    print(f"  Adjacent state: {(~pairs_df['same_state']).sum():,}")
    print(f"  Same county: {pairs_df['same_county'].sum():,}")
    print(f"  Same city: {pairs_df['same_city'].sum():,}")
    print(f"  Avg matches per MP: {len(pairs_df) / len(mp):.1f}")
    print(f"  Avg matches per UP: {len(pairs_df) / len(up):.1f}")
    print("=" * 60)

    print("\nRecommendation: Start manual review with candidate_pairs_tier1.csv")


if __name__ == '__main__':
    main()
