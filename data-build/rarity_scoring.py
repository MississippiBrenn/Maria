"""
Rarity scoring: Calculate how unusual each MP/UP is in the dataset.
Rare individuals matching rare individuals = high confidence signal.
"""

import pandas as pd
import numpy as np


def calculate_rarity_scores(df, case_type='MP'):
    """
    Calculate rarity score for each case based on demographic outliers.

    Returns a dict: {case_id: rarity_score}
    where rarity_score is 0-1 (higher = more rare/unusual)
    """
    rarity_scores = {}

    # Get value counts for each dimension
    state_counts = df['state'].value_counts()
    sex_counts = df['sex'].value_counts()
    race_counts = df['race'].value_counts()

    # Age rarity (very young or very old is rare)
    if case_type == 'MP':
        ages = df['age_min'].dropna()
    else:
        ages = df[['age_min', 'age_max']].mean(axis=1).dropna()

    for idx, row in df.iterrows():
        case_id = row['id']
        rarity_components = []

        # 1. State rarity (less common states = more rare)
        state = row['state']
        if pd.notna(state) and state in state_counts:
            state_freq = state_counts[state] / len(df)
            state_rarity = 1 - state_freq  # Inverse frequency
            rarity_components.append(state_rarity)

        # 2. Sex rarity (Unknown is rare, M/F common)
        sex = row['sex']
        if pd.notna(sex) and sex in sex_counts:
            sex_freq = sex_counts[sex] / len(df)
            sex_rarity = 1 - sex_freq
            rarity_components.append(sex_rarity)

        # 3. Race rarity (minority races in dataset are rare)
        race = row['race']
        if pd.notna(race) and race != "" and race in race_counts:
            race_freq = race_counts[race] / len(df)
            race_rarity = 1 - race_freq
            rarity_components.append(race_rarity)

        # 4. Age rarity (very young < 5 or very old > 70 is rare)
        if case_type == 'MP':
            age = row['age_min']
        else:
            age = (row['age_min'] + row['age_max']) / 2.0 if pd.notna(row['age_min']) and pd.notna(row['age_max']) else None

        if pd.notna(age):
            if age < 5:
                age_rarity = 0.8  # Very young is rare
            elif age > 70:
                age_rarity = 0.6  # Elderly is somewhat rare
            elif age < 12:
                age_rarity = 0.4  # Children
            elif age > 60:
                age_rarity = 0.3  # Seniors
            else:
                age_rarity = 0.1  # Adults are common
            rarity_components.append(age_rarity)

        # Combine rarity components (average)
        if rarity_components:
            rarity_scores[case_id] = np.mean(rarity_components)
        else:
            rarity_scores[case_id] = 0.0  # No rarity if no data

    return rarity_scores


def calculate_combined_rarity_boost(mp_rarity, up_rarity):
    """
    Calculate boost based on combined rarity of MP and UP.

    Logic:
    - If both are rare (>0.5) → BIG boost
    - If one is rare → medium boost
    - If both common → no boost
    """
    # Both rare
    if mp_rarity > 0.5 and up_rarity > 0.5:
        return 0.20  # Significant boost

    # One rare
    if mp_rarity > 0.5 or up_rarity > 0.5:
        return 0.10  # Medium boost

    # Both somewhat rare
    if mp_rarity > 0.3 and up_rarity > 0.3:
        return 0.05  # Small boost

    # Common cases
    return 0.0


def add_rarity_scores_to_matches(matches_df, mp_df, up_df):
    """
    Add rarity scores to matches dataframe.

    Args:
        matches_df: DataFrame with mp_id, up_id columns
        mp_df: Missing persons master DataFrame
        up_df: Unidentified persons master DataFrame

    Returns:
        matches_df with added columns: mp_rarity, up_rarity, rarity_boost
    """
    print("Calculating rarity scores...")

    # Calculate rarity for all MPs and UPs
    mp_rarity_scores = calculate_rarity_scores(mp_df, case_type='MP')
    up_rarity_scores = calculate_rarity_scores(up_df, case_type='UP')

    # Add to matches
    matches_df['mp_rarity'] = matches_df['mp_id'].map(mp_rarity_scores).fillna(0.0)
    matches_df['up_rarity'] = matches_df['up_id'].map(up_rarity_scores).fillna(0.0)

    # Calculate combined rarity boost
    matches_df['rarity_boost'] = matches_df.apply(
        lambda r: calculate_combined_rarity_boost(r['mp_rarity'], r['up_rarity']),
        axis=1
    )

    # Stats
    rare_matches = matches_df[matches_df['rarity_boost'] > 0]
    print(f"  Total matches: {len(matches_df):,}")
    print(f"  Matches with rarity boost: {len(rare_matches):,} ({100*len(rare_matches)/len(matches_df):.1f}%)")
    print(f"  Max rarity boost: {matches_df['rarity_boost'].max():.3f}")

    return matches_df
