"""
County adjacency scoring.
People often cross county lines, so adjacent counties should score higher than distant counties.

Since we don't have a complete adjacency database, we use heuristics:
1. Same county = 1.0
2. Adjacent county (estimated) = 0.6
3. Same state = 0.3
"""

import pandas as pd
from difflib import SequenceMatcher


def normalize_county(county_str):
    """Normalize county name for comparison."""
    if pd.isna(county_str) or str(county_str).strip() == "":
        return ""
    # Remove "County", "Parish", etc. and normalize case
    s = str(county_str).upper().strip()
    s = s.replace(" COUNTY", "").replace(" PARISH", "").replace(" BOROUGH", "")
    return s.strip()


def are_counties_adjacent_heuristic(county1, county2, state1, state2):
    """
    Heuristic to guess if counties are adjacent.

    Rules:
    1. Must be in same state
    2. If county names are very similar (e.g., "North County" vs "South County") → likely adjacent
    3. If one is a major metro county and other nearby → likely adjacent

    Returns: (is_adjacent, confidence)
    """
    if state1 != state2 or state1 == "" or county1 == "" or county2 == "":
        return False, 0.0

    if county1 == county2:
        return True, 1.0  # Same county

    # Check for directional variants (North/South/East/West)
    directions = ["NORTH", "SOUTH", "EAST", "WEST", "NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]

    c1_base = county1
    c2_base = county2

    for direction in directions:
        c1_base = c1_base.replace(direction, "").strip()
        c2_base = c2_base.replace(direction, "").strip()

    # If base names match after removing directions → likely adjacent
    if c1_base == c2_base and c1_base != "":
        return True, 0.8  # High confidence they're adjacent

    # Check for similar names (might be adjacent)
    similarity = SequenceMatcher(None, county1, county2).ratio()
    if similarity > 0.7:
        return True, 0.5  # Medium confidence

    # Otherwise, assume not adjacent
    return False, 0.0


def calculate_geographic_score_with_adjacency(mp_state, mp_county, mp_city, up_state, up_county, up_city):
    """
    Enhanced geographic scoring with county adjacency.

    Returns: (score, reason)
    """
    mp_s = str(mp_state).strip().upper()
    up_s = str(up_state).strip().upper()
    mp_co = normalize_county(mp_county)
    up_co = normalize_county(up_county)
    mp_ci = str(mp_city).strip().upper()
    up_ci = str(up_city).strip().upper()

    # Different state
    if mp_s != up_s or mp_s == "":
        return 0.0, "Different states"

    # Same state + same county + same city
    if mp_co == up_co and mp_co != "" and mp_ci == up_ci and mp_ci != "":
        return 1.0, "Same city and county"

    # Same state + same county
    if mp_co == up_co and mp_co != "":
        return 0.85, "Same county"

    # Check if counties are adjacent (heuristic)
    is_adjacent, confidence = are_counties_adjacent_heuristic(mp_co, up_co, mp_s, up_s)
    if is_adjacent:
        return 0.6 * confidence, f"Adjacent counties (confidence: {confidence:.1f})"

    # Same state only
    return 0.3, "Same state, different counties"


def add_adjacency_to_pairs(pairs_df, mp_df, up_df):
    """
    Add county adjacency information to candidate pairs.

    Args:
        pairs_df: DataFrame with mp_id, up_id
        mp_df: Missing persons DataFrame
        up_df: Unidentified persons DataFrame

    Returns:
        pairs_df with added columns: geo_score, geo_reason
    """
    print("Calculating geographic scores with county adjacency...")

    # Create lookup dicts for faster access
    mp_geo = mp_df.set_index('id')[['state', 'county', 'city']].to_dict('index')
    up_geo = up_df.set_index('id')[['state', 'county', 'city']].to_dict('index')

    geo_scores = []
    geo_reasons = []

    for _, row in pairs_df.iterrows():
        mp_id = row['mp_id']
        up_id = row['up_id']

        mp_data = mp_geo.get(mp_id, {})
        up_data = up_geo.get(up_id, {})

        score, reason = calculate_geographic_score_with_adjacency(
            mp_data.get('state', ''),
            mp_data.get('county', ''),
            mp_data.get('city', ''),
            up_data.get('state', ''),
            up_data.get('county', ''),
            up_data.get('city', '')
        )

        geo_scores.append(score)
        geo_reasons.append(reason)

    pairs_df['geo_score'] = geo_scores
    pairs_df['geo_reason'] = geo_reasons

    # Stats
    print(f"  Same county: {len(pairs_df[pairs_df['geo_score'] >= 0.85]):,}")
    print(f"  Adjacent counties: {len(pairs_df[(pairs_df['geo_score'] >= 0.5) & (pairs_df['geo_score'] < 0.85)]):,}")
    print(f"  Same state only: {len(pairs_df[pairs_df['geo_score'] < 0.5]):,}")

    return pairs_df
