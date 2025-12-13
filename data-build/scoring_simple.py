"""
Simplified scoring algorithm using only basic NamUs fields:
- Sex, Age, Race
- City/County/State (with county adjacency)
- Dates (DLC vs DBF)
"""

import math
from county_adjacency import calculate_geographic_score_with_adjacency


def normalize_str(s):
    """Normalize string for comparison."""
    if not s or str(s).strip() == "":
        return ""
    return str(s).strip().upper()


def sex_match_score(mp_sex, up_sex):
    """
    Score sex match.
    Returns: (score, is_hard_reject)
    """
    mp = normalize_str(mp_sex)
    up = normalize_str(up_sex)

    # Perfect match
    if mp == up:
        return 1.0, False

    # Unknown can match either (with penalty)
    if mp == "UNKNOWN" or up == "UNKNOWN":
        return 0.5, False

    # Mismatch
    return 0.0, True  # Hard reject


def age_similarity(mp_age_at_missing, up_min, up_max, years_between):
    """
    Calculate age similarity accounting for time passage.

    Args:
        mp_age_at_missing: Age when person went missing
        up_min, up_max: Estimated age range of unidentified person when found
        years_between: Years between last seen date and found date

    Returns 0.0 to 1.0 based on overlap after projecting MP's age forward.
    """
    if mp_age_at_missing is None or None in (up_min, up_max):
        return 0.5  # Neutral if data missing

    # Project MP's age to when UP was found
    if years_between is not None and years_between >= 0:
        mp_projected_age = mp_age_at_missing + years_between
    else:
        # If no date info, use age at missing
        mp_projected_age = mp_age_at_missing

    # Create a range for MP (±2 years to account for estimation error)
    mp_min = mp_projected_age - 2
    mp_max = mp_projected_age + 2

    # Calculate overlap with UP's age range
    overlap_start = max(mp_min, up_min)
    overlap_end = min(mp_max, up_max)
    overlap = max(0, overlap_end - overlap_start + 1)

    if overlap == 0:
        # Check how far apart they are
        gap = min(abs(mp_min - up_max), abs(up_min - mp_max))
        # Exponential decay for near misses
        return max(0, 0.5 * math.exp(-gap / 5.0))

    # Normalize by average range size
    mp_range = mp_max - mp_min + 1
    up_range = up_max - up_min + 1
    avg_range = (mp_range + up_range) / 2.0

    return min(1.0, overlap / avg_range)


def geographic_score(mp_state, mp_county, mp_city, up_state, up_county, up_city):
    """
    Score geographic proximity based on state/county/city match.
    """
    mp_s = normalize_str(mp_state)
    up_s = normalize_str(up_state)

    # Different state = shouldn't happen (filtered earlier)
    if mp_s != up_s or mp_s == "":
        return 0.0

    mp_co = normalize_str(mp_county)
    up_co = normalize_str(up_county)
    mp_ci = normalize_str(mp_city)
    up_ci = normalize_str(up_city)

    # Same state + same county + same city
    if mp_co == up_co and mp_co != "" and mp_ci == up_ci and mp_ci != "":
        return 1.0

    # Same state + same county
    if mp_co == up_co and mp_co != "":
        return 0.8

    # Same state only
    return 0.3


def race_match_score(mp_race, up_race):
    """Score race/ethnicity match (soft - can be misidentified)."""
    mp = normalize_str(mp_race)
    up = normalize_str(up_race)

    if mp == "" or up == "":
        return 0.5  # Neutral

    if mp == up:
        return 1.0

    # Different race - still possible (misidentification, decomposition)
    return 0.3


def temporal_score(days_gap):
    """
    Score based on time between last seen and found.
    Closer in time = higher score.
    """
    if days_gap is None or days_gap < 0:
        return 0.5  # Neutral if unknown

    # Negative shouldn't happen (filtered earlier)
    if days_gap < 0:
        return 0.0

    # Within 30 days
    if days_gap <= 30:
        return 1.0

    # Within 6 months
    if days_gap <= 180:
        return 0.8

    # Within 1 year
    if days_gap <= 365:
        return 0.6

    # Within 5 years
    if days_gap <= 1825:
        return 0.4

    # Over 5 years - exponential decay
    return 0.4 * math.exp(-(days_gap - 1825) / 3650)


# Weights for each component
DEFAULT_WEIGHTS = {
    'sex': 2.0,        # Critical - must match
    'age': 1.5,        # Very important
    'geography': 2.0,  # Very important (same state/county)
    'race': 0.8,       # Less reliable
    'temporal': 1.2,   # Important but not critical
}


def score_match(mp_row, up_row, weights=None):
    """
    Score a potential MP <-> UP match.

    Args:
        mp_row: dict with keys: sex, age_min, age_max, state, county, city, race, last_seen_days
        up_row: dict with keys: sex, age_min, age_max, state, county, city, race, found_days

    Returns:
        (score, components_dict) or (None, reject_reason) if hard reject
    """
    W = DEFAULT_WEIGHTS.copy()
    if weights:
        W.update(weights)

    # Hard filters
    sex_score, sex_reject = sex_match_score(mp_row.get('sex'), up_row.get('sex'))
    if sex_reject:
        return None, "Sex mismatch"

    # Calculate temporal gap
    days_gap = None
    if mp_row.get('last_seen_days') and up_row.get('found_days'):
        days_gap = up_row['found_days'] - mp_row['last_seen_days']
        if days_gap < -7:  # Allow 7 day tolerance
            return None, "Found before last seen"

    # Component scores
    geo_score, geo_reason = calculate_geographic_score_with_adjacency(
        mp_row.get('state'), mp_row.get('county'), mp_row.get('city'),
        up_row.get('state'), up_row.get('county'), up_row.get('city')
    )

    # Calculate years between dates for age projection
    years_between = None
    if days_gap is not None:
        years_between = days_gap / 365.25

    components = {
        'sex': sex_score,
        'age': age_similarity(
            mp_row.get('age_min'),  # Age at time of missing
            up_row.get('age_min'), up_row.get('age_max'),  # Age when found
            years_between  # Years to project MP's age forward
        ),
        'geography': geo_score,
        'race': race_match_score(mp_row.get('race'), up_row.get('race')),
        'temporal': temporal_score(days_gap),
    }

    # Weighted average
    weighted_sum = sum(components[k] * W[k] for k in components)
    weight_total = sum(W[k] for k in components)

    final_score = weighted_sum / weight_total if weight_total > 0 else 0.0

    # Ensure score is between 0 and 1
    final_score = max(0.0, min(1.0, final_score))

    return final_score, components
