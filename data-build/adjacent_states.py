"""
Adjacent state mapping for cross-state matching.
Missing persons often cross state lines, especially to adjacent states.
"""

# Each state maps to its adjacent states
ADJACENT_STATES = {
    'AL': ['FL', 'GA', 'MS', 'TN'],
    'AK': [],  # No adjacent US states
    'AZ': ['CA', 'CO', 'NM', 'NV', 'UT'],
    'AR': ['LA', 'MO', 'MS', 'OK', 'TN', 'TX'],
    'CA': ['AZ', 'NV', 'OR'],
    'CO': ['AZ', 'KS', 'NE', 'NM', 'OK', 'UT', 'WY'],
    'CT': ['MA', 'NY', 'RI'],
    'DE': ['MD', 'NJ', 'PA'],
    'FL': ['AL', 'GA'],
    'GA': ['AL', 'FL', 'NC', 'SC', 'TN'],
    'HI': [],  # No adjacent US states
    'ID': ['MT', 'NV', 'OR', 'UT', 'WA', 'WY'],
    'IL': ['IN', 'IA', 'KY', 'MO', 'WI'],
    'IN': ['IL', 'KY', 'MI', 'OH'],
    'IA': ['IL', 'MN', 'MO', 'NE', 'SD', 'WI'],
    'KS': ['CO', 'MO', 'NE', 'OK'],
    'KY': ['IL', 'IN', 'MO', 'OH', 'TN', 'VA', 'WV'],
    'LA': ['AR', 'MS', 'TX'],
    'ME': ['NH'],
    'MD': ['DE', 'PA', 'VA', 'WV', 'DC'],
    'MA': ['CT', 'NH', 'NY', 'RI', 'VT'],
    'MI': ['IN', 'OH', 'WI'],  # Connected via water
    'MN': ['IA', 'ND', 'SD', 'WI'],
    'MS': ['AL', 'AR', 'LA', 'TN'],
    'MO': ['AR', 'IL', 'IA', 'KS', 'KY', 'NE', 'OK', 'TN'],
    'MT': ['ID', 'ND', 'SD', 'WY'],
    'NE': ['CO', 'IA', 'KS', 'MO', 'SD', 'WY'],
    'NV': ['AZ', 'CA', 'ID', 'OR', 'UT'],
    'NH': ['MA', 'ME', 'VT'],
    'NJ': ['DE', 'NY', 'PA'],
    'NM': ['AZ', 'CO', 'OK', 'TX', 'UT'],
    'NY': ['CT', 'MA', 'NJ', 'PA', 'VT'],
    'NC': ['GA', 'SC', 'TN', 'VA'],
    'ND': ['MN', 'MT', 'SD'],
    'OH': ['IN', 'KY', 'MI', 'PA', 'WV'],
    'OK': ['AR', 'CO', 'KS', 'MO', 'NM', 'TX'],
    'OR': ['CA', 'ID', 'NV', 'WA'],
    'PA': ['DE', 'MD', 'NJ', 'NY', 'OH', 'WV'],
    'RI': ['CT', 'MA'],
    'SC': ['GA', 'NC'],
    'SD': ['IA', 'MN', 'MT', 'ND', 'NE', 'WY'],
    'TN': ['AL', 'AR', 'GA', 'KY', 'MO', 'MS', 'NC', 'VA'],
    'TX': ['AR', 'LA', 'NM', 'OK'],
    'UT': ['AZ', 'CO', 'ID', 'NM', 'NV', 'WY'],
    'VT': ['MA', 'NH', 'NY'],
    'VA': ['KY', 'MD', 'NC', 'TN', 'WV', 'DC'],
    'WA': ['ID', 'OR'],
    'WV': ['KY', 'MD', 'OH', 'PA', 'VA'],
    'WI': ['IL', 'IA', 'MI', 'MN'],
    'WY': ['CO', 'ID', 'MT', 'NE', 'SD', 'UT'],
    'DC': ['MD', 'VA'],
    'PR': [],
    'VI': [],
    'GU': [],
}


def are_states_adjacent(state1, state2):
    """Check if two states are adjacent."""
    if not state1 or not state2:
        return False
    s1 = state1.upper().strip()
    s2 = state2.upper().strip()

    if s1 == s2:
        return True  # Same state counts as "adjacent"

    return s2 in ADJACENT_STATES.get(s1, [])


def get_adjacent_states(state):
    """Get list of states adjacent to given state."""
    if not state:
        return []
    return ADJACENT_STATES.get(state.upper().strip(), [])
