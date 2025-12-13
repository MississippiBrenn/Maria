# Maria Matching Enhancements

## Core Philosophy: Uniqueness First

**Key Insight:** Focus human time on truly unique matches where both the missing person and unidentified person have very few candidates.

**Hard Filter:** Only matches where BOTH sides have ≤10 total candidates are kept.
- **Before filter:** 5.68M potential matches
- **After filter:** 165 actionable matches
- **High priority:** 13 matches where both sides have ≤5 candidates

This ensures every match you investigate is worth your time.

---

## New Features Added

### 1. Rarity Scoring
**File:** `data-build/rarity_scoring.py`

**Concept:** Rare individuals matching rare individuals = high confidence signal

**Rarity Factors:**
- **State rarity**: Less common states are more rare
- **Sex rarity**: "Unknown" is rare, M/F are common
- **Race rarity**: Minority races in dataset are rare
- **Age rarity**:
  - Very young (< 5 years): 0.8 rarity
  - Elderly (> 70 years): 0.6 rarity
  - Children (< 12 years): 0.4 rarity
  - Seniors (> 60 years): 0.3 rarity
  - Adults (12-60): 0.1 rarity (common)

**Boost Calculation:**
- Both MP and UP rare (>0.5): **+0.20 boost**
- One is rare (>0.5): **+0.10 boost**
- Both somewhat rare (>0.3): **+0.05 boost**
- Common cases: No boost

**Example:**
- 4-year-old Hispanic MP in rural Montana (rare)
- 3-6 year old Hispanic UP in rural Montana (rare)
- **Result:** +0.20 rarity boost to match score

---

### 2. County Adjacency
**File:** `data-build/county_adjacency.py`

**Concept:** People often cross county lines, so adjacent counties should score higher

**Geographic Scoring (Enhanced):**
- Same city + county: **1.0**
- Same county: **0.85**
- Adjacent counties: **0.6** (heuristic-based)
- Same state only: **0.3**

**Adjacency Detection Heuristics:**
1. **Directional variants**: "North County" + "South County" → likely adjacent (0.8 confidence)
2. **Name similarity**: Similar county names → might be adjacent (0.5 confidence)
3. **Exact match**: Same county → perfect (1.0)

**Example:**
- MP last seen in "Los Angeles County"
- UP found in "Orange County"
- Heuristic checks if counties might be adjacent
- Geographic score: ~0.3-0.6 (depending on confidence)

---

### 3. Enhanced Match Scoring

**New Final Score Formula:**
```
final_score = base_score + uniqueness_boost + rarity_boost
              (capped at 1.0)
```

**Components:**
1. **Base Score** (0-1): Demographic + geographic + temporal similarity
2. **Uniqueness Boost** (0-0.50): How few matches each case has
   - Both ≤2 matches: +0.50 (ultra-rare)
   - Both ≤5 matches: +0.30 (very rare)
   - Both ≤10 matches: +0.16-0.23 (rare)
3. **Rarity Boost** (0-0.20): How unusual the demographic combo is

**Maximum Possible Score:** 1.0 (capped)

**Note:** Cases with >10 matches on either side are filtered out entirely to focus human effort on actionable leads.

---

## Example High-Quality Match

```
MP-12345 (Sarah Johnson) <-> UP-6789
  Final Score: 0.95
    Base: 0.65 (demographics + geography + temporal)
    Uniqueness: +0.15 (MP has 2 matches, UP has 1 match)
    Rarity: +0.15 (5yo female, rare race/state combo)

  Details:
    - Same county (Los Angeles)
    - Age: MP 5yo, UP 4-6yo
    - Sex: Both Female
    - Race: Both Hispanic
    - Temporal: Found 45 days after last seen
    - MP has only 2 possible matches
    - UP has only 1 possible match
    - Both are demographically rare (young age + specific location)
```

---

## Future Enhancements (Not Yet Implemented)

### Easy Wins:
- [ ] Temporal window refinement (within 1 week = very high boost)
- [ ] Name similarity (Levenshtein distance for partial matches)
- [ ] Data quality flags (cases with complete data get boost)

### Medium Complexity:
- [ ] Real county adjacency database (instead of heuristics)
- [ ] Interstate corridor analysis (I-5, I-95, etc.)
- [ ] Seasonal patterns (winter disappearance → spring found)

### Complex:
- [ ] Case narrative NLP (if API access granted)
- [ ] Tattoo/distinctive feature matching
- [ ] External data integration (weather, crime data)

---

## How to Use

The new features are automatically integrated into the pipeline:

```bash
# Activate virtual environment
source venv/bin/activate

# Step 1: Generate candidate pairs (state-by-state filtering)
python3 data-build/build_graph_artifacts_optimized.py

# Step 2: Score with all enhancements + uniqueness filter
python3 data-build/build_candidates_vectorized.py
```

### Output Files

1. **`all_matches_scored.csv`** - All 165 matches (both sides ≤10 candidates)
   - Columns: mp_id, up_id, names, locations, scores, match counts, etc.

2. **`high_priority_matches.csv`** - 13 matches where both sides ≤5 candidates AND score ≥0.7
   - **Start here!** These are your best leads.

3. **`match_investigation_tracker.csv`** - Human-friendly tracking spreadsheet
   - Columns: Match_ID, Status, Score, MP_Matches, UP_Matches, contact info, notes
   - Open in Excel/Google Sheets to track outreach progress

4. **`top_100_summary.csv`** - Top matches in readable format

### Priority System

**Ultra-Priority (Start Here!):**
- Both MP and UP have ≤2 matches
- Final score ≥ 0.95
- Result: 2 matches

**High Priority:**
- Both sides have ≤5 matches
- Final score ≥ 0.70
- Result: 13 matches

**Medium Priority:**
- Both sides have ≤10 matches
- Any score
- Result: 165 total matches

**All other matches are filtered out to save your time.**
