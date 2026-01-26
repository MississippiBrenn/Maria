# Algorithm Enhancements

Documentation of the core algorithmic innovations in Maria.

## Uniqueness-Based Prioritization

**Key Insight:** Focus human effort on matches where both the missing person and unidentified person have very few candidates.

### Hard Filters Applied

1. **Infant remains exclusion**: Unidentified persons with no estimated age are excluded (likely infant/fetal remains)
2. **Uniqueness filter**: Only matches where BOTH sides have ≤15 total candidates are kept
   - **Before filter:** 5.68M potential matches
   - **After filter:** ~165 actionable matches
   - **High priority:** ~13 matches where both sides have ≤5 candidates

This ensures every match you investigate is worth your time.

---

## Scoring Enhancements

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

---

### 2. Uniqueness Boost
**File:** `data-build/score_candidates.py`

**Concept:** Matches with few candidates on both sides deserve higher priority

**Boost Table:**

| Match Count | Boost per Side |
|-------------|---------------|
| 1-2 matches | +0.25         |
| 3-5 matches | +0.15         |
| 6-10 matches| +0.08         |
| 11-15 matches| +0.03        |

Both sides contribute independently, so maximum uniqueness boost is +0.50.

---

### 3. Era Prioritization
**File:** `data-build/score_candidates.py`

**Concept:** Cases from 1980-2006 have better data quality and are more actionable

**Implementation:** +0.10 boost for cases where last_seen_date is between 1980-2006

**Rationale:**
- DNA technology matured during this period
- Better documentation practices
- Still within living memory for witnesses

---

## Final Score Formula

```
final_score = base_score + uniqueness_boost + rarity_boost + era_boost
              (capped at 1.0)
```

**Components:**
1. **Base Score** (0-1): Weighted average of demographic, geographic, and temporal similarity
2. **Uniqueness Boost** (0-0.50): How few matches each case has
3. **Rarity Boost** (0-0.20): How unusual the demographic combination is
4. **Era Boost** (0-0.10): Priority for 1980-2006 era cases

---

## Priority System

| Priority Level | Criteria | Typical Count |
|----------------|----------|---------------|
| Ultra-Priority | Both sides ≤2 matches | ~2 matches |
| High Priority | Both sides ≤5 matches, score ≥0.70 | ~13 matches |
| Medium Priority | Both sides ≤10 matches | ~50 matches |
| Standard | Both sides ≤15 matches | ~165 matches |

**All other matches are filtered out to save your time.**

---

## Pipeline Usage

```bash
# Step 1: Generate candidate pairs
python3 data-build/generate_candidate_pairs.py

# Step 2: Score with all enhancements
python3 data-build/score_candidates.py
```

### Output Files

| File | Description |
|------|-------------|
| `all_matches_scored.csv` | All matches passing uniqueness filter |
| `high_priority_matches.csv` | Both sides ≤5 matches AND score ≥0.70 |
| `TOP_MATCHES.md` | Human-readable investigation guide |
| `candidates.jsonl` | Top 20 matches per MP (JSON Lines) |

---

## Future Enhancements

### Planned
- [ ] Geographic clustering visualization
- [ ] Temporal pattern analysis
- [ ] Multi-source data integration
- [ ] Real county adjacency database

### Under Consideration
- [ ] Name similarity matching (Levenshtein distance)
- [ ] Interstate corridor analysis
- [ ] Seasonal pattern detection
- [ ] Case narrative NLP
