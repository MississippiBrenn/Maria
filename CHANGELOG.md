# Changelog

All notable changes to the Maria project are documented here.

## [Unreleased]

### Added
- **Race overlap filtering** in candidate pair generation - Only keeps pairs where MP and UP share at least one race category (or one is unknown/uncertain)
- Geographic visualization module for mapping cases by location
- Temporal analysis for identifying time-based patterns
- Pattern identification algorithms (geographic/demographic clustering)
- Multi-source data architecture for integrating additional databases
- Interview questions documentation for portfolio presentation
- Comprehensive CHANGELOG

### Fixed
- **State normalization** - MP files used abbreviations (FL, CA) while UP files used full names (Florida, California). Now both are normalized to 2-letter codes via `state_normalizer.py`

### Changed
- Race matching upgraded from soft scoring to hard filtering with partial overlap support
  - Previously: Race mismatch scored 0.3 (soft penalty)
  - Now: Race mismatch filtered out entirely, unless one side is unknown/uncertain
  - Multi-race values (e.g., "White / Caucasian, Hispanic / Latino") match if any category overlaps
- Renamed pipeline files for clarity:
  - `build_graph_artifacts_optimized.py` → `generate_candidate_pairs.py`
  - `build_candidates_vectorized.py` → `score_candidates.py`
  - `scoring_simple.py` → `scoring.py`
- Rewrote README for portfolio presentation

### Removed
- Deprecated duplicate implementations:
  - `scoring.py` (old version with unused features)
  - `build_graph_artifacts.py` (Neptune graph loader)
  - `build_graph_artifacts_simple.py` (nested loop version)
  - `build_graph_artifacts_spatial.py` (experimental KD-tree)
  - `build_candidates.py` (Neptune-based scoring)
  - `build_candidates_simple.py` (row-by-row scoring)

---

## [0.4.0] - 2026-04-28

### Added
- **Improved pair generation v2** (`generate_candidate_pairs_v2.py`)
  - Applies exclusions at generation time (infants, partial remains, historical cases)
  - Priority tier system to deprioritize same-city matches (likely already investigated)
  - Cross-state matching for adjacent states (people cross state lines)
- **Adjacent states mapping** (`adjacent_states.py`) - Geographic adjacency for all US states
- **Match prioritization** (`prioritize_matches.py`) - Composite scoring combining:
  - Priority tier (geography-based)
  - Match specificity (fewer matches = more valuable)
  - Rarity score (rare demographics matching)
  - Temporal proximity
- **Interactive review helper** (`review_matches.py`) - Streamlined manual review workflow with:
  - Case comparison display
  - One-key exclusion workflow
  - Review progress tracking
- **Exclusions system** (`data/exclusions.csv`) - Centralized tracking of cases to exclude:
  - INFANT - Fetus/infant remains
  - PARTIAL_REMAINS - Skull only, torso only, etc.
  - HISTORICAL - Pre-dates modern MP database (e.g., 1930s)
- **Rebuild script** (`rebuild.sh`) - One-command pipeline rebuild

### Changed
- **Priority tiers replace flat matching**:
  - Tier 1 (HIGH): Different county within same state - likely NOT already checked
  - Tier 2: Adjacent state matches - cross-state cases
  - Tier 3: Same county, different city
  - Tier 4 (LOW): Same city - authorities likely already checked
- **Output files reorganized**:
  - `candidate_pairs.csv` - All pairs
  - `candidate_pairs_high_priority.csv` - Tier 1-2 only
  - `candidate_pairs_tier1.csv` - Tier 1 only (best for manual review)
  - `candidate_pairs_prioritized.csv` - Scored and sorted
  - `best_match_per_up.csv` - UPs with 1-10 matches, best match each
  - `top_matches_for_review.csv` - Top 1000 pairs overall

### Insights from Manual Review
- Same-city matches are almost always already checked by authorities
- Height differences of 3+ inches typically rule out matches
- Weight differences of 20+ lbs can rule out matches (for non-decomposed remains)
- UPs with few potential matches are most valuable to investigate
- Cross-state matching catches cases where people crossed state lines
- 713 UPs identified with 1-10 high-priority matches (focused review list)
- 105 UPs identified with exactly 1 potential match (highest confidence)

---

## [0.3.0] - 2024-12

### Added
- **Uniqueness-based prioritization** - Core innovation that boosts matches where both sides have few candidates
- **Rarity scoring module** (`rarity_scoring.py`) - Demographic uniqueness calculation
- **Era boost** - Priority scoring for 1980-2006 cases
- Infant remains filtering - Excludes UPs with no age estimate
- Auto-generated investigation tracker CSV
- `TOP_MATCHES.md` - Human-readable match investigation guide

### Changed
- Reduced actionable matches from 5.68M to ~165 high-priority cases
- Hard filter: Only matches where both sides have ≤15 candidates are kept
- High priority threshold: Both sides ≤5 matches AND score ≥0.70

### Performance
- Vectorized pandas/numpy operations throughout scoring pipeline
- Sex-based chunking in pair generation for memory efficiency

---

## [0.2.0] - 2024-11

### Added
- **Vectorized candidate pair generation** (`build_graph_artifacts_optimized.py`)
  - Replaced O(n×m) nested loops with pandas merge operations
  - Added sex-based chunking (CHUNK_SIZE=200) for memory management
- **Vectorized scoring engine** (`build_candidates_vectorized.py`)
  - All scoring components computed via numpy/pandas vectorization
- County adjacency heuristics (`county_adjacency.py`)
- State name normalization (`state_normalizer.py`)

### Changed
- Hard filters moved to pair generation phase (earlier filtering)
- Geographic scoring: city+county (1.0) → county (0.85) → state (0.3)
- Temporal scoring: Exponential decay function for gaps >5 years

### Performance
- ~100x speedup over simple implementation
- Full pipeline completes in minutes vs hours

---

## [0.1.0] - 2024-10

### Added
- Initial matching algorithm with basic scoring
- Process NamUs CSV downloads (`process_namus_downloads.py`)
- Simple candidate pair generation with hard filters:
  - Sex match (or Unknown)
  - Same state
  - Age range overlap
  - Found date ≥ last seen date
- Weighted scoring components:
  - Sex match (weight: 2.0)
  - Age similarity (weight: 1.5)
  - Geography (weight: 2.0)
  - Race match (weight: 0.8)
  - Temporal proximity (weight: 1.2)
- JSON/JSONL output formats for matches
- Merge utility for partial NamUs exports

---

## [0.0.1] - 2024-09

### Added
- Initial project scaffolding
- Neptune graph database integration (later deprecated)
- RAGnet visualization page
- Basic haversine distance calculation
- Tattoo/clothing item tokenization (later removed due to data quality)

### Technical Notes
- Original design used AWS Neptune for graph storage
- Pivoted to CSV-based pipeline for simplicity and portability
- Removed location coordinates (lat/lon) due to incomplete NamUs data
