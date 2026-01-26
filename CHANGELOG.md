# Changelog

All notable changes to the Maria project are documented here.

## [Unreleased]

### Added
- Geographic visualization module for mapping cases by location
- Temporal analysis for identifying time-based patterns
- Pattern identification algorithms (geographic/demographic clustering)
- Multi-source data architecture for integrating additional databases
- Interview questions documentation for portfolio presentation
- Comprehensive CHANGELOG

### Changed
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
