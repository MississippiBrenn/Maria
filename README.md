# Maria

**A probabilistic matching system for identifying connections between missing persons and unidentified remains**

Maria processes NamUs (National Missing and Unidentified Persons System) data to surface high-confidence match candidates for human investigation. The system applies hard filters to eliminate impossible matches, then scores remaining candidates using weighted demographic and temporal features, enhanced by a novel **uniqueness-based prioritization** algorithm.

## Problem Domain

The United States has approximately:
- **90,000+** active missing persons cases
- **14,000+** unidentified human remains cases

Traditional approaches generate millions of potential matches (90K × 14K = 1.26B pairs), making human review impossible. Maria solves this through intelligent filtering and prioritization.

## Key Innovation: Uniqueness Scoring

**Core insight:** If a missing person only matches 2-3 unidentified remains (and vice versa), those matches are *far more likely* to be correct than matches where each side has hundreds of candidates.

```
final_score = base_score + uniqueness_boost + rarity_boost + era_boost
```

| Match Count (both sides) | Uniqueness Boost |
|--------------------------|------------------|
| 1-2 matches              | +0.50            |
| 3-5 matches              | +0.30            |
| 6-10 matches             | +0.16            |
| 11-15 matches            | +0.06            |
| >15 matches              | Filtered out     |

This reduces actionable matches from **5.68 million** to approximately **165** high-priority cases.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION                              │
│  NamUs CSV Exports → process_namus_downloads.py → Master CSVs       │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CANDIDATE PAIR GENERATION                        │
│  generate_candidate_pairs.py                                        │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Hard Filters (eliminate impossible matches):                 │   │
│  │  • Sex must match (M↔M, F↔F, Unknown↔any)                   │   │
│  │  • Age ranges must overlap                                   │   │
│  │  • Found date ≥ last seen date (−7 day tolerance)           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  Optimization: Vectorized pandas operations, sex-based chunking     │
│  Output: candidate_pairs.csv (~3.5GB for full dataset)              │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SCORING ENGINE                              │
│  score_candidates.py                                                │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Weighted Component Scoring:                                  │   │
│  │  • Sex match      (weight: 2.0)  Perfect=1.0, Unknown=0.5   │   │
│  │  • Age similarity (weight: 1.5)  Projected age overlap      │   │
│  │  • Geography      (weight: 2.0)  City→County→State cascade  │   │
│  │  • Race match     (weight: 0.8)  Soft match (allows error)  │   │
│  │  • Temporal       (weight: 1.2)  Decay function over time   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Enhancement Boosts:                                          │   │
│  │  • Uniqueness boost (0.0–0.50): Few matches = high priority │   │
│  │  • Rarity boost (0.0–0.20): Rare demographics matching      │   │
│  │  • Era boost (+0.10): 1980–2006 prioritization              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  Output: all_matches_scored.csv, high_priority_matches.csv          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      INVESTIGATION OUTPUT                           │
│  • TOP_MATCHES.md - Human-readable investigation guide              │
│  • candidates.jsonl - Top 20 matches per MP (JSON Lines)            │
│  • Investigation tracker CSV (for case management)                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
Maria/
├── data-build/                    # Core pipeline
│   ├── process_namus_downloads.py # Data ingestion & normalization
│   ├── generate_candidate_pairs.py# Vectorized pair generation
│   ├── score_candidates.py        # Scoring engine with boosts
│   ├── scoring.py                 # Component scoring functions
│   ├── rarity_scoring.py          # Demographic rarity calculation
│   ├── county_adjacency.py        # Geographic proximity heuristics
│   ├── state_normalizer.py        # State name standardization
│   └── merge_raw_downloads.py     # Utility for combining exports
│
├── data/
│   ├── raw/                       # NamUs CSV exports (partial files)
│   │   ├── Missing/               # Missing persons exports
│   │   └── Unidentified/          # Unidentified persons exports
│   ├── compiled/                  # Merged exports
│   │   ├── missing_persons.csv
│   │   └── unidentified_persons.csv
│   └── clean/                     # Processed master files
│       ├── MP_master.csv          # Missing persons (normalized)
│       └── UP_master.csv          # Unidentified persons (normalized)
│
├── out/                           # Generated outputs
│   ├── candidate_pairs.csv        # Filtered candidate pairs
│   ├── all_matches_scored.csv     # All scored matches
│   ├── high_priority_matches.csv  # Top matches for investigation
│   └── candidates.jsonl           # Top 20 per MP
│
├── TOP_MATCHES.md                 # Auto-generated investigation guide
└── ENHANCEMENTS.md                # Algorithm documentation
```

## Algorithm Details

### Scoring Components

**1. Sex Match (weight: 2.0)**
- Exact match: 1.0
- Unknown match: 0.5
- Mismatch: Hard reject

**2. Age Similarity (weight: 1.5)**
- Projects MP's age forward based on time elapsed
- Calculates overlap between projected age range and UP's estimated age
- Exponential decay for near-misses

**3. Geographic Proximity (weight: 2.0)**
- Same city + county: 1.0
- Same county: 0.85
- Same state: 0.3
- Different state: Hard filtered in pair generation

**4. Race Match (weight: 0.8)**
- Match: 1.0
- Mismatch: 0.3 (soft—allows for identification errors)

**5. Temporal Score (weight: 1.2)**
- Within 30 days: 1.0
- Within 6 months: 0.8
- Within 1 year: 0.6
- >5 years: Exponential decay

### Enhancement Modules

**Rarity Scoring** (`rarity_scoring.py`)
- Calculates demographic uniqueness for each case
- Factors: state frequency, sex distribution, race distribution, age extremes
- Rare MP matching rare UP = +0.20 boost

**Era Prioritization**
- Cases from 1980–2006 receive +0.10 boost
- Rationale: Better data quality, active investigation period

## Performance

The pipeline uses **vectorized pandas/numpy operations** for efficiency:

| Stage | Naive Approach | Maria |
|-------|----------------|-------|
| Pair generation | O(n × m) loops | Vectorized merge + filter |
| Scoring | Row iteration | Vectorized column operations |
| Full pipeline | ~hours | ~minutes |

Memory management through sex-based chunking allows processing on standard hardware.

## Setup

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r data-build/requirements.txt
```

### Dependencies
- pandas
- numpy
- tqdm

## Usage

### 1. Download NamUs Data
Export bulk downloads from [NamUs](https://namus.gov):
- Missing Persons (all available columns)
- Unidentified Persons (all available columns)

NamUs limits exports to 10,000 records, so you may need multiple partial exports.
Save them to `data/raw/Missing/` and `data/raw/Unidentified/`

### 2. Merge Partial Exports
```bash
python3 data-build/merge_raw_downloads.py
```
This creates `data/compiled/missing_persons.csv` and `data/compiled/unidentified_persons.csv`

### 3. Process into Master Files
```bash
python3 data-build/process_namus_downloads.py \
    --mp data/compiled/missing_persons.csv \
    --up data/compiled/unidentified_persons.csv
```
This creates `data/clean/MP_master.csv` and `data/clean/UP_master.csv`

### 4. Generate Candidate Pairs
```bash
python3 data-build/generate_candidate_pairs.py
```

### 5. Score Matches
```bash
python3 data-build/score_candidates.py
```

### 6. Review Results
- Open `TOP_MATCHES.md` for the investigation guide
- `high_priority_matches.csv` contains the best leads
- Each match includes links to NamUs case pages

## Output Interpretation

### Priority Levels
- **Ultra-priority**: Both MP and UP have ≤2 matches
- **High priority**: Both sides ≤5 matches, score ≥0.70
- **Standard**: Both sides ≤15 matches

### Score Breakdown
```
MP-12345 <-> UP-6789
  Final Score: 0.92
    Base: 0.62 (demographics + geography + temporal)
    Uniqueness: +0.20 (MP has 2 matches, UP has 3 matches)
    Rarity: +0.10 (uncommon demographic profile)
```

## Future Enhancements

- [ ] Geographic clustering visualization
- [ ] Temporal pattern analysis
- [ ] Multi-source data integration (FBI, Charley Project)
- [ ] Name similarity matching (Levenshtein distance)
- [ ] Real county adjacency database

## Technical Decisions

**Why state-based filtering?**
Cross-state matching is rare in reality and would multiply candidate pairs. The system keeps results actionable by focusing on in-state matches first.

**Why soft race matching?**
Race identification from remains can be unreliable due to decomposition, ancestry complexity, and subjective classification. A hard filter would miss valid matches.

**Why the era boost?**
Cases from 1980–2006 tend to have better documentation and are within the window where matches are most actionable for families.

## License

MIT
