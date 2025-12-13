# Maria - Missing Persons Matching System

A simple matching system to find potential connections between missing persons and unidentified remains.

## Project Structure

```
Maria/
├── data/
│   ├── raw/                    # NamUs CSV downloads go here
│   └── clean/
│       ├── MP_master.csv       # Processed missing persons data
│       └── UP_master.csv       # Processed unidentified persons data
├── data-build/
│   ├── build_graph_artifacts.py  # Generates cases JSON files
│   ├── build_candidates.py       # Scores matches between MP and UP
│   ├── scoring.py                # Core matching algorithm
│   └── requirements.txt
└── out/
    ├── cases_mp.json          # Missing persons cases
    ├── cases_uid.json         # Unidentified persons cases
    └── candidates.jsonl       # Scored matches (top 20 per MP)
```

## Setup

1. Create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r data-build/requirements.txt
```

## Workflow

### 1. Download NamUs Data
- Log into NamUs and export bulk downloads for:
  - Missing Persons (all columns available)
  - Unidentified Persons (all columns available)
- Save CSV files to `data/raw/`

### 2. Process into Master Files
```bash
python3 data-build/process_namus_downloads.py \
    --mp data/raw/missing_persons_download.csv \
    --up data/raw/unidentified_persons_download.csv
```
This creates `data/clean/MP_master.csv` and `data/clean/UP_master.csv`

### 3. Generate Candidate Pairs
```bash
python3 data-build/build_graph_artifacts_simple.py
```
This applies hard filters and generates:
- `out/candidate_pairs.csv` - Filtered pairs for scoring
- `out/cases_mp.json` - Missing persons case data
- `out/cases_up.json` - Unidentified persons case data

### 4. Score Matches with Uniqueness Boost
```bash
python3 data-build/build_candidates_simple.py
```
This scores all matches and outputs:
- `out/all_matches_scored.csv` - All valid matches with scores
- `out/candidates.jsonl` - Top 20 matches per MP
- `out/high_priority_matches.csv` - **Matches with high uniqueness scores**

## Matching Algorithm

The simplified algorithm ([scoring_simple.py](data-build/scoring_simple.py)) works with limited NamUs fields:

### Hard Filters (must pass):
1. **Sex must match** (M/F/Unknown)
   - M must match M
   - F must match F
   - Unknown can match either (with score penalty)
2. **Same state** (geographic constraint)
3. **Age ranges overlap** (allows for estimation errors)
4. **Found date >= Last seen date** (±7 day tolerance)

### Weighted Scoring Components:
- **Sex match** (weight: 2.0)
  - Exact match: 1.0
  - Unknown match: 0.5
- **Age similarity** (weight: 1.5)
  - Based on age range overlap
- **Geographic proximity** (weight: 2.0)
  - Same city + county: 1.0
  - Same county: 0.8
  - Same state: 0.3
- **Race match** (weight: 0.8)
  - Match: 1.0
  - Differ: 0.3 (soft - allows misidentification)
- **Temporal proximity** (weight: 1.2)
  - Within 30 days: 1.0
  - Within 6 months: 0.8
  - Within 1 year: 0.6
  - Decays over time

### Uniqueness Boost (Key Innovation!)
Matches get bonus scores based on how rare they are:
- **MP has ≤3 matches**: +0.15
- **UP has ≤3 matches**: +0.15
- **Both are unique**: +0.30 (cumulative)

**Rationale:** If a missing person only matches 1-3 unidentified persons (or vice versa), those are much more likely to be correct matches!

Final score = base_score (0-1) + uniqueness_boost, capped at 1.0

## Next Steps

1. Download fresh NamUs data exports
2. Run the full pipeline (steps 2-4 above)
3. Review `high_priority_matches.csv` first - these have the best uniqueness signals
4. For each high-priority match, investigate further using NamUs case details
