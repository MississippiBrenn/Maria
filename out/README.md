# Output Files

This directory contains generated matching results. Files are not tracked in git due to size.

## Generated Files

Run the pipeline to generate these files:

```bash
source venv/bin/activate
python3 data-build/build_graph_artifacts_optimized.py
python3 data-build/build_candidates_vectorized.py
```

### Output Files:

- **all_matches_scored.csv** (1.9GB) - All 165 unique matches where both sides have ≤10 candidates
- **high_priority_matches.csv** - 13 matches where both sides have ≤5 candidates
- **match_investigation_tracker.csv** - Simple tracking spreadsheet for investigations
- **top_100_summary.csv** - Top matches in readable format
- **candidate_pairs.csv** - Intermediate file from pair generation
- **candidates.jsonl** - Top 20 matches per MP in JSON format
- **cases_mp.json** - Missing person case data
- **cases_up.json** - Unidentified person case data

### Priority Files for Investigation:

1. **match_investigation_tracker.csv** - Start here! Open in Excel/Sheets
2. **high_priority_matches.csv** - Focus on these 13 matches first
