#!/bin/bash
# Run the complete Maria matching pipeline

set -e  # Exit on error

echo "=== Maria Matching Pipeline ==="
echo ""

# Check if raw data files exist
if [ ! -f "data/raw/missing_persons.csv" ] || [ ! -f "data/raw/unidentified_persons.csv" ]; then
    echo "ERROR: Missing raw data files!"
    echo "Please download NamUs exports and place them in data/raw/ as:"
    echo "  - data/raw/missing_persons.csv"
    echo "  - data/raw/unidentified_persons.csv"
    echo ""
    echo "Or run with custom paths:"
    echo "  python3 data-build/process_namus_downloads.py --mp /path/to/mp.csv --up /path/to/up.csv"
    exit 1
fi

echo "Step 1: Processing NamUs downloads..."
python3 data-build/process_namus_downloads.py \
    --mp data/raw/missing_persons.csv \
    --up data/raw/unidentified_persons.csv

echo ""
echo "Step 2: Generating candidate pairs..."
python3 data-build/build_graph_artifacts_simple.py

echo ""
echo "Step 3: Scoring matches with uniqueness boost..."
python3 data-build/build_candidates_simple.py

echo ""
echo "=== Pipeline Complete! ==="
echo ""
echo "Output files:"
echo "  - out/high_priority_matches.csv  (START HERE - best matches!)"
echo "  - out/all_matches_scored.csv     (all valid matches)"
echo "  - out/candidates.jsonl           (top 20 per MP)"
echo ""
