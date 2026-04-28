#!/bin/bash
# Rebuild candidate pairs with improved algorithm

set -e

echo "=========================================="
echo "Maria - Rebuild Candidate Pairs"
echo "=========================================="

cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo ""
echo "Step 1: Generate candidate pairs (v2 - with improvements)"
echo "------------------------------------------"
python3 data-build/generate_candidate_pairs_v2.py

echo ""
echo "Step 2: Prioritize matches for review"
echo "------------------------------------------"
python3 data-build/prioritize_matches.py

echo ""
echo "=========================================="
echo "Build complete!"
echo ""
echo "Output files in out/:"
echo "  - candidate_pairs.csv (all pairs)"
echo "  - candidate_pairs_high_priority.csv (Tier 1-2)"
echo "  - candidate_pairs_tier1.csv (Tier 1 only)"
echo "  - candidate_pairs_prioritized.csv (scored and sorted)"
echo "  - best_match_per_up.csv (recommended for review)"
echo "  - top_matches_for_review.csv (top 1000)"
echo ""
echo "To start reviewing: python3 review_matches.py"
echo "=========================================="
