# Maria
# 1. Create a virtual environment (only once)
python3 -m venv .venv

# 2. Activate the venv (every time you start fresh)
source .venv/bin/activate

# 3. Install Python dependencies
pip install -r data-build/requirements.txt

# 4. Run the mapper to normalize raw CSVs → data/sample_mp.csv + data/sample_uid.csv
python3 data-build/namus_mapper.py

# 5. Build graph artifacts (nodes/edges, JSON cases) into out/
python3 data-build/build_graph_artifacts.py

# 6. Score candidate matches and write out/candidates.jsonl
python3 data-build/build_candidates.py

