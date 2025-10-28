# Maria Project - Current Status

## Overview
Missing persons matching system using graph databases (AWS Neptune) and planned RAG semantic search.

**Dataset:** 25,743 Missing Persons + 15,458 Unidentified Persons = 13.9M candidate matches

---

## ✅ Completed

### Data Generation
- [x] Generated Neptune bulk-load CSV files
  - Nodes: 41,201 (3.3MB)
  - Edges: 13,945,843 (720MB)
  - Location: `/Users/a/Projects/Maria/out/neptune_load/`

### S3 Upload
- [x] Created S3 bucket: `maria-neptune-data`
- [x] Uploaded data to: `s3://maria-neptune-data/neptune_load/20251026_190323/`
  - `nodes.csv` ✓
  - `edges.csv` ✓

### Scripts Created
- [x] `generate_neptune_simple.py` - Generate graph data from master CSVs
- [x] `neptune_bulkload.py` - Upload to S3 and trigger Neptune load
- [x] `run_cypher_jobs.py` - Query Neptune database (Gremlin)
- [x] `.env` - Environment configuration

### AWS Setup
- [x] Neptune cluster: `db-neptune-1.cluster-cdmawwgk2f84.us-east-2.neptune.amazonaws.com`
- [x] IAM policies configured:
  - User: `lambdadbuser` (S3 + Neptune access)
  - Role: `AWSNeptuneNotebookRole-createdrole` (Neptune bulk load)

---

## ⏳ In Progress

### Neptune Data Load
**Status:** Loading via Neptune Workbench

**Command used:**
```python
%load -s s3://maria-neptune-data/neptune_load/20251026_190323/ -f csv -p ALLOW_USER_PROPERTY_DUPLICATE -r arn:aws:iam::183295419522:role/service-role/AWSNeptuneNotebookRole-createdrole
```

**Expected time:** 10-15 minutes

**Check status:**
```python
%load_status
```

---

## 📋 Next Steps

### 1. Verify Data Loaded
Once Neptune load completes, verify with:

**Via Neptune Workbench:**
```python
%%gremlin
g.V().count()  # Should return 41,201
g.E().count()  # Should return 13,945,843
```

**Via Local Script (requires VPN/VPC access):**
```bash
python3 data-build/run_cypher_jobs.py stats
```

### 2. Query Top Matches
```bash
# Find matches for a specific MP
python3 data-build/run_cypher_jobs.py find-mp MP-12345 --limit 50

# Find all matches in California
python3 data-build/run_cypher_jobs.py find-state CA --limit 100
```

### 3. Build Static Site
- Extract top 50 matches from Neptune
- Generate JSON for GitHub Pages
- Deploy to `gh-pages` branch

### 4. Implement RAG (Phase 2)
- Add semantic embeddings for case narratives
- Implement vector similarity search
- LLM-powered match explanations

---

## 🗂️ File Structure

```
Maria/
├── data/
│   └── clean/
│       ├── MP_master.csv (25,743 MPs)
│       └── UP_master.csv (15,458 UIDs)
├── data-build/
│   ├── generate_neptune_simple.py ← Generate graph CSVs
│   ├── neptune_bulkload.py ← Upload to Neptune
│   ├── run_cypher_jobs.py ← Query Neptune
│   └── requirements.txt
├── out/
│   └── neptune_load/
│       ├── nodes.csv (3.3MB)
│       └── edges.csv (720MB)
├── .env ← Neptune/AWS configuration
└── NEPTUNE_SETUP.md ← Setup instructions
```

---

## 🔑 Configuration

**Environment Variables (.env):**
```bash
NEPTUNE_ENDPOINT=db-neptune-1.cluster-cdmawwgk2f84.us-east-2.neptune.amazonaws.com
NEPTUNE_S3_BUCKET=maria-neptune-data
NEPTUNE_IAM_ROLE=arn:aws:iam::183295419522:role/service-role/AWSNeptuneNotebookRole-createdrole
AWS_REGION=us-east-2
```

---

## 📊 Data Schema

### Nodes
**MissingPerson** (25,743)
- Properties: namus_number, sex, race, age_min, age_max, height_in, weight_lb, city, state, county

**UnidentifiedPerson** (15,458)
- Same properties as MissingPerson

### Edges
**CANDIDATE_MATCH** (13,945,843)
- Connects: MissingPerson → UnidentifiedPerson
- Match criteria: Same state + Same sex + Overlapping age ranges
- Properties: match_type='state_sex_age'

**Average:** ~542 candidate matches per missing person

---

## 🎯 Project Goals

### Short-term (Current)
1. ✅ Generate graph data
2. ✅ Upload to S3
3. ⏳ Load into Neptune
4. 🔜 Query and verify
5. 🔜 Build static website (GitHub Pages)

### Medium-term
- Monthly auto-updates from NamUs
- Top 50 most likely matches with scoring
- Improved filtering (geographic distance, temporal windows)

### Long-term (Future)
- RAG semantic search (context-aware matching)
- Facial recognition integration
- API for investigators
- Mobile-friendly interface

---

## 🚀 Resume Prompt

To continue this project in a new session:

```
I'm working on the Maria project - a missing persons matching system using AWS Neptune and RAG. Here's where we left off:

COMPLETED:
✅ Generated Neptune bulk-load CSV files with 41,201 nodes and 13,945,843 edges
✅ Uploaded data to S3: s3://maria-neptune-data/neptune_load/20251026_190323/
✅ Created all Python scripts: neptune_bulkload.py, run_cypher_jobs.py, generate_neptune_simple.py
✅ Configured IAM policies for lambdadbuser and AWSNeptuneNotebookRole-createdrole

CURRENT STATUS:
- Loading data into Neptune via Workbench (10-15 min expected)
- Load command: %load -s s3://maria-neptune-data/neptune_load/20251026_190323/ -f csv ...

NEXT STEPS:
1. Verify data loaded successfully
2. Query top matches
3. Build static site for GitHub Pages
4. Implement RAG semantic search (Phase 2)

MY GOAL: Free static website with top 50 matches, monthly updates, eventually add RAG and facial recognition.

Neptune endpoint: db-neptune-1.cluster-cdmawwgk2f84.us-east-2.neptune.amazonaws.com
Region: us-east-2

Can you help me continue from here?
```

---

## 📝 Notes

- Neptune is in VPC - can only access via Workbench, VPN, or EC2
- IAM user has S3 write permissions to `maria-neptune-*` buckets
- Data matching uses state/sex/age filters (basic - RAG will improve this)
- Total data size: ~723MB (may take 10-15 min to load)

---

**Last Updated:** October 26, 2024
**Status:** Neptune bulk load in progress
