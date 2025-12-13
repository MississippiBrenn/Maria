# Project Notes - Maria Missing Persons Matching System

## Project Context

**Goal:** Create a free static website that matches missing persons (MPs) with unidentified persons (UIDs) using AWS Neptune graph database.

**Current Status (Nov 3, 2025):**
- Neptune cluster deployed in us-east-2 with 41,201 nodes loaded
- 13.9M candidate matches generated but quality is POOR
- Current matching only uses state + sex + age (no dates, no scoring)
- Need to improve match quality before deploying to website

## Data Sources

**Raw NamUs Data:**
- Missing Persons: `raw/namus_missing.csv` (sample only - need full data)
- Unidentified Persons: `raw/namus_unidentified.csv` (sample only)

**Available Fields:**
- Demographics: sex, race, age_min, age_max, height, weight, eye color, hair color
- Location: lat, lon, state, city
- Temporal: date_missing (MP), date_found (UID)
- Descriptive: tattoos, clothing, circumstances

**Current Problem:**
- Master CSV files (MP_master.csv, UP_master.csv) are missing or incomplete
- Neptune data was generated without dates and coordinates
- Missing critical fields needed for quality scoring

## Match Quality Issues Identified (Nov 3, 2025)

**Problem:** Query returned 100 matches all to the SAME UID (UP140320)
- Person went missing in 2021 matched to body found in 1974 (IMPOSSIBLE)
- No temporal validation
- No scoring/ranking
- Just matching state + sex + age overlap

**What We Need:**
1. **Temporal validation:** UID found date must be >= MP missing date
2. **Match scoring:** Weight multiple factors (0.0 to 1.0 score)
3. **Geographic distance:** Calculate km between locations
4. **Better filtering:** Top N matches per MP, not all possible combos

## Improved Matching Algorithm Design

**Scoring Factors (weighted):**
1. **Temporal consistency** (HARD FILTER): found_date >= missing_date
2. **Geographic distance** (weight: 1.0): Closer is better (exponential decay)
3. **Age similarity** (weight: 1.2): Age range overlap
4. **Physical characteristics:**
   - Height (weight: 0.8)
   - Weight (weight: 0.4)
   - Sex (weight: 1.0) - must match
   - Race (weight: 0.6) - partial credit for uncertain
   - Eye color (weight: 0.5)
5. **Descriptive features:**
   - Tattoo keywords (weight: 1.4)
   - Clothing items (weight: 0.8)

**Output:** Each edge gets a score (0.0-1.0), keep only top 50-100 matches per MP

## AWS Neptune Configuration

**Cluster:** db-neptune-1.cluster-cdmawwgk2f84.us-east-2.neptune.amazonaws.com
**Region:** us-east-2
**S3 Bucket:** maria-neptune-data
**Workbench:** aws-neptune-maria-DB
**IAM Role:** arn:aws:iam::183295419522:role/service-role/AWSNeptuneNotebookRole-createdrole

## Next Steps

1. Locate or regenerate full MP_master.csv and UP_master.csv files with all fields
2. Create new generation script with scoring algorithm
3. Regenerate Neptune CSV files with match scores
4. Upload to S3 and reload into Neptune
5. Query top 100 SCORED matches
6. Review deployed website code
7. Update website with quality matches

## User Preferences

- Focus on match quality over quantity
- Temporal consistency is CRITICAL (can't match impossible dates)
- Want top 50-100 best matches for static site
- Will add RAG/semantic search later (Phase 2)

---

*Last updated: November 3, 2025*
