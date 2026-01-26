# Likely Interview Questions

Questions an interviewer might ask about the Maria project, organized by topic.

---

## Algorithm Design

### Q: Why did you choose a weighted scoring approach instead of machine learning?

**Answer:** Several factors led to this decision:

1. **Interpretability** - In missing persons work, investigators need to understand *why* a match is suggested. A weighted scoring system provides clear explanations ("same county, overlapping age range, found 45 days later") that ML black boxes cannot.

2. **Limited labeled data** - There's no large dataset of confirmed MP↔UP matches to train on. The few confirmed matches would be insufficient for supervised learning.

3. **Domain constraints** - Hard filters (sex match, temporal validity) are non-negotiable rules that must always be enforced. A pure ML approach might learn to violate these constraints.

4. **Auditability** - Cases may be used in legal proceedings. A transparent scoring system can be explained to a jury; a neural network cannot.

**Trade-off acknowledged:** ML could potentially discover non-obvious patterns. A hybrid approach (ML-generated features fed into interpretable scoring) could be future work.

---

### Q: Explain the uniqueness boost. Why is it the "key innovation"?

**Answer:** The uniqueness boost addresses a fundamental problem in entity matching: **signal dilution**.

Consider two scenarios:
- **Scenario A:** A missing person matches 500 unidentified remains
- **Scenario B:** A missing person matches only 2 unidentified remains

In Scenario A, even a high-scoring match might be coincidental—there are simply too many candidates. In Scenario B, having only 2 matches is *itself* a strong signal that both demographic and temporal filters are working well.

The boost formula:
```
boost = mp_boost(mp_match_count) + up_boost(up_match_count)
```

Where each side contributes:
- 1-2 matches: +0.25
- 3-5 matches: +0.15
- 6-10 matches: +0.08
- 11-15 matches: +0.03

**Result:** This single enhancement reduced actionable matches from 5.68M to ~165 while increasing the true positive rate in top results.

---

### Q: How did you determine the scoring weights (2.0 for sex, 1.5 for age, etc.)?

**Answer:** The weights were set through domain analysis and iterative refinement:

1. **Sex (2.0):** Highest weight because sex is reliably determinable from remains and rarely misidentified. Mismatch is a hard reject.

2. **Geography (2.0):** Equal to sex because most missing persons are found within 100 miles of where they disappeared. Strong geographic correlation.

3. **Age (1.5):** Important but subject to estimation error. Forensic age estimation from remains has ±5-10 year uncertainty.

4. **Temporal (1.2):** Relevant but has edge cases (cold cases found decades later can still be matches).

5. **Race (0.8):** Lower weight because race determination from remains is unreliable, and self-reported race may differ from forensic assessment.

**Validation approach:** Manually reviewed top-100 matches across different weight configurations, looking for known confirmed matches in NamUs data.

---

## Data Engineering

### Q: How do you handle the O(n×m) pair generation problem?

**Answer:** With 90K missing persons × 14K unidentified remains = 1.26 billion potential pairs, brute force is infeasible.

**Solution layers:**

1. **Hard filter elimination (pair generation phase)**
   - Sex mismatch: Eliminates ~50% immediately
   - Age range non-overlap: Eliminates ~60% of remaining
   - Temporal impossibility: Eliminates another ~20%
   - Result: ~3.5M pairs (0.3% of original)

2. **Vectorized operations**
   - Used pandas merge (Cartesian product) instead of nested loops
   - Sex-based chunking: Process M×M, then F×F, then Unknown combinations
   - Chunk size of 200 MPs at a time to manage memory

3. **Uniqueness filter (scoring phase)**
   - Discard pairs where either side has >15 matches
   - Result: ~165 actionable matches

**Complexity reduction:** From O(n×m) = O(1.26B) to O(k) where k ≈ 3.5M after filters, then to 165 final matches.

---

### Q: Why CSV files instead of a database?

**Answer:** Pragmatic trade-off for this use case:

**Advantages of CSV:**
- Zero infrastructure (no database server)
- Portable (share via GitHub, email)
- Transparent (human-readable, diffable)
- Pandas integration is seamless

**Disadvantages:**
- No indexing (full table scans)
- No concurrent writes
- Memory-bound (must fit in RAM)

**Why it works here:**
- Batch processing, not real-time queries
- Single user (me), not multi-tenant
- Data size is manageable (~4GB total)
- Pandas vectorization makes performance acceptable

**When I'd switch to a database:**
- Multi-source integration (normalized schema)
- Real-time API access
- Multi-user access patterns
- Data exceeding RAM capacity

---

### Q: How would you scale this to real-time matching?

**Answer:** The current system is batch-oriented. Real-time would require:

1. **Precomputed indices**
   - Build inverted indices on sex, state, age buckets
   - When new case arrives, only compare against matching index entries
   - Use spatial indices (KD-tree, R-tree) for geographic proximity

2. **Incremental updates**
   - Don't recompute all pairs when one case is added
   - Compute pairs only for the new case
   - Update match counts incrementally (affects uniqueness boost)

3. **Database with proper indexing**
   - PostgreSQL with composite indices on (sex, state, age_bucket)
   - Or Elasticsearch for fuzzy matching

4. **Caching layer**
   - Cache rarity scores (changes slowly)
   - Cache match counts per case

5. **Architecture change**
   - Event-driven: New case → compute pairs → score → notify
   - Message queue (Kafka/SQS) for case ingestion
   - Separate workers for pair generation vs scoring

---

## System Design

### Q: How would you add a new data source (e.g., FBI)?

**Answer:** The architecture should support pluggable data sources:

```
DataSource (abstract base)
├── NamusSource (existing)
├── FBISource (new)
├── CharleyProjectSource (new)
└── DoeNetworkSource (new)
```

**Each source implements:**
1. `fetch()` - Download/access raw data
2. `parse()` - Extract to common schema
3. `normalize()` - Standardize fields (state names, date formats)
4. `validate()` - Quality checks

**Common schema:**
```python
UnifiedCase:
    source_id: str          # "NAMUS-MP-12345"
    case_type: MP | UP
    sex: M | F | Unknown
    age_min: int
    age_max: int
    race: str
    state: str
    county: str
    city: str
    date: date              # last_seen or found
    source: str             # "NamUs", "FBI", etc.
```

**Deduplication challenge:** Same case may appear in multiple sources. Need matching logic to link records (probably by name + location + date similarity).

---

### Q: What metrics would you track in production?

**Answer:**

**Quality metrics:**
- Precision@K: Of top-K matches, how many are confirmed?
- Recall: Of known confirmed matches, how many did we surface?
- Match rank: Where do confirmed matches appear in our rankings?

**Operational metrics:**
- Pipeline latency (end-to-end processing time)
- Memory usage per stage
- Pair generation reduction ratio
- Score distribution histogram

**Data quality metrics:**
- % of cases with missing fields
- % of cases filtered as infant remains
- State coverage
- Temporal coverage (oldest/newest cases)

**Investigation metrics:**
- Matches reviewed per day
- Status transition rates (To Review → Investigating → Match/Excluded)
- Time to first investigation action

---

## Performance Optimization

### Q: Walk me through a specific optimization you made.

**Answer:** The sex-based chunking optimization in pair generation.

**Problem:** Generating all pairs at once caused memory exhaustion (90K × 14K = 1.26B rows).

**Initial approach:** Process all at once → OOM crash.

**First optimization:** Process in chunks of MPs.
- But still comparing each chunk against ALL UPs.

**Key insight:** Male MPs can only match Male or Unknown UPs. So:
- Male MPs (45K) × Male/Unknown UPs (8K) = 360M pairs
- Female MPs (40K) × Female/Unknown UPs (5K) = 200M pairs
- Unknown MPs (5K) × All UPs (14K) = 70M pairs

**Implementation:**
```python
sex_matching_rules = [
    ('M', ['M', 'Unknown']),
    ('F', ['F', 'Unknown']),
    ('Unknown', ['M', 'F', 'Unknown'])
]

for mp_sex, up_sexes in sex_matching_rules:
    mp_group = mp_by_sex[mp_sex]
    up_group = concat([up_by_sex[s] for s in up_sexes])
    # Process in CHUNK_SIZE batches
```

**Result:**
- Memory usage reduced by ~50%
- Processing time reduced by ~40% (fewer comparisons)
- No accuracy loss (sex filter would have eliminated these pairs anyway)

---

### Q: The scoring engine uses numpy—why not Cython or Numba?

**Answer:** Profiling showed pandas/numpy vectorization was sufficient.

**Benchmarks:**
- Pure Python loops: ~4 hours for 3.5M pairs
- Pandas/numpy vectorized: ~3 minutes
- Numba JIT: ~2.5 minutes (marginal improvement)

**Why not Cython/Numba:**
1. **Diminishing returns** - 100x speedup from vectorization already achieved
2. **Development overhead** - Cython requires compilation, Numba has type annotation overhead
3. **Debugging difficulty** - Harder to inspect intermediate values
4. **Dependency burden** - Cython requires C compiler on user's machine

**When I would use them:**
- If processing time exceeded 30 minutes
- If running in a tight loop (real-time matching)
- If vectorization couldn't be applied (complex conditional logic)

---

## Domain Expertise

### Q: What domain knowledge did you incorporate into the scoring?

**Answer:**

1. **Age projection** - Missing persons age over time. A 20-year-old missing in 2000 would be ~25 if found in 2005. The scoring projects MP age forward before comparing.

2. **Race uncertainty** - Forensic anthropology can estimate ancestry from skeletal features, but it's imperfect. Race is a soft match (0.3 for mismatch) not a hard filter.

3. **Temporal windows** - Most found remains are discovered within months to years. The scoring uses decay functions that reflect this distribution.

4. **Geographic patterns** - Most people are found near where they disappeared. County-level matching gets 0.85 score even without city match.

5. **Era prioritization** - Cases from 1980-2006 have better documentation and are within DNA-matching timelines. These get a +0.10 era boost.

6. **Infant remains exclusion** - Unidentified remains without age estimates are typically infant/fetal remains, which have different matching considerations. These are filtered out.

---

### Q: How did you validate that the algorithm works?

**Answer:** Multiple validation approaches:

1. **Known matches** - NamUs marks some cases as "closed" with identified matches. Verified these appeared highly ranked in our output.

2. **Manual review** - Examined top-100 matches for face validity (locations make sense, timelines work, demographics align).

3. **Sanity checks**:
   - No Male↔Female matches (hard filter working)
   - No temporal impossibilities (found before missing)
   - Age ranges overlap on all matches

4. **Distribution analysis**:
   - Score histogram should be roughly normal
   - Uniqueness boost should correlate with expert review outcomes

5. **A/B comparison** - Compared results with and without uniqueness boost. With boost, known matches ranked higher.

**Limitation:** No large-scale ground truth dataset exists. True validation would require partnership with law enforcement agencies.

---

## Trade-offs and Decisions

### Q: What's a decision you'd revisit with more time?

**Answer:** The county adjacency heuristics.

**Current implementation:** Uses name-based heuristics ("North County" + "South County" → probably adjacent). This is unreliable.

**Better approach:** Use actual county adjacency data from Census Bureau TIGER files. This would:
- Provide authoritative adjacency relationships
- Handle edge cases (counties across state lines)
- Enable distance-based scoring refinements

**Why I didn't do it initially:**
- Time constraint
- Marginal impact (most matches are same-county anyway)
- Complexity of loading/parsing TIGER data

**Implementation plan:**
1. Download Census county adjacency file
2. Build lookup table: county_fips → adjacent_county_fips[]
3. Replace heuristics with database lookup
4. Consider weighted adjacency (shares long border vs corner touch)

---

### Q: Why not use name matching?

**Answer:** Names are deliberately excluded from the current scoring, but this is a considered omission.

**Arguments against name matching:**
1. **Unidentified remains have no name** - The UP side has no name to match against
2. **Alias/nickname variations** - "William" vs "Bill" vs "Billy"
3. **Data quality** - Misspellings in both directions
4. **Privacy concerns** - Names are sensitive data

**Where name matching could help:**
1. **Possible identification field** - Some UPs have "possibly named John Doe" notes
2. **Cross-source deduplication** - Matching the same MP across NamUs, FBI, Charley Project
3. **Family relationship inference** - Last names might indicate family connections

**If implemented:** Would use fuzzy matching (Levenshtein, Jaro-Winkler, Soundex) with low weight (~0.3) to avoid false positives.

---

## Behavioral / Problem-Solving

### Q: Tell me about a challenge you faced and how you solved it.

**Answer:** The "5.68 million matches" problem.

**Initial state:** After applying all hard filters and scoring, we had 5.68 million "valid" matches. Reviewing even 1% would take years.

**Analysis:** Most high-scoring matches had hundreds of candidates on each side—these were demographic "common patterns" (e.g., 25-35 year old white male in California).

**Insight:** The matches with few candidates were more actionable. If someone only matches 2 people, those 2 matches are worth investigating.

**Solution:** The uniqueness boost and filter:
1. Count matches per MP and per UP
2. Filter to pairs where both sides have ≤15 matches
3. Boost score based on match scarcity

**Result:** 5.68M → 165 matches. Top matches now had clear investigative value.

**Lesson learned:** Sometimes the best feature engineering comes from understanding what makes results *actionable*, not just *accurate*.

---

### Q: How would you explain this project to a non-technical stakeholder?

**Answer:**

"Imagine you're trying to find a needle in a haystack, but there are 90,000 possible needles and 14,000 haystacks. That's the missing persons matching problem.

Maria is a filtering system. It starts with over a billion possible combinations and asks: which of these could even theoretically be matches? It eliminates the impossible ones first—wrong gender, wrong time period, incompatible ages.

Then for the remaining candidates, it scores how *likely* each match is based on factors investigators care about: Was the person found near where they went missing? Does the age make sense given how much time passed? Does the physical description match?

The key insight is that scarcity itself is a signal. If a missing person only matches 2 unidentified remains in the whole database, those 2 are much more likely to be real matches than someone who matches 500 people. We boost those rare matches to the top.

The result: from billions of possibilities down to about 165 high-priority cases that deserve human investigation."
