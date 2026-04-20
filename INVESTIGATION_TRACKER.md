# Maria Investigation Tracker

Tracking real-world investigation of high-priority matches. Going year by year starting from 1950.

## Status Legend
- **NEW** - Not yet investigated
- **RESEARCHING** - Looking into case details
- **CONTACTED** - Reached out to sheriff/coroner/agency
- **AWAITING** - Waiting for response
- **CONFIRMED** - Match confirmed by authorities
- **RULED_OUT** - Match ruled out by authorities
- **EXCLUDED** - Added to exclusion list (see EXCLUSIONS.md)

---

## 1950

### Match 1: Lora Skaggs <-> UP10026
| Field | Missing Person | Unidentified |
|-------|---------------|--------------|
| ID | MP29446 | UP10026 |
| Name | Lora Skaggs | ME/C Case 06-10419 |
| Last Seen / Found | 1948-01-01 | 1950-05-10 |
| Location | Charleston, Kanawha, WV | Bath (Berkeley Springs), Morgan, WV |
| Race | White / Caucasian | White / Caucasian |
| Score | 1.000 | |

**NamUs Links:**
- MP: https://namus.gov/MissingPersons/Case#/29446
- UP: https://namus.gov/UnidentifiedPersons/Case#/10026

**Status:** RESEARCHING

**Investigation Notes:**
- [x] Review NamUs case details
- [ ] Call WVSP Berkeley Springs (304) 258-0000 - **Hours: 9am-3pm**
- [ ] Ask if MP29446 and UP10026 have been compared
- [ ] Document response

**Key Evidence:**
- Both have RED/AUBURN hair (rare)
- Both have APPENDECTOMY SCAR
- Same investigating agency (WVSP Berkeley Springs) for both cases
- Timeline fits: MP disappeared 1948-1950, UP found May 10, 1950
- Photo comparison: Not inconsistent, but MP photo too degraded for confirmation

**Concerns:**
- Age discrepancy: Lora would be ~29, UP estimated 35-50
- UP has hysterectomy scar not mentioned in MP record
- UP has additional scars (wrist, forehead) not in MP file

**Timeline:**
| Date | Action | Result |
|------|--------|--------|
| 2026-04-20 | Reviewed NamUs case details | Strong circumstantial match |
| 2026-04-20 | Photo comparison | Inconclusive due to MP photo quality |
| 2026-04-20 | Called WVSP | Voicemail - call back 9am-3pm |

---

### Match 2: Frances Sessions <-> UP75929 - EXCLUDED

| Field | Missing Person | Unidentified |
|-------|---------------|--------------|
| ID | MP89538 | UP75929 |
| Name | Frances Sessions | ME/C Case 20UC11758 |

**Status:** EXCLUDED

**Reason:** UP75929 confirmed as infant remains via NamUs page review. Added to exclusions list.

---

## 1963

### Matches 1-3: UP65541 - EXCLUDED

UP65541 (ME/C Case U-2349) matched to 3 different MPs:
- Connie Smith (MP4525)
- Frances Tuccitto (MP14289)
- Anna Kenneway (MP21421)

**Status:** EXCLUDED

**Reason:** UP65541 confirmed as fetus/infant via NamUs page review. All 3 matches excluded.

---

## 1970

### UP12137 - 5 Possible Matches

| Field | Unidentified |
|-------|--------------|
| ID | UP12137 |
| ME/C Case | 197000765 |
| Found | 1970-10-01 |
| Location | Rural, Utah |
| Sex | Female |
| Race | White / Caucasian |
| Age | 20-60 |
| Height | ~5'3" |

**NamUs Link:** https://namus.gov/UnidentifiedPersons/Case#/12137

**Possible Matches (all within 3" of height):**

| MP | Name | Last Seen | From |
|----|------|-----------|------|
| MP58753 | Sarah Snow | 1953-09-13 | Springville, UT |
| MP127055 | Ruby Bosen | 1955-07-29 | Duchesne, UT |
| MP73780 | Vilate Young | 1956-07-04 | Widtsoe, UT |
| MP8784 | Dennise Sullivan | 1961-07-04 | Moab, UT |
| MP89538 | Frances Sessions | 1946-05-20 | Ogden, UT |

**Status:** AWAITING

**Investigation Notes:**
- [x] Review UP12137 NamUs page for additional details
- [x] Check each MP for distinguishing features - all within 3" of 5'3"
- [x] Contact Utah authorities
- [ ] Await results from Detective Gray

**Timeline:**
| Date | Action | Result |
|------|--------|--------|
| 2026-04-20 | Initial review | 5 viable candidates, none excluded |
| 2026-04-20 | Called Washington County Sheriff | Spoke with Detective Gray |
| 2026-04-20 | | Body has been EXHUMED and is being checked |
| 2026-04-20 | | Passed on all 5 MP candidates to detective |

---

## Investigation Insights

### What's Working
- (Add notes as we learn what contact methods work best)

### What's Not Working
- NamUs bulk export lacks key fields (Estimated Age Group, Height/Weight) that would help filter infants automatically

### Process Improvements
- Manual review of "needs_age_review" flagged cases catches infants missed by algorithm
- Consider scraping age group field from NamUs pages in future

---

## Statistics

| Year | Matches | Researching | Awaiting | Confirmed | Ruled Out | Excluded |
|------|---------|-------------|----------|-----------|-----------|----------|
| 1950 | 2 | 1 | 0 | 0 | 0 | 1 |
| 1963 | 3 | 0 | 0 | 0 | 0 | 3 |
| 1970 | 5 | 5 | 0 | 0 | 0 | 0 |

*Last updated: 2026-04-20 - Paused at 1970, next up: 1972*
