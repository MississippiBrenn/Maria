# Case Exclusions

Cases listed here are excluded from future matching runs. This is the master exclusion list - do not edit on the fly.

## How to Add Exclusions

Add cases to the appropriate section below with:
- Case ID (MP##### or UP#####)
- Reason for exclusion
- Date added

The data pipeline will read `data/exclusions.csv` for programmatic filtering.

---

## Excluded Missing Persons (MP)

| ID | Name | Reason | Date Added |
|----|------|--------|------------|
| | | | |

---

## Excluded Unidentified Persons (UP)

| ID | ME/C Case | Reason | Date Added |
|----|-----------|--------|------------|
| UP75929 | 20UC11758 | INFANT - confirmed via NamUs page | 2026-04-20 |
| UP65541 | U-2349 | FETUS/INFANT - confirmed via NamUs page | 2026-04-20 |

---

## Exclusion Reasons Reference

- **RESOLVED** - Case has been resolved/identified
- **DUPLICATE** - Duplicate entry in NamUs
- **DATA_QUALITY** - Poor data quality makes matching unreliable
- **OUT_OF_SCOPE** - Case type outside project scope (e.g., international)
- **AGENCY_REQUEST** - Excluded at request of investigating agency
- **FALSE_POSITIVE** - Confirmed not a match after investigation

---

## Notes

- Review exclusions periodically - some may become relevant again
- Always document the reason for exclusion
- Check NamUs for case status updates before excluding
