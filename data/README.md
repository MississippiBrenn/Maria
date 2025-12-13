# Data Directory

This directory contains NamUs data files. Files are not tracked in git due to size.

## Directory Structure

### raw/
Raw CSV downloads from NamUs (not tracked in git)
- `Missing/` - Missing person CSV files
- `Unidentified/` - Unidentified person CSV files

### compiled/
Merged CSV files (not tracked in git)
- `missing_persons.csv` - All MP downloads merged
- `unidentified_persons.csv` - All UP downloads merged

### clean/
Processed master files (not tracked in git)
- `MP_master.csv` - Processed missing persons
- `UP_master.csv` - Processed unidentified persons

## How to Generate

1. Download data from NamUs and place in `raw/Missing/` and `raw/Unidentified/`
2. Run merge script:
   ```bash
   python3 data-build/merge_raw_downloads.py
   ```
3. Run processing script:
   ```bash
   python3 data-build/process_namus_downloads.py
   ```
