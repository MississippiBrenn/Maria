"""
NamUs Data Source Implementation

Handles loading and parsing data from the National Missing and
Unidentified Persons System (NamUs).
"""

import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

from .base import DataSource, UnifiedCase, CaseType, ValidationResult


class NamusSource(DataSource):
    """
    Data source for NamUs (National Missing and Unidentified Persons System).

    NamUs is the primary US government database for missing persons and
    unidentified human remains. Data is exported via bulk CSV downloads
    from https://namus.gov/

    Supported export formats:
    - Missing Persons bulk export (CSV)
    - Unidentified Persons bulk export (CSV)
    """

    # Column mappings from NamUs exports to unified schema
    MP_COLUMN_MAP = {
        'Case Number': 'source_id',
        'Biological Sex': 'sex',
        'Race / Ethnicity': 'race',
        'Current Age From': 'age_min',
        'Current Age To': 'age_max',
        'City of Last Contact': 'city',
        'County of Last Contact': 'county',
        'State of Last Contact': 'state',
        'Date of Last Contact': 'event_date',
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'Date Modified': 'date_modified',
    }

    UP_COLUMN_MAP = {
        'Case Number': 'source_id',
        'Biological Sex': 'sex',
        'Race / Ethnicity': 'race',
        'Age Estimate From': 'age_min',
        'Age Estimate To': 'age_max',
        'City of Recovery': 'city',
        'County of Recovery': 'county',
        'State of Recovery': 'state',
        'Date of Discovery': 'event_date',
        'ME/C Case Number': 'mec_case',
        'Date Modified': 'date_modified',
    }

    def __init__(self):
        """Initialize NamUs data source."""
        super().__init__("NamUs")

    def load_missing_persons(self, path: str) -> List[UnifiedCase]:
        """
        Load missing persons data from NamUs CSV export.

        Args:
            path: Path to NamUs MP CSV file

        Returns:
            List of UnifiedCase objects
        """
        df = pd.read_csv(path)
        cases = []

        for _, row in df.iterrows():
            try:
                case = self.parse_row(row.to_dict(), CaseType.MISSING_PERSON)
                cases.append(case)
            except Exception as e:
                print(f"Warning: Failed to parse MP row {row.get('Case Number', '?')}: {e}")

        self._cases.extend(cases)
        return cases

    def load_unidentified_persons(self, path: str) -> List[UnifiedCase]:
        """
        Load unidentified persons data from NamUs CSV export.

        Args:
            path: Path to NamUs UP CSV file

        Returns:
            List of UnifiedCase objects
        """
        df = pd.read_csv(path)
        cases = []

        for _, row in df.iterrows():
            try:
                case = self.parse_row(row.to_dict(), CaseType.UNIDENTIFIED_PERSON)
                cases.append(case)
            except Exception as e:
                print(f"Warning: Failed to parse UP row {row.get('Case Number', '?')}: {e}")

        self._cases.extend(cases)
        return cases

    def parse_row(self, row: Dict[str, Any], case_type: CaseType) -> UnifiedCase:
        """
        Parse a single NamUs row into a UnifiedCase.

        Args:
            row: Dictionary of field values from CSV
            case_type: Type of case (MP or UP)

        Returns:
            UnifiedCase object
        """
        col_map = self.MP_COLUMN_MAP if case_type == CaseType.MISSING_PERSON else self.UP_COLUMN_MAP

        # Extract and normalize fields
        source_id = str(row.get('Case Number', '')).strip()

        sex = self._normalize_sex(row.get('Biological Sex', ''))
        race = str(row.get('Race / Ethnicity', '')).strip()

        # Age handling differs between MP and UP
        if case_type == CaseType.MISSING_PERSON:
            age_min = self._parse_int(row.get('Current Age From'))
            age_max = self._parse_int(row.get('Current Age To'))
            city = str(row.get('City of Last Contact', '')).strip()
            county = str(row.get('County of Last Contact', '')).strip()
            state = self._normalize_state(row.get('State of Last Contact', ''))
            event_date = self._parse_date(row.get('Date of Last Contact'))
            first_name = str(row.get('First Name', '')).strip()
            last_name = str(row.get('Last Name', '')).strip()
            mec_case = ''
        else:
            age_min = self._parse_int(row.get('Age Estimate From'))
            age_max = self._parse_int(row.get('Age Estimate To'))
            city = str(row.get('City of Recovery', '')).strip()
            county = str(row.get('County of Recovery', '')).strip()
            state = self._normalize_state(row.get('State of Recovery', ''))
            event_date = self._parse_date(row.get('Date of Discovery'))
            first_name = ''
            last_name = ''
            mec_case = str(row.get('ME/C Case Number', '')).strip()

        # Expand age range by ±2 years for estimation error
        if age_min is not None:
            age_min = max(0, age_min - 2)
        if age_max is not None:
            age_max = age_max + 2

        date_modified = self._parse_date(row.get('Date Modified'))

        return UnifiedCase(
            source_id=source_id,
            source_name=self.source_name,
            case_type=case_type,
            sex=sex,
            race=race,
            age_min=age_min,
            age_max=age_max,
            city=city,
            county=county,
            state=state,
            country='US',
            event_date=event_date,
            first_name=first_name,
            last_name=last_name,
            mec_case=mec_case,
            date_modified=date_modified,
            raw_data=row,
        )

    def _normalize_sex(self, value: Any) -> str:
        """Normalize sex value to M/F/Unknown."""
        if not value:
            return 'Unknown'

        v = str(value).strip().upper()

        if v in ('M', 'MALE'):
            return 'M'
        elif v in ('F', 'FEMALE'):
            return 'F'
        else:
            return 'Unknown'

    def _normalize_state(self, value: Any) -> str:
        """Normalize state to 2-letter code."""
        if not value:
            return ''

        v = str(value).strip().upper()

        # Handle common state name variations
        state_map = {
            'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT',
            'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI',
            'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
            'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME',
            'MARYLAND': 'MD', 'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI',
            'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
            'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
            'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM',
            'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND',
            'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA',
            'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD',
            'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
            'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
            'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC',
            'PUERTO RICO': 'PR', 'GUAM': 'GU', 'VIRGIN ISLANDS': 'VI',
        }

        # If already a 2-letter code
        if len(v) == 2 and v.isalpha():
            return v

        return state_map.get(v, v[:2] if len(v) >= 2 else '')

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """Parse date value to datetime object."""
        if not value or pd.isna(value):
            return None

        v = str(value).strip()
        if not v:
            return None

        # Try common date formats
        formats = [
            '%m/%d/%Y',
            '%Y-%m-%d',
            '%m-%d-%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_int(self, value: Any) -> Optional[int]:
        """Parse integer value, handling NaN and empty strings."""
        if value is None or pd.isna(value):
            return None

        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def export_to_master_csv(
        self,
        output_dir: str,
        cases: Optional[List[UnifiedCase]] = None
    ) -> Dict[str, str]:
        """
        Export cases to Maria master CSV format.

        Args:
            output_dir: Directory to write CSV files
            cases: List of cases (uses self._cases if not provided)

        Returns:
            Dict with paths to created files
        """
        if cases is None:
            cases = self._cases

        os.makedirs(output_dir, exist_ok=True)

        # Split by case type
        mp_cases = [c for c in cases if c.case_type == CaseType.MISSING_PERSON]
        up_cases = [c for c in cases if c.case_type == CaseType.UNIDENTIFIED_PERSON]

        paths = {}

        # Export MP
        if mp_cases:
            mp_df = pd.DataFrame([{
                'id': c.unified_id,
                'first_name': c.first_name,
                'last_name': c.last_name,
                'sex': c.sex,
                'race': c.race,
                'age_min': c.age_min,
                'age_max': c.age_max,
                'last_seen_date': c.event_date.isoformat() if c.event_date else '',
                'city': c.city,
                'county': c.county,
                'state': c.state,
                'date_modified': c.date_modified.isoformat() if c.date_modified else '',
            } for c in mp_cases])

            mp_path = os.path.join(output_dir, 'MP_master.csv')
            mp_df.to_csv(mp_path, index=False)
            paths['mp'] = mp_path

        # Export UP
        if up_cases:
            up_df = pd.DataFrame([{
                'id': c.unified_id,
                'mec_case': c.mec_case,
                'sex': c.sex,
                'race': c.race,
                'age_min': c.age_min,
                'age_max': c.age_max,
                'found_date': c.event_date.isoformat() if c.event_date else '',
                'city': c.city,
                'county': c.county,
                'state': c.state,
                'date_modified': c.date_modified.isoformat() if c.date_modified else '',
            } for c in up_cases])

            up_path = os.path.join(output_dir, 'UP_master.csv')
            up_df.to_csv(up_path, index=False)
            paths['up'] = up_path

        return paths


def main():
    """Test NamUs source loading."""
    import os

    ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    RAW_DIR = os.path.join(ROOT, 'data', 'raw')
    CLEAN_DIR = os.path.join(ROOT, 'data', 'clean')

    source = NamusSource()

    # Look for NamUs files
    mp_file = None
    up_file = None

    for f in os.listdir(RAW_DIR) if os.path.exists(RAW_DIR) else []:
        if 'missing' in f.lower() and f.endswith('.csv'):
            mp_file = os.path.join(RAW_DIR, f)
        elif 'unidentified' in f.lower() and f.endswith('.csv'):
            up_file = os.path.join(RAW_DIR, f)

    if mp_file:
        print(f"Loading MP from: {mp_file}")
        mp_cases = source.load_missing_persons(mp_file)
        print(f"  Loaded {len(mp_cases)} missing persons")

    if up_file:
        print(f"Loading UP from: {up_file}")
        up_cases = source.load_unidentified_persons(up_file)
        print(f"  Loaded {len(up_cases)} unidentified persons")

    # Show statistics
    stats = source.get_statistics()
    print("\nStatistics:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Validate sample cases
    print("\nValidating sample cases...")
    for case in source._cases[:5]:
        result = source.validate_case(case)
        status = "Valid" if result.is_valid else "Invalid"
        print(f"  {case.unified_id}: {status}")
        for err in result.errors:
            print(f"    ERROR: {err}")
        for warn in result.warnings:
            print(f"    WARNING: {warn}")


if __name__ == '__main__':
    main()
