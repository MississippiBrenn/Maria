"""
Base classes for multi-source data architecture.

Defines the abstract DataSource interface and unified data schema
that all data sources must conform to.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Dict, List, Optional, Any
import pandas as pd


class CaseType(Enum):
    """Type of case in the system."""
    MISSING_PERSON = "MP"
    UNIDENTIFIED_PERSON = "UP"


class Sex(Enum):
    """Biological sex classification."""
    MALE = "M"
    FEMALE = "F"
    UNKNOWN = "Unknown"


@dataclass
class UnifiedCase:
    """
    Unified case representation across all data sources.

    All data sources must map their data to this schema to enable
    cross-source matching and analysis.
    """
    # Identifiers
    source_id: str              # Original ID from source (e.g., "12345")
    source_name: str            # Source system name (e.g., "NamUs", "FBI")
    case_type: CaseType         # MP or UP
    unified_id: str = ""        # Maria-generated ID (e.g., "NAMUS-MP-12345")

    # Demographics
    sex: str = "Unknown"        # M, F, or Unknown
    race: str = ""              # Race/ethnicity description
    age_min: Optional[int] = None
    age_max: Optional[int] = None

    # Location
    city: str = ""
    county: str = ""
    state: str = ""             # 2-letter state code
    country: str = "US"         # Country code

    # Temporal
    event_date: Optional[date] = None  # Last seen (MP) or found (UP)

    # Additional fields
    first_name: str = ""        # MP only
    last_name: str = ""         # MP only
    mec_case: str = ""          # UP only (medical examiner case number)

    # Metadata
    date_modified: Optional[date] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Generate unified_id if not provided."""
        if not self.unified_id:
            prefix = self.case_type.value
            self.unified_id = f"{self.source_name.upper()}-{prefix}-{self.source_id}"

    @property
    def full_name(self) -> str:
        """Return full name for MP cases."""
        return f"{self.first_name} {self.last_name}".strip()

    @property
    def age_range(self) -> Optional[tuple]:
        """Return age range as tuple."""
        if self.age_min is not None and self.age_max is not None:
            return (self.age_min, self.age_max)
        return None

    @property
    def location_string(self) -> str:
        """Return formatted location string."""
        parts = [p for p in [self.city, self.county, self.state] if p]
        return ", ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            'id': self.unified_id,
            'source_id': self.source_id,
            'source': self.source_name,
            'case_type': self.case_type.value,
            'sex': self.sex,
            'race': self.race,
            'age_min': self.age_min,
            'age_max': self.age_max,
            'city': self.city,
            'county': self.county,
            'state': self.state,
            'country': self.country,
            'event_date': self.event_date.isoformat() if self.event_date else None,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'mec_case': self.mec_case,
            'date_modified': self.date_modified.isoformat() if self.date_modified else None,
        }


@dataclass
class ValidationResult:
    """Result of validating a case."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, message: str):
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)


class DataSource(ABC):
    """
    Abstract base class for all data sources.

    Each data source must implement methods to:
    1. Load raw data from files or APIs
    2. Parse raw data into UnifiedCase objects
    3. Validate cases meet quality requirements
    4. Export to standardized formats
    """

    def __init__(self, name: str):
        """
        Initialize data source.

        Args:
            name: Human-readable name of the data source
        """
        self.name = name
        self._cases: List[UnifiedCase] = []

    @property
    def source_name(self) -> str:
        """Return normalized source name for IDs."""
        return self.name.upper().replace(" ", "_")

    @abstractmethod
    def load_missing_persons(self, path: str) -> List[UnifiedCase]:
        """
        Load missing persons data from source.

        Args:
            path: Path to data file

        Returns:
            List of UnifiedCase objects
        """
        pass

    @abstractmethod
    def load_unidentified_persons(self, path: str) -> List[UnifiedCase]:
        """
        Load unidentified persons data from source.

        Args:
            path: Path to data file

        Returns:
            List of UnifiedCase objects
        """
        pass

    @abstractmethod
    def parse_row(self, row: Dict[str, Any], case_type: CaseType) -> UnifiedCase:
        """
        Parse a single row of source data into a UnifiedCase.

        Args:
            row: Dictionary of field values
            case_type: Type of case (MP or UP)

        Returns:
            UnifiedCase object
        """
        pass

    def validate_case(self, case: UnifiedCase) -> ValidationResult:
        """
        Validate a case meets minimum quality requirements.

        Args:
            case: Case to validate

        Returns:
            ValidationResult with any errors/warnings
        """
        result = ValidationResult(is_valid=True)

        # Required fields
        if not case.source_id:
            result.add_error("Missing source_id")

        if case.sex not in ("M", "F", "Unknown"):
            result.add_warning(f"Invalid sex value: {case.sex}")

        if not case.state:
            result.add_warning("Missing state")

        # Age validation
        if case.age_min is not None and case.age_max is not None:
            if case.age_min > case.age_max:
                result.add_error(f"age_min ({case.age_min}) > age_max ({case.age_max})")
            if case.age_min < 0 or case.age_max < 0:
                result.add_error("Negative age values")
            if case.age_max > 120:
                result.add_warning(f"Unusually high age_max: {case.age_max}")

        return result

    def to_dataframe(self, cases: Optional[List[UnifiedCase]] = None) -> pd.DataFrame:
        """
        Convert cases to pandas DataFrame.

        Args:
            cases: List of cases (uses self._cases if not provided)

        Returns:
            DataFrame with unified schema
        """
        if cases is None:
            cases = self._cases

        if not cases:
            return pd.DataFrame()

        return pd.DataFrame([c.to_dict() for c in cases])

    def get_statistics(self, cases: Optional[List[UnifiedCase]] = None) -> Dict[str, Any]:
        """
        Get summary statistics for loaded cases.

        Args:
            cases: List of cases (uses self._cases if not provided)

        Returns:
            Dict with statistics
        """
        if cases is None:
            cases = self._cases

        if not cases:
            return {'count': 0}

        mp_cases = [c for c in cases if c.case_type == CaseType.MISSING_PERSON]
        up_cases = [c for c in cases if c.case_type == CaseType.UNIDENTIFIED_PERSON]

        # State distribution
        states = {}
        for c in cases:
            states[c.state] = states.get(c.state, 0) + 1

        return {
            'source': self.name,
            'total_cases': len(cases),
            'missing_persons': len(mp_cases),
            'unidentified_persons': len(up_cases),
            'states_covered': len(states),
            'top_states': dict(sorted(states.items(), key=lambda x: -x[1])[:5]),
            'has_dates': sum(1 for c in cases if c.event_date),
            'has_age': sum(1 for c in cases if c.age_min is not None),
        }
