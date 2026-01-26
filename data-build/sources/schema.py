"""
Unified Schema Module

Defines the canonical data schema for Maria and provides
utilities for schema validation and transformation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Set
import pandas as pd


@dataclass
class FieldDefinition:
    """Definition of a field in the unified schema."""
    name: str
    data_type: str  # 'string', 'int', 'float', 'date', 'bool'
    required: bool = False
    description: str = ""
    allowed_values: Optional[List[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


class UnifiedSchema:
    """
    Canonical schema definition for Maria data.

    All data sources must map to this schema to enable cross-source
    matching and analysis.
    """

    # Core identity fields
    ID_FIELDS = [
        FieldDefinition('id', 'string', True, 'Maria unified ID (e.g., NAMUS-MP-12345)'),
        FieldDefinition('source_id', 'string', True, 'Original ID from source system'),
        FieldDefinition('source', 'string', True, 'Source system name'),
        FieldDefinition('case_type', 'string', True, 'MP or UP', allowed_values=['MP', 'UP']),
    ]

    # Demographic fields
    DEMOGRAPHIC_FIELDS = [
        FieldDefinition('sex', 'string', True, 'Biological sex',
                        allowed_values=['M', 'F', 'Unknown']),
        FieldDefinition('race', 'string', False, 'Race/ethnicity description'),
        FieldDefinition('age_min', 'int', False, 'Minimum estimated age',
                        min_value=0, max_value=120),
        FieldDefinition('age_max', 'int', False, 'Maximum estimated age',
                        min_value=0, max_value=120),
    ]

    # Location fields
    LOCATION_FIELDS = [
        FieldDefinition('city', 'string', False, 'City name'),
        FieldDefinition('county', 'string', False, 'County name'),
        FieldDefinition('state', 'string', True, '2-letter state code'),
        FieldDefinition('country', 'string', False, '2-letter country code'),
    ]

    # Temporal fields
    TEMPORAL_FIELDS = [
        FieldDefinition('event_date', 'date', False,
                        'Last seen date (MP) or found date (UP)'),
        FieldDefinition('date_modified', 'date', False,
                        'Date case was last modified'),
    ]

    # MP-specific fields
    MP_FIELDS = [
        FieldDefinition('first_name', 'string', False, 'First name'),
        FieldDefinition('last_name', 'string', False, 'Last name'),
    ]

    # UP-specific fields
    UP_FIELDS = [
        FieldDefinition('mec_case', 'string', False, 'Medical examiner case number'),
    ]

    @classmethod
    def all_fields(cls) -> List[FieldDefinition]:
        """Get all field definitions."""
        return (cls.ID_FIELDS + cls.DEMOGRAPHIC_FIELDS +
                cls.LOCATION_FIELDS + cls.TEMPORAL_FIELDS +
                cls.MP_FIELDS + cls.UP_FIELDS)

    @classmethod
    def required_fields(cls) -> List[str]:
        """Get names of required fields."""
        return [f.name for f in cls.all_fields() if f.required]

    @classmethod
    def field_names(cls) -> List[str]:
        """Get all field names."""
        return [f.name for f in cls.all_fields()]

    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate a DataFrame against the unified schema.

        Args:
            df: DataFrame to validate

        Returns:
            Dict with validation results
        """
        errors = []
        warnings = []

        # Check required columns exist
        for field_name in cls.required_fields():
            if field_name not in df.columns:
                errors.append(f"Missing required column: {field_name}")

        # Check for unexpected columns
        expected_cols = set(cls.field_names())
        actual_cols = set(df.columns)
        unexpected = actual_cols - expected_cols
        if unexpected:
            warnings.append(f"Unexpected columns: {unexpected}")

        # Validate field constraints
        for field in cls.all_fields():
            if field.name not in df.columns:
                continue

            col = df[field.name]

            # Check allowed values
            if field.allowed_values:
                invalid = col[~col.isin(field.allowed_values + [None, ''])]
                if len(invalid) > 0:
                    sample = invalid.head(3).tolist()
                    warnings.append(
                        f"{field.name}: {len(invalid)} invalid values (e.g., {sample})"
                    )

            # Check numeric ranges
            if field.min_value is not None and pd.api.types.is_numeric_dtype(col):
                below_min = col[col < field.min_value]
                if len(below_min) > 0:
                    errors.append(
                        f"{field.name}: {len(below_min)} values below minimum {field.min_value}"
                    )

            if field.max_value is not None and pd.api.types.is_numeric_dtype(col):
                above_max = col[col > field.max_value]
                if len(above_max) > 0:
                    warnings.append(
                        f"{field.name}: {len(above_max)} values above maximum {field.max_value}"
                    )

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'rows': len(df),
            'columns': list(df.columns),
        }

    @classmethod
    def get_column_mapping_template(cls, source_columns: List[str]) -> Dict[str, str]:
        """
        Generate a template for mapping source columns to unified schema.

        Args:
            source_columns: List of column names from source data

        Returns:
            Dict mapping unified field names to suggested source columns
        """
        mapping = {}

        # Common mapping patterns
        patterns = {
            'id': ['id', 'case_number', 'case_id', 'case number'],
            'sex': ['sex', 'gender', 'biological_sex', 'biological sex'],
            'race': ['race', 'ethnicity', 'race_ethnicity', 'race / ethnicity'],
            'age_min': ['age_min', 'age_from', 'min_age', 'age estimate from'],
            'age_max': ['age_max', 'age_to', 'max_age', 'age estimate to'],
            'city': ['city', 'city of recovery', 'city of last contact'],
            'county': ['county', 'county of recovery', 'county of last contact'],
            'state': ['state', 'state of recovery', 'state of last contact'],
            'first_name': ['first_name', 'firstname', 'first'],
            'last_name': ['last_name', 'lastname', 'last', 'surname'],
        }

        source_cols_lower = {c.lower(): c for c in source_columns}

        for unified_field, source_patterns in patterns.items():
            for pattern in source_patterns:
                if pattern in source_cols_lower:
                    mapping[unified_field] = source_cols_lower[pattern]
                    break
            else:
                mapping[unified_field] = None  # No match found

        return mapping

    @classmethod
    def to_documentation(cls) -> str:
        """Generate markdown documentation for the schema."""
        lines = ["# Maria Unified Schema\n"]

        sections = [
            ("Identity Fields", cls.ID_FIELDS),
            ("Demographic Fields", cls.DEMOGRAPHIC_FIELDS),
            ("Location Fields", cls.LOCATION_FIELDS),
            ("Temporal Fields", cls.TEMPORAL_FIELDS),
            ("Missing Person Fields", cls.MP_FIELDS),
            ("Unidentified Person Fields", cls.UP_FIELDS),
        ]

        for section_name, fields in sections:
            lines.append(f"\n## {section_name}\n")
            lines.append("| Field | Type | Required | Description |")
            lines.append("|-------|------|----------|-------------|")

            for f in fields:
                req = "Yes" if f.required else "No"
                desc = f.description
                if f.allowed_values:
                    desc += f" Values: {f.allowed_values}"
                lines.append(f"| {f.name} | {f.data_type} | {req} | {desc} |")

        return "\n".join(lines)
