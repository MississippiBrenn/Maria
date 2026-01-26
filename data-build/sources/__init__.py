"""
Multi-Source Data Architecture

Provides a pluggable architecture for integrating multiple data sources
into the Maria matching pipeline.

Supported Sources:
- NamUs (National Missing and Unidentified Persons System)
- FBI (planned)
- Charley Project (planned)
- Doe Network (planned)

Usage:
    from sources import NamusSource, UnifiedSchema

    source = NamusSource()
    mp_cases = source.load_missing_persons('path/to/namus_mp.csv')
    up_cases = source.load_unidentified_persons('path/to/namus_up.csv')
"""

from .base import DataSource, UnifiedCase, CaseType, ValidationResult
from .namus import NamusSource
from .schema import UnifiedSchema

__all__ = [
    'DataSource',
    'UnifiedCase',
    'CaseType',
    'ValidationResult',
    'NamusSource',
    'UnifiedSchema',
]
