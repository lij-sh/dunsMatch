"""
dunsMatchAPI - Main Package
API for performing DUNS number matching using D&B Identity Resolution services.
"""

from .client import match_companies_from_excel

__version__ = "0.1.0"
__all__ = ["match_companies_from_excel"]