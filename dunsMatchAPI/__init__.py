"""
dunsMatchAPI - API for performing DUNS number matching using D&B Identity Resolution services.
"""

from .client import DIR_API, initialize_database, process_companies_to_json, populate_database_from_json

__version__ = "0.1.0"
__all__ = ["DIR_API", "initialize_database", "process_companies_to_json", "populate_database_from_json"]