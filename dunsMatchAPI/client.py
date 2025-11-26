"""
D&B Identity Resolution API Client
Main client class that orchestrates authentication, matching, and data processing.
"""

import logging
from typing import Optional, List, Union
from sqlalchemy import create_engine
import pandas as pd

from .auth import Authenticator
from .matcher import Matcher
from .data_processor import DataProcessor
from .database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DIR_API:
    """Client for D&B Direct+ Identity Resolution API."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 api_url: Optional[str] = None, database_url: Optional[str] = None):
        """
        Initialize D&B Identity Resolution API client.

        Args:
            api_key: D&B API key (reads from DNB_API_KEY env var if not provided)
            api_secret: D&B API secret (reads from DNB_API_SECRET env var if not provided)
            api_url: D&B API base URL (reads from DNB_API_URL env var if not provided)
            database_url: Database URL for SQLAlchemy (optional)
        """
        self.authenticator = Authenticator(api_key, api_secret, api_url)
        self.matcher = Matcher(self.authenticator)
        self.data_processor = DataProcessor(self.matcher)

        # Initialize database if URL provided
        if database_url:
            self.database_engine = create_engine(database_url)
            self.db_manager = DatabaseManager(self.database_engine)
            self.db_manager.ensure_initialized()
        else:
            self.database_engine = None
            self.db_manager = None

    def initialize_database(self, database_url: str) -> None:
        """
        Initialize database with data models.

        Args:
            database_url: Database URL for SQLAlchemy
        """
        self.database_engine = create_engine(database_url)
        self.db_manager = DatabaseManager(self.database_engine)
        self.db_manager.ensure_initialized()
        logger.info("Database initialized successfully")

    def process_companies_to_json(self, input_data: Union[str, pd.DataFrame], output_dir: str = "responses") -> List[str]:
        """
        Process companies from Excel/CSV file or DataFrame and save API responses as JSON files.

        Args:
            input_data: Path to input Excel/CSV file or pandas DataFrame with columns: company_name, country, address
            output_dir: Directory to save JSON response files

        Returns:
            List of saved JSON file paths
        """
        # Load and process input data
        df_input = self.data_processor.load_excel(input_data)

        # Request matches and save JSON responses
        json_files = self.data_processor.request_matches(df_input, output_dir)

        logger.info(f"Processed {len(df_input)} companies, saved {len(json_files)} JSON files")
        return json_files

    def populate_database_from_json(self, json_files: Union[str, List[str]]) -> int:
        """
        Populate database from JSON response files.

        Args:
            json_files: Path to JSON file, directory containing JSON files, or list of JSON file paths

        Returns:
            Number of files processed
        """
        if not self.db_manager:
            raise ValueError("Database not initialized. Call initialize_database() first or provide database_url in constructor.")

        # Handle different input types
        if isinstance(json_files, str):
            if os.path.isdir(json_files):
                # Directory path - get all JSON files
                json_files = [os.path.join(json_files, f) for f in os.listdir(json_files)
                             if f.endswith('.json')]
            else:
                # Single file path
                json_files = [json_files]

        return self.db_manager.populate_from_json_files(json_files)

    def run_full_workflow(self, input_data: Union[str, pd.DataFrame],
                         database_url: str, output_dir: str = "responses") -> dict:
        """
        Run the complete workflow: initialize DB, process companies, save JSON responses, populate DB.

        Args:
            input_data: Path to input Excel/CSV file or pandas DataFrame
            database_url: Database URL for SQLAlchemy
            output_dir: Directory to save JSON response files

        Returns:
            Dictionary with workflow results
        """
        # Initialize database
        self.initialize_database(database_url)

        # Process companies and save JSON responses
        json_files = self.process_companies_to_json(input_data, output_dir)

        # Populate database from JSON files
        processed_count = self.populate_database_from_json(json_files)

        return {
            'json_files_saved': len(json_files),
            'database_records_processed': processed_count,
            'output_directory': output_dir
        }

    def match_company(self, company_name: str, country: str, address: str = "") -> List[dict]:
        """
        Match a single company.

        Args:
            company_name: Name of the company
            country: Country code (e.g., US, GB, DE)
            address: Company address (optional)

        Returns:
            List of matched companies with comprehensive information
        """
        return self.matcher.match_company(company_name, country, address)

# Legacy methods for backward compatibility
def initialize_database(database_url: str) -> DIR_API:
    """Legacy function for database initialization."""
    logger.warning("initialize_database() is deprecated. Use DIR_API.initialize_database() instead.")
    client = DIR_API()
    client.initialize_database(database_url)
    return client

def process_companies_to_json(input_data: Union[str, pd.DataFrame], output_dir: str = "responses") -> List[str]:
    """Legacy function for processing companies to JSON."""
    logger.warning("process_companies_to_json() is deprecated. Use DIR_API.process_companies_to_json() instead.")
    client = DIR_API()
    return client.process_companies_to_json(input_data, output_dir)

def populate_database_from_json(json_files: Union[str, List[str]]) -> int:
    """Legacy function for populating database from JSON."""
    logger.warning("populate_database_from_json() is deprecated. Use DIR_API.populate_database_from_json() instead.")
    client = DIR_API()
    return client.populate_database_from_json(json_files)