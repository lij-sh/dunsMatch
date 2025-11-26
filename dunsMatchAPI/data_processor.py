"""
Data processing module for Excel/JSON processing and output generation.
"""

import os
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Any, Union
from datetime import datetime
import logging

from .matcher import Matcher

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing and output generation."""

    def __init__(self, matcher: Matcher):
        """
        Initialize data processor.

        Args:
            matcher: Matcher instance for API calls
        """
        self.matcher = matcher

    def load_excel(self, input_file: Union[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Load company data from Excel file or pandas DataFrame.

        Args:
            input_file: Path to input Excel file with columns: company_name, country, address
                       OR pandas DataFrame with the same columns

        Returns:
            DataFrame with company data
        """
        # Handle DataFrame input directly
        if isinstance(input_file, pd.DataFrame):
            df_input = input_file.copy()
            logger.info("Loaded company data from DataFrame")
        else:
            # Handle file path input
            df_input = pd.read_excel(input_file, engine='openpyxl')
            logger.info(f"Loaded company data from Excel file: {input_file}")

        # Handle different possible column names
        column_mapping = {
            'name': 'company_name',
            'company_name': 'company_name',
            'companyname': 'company_name',
            '企业名称': 'company_name',
            '企业名': 'company_name',

            'country': 'country',
            'countryISOAlpha2Code': 'country',
            'country_code': 'country',
            'countrycode': 'country',
            '国家': 'country',
            'countrycountryISOAlpha2Code': 'country',

            'address': 'address',
            'streetAddressLine1': 'address',
            'street_address': 'address',
            '地址': 'address'
        }

        # Rename columns to standard names
        df_input.rename(columns=column_mapping, inplace=True)

        # Validate required columns
        required_columns = ['company_name', 'country']
        for col in required_columns:
            if col not in df_input.columns:
                raise ValueError(f"Missing required column: {col}")

        # Add address column if not present
        if 'address' not in df_input.columns:
            df_input['address'] = ''

        # Fix country codes if needed (common mistakes)
        df_input['country'] = df_input['country'].replace({'CH': 'CN'})

        # Replace NaN values with empty strings, ensuring correct data types
        for col in ['company_name', 'country', 'address']:
            if col in df_input.columns:
                df_input[col] = df_input[col].fillna('').astype(str)

        logger.info(f"Processed {len(df_input)} company records")
        return df_input

    def request_matches(self, company_data: pd.DataFrame, output_dir: str = "responses") -> List[str]:
        """
        Request matches for companies and save JSON responses locally.

        Args:
            company_data: DataFrame with company information
            output_dir: Directory to save JSON response files

        Returns:
            List of file paths where JSON responses were saved
        """
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Prepare file paths list
        saved_files = []

        # Process each row
        for index, row in company_data.iterrows():
            company_name = row['company_name']
            country = row['country']
            address = row['address']

            try:
                # Perform matching
                matches = self.matcher.match_company(company_name, country, address)

                # Create result entry
                result_entry = {
                    'input_company_name': company_name,
                    'input_country': country,
                    'input_address': address,
                    'matches': matches,
                    'timestamp': datetime.now().isoformat()
                }

                # Save JSON response to file
                safe_company_name = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_company_name.replace('/', '_').replace('\\', '_')}_{index}.json"
                filepath = os.path.join(output_dir, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(result_entry, f, ensure_ascii=False, indent=2)

                saved_files.append(filepath)
                logger.info(f"Saved response to: {filepath}")

            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                error_entry = {
                    'input_company_name': company_name,
                    'input_country': country,
                    'input_address': address,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }

                # Save error response to file
                safe_company_name = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_company_name.replace('/', '_').replace('\\', '_')}_{index}_error.json"
                filepath = os.path.join(output_dir, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(error_entry, f, ensure_ascii=False, indent=2)

                saved_files.append(filepath)
                logger.info(f"Saved error response to: {filepath}")

        return saved_files