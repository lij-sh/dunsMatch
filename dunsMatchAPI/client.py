"""
D&B Identity Resolution API Client
Handles authentication and company matching requests to D&B Identity Resolution API.
"""

import os
import requests
import base64
import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import sqlite3
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

class DIR_API:
    """Client for D&B Direct+ Identity Resolution API."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 api_url: Optional[str] = None):
        """
        Initialize D&B Identity Resolution API client.

        Args:
            api_key: D&B API key (reads from DNB_API_KEY env var if not provided)
            api_secret: D&B API secret (reads from DNB_API_SECRET env var if not provided)
            api_url: D&B API base URL (reads from DNB_API_URL env var if not provided)
        """
        self.api_key = api_key or os.getenv('DNB_API_KEY')
        self.api_secret = api_secret or os.getenv('DNB_API_SECRET')
        self.api_url = api_url or os.getenv('DNB_API_URL', 'https://plus.dnb.com')

        if not self.api_key or not self.api_secret:
            raise ValueError(
                "API credentials not provided. Set DNB_API_KEY and DNB_API_SECRET "
                "environment variables or pass them to the constructor."
            )

        self.access_token = None
        self.token_expiry = None
        self.session = requests.Session()

    def init(self, database):
        """
        Initialize database with required tables using SQLAlchemy declarative models.
        If tables already exist, this operation has no effect.
        
        Args:
            database: Database connection object or SQLAlchemy engine
                      For sqlite3 connections, pass the connection object
                      For SQLAlchemy engines, pass the engine directly
        """
        # Try different ways to import the models
        try:
            # Try relative import first
            from . import models
        except ImportError:
            try:
                # Try absolute import
                import models
            except ImportError:
                # Try importing from the current directory
                sys.path.append(os.path.dirname(__file__))
                import models
        
        # Handle different types of database connections
        if isinstance(database, sqlite3.Connection):
            # For SQLite connections, get the database file path
            db_path = database.execute("PRAGMA database_list").fetchone()[2]
            engine = create_engine(f'sqlite:///{db_path}')
        elif hasattr(database, 'execute'):
            # For other DBAPI connections, we would need more specific handling
            # This is a simplified approach - in production, prefer passing an engine
            raise NotImplementedError("For non-SQLite databases, please pass an SQLAlchemy engine directly")
        else:
            # Assume it's already an SQLAlchemy engine
            engine = database
        
        # Create all tables using SQLAlchemy
        models.Base.metadata.create_all(engine)
        print("✓ Database tables initialized using SQLAlchemy models")

    def authenticate(self) -> str:
        """
        Authenticate with D&B API and get access token.
        Uses Basic Authentication with API key and secret.

        Returns:
            Access token string
        """
        auth_url = f"{self.api_url}/v3/token"

        # Create Basic Auth header with base64 encoded key:secret
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # Use form data instead of JSON
        payload = {
            'grant_type': 'client_credentials'
        }

        try:
            response = self.session.post(auth_url, data=payload, headers=headers)
            response.raise_for_status()

            auth_data = response.json()
            self.access_token = auth_data.get('access_token')

            # Token typically expires in 24 hours
            expires_in = auth_data.get('expiresIn', 86400)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)

            print(f"✓ Authenticated successfully. Token expires at {self.token_expiry}")
            return self.access_token

        except requests.exceptions.RequestException as e:
            # Show more detailed error info
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            raise Exception(f"Authentication failed: {e}")

    def _ensure_authenticated(self):
        """Ensure we have a valid access token."""
        if not self.access_token or not self.token_expiry or datetime.now() >= self.token_expiry:
            self.authenticate()

    def _ensure_database_initialized(self, engine):
        """
        Ensure the database schema is created.
        This uses the SQLAlchemy models to create tables if they don't exist.

        Args:
            engine: SQLAlchemy engine instance
        """
        # Import models here to avoid circular imports
        from .models import Base
        Base.metadata.create_all(engine)
        print("✓ Database tables initialized (via SQLAlchemy models)")

    def _clean_value(self, value):
        """Clean a value to ensure it's JSON serializable."""
        if pd.isna(value) or value is None:
            return ""
        if isinstance(value, (np.float64, np.float32, np.int64, np.int32)):
            if np.isnan(value) or np.isinf(value):
                return ""
        return str(value)

    def match_company(self, company_name: str, country: str, address: str) -> List[Dict[str, Any]]:
        """
        Match a company using D&B Identity Resolution CleanseMatch service.

        Args:
            company_name: Name of the company
            country: Country code (e.g., US, GB, DE)
            address: Company address

        Returns:
            List of matched companies with comprehensive information
        """
        self._ensure_authenticated()

        # Clean inputs to handle NaN values
        company_name = self._clean_value(company_name)
        country = self._clean_value(country)
        address = self._clean_value(address)

        # Skip if essential fields are empty
        if not company_name:
            raise ValueError("Company name is required")

        # Build URL for Identity Resolution CleanseMatch endpoint
        url = f"{self.api_url}/v1/match/cleanseMatch"

        # Build request data - Based on the API documentation, we should use query parameters
        # for simple matching rather than a request body
        params = {
            'name': company_name,
            'countryISOAlpha2Code': country
        }

        # Only add address if it's not empty
        if address:
            params['streetAddressLine1'] = address

        # For Chinese companies, set the language to Simplified Chinese
        if country.upper() == 'CN':
            params['inLanguage'] = 'zh-hans-CN'
        else:
            # For automatic language detection
            params['inLanguage'] = 'auto'

        # Build headers
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

        try:
            print(f"\n📡 Matching company: {company_name}")
            print(f"   Country: {country}")
            print(f"   Address: {address if address else '(not provided)'}")
            print(f"   Language: {params['inLanguage']}")

            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()

            data = response.json()
        
            # Extract comprehensive matches from the response
            matches = []
            match_candidates = data.get('matchCandidates', [])
        
            for candidate in match_candidates:
                # Extract comprehensive information
                comprehensive_info = _extract_comprehensive_info(candidate)
                matches.append(comprehensive_info)

            print(f"✓ Match request completed successfully")
            return matches

        except requests.exceptions.HTTPError as e:
            # Handle 404 as "no matches found" which is a valid response
            if response.status_code == 404:
                # Parse the response to confirm it's a "no matches" response
                try:
                    error_data = response.json()
                    error_code = error_data.get('error', {}).get('errorCode', '')
                    if error_code == '20505':  # No Match found error code
                        print("✓ No matches found for this company")
                        return []  # Return empty list for no matches
                except:
                    pass
        
            if response.status_code == 401:
                raise Exception("Authentication failed. Check your API credentials.")
            else:
                raise Exception(f"API request failed: {e}\n{response.text}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Network error: {e}")

    def load_excel(self, input_file: str) -> pd.DataFrame:
        """
        Load company data from Excel file.
        
        Args:
            input_file: Path to input Excel file with columns: company_name, country, address
            
        Returns:
            DataFrame with company data
        """
        # Read input Excel file
        df_input = pd.read_excel(input_file, engine='openpyxl')

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
                
        return df_input

    def request(self, company_data: pd.DataFrame, output_dir: str = "responses") -> List[str]:
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
        
        # Initialize API client
        client = DIR_API()

        # Prepare file paths list
        saved_files = []

        # Process each row
        for index, row in company_data.iterrows():
            company_name = row['company_name']
            country = row['country']
            address = row['address']

            try:
                # Perform matching
                matches = client.match_company(company_name, country, address)
                
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
                print(f"✓ Saved response to: {filepath}")

            except Exception as e:
                print(f"Error processing row {index}: {e}")
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
                print(f"✓ Saved error response to: {filepath}")

        return saved_files

    def populate(self, json_files: List[str], database) -> int:
        """
        Load JSON content into database using SQLAlchemy models and session.
        
        Args:
            json_files: List of paths to JSON files with match results
            database: Database connection object or SQLAlchemy engine/session
            
        Returns:
            Number of files processed
        """
        # Import models
        try:
            from . import models
        except ImportError:
            try:
                import models
            except ImportError:
                sys.path.append(os.path.dirname(__file__))
                import models
        
        # Handle different types of database connections
        if isinstance(database, sqlite3.Connection):
            # For SQLite connections, get the database file path
            db_path = database.execute("PRAGMA database_list").fetchone()[2]
            engine = create_engine(f'sqlite:///{db_path}')
        elif hasattr(database, 'execute'):
            # For other DBAPI connections, we would need more specific handling
            raise NotImplementedError("For non-SQLite databases, please pass an SQLAlchemy engine directly")
        else:
            # Assume it's already an SQLAlchemy engine
            engine = database
            
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        processed_count = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Check if this is an error response
                if 'error' in data:
                    print(f"Skipping error file: {json_file}")
                    processed_count += 1
                    continue
                    
                # Extract basic info
                input_company_name = data.get('input_company_name', '')
                input_country = data.get('input_country', '')
                input_address = data.get('input_address', '')
                matches = data.get('matches', [])
                timestamp_str = data.get('timestamp', '')
                
                # Convert timestamp string to datetime object
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = datetime.utcnow()
                
                # Create and add match query
                match_query = models.MatchQuery(
                    company_name=input_company_name,
                    country=input_country,
                    address=input_address,
                    total_matches=len(matches),
                    created_at=timestamp
                )
                session.add(match_query)
                session.flush()  # To get the ID
                
                # Process each match
                for match in matches:
                    # Extract company data
                    duns = match.get('duns', '')
                    primary_name = match.get('primary_name', '')
                    operating_status = match.get('operating_status', {})
                    operating_status_desc = operating_status.get('description', '')
                    operating_status_code = operating_status.get('dnb_code', None)
                    is_mail_undeliverable = match.get('is_mail_undeliverable', None)
                    
                    # Check if company already exists
                    company = session.query(models.Company).filter_by(duns=duns).first()
                    if not company:
                        # Create company if it doesn't exist
                        company = models.Company(
                            duns=duns,
                            primary_name=primary_name,
                            operating_status_description=operating_status_desc,
                            operating_status_dnb_code=operating_status_code,
                            is_mail_undeliverable=is_mail_undeliverable,
                            created_at=timestamp
                        )
                        session.add(company)
                        session.flush()  # To get the ID
                    
                    # Extract and create address data
                    address_data = match.get('address', {})
                    country_data = address_data.get('country', {})
                    region_data = address_data.get('region', {})
                    street_data = address_data.get('street', {})
                    
                    address_obj = models.Address(
                        company_id=company.id,
                        country_iso_alpha2_code=country_data.get('iso_alpha2_code', ''),
                        country_name=country_data.get('name', ''),
                        region_name=region_data.get('name', ''),
                        region_abbreviated_name=region_data.get('abbreviated_name', ''),
                        postal_code=address_data.get('postal_code', ''),
                        postal_code_extension=address_data.get('postal_code_extension', ''),
                        street_line1=street_data.get('line1', ''),
                        street_line2=street_data.get('line2', ''),
                        created_at=timestamp
                    )
                    session.add(address_obj)
                    
                    # Extract and create telephone numbers
                    telephone_data = match.get('telephone', [])
                    for tel in telephone_data:
                        telephone_obj = models.TelephoneNumber(
                            company_id=company.id,
                            telephone_number=tel.get('telephoneNumber', ''),
                            is_unreachable=tel.get('isUnreachableIndicator', False),
                            created_at=timestamp
                        )
                        session.add(telephone_obj)
                    
                    # Extract and create website addresses
                    website_data = match.get('website_address', [])
                    for website in website_data:
                        website_obj = models.WebsiteAddress(
                            company_id=company.id,
                            website_address=website,
                            created_at=timestamp
                        )
                        session.add(website_obj)
                        
                    # Extract and create trade style names
                    trade_style_data = match.get('trade_style_names', [])
                    for trade_style in trade_style_data:
                        trade_style_obj = models.TradeStyleName(
                            company_id=company.id,
                            name=trade_style.get('name', ''),
                            created_at=timestamp
                        )
                        session.add(trade_style_obj)
                    
                    # Create match result
                    match_quality = match.get('match_quality', {})
                    match_result = models.MatchResult(
                        company_id=company.id,
                        input_company_name=input_company_name,
                        input_country=input_country,
                        input_address=input_address,
                        match_confidence_code=match_quality.get('confidence_code', 0),
                        match_grade=match_quality.get('match_grade', ''),
                        full_response=json.dumps(data, ensure_ascii=False),
                        created_at=timestamp
                    )
                    session.add(match_result)
                
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing file {json_file}: {e}")
                session.rollback()
                
        # Commit all changes
        try:
            session.commit()
            print(f"✓ Populated database with data from {processed_count} files")
        except Exception as e:
            session.rollback()
            print(f"Error committing to database: {e}")
            raise
        finally:
            session.close()
            
        return processed_count

def _extract_comprehensive_info(candidate):
    """
    Extract comprehensive information from a match candidate.
    
    Args:
        candidate: A match candidate from the API response
        
    Returns:
        dict: Comprehensive information about the match
    """
    # Extract organization information
    org = candidate.get('organization', {})
    
    # Basic company information
    comprehensive_info = {
        'duns': org.get('duns', ''),
        'primary_name': org.get('primaryName', ''),
        'website_address': org.get('websiteAddress', []),
        'trade_style_names': org.get('tradeStyleNames', []),
        'telephone': org.get('telephone', []),
    }
    
    # Operating status information
    duns_control_status = org.get('dunsControlStatus', {})
    operating_status = duns_control_status.get('operatingStatus', {})
    comprehensive_info['operating_status'] = {
        'description': operating_status.get('description', ''),
        'dnb_code': operating_status.get('dnbCode', '')
    }
    comprehensive_info['is_mail_undeliverable'] = duns_control_status.get('isMailUndeliverable', None)
    
    # Address information
    primary_address = org.get('primaryAddress', {})
    address_country = primary_address.get('addressCountry', {})
    address_region = primary_address.get('addressRegion', {})
    street_address = primary_address.get('streetAddress', {})
    
    comprehensive_info['address'] = {
        'country': {
            'iso_alpha2_code': address_country.get('isoAlpha2Code', ''),
            'name': address_country.get('name', '')
        },
        'region': {
            'name': address_region.get('name', ''),
            'abbreviated_name': address_region.get('abbreviatedName', '')
        },
        'postal_code': primary_address.get('postalCode', ''),
        'postal_code_extension': primary_address.get('postalCodeExtension', ''),
        'street': {
            'line1': street_address.get('line1', ''),
            'line2': street_address.get('line2', '')
        }
    }
    
    # Match quality information
    match_quality_info = candidate.get('matchQualityInformation', {})
    comprehensive_info['match_quality'] = {
        'confidence_code': match_quality_info.get('confidenceCode', 0),
        'match_grade': match_quality_info.get('matchGrade', ''),
        'match_grade_components_count': match_quality_info.get('matchGradeComponentsCount', 0)
    }
    
    # Extract match grade components
    match_grade_components = match_quality_info.get('matchGradeComponents', [])
    comprehensive_info['match_grade_components'] = []
    for component in match_grade_components:
        comprehensive_info['match_grade_components'].append({
            'component_type': component.get('componentType', ''),
            'component_rating': component.get('componentRating', '')
        })
    
    # Additional match data
    comprehensive_info['match_data_profile'] = match_quality_info.get('matchDataProfile', '')
    comprehensive_info['name_match_score'] = match_quality_info.get('nameMatchScore', None)
    
    return comprehensive_info


def match_companies_from_excel(input_file: str, output_file: str) -> None:
    """
    Match companies from Excel file and output results to another Excel file.

    Args:
        input_file: Path to input Excel file with columns: company_name, country, address
        output_file: Path to output Excel file with match results
    """
    # Read input Excel file
    df_input = pd.read_excel(input_file, engine='openpyxl')

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

    # Initialize API client
    client = DIR_API()

    # Prepare results list
    results = []

    # Process each row
    for index, row in df_input.iterrows():
        company_name = row['company_name']
        country = row['country']
        address = row['address']

        try:
            # Perform matching
            matches = client.match_company(company_name, country, address)

            # Process matches
            if matches:
                for i, match in enumerate(matches):
                    # Extract comprehensive data
                    result = {
                        'input_company_name': company_name,
                        'input_country': country,
                        'input_address': address,
                        'matched_duns': match.get('duns', ''),
                        'matched_primary_name': match.get('primary_name', ''),
                        'match_confidence_code': match.get('match_quality', {}).get('confidence_code', 0),
                        'match_grade': match.get('match_quality', {}).get('match_grade', ''),
                        'operating_status': match.get('operating_status', {}).get('description', ''),
                        'country_name': match.get('address', {}).get('country', {}).get('name', ''),
                        'region_name': match.get('address', {}).get('region', {}).get('name', ''),
                        'postal_code': match.get('address', {}).get('postal_code', ''),
                        'street_address': match.get('address', {}).get('street', {}).get('line1', ''),
                        'match_number': i + 1,
                        'total_matches': len(matches),
                        'full_response': json.dumps(match, ensure_ascii=False)
                    }
                    results.append(result)
            else:
                # No matches found
                results.append({
                    'input_company_name': company_name,
                    'input_country': country,
                    'input_address': address,
                    'matched_duns': 'NO MATCH FOUND',
                    'matched_primary_name': '',
                    'match_confidence_code': 0,
                    'match_grade': '',
                    'operating_status': '',
                    'country_name': '',
                    'region_name': '',
                        'postal_code': '',
                        'street_address': '',
                        'match_number': 0,
                        'total_matches': 0,
                        'full_response': 'No matches found for the given input criteria'
                })

        except Exception as e:
            print(f"Error processing row {index}: {e}")
            results.append({
                'input_company_name': company_name,
                'input_country': country,
                'input_address': address,
                'matched_duns': 'ERROR',
                'matched_primary_name': '',
                'match_confidence_code': 0,
                'match_grade': '',
                'operating_status': '',
                'country_name': '',
                'region_name': '',
                    'postal_code': '',
                    'street_address': '',
                    'match_number': 0,
                    'total_matches': 0,
                    'full_response': str(e)
                })

    # Convert results to DataFrame
    df_output = pd.DataFrame(results)

    # Write to Excel file
    df_output.to_excel(output_file, index=False, engine='openpyxl')
    print(f"\n✓ Results written to: {output_file}")