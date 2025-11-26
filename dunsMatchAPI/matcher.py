"""
Core matching logic for D&B Identity Resolution API.
"""

import logging
from typing import List, Dict, Any, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .auth import Authenticator
from .utils import _clean_value, _extract_comprehensive_info

logger = logging.getLogger(__name__)


class Matcher:
    """Handles company matching operations."""

    def __init__(self, authenticator: Authenticator):
        """
        Initialize matcher with authenticator.

        Args:
            authenticator: Authenticator instance for API access
        """
        self.authenticator = authenticator

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
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
        # Clean inputs to handle NaN values
        company_name = _clean_value(company_name)
        country = _clean_value(country)
        address = _clean_value(address)

        # Skip if essential fields are empty
        if not company_name:
            raise ValueError("Company name is required")

        # Build URL for Identity Resolution CleanseMatch endpoint
        url = f"{self.authenticator.api_url}/v1/match/cleanseMatch"

        # Build request data
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
        headers = self.authenticator.get_auth_headers()

        logger.info(f"Matching company: {company_name}, country: {country}, address: {address}")

        try:
            response = self.authenticator.session.get(url, params=params, headers=headers)
            response.raise_for_status()

            data = response.json()

            # Extract comprehensive matches from the response
            matches = []
            match_candidates = data.get('matchCandidates', [])

            for candidate in match_candidates:
                # Extract comprehensive information
                comprehensive_info = _extract_comprehensive_info(candidate)
                matches.append(comprehensive_info)

            logger.info(f"Match request completed successfully, found {len(matches)} matches")
            return matches

        except requests.exceptions.HTTPError as e:
            # Handle 404 as "no matches found" which is a valid response
            if response.status_code == 404:
                # Parse the response to confirm it's a "no matches" response
                try:
                    error_data = response.json()
                    error_code = error_data.get('error', {}).get('errorCode', '')
                    if error_code == '20505':  # No Match found error code
                        logger.info("No matches found for this company")
                        return []  # Return empty list for no matches
                except:
                    pass

            if response.status_code == 401:
                logger.error("Authentication failed. Check your API credentials.")
                raise Exception("Authentication failed. Check your API credentials.")
            else:
                logger.error(f"API request failed: {e}, response: {response.text}")
                raise Exception(f"API request failed: {e}\n{response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {e}")
            raise Exception(f"Network error: {e}")