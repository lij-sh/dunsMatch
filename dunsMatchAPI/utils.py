"""
Utility functions for dunsMatchAPI.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any


def _clean_value(value):
    """Clean a value to ensure it's JSON serializable."""
    if pd.isna(value) or value is None:
        return ""
    if isinstance(value, (np.float64, np.float32, np.int64, np.int32)):
        if np.isnan(value) or np.isinf(value):
            return ""
    return str(value)


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