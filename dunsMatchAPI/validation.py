"""
Pydantic models for input/output validation.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, validator


class CompanyInput(BaseModel):
    """Input model for company data."""
    company_name: str = Field(..., min_length=1, description="Name of the company")
    country: str = Field(..., min_length=2, max_length=2, description="ISO Alpha-2 country code")
    address: str = Field("", description="Company address")

    @validator('country')
    def validate_country(cls, v):
        """Validate country code format."""
        if not v.isalpha() or len(v) != 2:
            raise ValueError('Country must be a 2-letter ISO Alpha-2 code')
        return v.upper()

    @validator('company_name')
    def clean_company_name(cls, v):
        """Clean and validate company name."""
        return v.strip()


class MatchQuality(BaseModel):
    """Model for match quality information."""
    confidence_code: int = Field(..., ge=0, le=10)
    match_grade: str = ""
    match_grade_components_count: int = 0
    match_grade_components: List[Dict[str, str]] = []
    match_data_profile: str = ""
    name_match_score: Optional[float] = None


class Address(BaseModel):
    """Model for address information."""
    country: Dict[str, str]
    region: Dict[str, str]
    postal_code: str = ""
    postal_code_extension: str = ""
    street: Dict[str, str]


class OperatingStatus(BaseModel):
    """Model for operating status."""
    description: str = ""
    dnb_code: Optional[int] = None


class CompanyMatch(BaseModel):
    """Model for a company match result."""
    duns: str = Field(..., min_length=9, max_length=9)
    primary_name: str = ""
    website_address: List[str] = []
    trade_style_names: List[Dict[str, str]] = []
    telephone: List[Dict[str, Any]] = []
    operating_status: OperatingStatus
    is_mail_undeliverable: Optional[bool] = None
    address: Address
    match_quality: MatchQuality


class MatchResult(BaseModel):
    """Model for complete match result."""
    input_company_name: str
    input_country: str
    input_address: str
    matches: List[CompanyMatch] = []
    timestamp: str


class ExcelOutputRow(BaseModel):
    """Model for Excel output row."""
    input_company_name: str
    input_country: str
    input_address: str
    matched_duns: str = ""
    matched_primary_name: str = ""
    match_confidence_code: int = 0
    match_grade: str = ""
    operating_status: str = ""
    country_name: str = ""
    region_name: str = ""
    postal_code: str = ""
    street_address: str = ""
    match_number: int = 0
    total_matches: int = 0
    full_response: str = ""