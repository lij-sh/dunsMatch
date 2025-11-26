"""
SQLAlchemy declarative models for the D&B API data.
"""

from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

class Company(Base):
    """
    Represents a company entity from D&B API.
    """
    __tablename__ = 'companies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    duns = Column(String(9), unique=True, nullable=False)
    primary_name = Column(String(255), nullable=False)
    operating_status_description = Column(String(100))
    operating_status_dnb_code = Column(Integer)
    is_mail_undeliverable = Column(Boolean)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    addresses = relationship("Address", back_populates="company")
    telephone_numbers = relationship("TelephoneNumber", back_populates="company")
    website_addresses = relationship("WebsiteAddress", back_populates="company")
    trade_style_names = relationship("TradeStyleName", back_populates="company")
    match_results = relationship("MatchResult", back_populates="company")


class Address(Base):
    """
    Represents an address associated with a company.
    """
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    country_iso_alpha2_code = Column(String(2))
    country_name = Column(String(100))
    region_name = Column(String(100))
    region_abbreviated_name = Column(String(10))
    postal_code = Column(String(20))
    postal_code_extension = Column(String(10))
    street_line1 = Column(String(255))
    street_line2 = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    company = relationship("Company", back_populates="addresses")


class TelephoneNumber(Base):
    """
    Represents a telephone number associated with a company.
    """
    __tablename__ = 'telephone_numbers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    telephone_number = Column(String(30))
    is_unreachable = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    company = relationship("Company", back_populates="telephone_numbers")


class WebsiteAddress(Base):
    """
    Represents a website address associated with a company.
    """
    __tablename__ = 'website_addresses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    website_address = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    company = relationship("Company", back_populates="website_addresses")


class TradeStyleName(Base):
    """
    Represents an alternative name (DBA, former name, etc.) for a company.
    """
    __tablename__ = 'trade_style_names'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    name = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    company = relationship("Company", back_populates="trade_style_names")


class MatchResult(Base):
    """
    Represents a match result from the D&B API.
    """
    __tablename__ = 'match_results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    input_company_name = Column(String(255))
    input_country = Column(String(2))
    input_address = Column(Text)
    match_confidence_code = Column(Integer)
    match_grade = Column(String(50))
    full_response = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    company = relationship("Company", back_populates="match_results")


class MatchQuery(Base):
    """
    Represents the original query parameters used for matching.
    """
    __tablename__ = 'match_queries'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(255), nullable=False)
    country = Column(String(2), nullable=False)
    address = Column(Text)
    total_matches = Column(Integer, default=0)
    best_match_duns = Column(String(9))
    best_match_confidence = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)