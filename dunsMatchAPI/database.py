"""
Database module for population and session management.
"""

import os
import sys
import json
import sqlite3
from typing import List
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

from . import models

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handles database operations and session management."""

    def __init__(self, database):
        """
        Initialize database manager.

        Args:
            database: Database connection object or SQLAlchemy engine
        """
        self.database = database
        self.engine = None
        self.Session = None

    def initialize_engine(self):
        """Initialize the SQLAlchemy engine and session."""
        # Handle different types of database connections
        if hasattr(self.database, 'dialect') and hasattr(self.database, 'execute'):
            # It's an SQLAlchemy engine
            self.engine = self.database
        elif isinstance(self.database, sqlite3.Connection):
            # For SQLite connections, get the database file path
            db_path = self.database.execute("PRAGMA database_list").fetchone()[2]
            self.engine = create_engine(f'sqlite:///{db_path}')
        elif hasattr(self.database, 'execute'):
            # For other DBAPI connections, we would need more specific handling
            raise NotImplementedError("For non-SQLite databases, please pass an SQLAlchemy engine directly")
        else:
            # Assume it's already an SQLAlchemy engine
            self.engine = self.database

        # Create session factory
        self.Session = sessionmaker(bind=self.engine)

        # Create all tables
        models.Base.metadata.create_all(self.engine)
        logger.info("Database tables initialized")

    def ensure_initialized(self):
        """Ensure the database schema is created."""
        if not self.engine:
            self.initialize_engine()

    def populate_from_json_files(self, json_files: List[str]) -> int:
        """
        Load JSON content into database using SQLAlchemy models.

        Args:
            json_files: List of paths to JSON files with match results

        Returns:
            Number of files processed
        """
        self.ensure_initialized()

        session = self.Session()
        processed_count = 0

        try:
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Check if this is an error response
                    if 'error' in data:
                        logger.info(f"Skipping error file: {json_file}")
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
                    logger.error(f"Error processing file {json_file}: {e}")
                    session.rollback()
                    continue

            # Commit all changes
            session.commit()
            logger.info(f"Populated database with data from {processed_count} files")

        except Exception as e:
            session.rollback()
            logger.error(f"Error committing to database: {e}")
            raise
        finally:
            session.close()

        return processed_count