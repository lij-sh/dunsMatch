"""
Authentication module for D&B Identity Resolution API.
Handles authentication and token management.
"""

import os
import base64
import requests
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Authenticator:
    """Handles authentication with D&B API."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 api_url: Optional[str] = None):
        """
        Initialize authenticator.

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

        response = self.session.post(auth_url, data=payload, headers=headers)
        response.raise_for_status()

        auth_data = response.json()
        self.access_token = auth_data.get('access_token')

        # Token typically expires in 24 hours
        expires_in = auth_data.get('expiresIn', 86400)
        self.token_expiry = datetime.now() + timedelta(seconds=expires_in)

        return self.access_token

    def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if not self.access_token or not self.token_expiry or datetime.now() >= self.token_expiry:
            self.authenticate()
        return self.access_token

    def get_auth_headers(self) -> dict:
        """Get headers with valid authorization token."""
        return {
            'Authorization': f'Bearer {self.get_valid_token()}',
            'Accept': 'application/json'
        }