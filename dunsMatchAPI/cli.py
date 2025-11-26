"""
Command Line Interface for dunsMatchAPI
"""

import argparse
import sys
from .client import match_companies_from_excel


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="DUNS Match API - Match companies to DUNS numbers using D&B Identity Resolution"
    )
    
    parser.add_argument(
        "input_file",
        help="Input Excel file with company data (columns: company_name, country, address)"
    )
    
    parser.add_argument(
        "output_file",
        help="Output Excel file with match results"
    )
    
    args = parser.parse_args()
    
    try:
        print("Starting DUNS matching process...")
        match_companies_from_excel(args.input_file, args.output_file)
        print("DUNS matching process completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()