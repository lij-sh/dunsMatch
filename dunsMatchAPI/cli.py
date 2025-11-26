"""
Command Line Interface for dunsMatchAPI
"""

import sys
import os
import click
from pathlib import Path
from typing import Optional
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from .client import DIR_API, match_companies_from_excel

console = Console()
logger = logging.getLogger(__name__)


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO', help='Set log level')
def cli(verbose, log_level):
    """DUNS Match API - Match companies to DUNS numbers using D&B Identity Resolution"""
    # Configure logging
    level = getattr(logging, log_level.upper())
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--output-dir', default='responses', help='Directory to save JSON response files')
@click.option('--config', type=click.Path(), help='Configuration file path')
def process(input_file, output_dir, config):
    """Process companies from input file and save API responses as JSON files.

    INPUT_FILE: Path to input Excel/CSV file or directory containing files
    """
    try:
        console.print(Panel.fit("🚀 Processing Companies to JSON", style="bold blue"))

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Initializing API client...", total=None)

            # Load configuration if provided
            api_config = {}
            if config and Path(config).exists():
                import json
                with open(config, 'r') as f:
                    api_config = json.load(f)

            # Initialize API client
            client = DIR_API(**api_config)

            progress.update(task, description="Processing input file...")

            # Process companies and save JSON responses
            json_files = client.process_companies_to_json(input_file, output_dir)

            progress.update(task, description="✅ Processing completed successfully!")

        console.print(f"📄 Saved {len(json_files)} JSON files to: {output_dir}", style="green")
        for json_file in json_files[:5]:  # Show first 5 files
            console.print(f"  - {os.path.basename(json_file)}", style="dim")
        if len(json_files) > 5:
            console.print(f"  ... and {len(json_files) - 5} more files", style="dim")

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        logger.exception("CLI process command failed")
        sys.exit(1)


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--database-url', default='sqlite:///duns_data.db', help='Database URL')
@click.option('--output-dir', default='responses', help='Directory to save JSON response files')
@click.option('--config', type=click.Path(), help='Configuration file path')
def workflow(input_file, database_url, output_dir, config):
    """Run the complete workflow: process companies → save JSON → populate database.

    INPUT_FILE: Path to input Excel/CSV file with company data
    """
    try:
        console.print(Panel.fit("🚀 Running Complete Workflow", style="bold magenta"))

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Initializing API client...", total=None)

            # Load configuration if provided
            api_config = {}
            if config and Path(config).exists():
                import json
                with open(config, 'r') as f:
                    api_config = json.load(f)

            # Initialize API client
            client = DIR_API(**api_config)

            progress.update(task, description="Running full workflow...")

            # Run complete workflow
            result = client.run_full_workflow(input_file, database_url, output_dir)

            progress.update(task, description="✅ Workflow completed successfully!")

        console.print(f"📄 Saved {result['json_files_saved']} JSON files to: {output_dir}", style="green")
        console.print(f"📊 Populated database with {result['database_records_processed']} records", style="green")

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        logger.exception("CLI workflow command failed")
        sys.exit(1)


@cli.command()
@click.option('--database-url', default='sqlite:///duns_data.db', help='Database URL')
@click.option('--query', help='Search query for companies')
@click.option('--limit', default=10, help='Limit number of results')
def search(database_url, query, limit):
    """Search companies in the database."""
    try:
        from sqlalchemy.orm import sessionmaker
        from .models import Company

        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Simple search implementation
        companies = session.query(Company).filter(
            Company.primary_name.contains(query)
        ).limit(limit).all()

        if companies:
            table = Table(title="Search Results")
            table.add_column("DUNS", style="cyan")
            table.add_column("Company Name", style="magenta")
            table.add_column("Country", style="green")

            for company in companies:
                table.add_row(
                    company.duns,
                    company.primary_name,
                    company.addresses[0].country_iso_alpha2_code if company.addresses else ""
                )

            console.print(table)
        else:
            console.print("No companies found", style="yellow")

        session.close()

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()