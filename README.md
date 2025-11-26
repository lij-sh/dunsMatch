# dunsMatch

A comprehensive Python package for performing DUNS number matching using D&B Identity Resolution services with robust error handling, async support, and extensive validation.

## Features

- **Advanced Matching**: Match companies to DUNS numbers using D&B Identity Resolution CleanseMatch API
- **Multiple Output Formats**: Excel, CSV, and JSON output support
- **Async Processing**: High-performance asynchronous API calls with aiohttp
- **Robust Error Handling**: Retry logic with exponential backoff for network resilience
- **Data Validation**: Pydantic models for input/output validation
- **Database Integration**: SQLAlchemy-based storage with support for multiple databases
- **Rich CLI**: Modern command-line interface with progress bars and colored output
- **Comprehensive Logging**: Structured logging with configurable levels
- **Testing**: Full test suite with mocking and fixtures

## Installation

### Recommended: Using Virtual Environment
```bash
git clone https://github.com/lij-sh/dunsMatch.git
cd dunsMatch

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
# source venv/bin/activate

# Install package
pip install -e .
```

### Alternative: From Source (without venv)
```bash
git clone https://github.com/lij-sh/dunsMatch.git
cd dunsMatch
pip install -e .
```

### Dependencies
The package requires Python 3.9+ and includes the following key dependencies:
- `requests` / `aiohttp` for HTTP operations
- `pandas` for data processing
- `SQLAlchemy` for database operations
- `pydantic` for data validation
- `tenacity` for retry logic
- `click` and `rich` for CLI
- `tqdm` for progress bars

## Setup

1. **Environment Variables**: Create a `.env` file in your project root:
   ```env
   DNB_API_URL=https://plus.dnb.com
   DNB_API_KEY=your_api_key_here
   DNB_API_SECRET=your_api_secret_here
   ```

2. **Database** (optional): Configure database URL if using database features:
   ```env
   DATABASE_URL=sqlite:///duns_data.db
   ```

## Usage

The API focuses on a streamlined workflow: **initialize database → process companies → save JSON responses → populate database**.

### Python API

#### Complete Workflow (Recommended)
```python
from dunsMatchAPI.client import DIR_API
import pandas as pd

# Initialize client
client = DIR_API()

# Run complete workflow
df = pd.DataFrame({
    'company_name': ['Apple Inc.', 'Microsoft Corp.'],
    'country': ['US', 'US'],
    'address': ['Cupertino, CA', 'Redmond, WA']
})

result = client.run_full_workflow(
    input_data=df,
    database_url='sqlite:///duns_data.db',
    output_dir='responses'
)

print(f"Processed {result['json_files_saved']} companies")
print(f"Database populated with {result['database_records_processed']} records")
```

#### Step-by-Step Usage
```python
from dunsMatchAPI.client import DIR_API
import pandas as pd

# 1. Initialize client
client = DIR_API()

# 2. Initialize database
client.initialize_database('sqlite:///duns_data.db')

# 3. Process companies and save JSON responses
df = pd.DataFrame({
    'company_name': ['Apple Inc.', 'Microsoft Corp.'],
    'country': ['US', 'US'],
    'address': ['Cupertino, CA', 'Redmond, WA']
})

json_files = client.process_companies_to_json(df, output_dir='responses')

# 4. Populate database from JSON files
processed_count = client.populate_database_from_json(json_files)
```

#### Input Formats Supported
```python
# From pandas DataFrame
df = pd.DataFrame({
    'company_name': ['Company A', 'Company B'],
    'country': ['US', 'GB'],
    'address': ['123 Main St', '456 High St']
})
client.process_companies_to_json(df)

# From Excel file
client.process_companies_to_json('companies.xlsx')

# From CSV file (if pandas can read it)
client.process_companies_to_json('companies.csv')
```

### Command Line Interface

First activate your virtual environment, then use the CLI:

```bash
# Activate virtual environment (if using venv)
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux

# Run complete workflow (recommended)
duns-match workflow input.xlsx --database-url sqlite:///duns_data.db

# Or step-by-step:

# 1. Process companies and save JSON responses
duns-match process input.xlsx --output-dir responses

# 2. Populate database from JSON files
duns-match populate responses/ --database-url sqlite:///duns_data.db

# Search companies in database
duns-match search --query "Apple" --limit 5
```

### CLI Commands

- **`workflow`**: Complete pipeline (process → JSON → database)
- **`process`**: Process companies and save API responses as JSON files
- **`populate`**: Populate database from JSON response files
- **`search`**: Search companies in the database

#### Asynchronous Usage
```python
import asyncio
from dunsMatchAPI.matcher import Matcher
from dunsMatchAPI.auth import Authenticator

async def main():
    auth = Authenticator()
    async with Matcher(auth) as matcher:
        matches = await matcher.match_company_async("Apple Inc.", "US", "Cupertino, CA")
        print(f"Found {len(matches)} matches")

asyncio.run(main())
```

#### Database Operations
```python
from dunsMatchAPI.database import DatabaseManager
from sqlalchemy import create_engine

engine = create_engine("sqlite:///duns_data.db")
db_manager = DatabaseManager(engine)
db_manager.ensure_initialized()

# Populate from JSON files
db_manager.populate_from_json(['results.json'])
```

## Input/Output Formats

### Input Excel Format
| company_name | country | address |
|-------------|---------|---------|
| Apple Inc. | US | 1 Apple Park Way |
| Microsoft | US | Redmond, WA |

### Output Formats
- **Excel**: Detailed spreadsheet with match results
- **CSV**: Comma-separated values for data analysis
- **JSON**: Structured data for API integration

## Configuration

Create a `config.json` for advanced settings:
```json
{
  "api_url": "https://plus.dnb.com",
  "retry_attempts": 3,
  "batch_size": 100,
  "log_level": "INFO"
}
```

## Testing

Run the test suite:
```bash
pip install pytest
pytest tests/
```

## API Reference

### DIR_API
Main client class orchestrating all operations.

**Methods:**
- `match_company(name, country, address)`: Match single company
- `match_companies_from_excel(input_file, output_file, format)`: Match from Excel file or DataFrame
- `match_companies_from_dataframe(df, output_file, format)`: Match from pandas DataFrame
- `load_excel(file_path)`: Load company data from Excel or DataFrame
- `request_matches_to_json(input, output_dir)`: Request matches and save JSON
- `populate_database_from_json(json_files)`: Populate database from JSON files

### Authenticator
Handles API authentication with token management.

### Matcher
Core matching logic with retry and async support.

### DatabaseManager
Database operations with SQLAlchemy.

## Error Handling

The package includes comprehensive error handling:
- **Network Errors**: Automatic retries with exponential backoff
- **Authentication**: Token refresh and credential validation
- **API Limits**: Rate limiting and quota management
- **Data Validation**: Pydantic models prevent invalid data
- **Database**: Transaction rollback on failures

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.