# E-Rate Data Analyzer

Python scripts to analyze E-Rate funding data from the USAC Open Data API.

## Scripts

- `usac_school_query.py` - Historical analysis for a specific school/organization
- `usac_year_query.py` - State-wide analysis for a specific year

## Requirements

- Python 3.6+
- `requests` library

```bash
pip install requests
```

## Usage

### School History
```bash
python3 usac_school_query.py "TULSA INDEP SCHOOL DISTRICT 1"
python3 usac_school_query.py "OKLAHOMA CITY PUBLIC SCHOOLS" --sku-threshold 50000 --save-csv
```

### State Analysis
```bash
python3 usac_year_query.py OK 2024
python3 usac_year_query.py TX 2024 --school-threshold 500000 --save-csv
```

## Options

### usac_school_query.py
- `--sku-threshold` - Minimum line item cost to display (default: $100,000)
- `--save-csv` - Save results to CSV file

### usac_year_query.py
- `--school-threshold` - Minimum school spending to display (default: $250,000)
- `--sku-threshold` - Minimum line item cost to display (default: $100,000)
- `--save-csv` - Save results to CSV file
- `--save-json` - Save results to JSON file

## Data Source

USAC Open Data API - publicly available E-Rate funding information.