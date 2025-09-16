#!/usr/bin/env python3
"""
E-Rate School History Analyzer
Fetches E-Rate data for a specific organization across all years (2016-2025) and analyzes vendor spending patterns
Usage: python3 usac_school_query.py "<organization_name>"
       python3 usac_school_query.py --find-school "PIEDMONT"
Example: python3 usac_school_query.py "TULSA INDEP SCHOOL DISTRICT 1"
         python3 usac_school_query.py --find-school "PIEDMONT" --state "CA"
"""

import requests
import json
import csv
import sys
import argparse
from typing import Dict, List, Set, Tuple
import time
from collections import defaultdict

# ANSI color codes for terminal formatting
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    
    # Vendor-specific colors
    VENDOR = '\033[1;36m'  # Bold cyan
    SCHOOL = '\033[1;33m'  # Bold yellow
    SKU = '\033[37m'       # Light gray

class SchoolERateAnalyzer:
    def __init__(self):
        # Common vendor mappings for standardization
        self.vendor_mappings = {
            'cisco': ['cisco', 'cisco systems', 'meraki', 'cisco meraki'],
            'hp': ['hp', 'hewlett-packard', 'hpe', 'hewlett packard', 'aruba', 'aruba networks', 'hewlett packard enterprise'],
            'juniper': ['juniper', 'juniper networks'],
            'ubiquiti': ['ubiquiti', 'ubnt', 'ubiquiti networks'],
            'arista': ['arista', 'arista networks'],
            'fortinet': ['fortinet', 'fortigate'],
            'ruckus': ['ruckus', 'ruckus wireless', 'commscope ruckus'],
            'netgear': ['netgear'],
            'dell': ['dell', 'dell emc', 'dell technologies'],
            'extreme': ['extreme', 'extreme networks'],
            'allied': ['allied', 'allied telesis'],
            'adtran': ['adtran'],
            'sonicwall': ['sonicwall'],
            'watchguard': ['watchguard', 'watch guard technologies'],
            'palo alto': ['palo alto', 'palo alto networks'],
        }
        
        # Create a flat set of all vendor names for quick lookup
        self.target_vendors = set()
        for vendor_list in self.vendor_mappings.values():
            self.target_vendors.update([v.lower() for v in vendor_list])
    
    def is_target_vendor(self, manufacturer_name: str) -> bool:
        """Check if manufacturer name matches our target vendors"""
        if not manufacturer_name:
            return False
        return manufacturer_name.lower().strip() in self.target_vendors
    
    def get_standardized_vendor(self, manufacturer_name: str) -> str:
        """Get the standardized vendor name"""
        if not manufacturer_name:
            return ""
        
        name_lower = manufacturer_name.lower().strip()
        for standard_name, variants in self.vendor_mappings.items():
            if name_lower in [v.lower() for v in variants]:
                return standard_name.title()
        return manufacturer_name
    
    def extract_cost_from_record(self, record: Dict) -> float:
        """Extract the cost amount from a record"""
        cost_field = record.get('pre_discount_extended_eligible_line_item_costs', '0')
        try:
            return float(cost_field) if cost_field else 0
        except (ValueError, TypeError):
            return 0
    
    def filter_record(self, record: Dict, organization_name: str) -> Dict:
        """Filter and extract required fields from a record for the specified organization"""
        # Since we're now filtering at the API level, we mainly need to check vendor targeting
        # The form_version='Current' and organization name are already filtered by the API
        
        # Check if manufacturer is in our target list
        manufacturer = record.get('form_471_manufacturer_name', '')
        if not self.is_target_vendor(manufacturer):
            return None
        
        # Extract required fields
        filtered_record = {
            'funding_year': record.get('funding_year', ''),
            'application_number': record.get('application_number', ''),
            'organization_name': record.get('organization_name', ''),
            'form_471_product_name': record.get('form_471_product_name', ''),
            'form_471_manufacturer_name': manufacturer,
            'standardized_vendor': self.get_standardized_vendor(manufacturer),
            'model_of_equipment': record.get('model_of_equipment', ''),
            'one_time_quantity': record.get('one_time_quantity', '0'),
            'cost': self.extract_cost_from_record(record),
        }
        
        return filtered_record
    
    def find_schools(self, search_term: str, state_filter: str = None, limit: int = 100) -> List[Tuple[str, str, List[str]]]:
        """Find schools that match the search term"""
        url = "https://opendata.usac.org/resource/hbj5-2bpj.json"
        
        # Escape single quotes in search term for SQL query
        search_escaped = search_term.replace("'", "''")
        
        # Start with a simple query - just search for the term in organization name
        # Use a more basic WHERE clause that the API definitely supports
        where_clause = f"upper(organization_name) like upper('%{search_escaped}%')"
        
        if state_filter:
            state_escaped = state_filter.replace("'", "''").upper()
            where_clause += f" and upper(state)='{state_escaped}'"
        
        params = {
            '$where': where_clause,
            '$limit': 1000,  # Get a reasonable number of records
        }
        
        print(f"Searching for organizations matching '{search_term}'...")
        if state_filter:
            print(f"Filtering by state: {state_filter.upper()}")
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                print(f"No organizations found matching '{search_term}'")
                return []
            
            print(f"  Found {len(data)} total records, processing unique organizations...")
            
            # Process results to get unique organizations with their years
            org_data = defaultdict(lambda: {'state': '', 'years': set()})
            
            for record in data:
                org_name = record.get('organization_name', '').strip()
                org_state = record.get('state', '').strip()  # Changed from organization_state to state
                year = record.get('funding_year', '')
                
                if org_name:
                    if org_state:  # Only update state if we have valid data
                        org_data[org_name]['state'] = org_state
                    if year:
                        org_data[org_name]['years'].add(year)
            
            # Convert to list of tuples and sort
            results = []
            for org_name, info in org_data.items():
                years_list = sorted(list(info['years']))
                results.append((org_name, info['state'], years_list))
            
            # Sort by organization name (case-insensitive)
            results.sort(key=lambda x: x[0].upper())
            
            print(f"  Found {len(results)} unique organizations")
            
            return results[:limit]  # Limit final results
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching for schools: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing search response: {e}")
            return []
    
    def display_school_search_results(self, results: List[Tuple[str, str, List[str]]], search_term: str):
        """Display the search results in a formatted way"""
        if not results:
            print(f"No schools found matching '{search_term}'")
            return
        
        print("\n" + Colors.BOLD + "="*80 + Colors.END)
        print(f"{Colors.BOLD}{Colors.HEADER}SCHOOL SEARCH RESULTS FOR '{search_term.upper()}'{Colors.END}")
        print(Colors.BOLD + "="*80 + Colors.END)
        print(f"Found {Colors.BOLD}{len(results)}{Colors.END} matching organizations:\n")
        
        for i, (org_name, state, years) in enumerate(results, 1):
            # Format years display
            if years:
                years_str = f"({', '.join(years)})"
                if len(years_str) > 30:
                    years_str = f"({len(years)} years: {min(years)}-{max(years)})"
            else:
                years_str = "(no data found)"
            
            # Format state display
            state_str = f"[{state}]" if state else "[State Unknown]"
            
            # Display with formatting - show state prominently next to the name
            print(f"{Colors.BLUE}{i:2d}.{Colors.END} "
                  f"{Colors.BOLD}{Colors.SCHOOL}{org_name}{Colors.END} "
                  f"{Colors.BOLD}{Colors.RED}{state_str}{Colors.END}")
            
            print(f"     {Colors.CYAN}E-Rate funding years:{Colors.END} {Colors.YELLOW}{years_str}{Colors.END}")
            print()
        
        print(Colors.BOLD + "─" * 80 + Colors.END)
        print(f"{Colors.CYAN}Usage:{Colors.END} Copy the exact organization name from above and run:")
        print(f'{Colors.GREEN}python3 usac_school_query.py "EXACT_ORGANIZATION_NAME"{Colors.END}')
        print(f"\n{Colors.CYAN}Tip:{Colors.END} Use quotes around organization names that contain spaces")
    
    def fetch_data_for_year(self, organization_name: str, year: str, limit: int = 10000) -> List[Dict]:
        """Fetch E-Rate data from USAC API for specified organization and year"""
        url = "https://opendata.usac.org/resource/hbj5-2bpj.json"
        
        # Escape single quotes in organization name for SQL query
        org_escaped = organization_name.replace("'", "''")
        
        # Build query with multiple conditions:
        # 1. Exact match first (most efficient)
        # 2. Form version is Current (reduces dataset significantly) 
        # 3. Funding year match
        params = {
            '$where': f"funding_year='{year}' AND form_version='Current' AND upper(organization_name)=upper('{org_escaped}')",
            '$limit': limit
        }
        
        print(f"  Fetching {year} data (exact match)...")
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # If exact match returns no results, try partial match
            if not data:
                print(f"  No exact match found, trying partial match for {year}...")
                params['$where'] = f"funding_year='{year}' AND form_version='Current' AND upper(organization_name) LIKE upper('%{org_escaped}%')"
                response = requests.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
            
            print(f"    Found {len(data)} records for {year}")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching {year} data: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"  Error parsing {year} JSON response: {e}")
            return []
    
    def fetch_all_years_data(self, organization_name: str) -> Dict[str, List[Dict]]:
        """Fetch data for all years from 2016-2025"""
        years = [str(year) for year in range(2016, 2026)]
        all_data = {}
        
        print(f"Fetching E-Rate data for '{organization_name}' across all years...")
        
        for year in years:
            year_data = self.fetch_data_for_year(organization_name, year)
            if year_data:
                all_data[year] = year_data
        
        return all_data
    
    def process_year_data(self, raw_data: List[Dict], organization_name: str, year: str) -> List[Dict]:
        """Process raw data for a specific year"""
        filtered_records = []
        
        for record in raw_data:
            filtered_record = self.filter_record(record, organization_name)
            if filtered_record:
                filtered_records.append(filtered_record)
        
        return filtered_records
    
    def generate_school_history_report(self, organization_name: str, all_data: Dict[str, List[Dict]], sku_threshold: float = 100000):
        """Generate comprehensive history report for the school"""
        if not all_data:
            print(f"No E-Rate data found for '{organization_name}'")
            return
        
        # Process all years data
        processed_data = {}
        total_all_years = 0
        
        for year, raw_data in all_data.items():
            processed_records = self.process_year_data(raw_data, organization_name, year)
            if processed_records:
                processed_data[year] = processed_records
                year_total = sum(record['cost'] for record in processed_records)
                total_all_years += year_total
        
        if not processed_data:
            print(f"No network equipment E-Rate data found for '{organization_name}' in any year")
            return
        
        # Enhanced header
        print("\n" + Colors.BOLD + "="*80 + Colors.END)
        print(f"{Colors.BOLD}{Colors.HEADER}{organization_name.upper()}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.HEADER}E-RATE NETWORK EQUIPMENT HISTORY (2016-2025){Colors.END}")
        print(Colors.BOLD + "="*80 + Colors.END)
        
        # Summary statistics
        years_with_data = list(processed_data.keys())
        print(f"{Colors.CYAN}Years with Data:{Colors.END} {Colors.BOLD}{', '.join(sorted(years_with_data))}{Colors.END}")
        print(f"{Colors.CYAN}Total Funding:{Colors.END} {Colors.BOLD}{Colors.GREEN}${total_all_years:,.2f}{Colors.END}")
        print(f"{Colors.CYAN}SKU Threshold:{Colors.END} {Colors.YELLOW}${sku_threshold:,.0f}{Colors.END}")
        print()
        
        # Process each year
        sorted_years = sorted(processed_data.keys())
        
        for year in sorted_years:
            records = processed_data[year]
            
            # Group by vendor and calculate totals
            vendor_data = defaultdict(lambda: {'total': 0, 'items': []})
            
            for record in records:
                vendor = record['standardized_vendor']
                cost = record['cost']
                
                vendor_data[vendor]['total'] += cost
                vendor_data[vendor]['items'].append({
                    'model': record['model_of_equipment'],
                    'product_name': record['form_471_product_name'],
                    'quantity': record['one_time_quantity'],
                    'cost': cost,
                    'app_number': record['application_number']
                })
            
            # Sort vendors by total spending (descending)
            sorted_vendors = sorted(vendor_data.items(), key=lambda x: x[1]['total'], reverse=True)
            
            # Display year header and vendors
            for vendor_idx, (vendor, data) in enumerate(sorted_vendors):
                total = data['total']
                
                if vendor_idx == 0:  # First vendor for the year
                    print(f"{Colors.BOLD}{Colors.BLUE}{year}{Colors.END} - "
                          f"{Colors.BOLD}{Colors.VENDOR}{vendor}{Colors.END} "
                          f"{Colors.BOLD}{Colors.GREEN}${total:,.0f}{Colors.END}")
                else:  # Additional vendors for same year
                    print(f"     - {Colors.BOLD}{Colors.VENDOR}{vendor}{Colors.END} "
                          f"{Colors.BOLD}{Colors.GREEN}${total:,.0f}{Colors.END}")
                
                # Show items above threshold
                items_above_threshold = [item for item in data['items'] if item['cost'] >= sku_threshold]
                
                if items_above_threshold:
                    # Sort items by cost (descending)
                    items_above_threshold.sort(key=lambda x: x['cost'], reverse=True)
                    
                    for item in items_above_threshold:
                        qty_str = f"Qty {item['quantity']}" if item['quantity'] and item['quantity'] != '0' else "Qty N/A"
                        
                        # Truncate long model names
                        model = item['model']
                        if len(model) > 45:
                            model = model[:42] + "..."
                        
                        # Try to extract application info from model or use app number
                        app_info = ""
                        if '(' in item['model'] and ')' in item['model']:
                            # Model already contains application info
                            pass
                        else:
                            # Add application number as prefix
                            app_info = f"(App {item['app_number']}) "
                            if len(app_info + model) > 45:
                                available_space = 45 - len(app_info) - 3  # -3 for "..."
                                if available_space > 10:  # Only add if we have reasonable space
                                    model = app_info + model[:available_space] + "..."
                                else:
                                    model = model[:42] + "..."
                            else:
                                model = app_info + model
                        
                        print(f"        {Colors.SKU}• {model:<45}{Colors.END} "
                              f"{Colors.BLUE}{qty_str:>8}{Colors.END} "
                              f"{Colors.BOLD}**{Colors.GREEN}${item['cost']:>10,.0f}{Colors.END}{Colors.BOLD}**{Colors.END}")
                else:
                    print(f"        {Colors.SKU}* (no single line items over ${sku_threshold:,.0f}){Colors.END}")
                
                print()  # Blank line after each vendor
        
        # Footer
        print(f"{Colors.BOLD}─{Colors.END}" * 80)
        print(f"{Colors.CYAN}Analysis completed at: {Colors.END}{time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{Colors.CYAN}Data source: {Colors.END}USAC Open Data API - FRN Line Items")
    
    def save_to_csv(self, all_processed_data: Dict[str, List[Dict]], organization_name: str):
        """Save all years data to CSV"""
        if not all_processed_data:
            print("No data to save")
            return
        
        # Flatten all data into single list
        all_records = []
        for year, records in all_processed_data.items():
            all_records.extend(records)
        
        if not all_records:
            print("No filtered records to save")
            return
        
        # Clean organization name for filename
        clean_org_name = "".join(c for c in organization_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_org_name = clean_org_name.replace(' ', '_').lower()
        
        filename = f'{clean_org_name}_erate_history.csv'
        
        # Get all unique field names
        all_fields = set()
        for record in all_records:
            all_fields.update(record.keys())
        
        fieldnames = sorted(list(all_fields))
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)
        
        print(f"Historical data saved to {filename}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Analyze E-Rate funding history for a specific school/organization across all years",
        epilog='''Examples:
  python3 usac_school_query.py "TULSA INDEP SCHOOL DISTRICT 1" --sku-threshold 50000
  python3 usac_school_query.py --find-school "PIEDMONT"
  python3 usac_school_query.py --find-school "PIEDMONT" --state "CA"
  python3 usac_school_query.py --find-school "UNIVERSITY" --state "TX" --limit 50'''
    )
    
    # Create mutually exclusive group for main action
    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument('organization', 
                       nargs='?',
                       help='Organization name for analysis (use quotes for names with spaces)')
    
    group.add_argument('--find-school',
                       metavar='SEARCH_TERM',
                       help='Search for schools/organizations matching the given term')
    
    # Optional arguments
    parser.add_argument('--state',
                       help='Filter search results by state (2-letter code, e.g., "CA", "TX")')
    
    parser.add_argument('--limit',
                       type=int,
                       default=50,
                       help='Maximum number of search results to return (default: 50)')
    
    parser.add_argument('--sku-threshold',
                       type=float,
                       default=100000,
                       help='Minimum line item cost threshold to display (default: $100,000)')
    
    parser.add_argument('--save-csv',
                       action='store_true',
                       help='Save results to CSV file')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    analyzer = SchoolERateAnalyzer()
    
    # Handle school search mode
    if args.find_school:
        search_term = args.find_school.strip()
        
        if not search_term:
            print("Error: Search term cannot be empty")
            sys.exit(1)
        
        # Validate state filter if provided
        if args.state and len(args.state) != 2:
            print("Error: State must be a 2-letter code (e.g., 'CA', 'TX')")
            sys.exit(1)
        
        # Validate limit
        if args.limit <= 0:
            print("Error: Limit must be positive")
            sys.exit(1)
        
        # Perform school search
        results = analyzer.find_schools(search_term, args.state, args.limit)
        analyzer.display_school_search_results(results, search_term)
        
        return
    
    # Handle analysis mode
    organization_name = args.organization.strip()
    
    if not organization_name:
        print("Error: Organization name cannot be empty")
        sys.exit(1)
    
    # Validate threshold
    if args.sku_threshold < 0:
        print("Error: SKU threshold must be non-negative")
        sys.exit(1)
    
    print(f"Starting E-Rate historical analysis for: {organization_name}")
    
    # Fetch data for all years
    all_data = analyzer.fetch_all_years_data(organization_name)
    
    if not all_data:
        print(f"No data found for '{organization_name}' in any year (2016-2025)")
        print(f"\nTip: Try searching for similar organizations first:")
        print(f"python3 usac_school_query.py --find-school \"{organization_name.split()[0]}\"")
        sys.exit(1)
    
    # Process all years and generate report
    processed_data = {}
    for year, raw_data in all_data.items():
        processed_records = analyzer.process_year_data(raw_data, organization_name, year)
        if processed_records:
            processed_data[year] = processed_records
    
    # Generate comprehensive report
    analyzer.generate_school_history_report(organization_name, all_data, args.sku_threshold)
    
    # Save to CSV if requested
    if args.save_csv:
        analyzer.save_to_csv(processed_data, organization_name)
    
    if args.save_csv:
        print(f"\nAnalysis complete! Check the CSV file for detailed data.")
    else:
        print(f"\nAnalysis complete! Use --save-csv to export detailed data.")

if __name__ == "__main__":
    main()