#!/usr/bin/env python3
"""
E-Rate Data Fetcher and Filter
Fetches E-Rate data for specified state and year, filters for specific vendors and current form versions
Usage: python3 erate_filter.py <state> <year>
Example: python3 erate_filter.py OK 2024
"""

import requests
import json
import csv
import sys
import argparse
from typing import Dict, List, Set
import time

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

class ERateDataProcessor:
    def __init__(self):
        # Common vendor mappings for standardization
        self.vendor_mappings = {
            'cisco': ['cisco', 'cisco systems', 'meraki', 'cisco meraki'],
            'hp': ['hp', 'hewlett-packard', 'hpe', 'hewlett packard', 'aruba', 'aruba networks', 'hewlett packard enterprise', 'juniper', 'juniper networks'],
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
    
    def extract_cost_fields(self, record: Dict) -> Dict:
        """Extract all cost-related fields from a record"""
        cost_fields = {}
        cost_keywords = [
            'cost', 'price', 'amount', 'eligible', 'ineligible', 
            'monthly', 'recurring', 'one_time', 'total', 'extended',
            'quantity', 'discount'
        ]
        
        for key, value in record.items():
            if any(keyword in key.lower() for keyword in cost_keywords):
                cost_fields[key] = value
        
        return cost_fields
    
    def filter_record(self, record: Dict) -> Dict:
        """Filter and extract required fields from a record"""
        # Check if form_version is "Current"
        if record.get('form_version', '').strip() != 'Current':
            return None
        
        # Check if manufacturer is in our target list
        manufacturer = record.get('form_471_manufacturer_name', '')
        if not self.is_target_vendor(manufacturer):
            return None
        
        # Extract required base fields
        filtered_record = {
            'application_number': record.get('application_number', ''),
            'funding_year': record.get('funding_year', ''),
            'state': record.get('state', ''),
            'ben': record.get('ben', ''),
            'organization_name': record.get('organization_name', ''),
            'applicant_type': record.get('applicant_type', ''),
            'cnct_email': record.get('cnct_email', ''),
            'funding_request_number': record.get('funding_request_number', ''),
            'form_471_product_name': record.get('form_471_product_name', ''),
            'form_471_manufacturer_name': manufacturer,
            'standardized_vendor': self.get_standardized_vendor(manufacturer),
            'model_of_equipment': record.get('model_of_equipment', ''),
        }
        
        # Add all cost fields
        cost_fields = self.extract_cost_fields(record)
        filtered_record.update(cost_fields)
        
        return filtered_record
    
    def fetch_data(self, state: str, year: str, limit: int = 50000) -> List[Dict]:
        """Fetch E-Rate data from USAC API for specified state and year"""
        url = "https://opendata.usac.org/resource/hbj5-2bpj.json"
        params = {
            '$where': f"funding_year='{year}' AND state='{state}'",
            '$limit': limit
        }
        
        print(f"Fetching data from USAC API...")
        print(f"State: {state}, Year: {year}")
        print(f"URL: {url}")
        print(f"Params: {params}")
        
        try:
            response = requests.get(url, params=params, timeout=300)
            response.raise_for_status()
            
            data = response.json()
            print(f"Successfully fetched {len(data)} records")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            sys.exit(1)
    
    def process_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Process raw data and apply filters"""
        filtered_records = []
        total_records = len(raw_data)
        
        print(f"Processing {total_records} records...")
        
        for i, record in enumerate(raw_data):
            if i % 1000 == 0:
                print(f"Processed {i}/{total_records} records...")
            
            filtered_record = self.filter_record(record)
            if filtered_record:
                filtered_records.append(filtered_record)
        
        print(f"Filtered to {len(filtered_records)} records matching criteria")
        return filtered_records
    
    def save_to_csv(self, data: List[Dict], state: str, year: str):
        """Save filtered data to CSV"""
        if not data:
            print("No data to save")
            return
        
        filename = f'{state.lower()}_erate_filtered_{year}.csv'
        
        # Get all unique field names
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
        
        fieldnames = sorted(list(all_fields))
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"Data saved to {filename}")
    
    def save_to_json(self, data: List[Dict], state: str, year: str):
        """Save filtered data to JSON"""
        filename = f'{state.lower()}_erate_filtered_{year}.json'
            
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"Data saved to {filename}")
    
    def generate_summary(self, data: List[Dict], state: str, year: str, school_threshold: float = 250000, sku_threshold: float = 100000):
        """Generate summary statistics with enhanced visual formatting"""
        if not data:
            print("No data for summary")
            return
        
        # Enhanced header with colors
        print("\n" + Colors.BOLD + "="*80 + Colors.END)
        print(f"{Colors.BOLD}{Colors.HEADER}{state.upper()} E-RATE {year} - VENDOR ANALYSIS SUMMARY{Colors.END}")
        print(Colors.BOLD + "="*80 + Colors.END)
        
        # Count by vendor
        vendor_counts = {}
        vendor_totals = {}
        vendor_schools = {}  # Track schools per vendor
        total_funding = 0
        
        # School-level analysis
        school_vendor_totals = {}  # {vendor: {school: total_amount}}
        school_vendor_items = {}   # {vendor: {school: [items]}}
        
        for record in data:
            vendor = record.get('standardized_vendor', 'Unknown')
            school = record.get('organization_name', 'Unknown School')
            
            # Calculate total cost for this record
            cost = 0
            cost_field = record.get('pre_discount_extended_eligible_line_item_costs', '0')
            try:
                cost = float(cost_field) if cost_field else 0
            except (ValueError, TypeError):
                cost = 0
            
            # Vendor totals
            vendor_counts[vendor] = vendor_counts.get(vendor, 0) + 1
            vendor_totals[vendor] = vendor_totals.get(vendor, 0) + cost
            total_funding += cost
            
            # School-vendor totals
            if vendor not in school_vendor_totals:
                school_vendor_totals[vendor] = {}
                school_vendor_items[vendor] = {}
            
            if school not in school_vendor_totals[vendor]:
                school_vendor_totals[vendor][school] = 0
                school_vendor_items[vendor][school] = []
            
            school_vendor_totals[vendor][school] += cost
            
            # Store item details for SKU analysis
            item_info = {
                'model': record.get('model_of_equipment', 'Unknown Model'),
                'quantity': record.get('one_time_quantity', '0'),
                'cost': cost,
                'product_name': record.get('form_471_product_name', 'Unknown Product')
            }
            school_vendor_items[vendor][school].append(item_info)
        
        # Summary statistics with colors
        print(f"{Colors.CYAN}Total Records:{Colors.END} {Colors.BOLD}{len(data):,}{Colors.END}")
        print(f"{Colors.CYAN}Total Funding:{Colors.END} {Colors.BOLD}{Colors.GREEN}${total_funding:,.2f}{Colors.END}")
        print(f"{Colors.CYAN}Unique Organizations:{Colors.END} {Colors.BOLD}{len(set(r.get('organization_name', '') for r in data)):,}{Colors.END}")
        print(f"{Colors.CYAN}School Threshold:{Colors.END} {Colors.YELLOW}${school_threshold:,.0f}{Colors.END}")
        print(f"{Colors.CYAN}SKU Threshold:{Colors.END} {Colors.YELLOW}${sku_threshold:,.0f}{Colors.END}")
        print()
        
        print(f"{Colors.BOLD}{Colors.UNDERLINE}VENDOR BREAKDOWN:{Colors.END}")
        print()
        
        # Sort vendors by funding amount
        sorted_vendors = sorted(vendor_totals.items(), key=lambda x: x[1], reverse=True)
        
        for vendor_idx, (vendor, total) in enumerate(sorted_vendors, 1):
            count = vendor_counts.get(vendor, 0)
            percentage = (total / total_funding * 100) if total_funding > 0 else 0
            
            # Main vendor line with enhanced formatting
            print(f"{Colors.BOLD}{Colors.VENDOR}#{vendor_idx:2d}  {vendor:<18}{Colors.END} "
                  f"{Colors.BOLD}{Colors.GREEN}${total:>13,.2f}{Colors.END} "
                  f"{Colors.CYAN}({percentage:5.1f}%){Colors.END} - "
                  f"{Colors.BOLD}{count:4d}{Colors.END} items")
            
            # Show schools with spending above threshold for this vendor
            if vendor in school_vendor_totals:
                # Sort schools by total spending (descending)
                school_totals = sorted(
                    school_vendor_totals[vendor].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )
                
                schools_shown = 0
                for school_name, school_total in school_totals:
                    if school_total >= school_threshold:
                        schools_shown += 1
                        
                        # School line with indentation and colors
                        print(f"    {Colors.SCHOOL}▶ {school_name:<50}{Colors.END} "
                              f"{Colors.BOLD}{Colors.GREEN}${school_total:>10,.0f}{Colors.END}")
                        
                        # Show individual SKUs above threshold
                        if school_name in school_vendor_items[vendor]:
                            items = school_vendor_items[vendor][school_name]
                            # Sort items by cost (descending)
                            sorted_items = sorted(items, key=lambda x: x['cost'], reverse=True)
                            
                            skus_shown = 0
                            for item in sorted_items:
                                if item['cost'] >= sku_threshold:
                                    skus_shown += 1
                                    qty_str = f"Qty {item['quantity']}" if item['quantity'] and item['quantity'] != '0' else "Qty N/A"
                                    
                                    # Truncate long model names
                                    model = item['model']
                                    if len(model) > 45:
                                        model = model[:42] + "..."
                                    
                                    print(f"        {Colors.SKU}• {model:<45}{Colors.END} "
                                          f"{Colors.BLUE}{qty_str:>8}{Colors.END} "
                                          f"{Colors.BOLD}{Colors.GREEN}${item['cost']:>10,.0f}{Colors.END}")
                            
                            if skus_shown > 0:
                                print()  # Add space after SKUs
                
                if schools_shown == 0:
                    print(f"    {Colors.SKU}(No schools above ${school_threshold:,.0f} threshold){Colors.END}")
            
            print()  # Add blank line after each vendor
        
        # Footer with analysis info
        print(f"{Colors.BOLD}─{Colors.END}" * 80)
        print(f"{Colors.CYAN}Analysis completed at: {Colors.END}{time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{Colors.CYAN}Data source: {Colors.END}USAC Open Data API - FRN Line Items")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Fetch and filter E-Rate data for network equipment vendors",
        epilog="Example: python3 erate_filter.py OK 2024 --school-threshold 500000 --sku-threshold 200000"
    )
    
    parser.add_argument('state', 
                       help='State abbreviation (e.g., OK, TX, CA)')
    
    parser.add_argument('year', 
                       help='Funding year (e.g., 2024, 2023)')
    
    parser.add_argument('--limit', 
                       type=int, 
                       default=50000,
                       help='Maximum number of records to fetch (default: 50000)')
    
    parser.add_argument('--school-threshold',
                       type=float,
                       default=250000,
                       help='Minimum school spending threshold to display (default: $250,000)')
    
    parser.add_argument('--sku-threshold',
                       type=float,
                       default=100000,
                       help='Minimum SKU cost threshold to display (default: $100,000)')
    
    parser.add_argument('--save-csv',
                       action='store_true',
                       help='Save results to CSV file')
    
    parser.add_argument('--save-json',
                       action='store_true',
                       help='Save results to JSON file')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Validate state format (should be 2-letter abbreviation)
    if len(args.state) != 2:
        print("Error: State should be a 2-letter abbreviation (e.g., OK, TX, CA)")
        sys.exit(1)
    
    # Validate year format
    try:
        year_int = int(args.year)
        if year_int < 2000 or year_int > 2030:
            print("Error: Year should be between 2000 and 2030")
            sys.exit(1)
    except ValueError:
        print("Error: Year must be a valid integer")
        sys.exit(1)
    
    # Validate thresholds
    if args.school_threshold < 0:
        print("Error: School threshold must be non-negative")
        sys.exit(1)
        
    if args.sku_threshold < 0:
        print("Error: SKU threshold must be non-negative")
        sys.exit(1)
    
    state = args.state.upper()
    year = args.year
    
    print(f"Starting E-Rate data analysis for {state} {year}")
    
    processor = ERateDataProcessor()
    
    # Fetch data
    raw_data = processor.fetch_data(state, year, args.limit)
    
    # Process and filter
    filtered_data = processor.process_data(raw_data)
    
    # Save results only if requested
    if args.save_csv:
        processor.save_to_csv(filtered_data, state, year)
    
    if args.save_json:
        processor.save_to_json(filtered_data, state, year)
    
    # Generate summary with configurable thresholds
    processor.generate_summary(filtered_data, state, year, args.school_threshold, args.sku_threshold)
    
    if args.save_csv or args.save_json:
        print(f"\nProcessing complete! Check the output files for results.")
    else:
        print(f"\nProcessing complete! Use --save-csv or --save-json to export data.")

if __name__ == "__main__":
    main()