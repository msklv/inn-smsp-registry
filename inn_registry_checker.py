#!/usr/bin/env python3
"""
Console application to check INN numbers against FNS SMSP registry.

Usage:
    python inn_registry_checker.py --input input.csv --registry registry.xml --output output.csv
"""

import argparse
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Set, Optional


class INNRegistryChecker:
    """Checker for INN numbers against FNS SMSP registry."""
    
    def __init__(self, registry_path: str):
        """
        Initialize the checker with registry XML file.
        
        Args:
            registry_path: Path to the registry XML file
        """
        self.registry_path = registry_path
        self.inn_data: Dict[str, str] = {}  # INN -> Region Code
        self._load_registry()
    
    def _load_registry(self):
        """Load and parse the XML registry file."""
        print(f"Loading registry from {self.registry_path}...")
        
        try:
            # Parse XML file iteratively to handle large files
            context = ET.iterparse(self.registry_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            
            count = 0
            current_element = {}
            
            for event, elem in context:
                if event == 'end':
                    # Strip namespace from tag for easier matching
                    tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    
                    # Check for INN fields in different formats
                    if tag == 'ИННЮЛ' and elem.text:
                        current_element['inn'] = elem.text.strip()
                    elif tag == 'ИННФЛ' and elem.text:
                        current_element['inn'] = elem.text.strip()
                    elif tag == 'ИНН' and elem.text:
                        current_element['inn'] = elem.text.strip()
                    
                    # Check for region code fields
                    if tag == 'ОГРН' and elem.text and len(elem.text) >= 2:
                        # Region code is typically in the first 2 digits of OGRN
                        current_element['region'] = elem.text.strip()[:2]
                    elif tag == 'КодРегион' and elem.text:
                        current_element['region'] = elem.text.strip()
                    elif tag == 'РегионКод' and elem.text:
                        current_element['region'] = elem.text.strip()
                    
                    # When we finish processing a record
                    if tag in ('СвЮЛ', 'СвИП', 'Запись', 'Record', 'Row'):
                        if 'inn' in current_element:
                            inn = current_element['inn']
                            region = current_element.get('region', '')
                            
                            # If region is not set yet, try to extract from INN
                            if not region and len(inn) >= 2:
                                region = inn[:2]
                            
                            self.inn_data[inn] = region
                            count += 1
                            
                            if count % 10000 == 0:
                                print(f"Loaded {count} records...")
                        
                        current_element = {}
                        elem.clear()
                
                # Clear the root element periodically to free memory
                if event == 'end' and count % 50000 == 0:
                    root.clear()
            
            print(f"Registry loaded successfully. Total records: {count}")
            
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            raise
        except FileNotFoundError:
            print(f"Registry file not found: {self.registry_path}")
            raise
    
    def check_inn(self, inn: str) -> tuple[bool, str]:
        """
        Check if INN exists in registry.
        
        Args:
            inn: INN number to check (can be 10 or 12 digits)
        
        Returns:
            Tuple of (is_in_registry, region_code)
        """
        inn = inn.strip()
        if inn in self.inn_data:
            return True, self.inn_data[inn]
        else:
            # If not found, try to extract region from INN anyway
            region = inn[:2] if len(inn) >= 2 else ''
            return False, region
    
    def process_csv(self, input_path: str, output_path: str):
        """
        Process CSV file with INNs and create output CSV.
        
        Args:
            input_path: Path to input CSV file with INNs
            output_path: Path to output CSV file
        """
        print(f"Processing input file: {input_path}")
        
        processed = 0
        found = 0
        not_found = 0
        
        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                 open(output_path, 'w', encoding='utf-8', newline='') as outfile:
                
                reader = csv.reader(infile)
                writer = csv.writer(outfile)
                
                # Write header
                writer.writerow(['ИНН', 'В реестре', 'Код региона'])
                
                # Process each row
                for row in reader:
                    if not row or not row[0].strip():
                        continue
                    
                    inn = row[0].strip()
                    
                    # Skip header row if present
                    if inn.upper() in ('ИНН', 'INN'):
                        continue
                    
                    is_in_registry, region_code = self.check_inn(inn)
                    
                    status = 'Да' if is_in_registry else 'Нет'
                    writer.writerow([inn, status, region_code])
                    
                    processed += 1
                    if is_in_registry:
                        found += 1
                    else:
                        not_found += 1
                    
                    if processed % 1000 == 0:
                        print(f"Processed {processed} INNs...")
        
        except FileNotFoundError:
            print(f"Input file not found: {input_path}")
            raise
        except Exception as e:
            print(f"Error processing CSV: {e}")
            raise
        
        print(f"\nProcessing complete!")
        print(f"Total processed: {processed}")
        print(f"Found in registry: {found}")
        print(f"Not found: {not_found}")
        print(f"Output saved to: {output_path}")


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description='Check INN numbers against FNS SMSP registry',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python inn_registry_checker.py --input inn_list.csv --registry registry.xml --output result.csv
  python inn_registry_checker.py -i input.csv -r data.xml -o output.csv
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Input CSV file with INN numbers (one per line or first column)'
    )
    
    parser.add_argument(
        '-r', '--registry',
        required=True,
        help='XML file with FNS SMSP registry data'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Output CSV file with results'
    )
    
    args = parser.parse_args()
    
    # Validate input files exist
    if not Path(args.input).exists():
        print(f"Error: Input file not found: {args.input}")
        return 1
    
    if not Path(args.registry).exists():
        print(f"Error: Registry file not found: {args.registry}")
        return 1
    
    # Process
    try:
        checker = INNRegistryChecker(args.registry)
        checker.process_csv(args.input, args.output)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
