#!/usr/bin/env python3
"""
Excel/CSV Converter Service
Converts apartment listings JSON files to Excel and CSV formats
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
import os

class ExcelConverterService:
    """Service for converting apartment listings to Excel and CSV formats"""
    
    def __init__(self, input_dir: str = "output", output_dir: str = "excel_output"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Define the preferred column order
        self.priority_columns = [
            'rooms',
            'price', 
            'rnpl_monthly_price',
            'distance_from_office_km',
            'distance_duration',
            'content',
            'full_url',
            'location_address',
            'location_lng',
            'location_lat',
            'create_time_riyadh',
            'published_at_riyadh',
            'last_update_riyadh'
        ]
    
    def convert_all_listings(self):
        """Convert all district listing files to Excel and CSV"""
        
        print("📊 EXCEL/CSV CONVERTER SERVICE")
        print("=" * 50)
        
        # Find all district listing files
        listing_files = list(self.input_dir.glob("*_listings.json"))
        
        print(f"📁 Found {len(listing_files)} district listing files")
        print(f"📂 Input directory: {self.input_dir}")
        print(f"📂 Output directory: {self.output_dir}")
        print("-" * 50)
        
        converted_count = 0
        failed_count = 0
        
        for file_path in listing_files:
            try:
                print(f"🔄 Processing: {file_path.name}")
                
                # Convert to Excel and CSV
                excel_file, csv_file = self.convert_district_file(file_path)
                
                if excel_file and csv_file:
                    print(f"✅ Created: {excel_file.name} & {csv_file.name}")
                    converted_count += 1
                else:
                    print(f"❌ Failed to convert: {file_path.name}")
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ Error processing {file_path.name}: {e}")
                failed_count += 1
        
        print("\n" + "=" * 50)
        print("📊 CONVERSION SUMMARY")
        print("=" * 50)
        print(f"✅ Successfully converted: {converted_count} files")
        print(f"❌ Failed conversions: {failed_count} files")
        print(f"📁 Output directory: {self.output_dir}")
    
    def convert_district_file(self, file_path: Path) -> tuple[Optional[Path], Optional[Path]]:
        """Convert a single district JSON file to Excel and CSV"""
        
        try:
            # Load JSON data
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract listings
            listings = data.get('data', {}).get('Web', {}).get('find', {}).get('listings', [])
            
            if not listings:
                print(f"⚠️ No listings found in {file_path.name}")
                return None, None
            
            # Convert to DataFrame
            df = self.listings_to_dataframe(listings)
            
            # Get district name from filename
            district_name = file_path.stem.replace('_listings', '')
            
            # Create output filenames
            excel_filename = f"{district_name}_listings.xlsx"
            csv_filename = f"{district_name}_listings.csv"
            
            excel_path = self.output_dir / excel_filename
            csv_path = self.output_dir / csv_filename
            
            # Save as Excel
            df.to_excel(excel_path, index=False, engine='openpyxl')
            
            # Save as CSV
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            return excel_path, csv_path
            
        except Exception as e:
            print(f"❌ Error converting {file_path.name}: {e}")
            return None, None
    
    def listings_to_dataframe(self, listings: List[Dict]) -> pd.DataFrame:
        """Convert listings to DataFrame with proper column ordering"""
        
        # Flatten the listings data
        flattened_listings = []
        
        for listing in listings:
            flattened = self.flatten_listing(listing)
            flattened_listings.append(flattened)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_listings)
        
        # Reorder columns with priority columns first
        df = self.reorder_columns(df)
        
        return df
    
    def flatten_listing(self, listing: Dict) -> Dict:
        """Flatten a single listing dictionary"""
        
        flattened = {}
        
        # Copy all top-level fields
        for key, value in listing.items():
            if key == 'location':
                # Flatten location data
                if isinstance(value, dict):
                    flattened['location_address'] = value.get('address', '')
                    flattened['location_lat'] = value.get('lat', '')
                    flattened['location_lng'] = value.get('lng', '')
                    flattened['location_district'] = value.get('district', '')
                    flattened['location_direction'] = value.get('direction', '')
                    flattened['location_city'] = value.get('city', '')
                    
                    # Flatten distance data
                    distance_info = value.get('distance_from_office', {})
                    if isinstance(distance_info, dict):
                        flattened['distance_from_office_km'] = distance_info.get('distance_km', '')
                        flattened['distance_duration'] = distance_info.get('duration_text', '')
                        flattened['distance_meters'] = distance_info.get('distance_meters', '')
                        flattened['distance_seconds'] = distance_info.get('duration_seconds', '')
                        flattened['distance_status'] = distance_info.get('status', '')
                else:
                    flattened['location'] = value
            elif key == 'user':
                # Flatten user data
                if isinstance(value, dict):
                    flattened['user_name'] = value.get('name', '')
                    flattened['user_phone'] = value.get('phone', '')
                    flattened['user_bml_license'] = value.get('bml_license_number', '')
                    flattened['user_bml_url'] = value.get('bml_url', '')
                else:
                    flattened['user'] = value
            else:
                flattened[key] = value
        
        return flattened
    
    def reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder DataFrame columns with priority columns first"""
        
        # Get all columns
        all_columns = list(df.columns)
        
        # Find priority columns that exist in the DataFrame
        existing_priority_columns = [col for col in self.priority_columns if col in all_columns]
        
        # Get remaining columns (not in priority list)
        remaining_columns = [col for col in all_columns if col not in self.priority_columns]
        
        # Sort remaining columns alphabetically
        remaining_columns.sort()
        
        # Combine: priority columns first, then remaining columns
        ordered_columns = existing_priority_columns + remaining_columns
        
        # Reorder DataFrame
        df = df[ordered_columns]
        
        return df
    
    def convert_specific_file(self, filename: str):
        """Convert a specific file by name"""
        
        file_path = self.input_dir / filename
        
        if not file_path.exists():
            print(f"❌ File not found: {filename}")
            return
        
        print(f"🔄 Converting specific file: {filename}")
        excel_file, csv_file = self.convert_district_file(file_path)
        
        if excel_file and csv_file:
            print(f"✅ Created: {excel_file.name} & {csv_file.name}")
        else:
            print(f"❌ Failed to convert: {filename}")
