#!/usr/bin/env python3
"""
Convert apartment listings JSON files to Excel and CSV formats
"""

from services.excel_converter_service import ExcelConverterService
import sys

def main():
    """Main function to convert listings to Excel/CSV"""
    
    print("🏠 APARTMENT LISTINGS TO EXCEL/CSV CONVERTER")
    print("=" * 60)
    
    # Initialize converter service
    converter = ExcelConverterService()
    
    # Check if specific file was requested
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        print(f"📄 Converting specific file: {filename}")
        converter.convert_specific_file(filename)
    else:
        print("📊 Converting all district listing files...")
        converter.convert_all_listings()
    
    print("\n✅ Conversion completed!")

if __name__ == "__main__":
    main()
