#!/usr/bin/env python3
"""
Verify the results from the latest scraping
"""

import json
from pathlib import Path

def verify_latest_results():
    """Verify the latest scraping results"""
    
    output_dir = Path("output")
    
    # Find the latest file
    json_files = list(output_dir.glob("*_listings.json"))
    if not json_files:
        print("❌ No results files found")
        return
    
    # Get the latest file (by modification time)
    latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
    
    print(f"📁 Latest file: {latest_file.name}")
    print(f"📊 File size: {latest_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Read the file
    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract metadata
    metadata = data.get('metadata', {})
    district_id = metadata.get('district_id')
    district_name = metadata.get('district_name')
    total_listings = metadata.get('total_listings')
    expected_listings = metadata.get('expected_listings')
    family_type = metadata.get('family_type')
    pagination = metadata.get('pagination', {})
    
    print(f"\n📋 METADATA:")
    print(f"  District: {district_name} (ID: {district_id})")
    print(f"  Total listings: {total_listings}")
    print(f"  Expected listings: {expected_listings}")
    print(f"  Family type: {family_type}")
    print(f"  Page size: {pagination.get('page_size')}")
    print(f"  Total pages: {pagination.get('total_pages')}")
    
    # Extract actual listings
    listings = data.get('data', {}).get('data', {}).get('Web', {}).get('find', {}).get('listings', [])
    actual_count = len(listings)
    
    print(f"\n✅ VERIFICATION:")
    print(f"  Actual listings in file: {actual_count}")
    print(f"  Metadata says: {total_listings}")
    print(f"  Match: {'✅' if actual_count == total_listings else '❌'}")
    
    # Show first listing sample
    if listings:
        first_listing = listings[0]
        print(f"\n📄 FIRST LISTING SAMPLE:")
        print(f"  ID: {first_listing.get('id')}")
        print(f"  Title: {first_listing.get('title', 'N/A')}")
        print(f"  Price: {first_listing.get('price', 'N/A')} SAR")
        print(f"  Area: {first_listing.get('area', 'N/A')} m²")
        print(f"  Rooms: {first_listing.get('rooms', 'N/A')}")
        print(f"  Address: {first_listing.get('address', 'N/A')}")
    
    # Show last listing sample
    if listings:
        last_listing = listings[-1]
        print(f"\n📄 LAST LISTING SAMPLE:")
        print(f"  ID: {last_listing.get('id')}")
        print(f"  Title: {last_listing.get('title', 'N/A')}")
        print(f"  Price: {last_listing.get('price', 'N/A')} SAR")
        print(f"  Area: {last_listing.get('area', 'N/A')} m²")
        print(f"  Rooms: {last_listing.get('rooms', 'N/A')}")
        print(f"  Address: {last_listing.get('address', 'N/A')}")

if __name__ == "__main__":
    verify_latest_results()
