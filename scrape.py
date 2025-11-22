#!/usr/bin/env python3
"""
Districts Scraper
Scrapes listings for districts with proper direction_id handling
"""

import json
import os
import requests
import time
from datetime import datetime, date
from pathlib import Path
import pytz
from dotenv import load_dotenv
from services.google_maps_service import GoogleMapsService
from services.excel_converter_service import ExcelConverterService

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION - Modify these values as needed
# =============================================================================

# API Configuration
API_URL = os.getenv("API_URL")
if not API_URL:
    raise ValueError("API_URL environment variable is required")
TIMEOUT = 30
MAX_RETRIES = 3
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Filtering criteria
MIN_ROOMS = 2
MAX_ROOMS = 4
MAX_PRICE = 60000

# Date filtering - modify this date to scrape listings after this date
AFTER_DATE = "2025-11-01"  # Format: YYYY-MM-DD

# Target districts - loaded from environment variable
TARGET_DISTRICTS_JSON = os.getenv("TARGET_DISTRICTS")
if not TARGET_DISTRICTS_JSON:
    raise ValueError("TARGET_DISTRICTS environment variable is required")
try:
    TARGET_DISTRICTS = json.loads(TARGET_DISTRICTS_JSON)
except json.JSONDecodeError as e:
    raise ValueError(f"TARGET_DISTRICTS must be valid JSON: {e}")

# Office coordinates for distance calculations
OFFICE_LAT = os.getenv("OFFICE_LAT")
OFFICE_LNG = os.getenv("OFFICE_LNG")
if not OFFICE_LAT or not OFFICE_LNG:
    raise ValueError("OFFICE_LAT and OFFICE_LNG environment variables are required")
OFFICE_COORDINATES = f"{OFFICE_LAT},{OFFICE_LNG}"
OFFICE_DESCRIPTION = "Office location for distance calculations"

# Output directories
OUTPUT_BASE_DIR = Path("output")
EXCEL_BASE_DIR = Path("excel_output")

# =============================================================================
# CONFIGURATION CLASS
# =============================================================================

class DistrictsConfig:
    """Configuration for districts scraper"""
    
    @classmethod
    def load_districts(cls):
        """Load district data from JSON file"""
        with open("raw/riyadh_districts.json", 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @classmethod
    def get_target_districts(cls):
        """Get target districts with full information from JSON file"""
        districts = cls.load_districts()
        target_districts = []
        
        for district_config in TARGET_DISTRICTS:
            district_id = district_config["id"]
            district_key = str(district_id)
            if district_key in districts:
                district_info = districts[district_key]
                target_districts.append({
                    "id": district_id,
                    "name": district_config["name"],  # Use the name from config for clarity
                    "direction_id": district_info.get("direction", {}).get("direction_id", 1),
                    "full_info": district_info
                })
            else:
                print(f"⚠️ District {district_id} ({district_config['name']}) not found in districts file")
        
        return target_districts
    
    @classmethod
    def get_scrape_date(cls):
        """Get current date for organizing outputs"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @classmethod
    def get_output_dirs(cls):
        """Get output directories with date-based structure"""
        scrape_date = cls.get_scrape_date()
        json_dir = OUTPUT_BASE_DIR / scrape_date
        excel_dir = EXCEL_BASE_DIR / scrape_date
        return json_dir, excel_dir

# =============================================================================
# API CLIENT
# =============================================================================

class DistrictsAPIClient:
    """API client for scraping districts with proper direction_id"""
    
    def __init__(self):
        self.url = API_URL
        self.headers = HEADERS
        self.timeout = TIMEOUT
    
    def get_listings_after_date(self, district_id, direction_id, after_date, family=0, page_size=20, offset=0):
        """Get listings for a district created after a specific date"""
        
        # Convert date string to timestamp
        try:
            after_datetime = datetime.strptime(after_date, "%Y-%m-%d")
            after_timestamp = int(after_datetime.timestamp())
        except ValueError:
            print(f"❌ Invalid date format: {after_date}. Use YYYY-MM-DD format.")
            return None
        
        payload = {
            "operationName": "findListings",
            "variables": {
                "size": page_size,
                "from": offset,
                "sort": {"create_time": "desc", "has_img": "desc"},
                "sov": {
                    "listing_category": 1,
                    "city_id": 21,
                    "district_id": district_id,
                    "direction_id": direction_id,
                    "enabled": True,
                    "campaign_category": "PROMOTED"
                },
                "where": {
                    "category": {"eq": 1},
                    "city_id": {"eq": 21},
                    "direction_id": {"eq": direction_id},
                    "district_id": {"eq": district_id},
                    "family": {"eq": family},
                    "create_time": {"gte": after_timestamp}  # Date filtering
                }
            },
            "query": """fragment WebResult on WebResults {
              total
              listings {
                id
                rnpl_monthly_price
                ac
                age
                apts
                area
                backyard
                beds
                category
                city_id
                create_time
                published_at
                direction_id
                district_id
                province_id
                extra_unit
                family
                family_section
                fb
                fl
                furnished
                ketchen
                last_update
                refresh
                lift
                livings
                location {
                  lat
                  lng
                  __typename
                }
                men_place
                price
                price_2_payments
                price_4_payments
                price_12_payments
                range_price
                rent_period
                rooms
                stairs
                stores
                status
                street_direction
                user {
                  phone
                  name
                  bml_license_number
                  bml_url
                }
                wc
                women_place
                published
                content
                address
                district
                direction
                city
                title
                path
                uri
                range_price
                original_range_price
                plan_no
                parcel_no
              }
            }

            query findListings($size: Int, $from: Int, $sort: SortInput, $where: WhereInput, $polygon: [LocationInput!], $daily_renting_filter: DailyRentingFilter, $sov: SovListingsFilter) {
              Web {
                find(
                  size: $size
                  from: $from
                  sort: $sort
                  where: $where
                  polygon: $polygon
                  daily_renting_filter: $daily_renting_filter
                ) {
                  ...WebResult
                  __typename
                }
                sov: find(
                  from: $from
                  sort: $sort
                  where: $where
                  polygon: $polygon
                  daily_renting_filter: $daily_renting_filter
                  size: 6
                  sov_listings: $sov
                ) {
                  ...WebResult
                  __typename
                }
                __typename
              }
            }"""
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    self.url, 
                    headers=self.headers, 
                    json=payload, 
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    print(f"HTTP {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    
        return None
    
    def get_all_new_listings(self, district_id, direction_id, after_date, family=0):
        """Get all new listings for a district with automatic pagination"""
        
        all_listings = []
        total_listings = 0
        offset = 0
        page_size = 20
        
        print(f"Fetching new listings for district {district_id} after {after_date}...")
        
        while True:
            data = self.get_listings_after_date(district_id, direction_id, after_date, family, page_size, offset)
            if not data:
                print(f"Failed to fetch data for district {district_id}")
                return None
            
            # Extract results
            web_data = data.get('data', {}).get('Web', {})
            main_results = web_data.get('find', {})
            
            # Get pagination info
            current_total = main_results.get('total', 0)
            current_listings = main_results.get('listings', [])
            
            # Update total on first request
            if offset == 0:
                total_listings = current_total
                print(f"Total new listings available: {total_listings}")
            
            # Add current page listings
            all_listings.extend(current_listings)
            print(f"Fetched {len(current_listings)} listings (offset: {offset})")
            
            # Check if we've got all listings
            if len(all_listings) >= total_listings or len(current_listings) < page_size:
                break
            
            offset += page_size
            time.sleep(1)  # Rate limiting
        
        print(f"✅ Successfully fetched {len(all_listings)}/{total_listings} new listings")
        
        # Return reconstructed response
        return {
            "data": {
                "Web": {
                    "find": {
                        "total": total_listings,
                        "listings": all_listings,
                        "__typename": "WebResults"
                    },
                    "sov": web_data.get('sov', {"total": 0, "listings": [], "__typename": "WebResults"}),
                    "__typename": "WebQryOps"
                }
            }
        }

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def convert_to_riyadh_datetime(timestamp):
    """Convert timestamp to Riyadh timezone datetime and return as ISO string"""
    if not timestamp:
        return None
    
    riyadh_tz = pytz.timezone('Asia/Riyadh')
    
    try:
        # Handle Unix timestamp (seconds since epoch)
        if isinstance(timestamp, (int, float)):
            dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        # Handle string timestamps
        elif isinstance(timestamp, str):
            # Try parsing as ISO format first
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                # Try parsing as Unix timestamp string
                try:
                    dt = datetime.fromtimestamp(float(timestamp), tz=pytz.UTC)
                except ValueError:
                    return None
        else:
            return None
        
        # Convert to Riyadh timezone and return as ISO string
        riyadh_dt = dt.astimezone(riyadh_tz)
        return riyadh_dt.isoformat()
    except (ValueError, OSError):
        return None

def filter_listings(listings):
    """Filter listings based on room count and price criteria"""
    filtered_listings = []
    filtered_out_count = 0
    
    for listing in listings:
        # Check room count (2-4 rooms only)
        rooms = listing.get('rooms')
        if rooms is None or not (MIN_ROOMS <= rooms <= MAX_ROOMS):
            filtered_out_count += 1
            continue
        
        # Check price (not more than MAX_PRICE)
        price = listing.get('price')
        if price is None or price > MAX_PRICE:
            filtered_out_count += 1
            continue
        
        # Convert date fields to Riyadh timezone
        listing['create_time_riyadh'] = convert_to_riyadh_datetime(listing.get('create_time'))
        listing['published_at_riyadh'] = convert_to_riyadh_datetime(listing.get('published_at'))
        listing['last_update_riyadh'] = convert_to_riyadh_datetime(listing.get('last_update'))
        
        # Add full listing URL (using base URL from environment)
        base_url = API_URL.replace('/graphql', '')
        listing['full_url'] = f"{base_url}{listing.get('path', '')}"
        
        # Reorder fields for better readability
        reordered_listing = {
            # Most important fields first
            'rooms': listing.get('rooms'),
            'price': listing.get('price'),
            'location': {
                'lat': listing.get('location', {}).get('lat'),
                'lng': listing.get('location', {}).get('lng'),
                'address': listing.get('address'),
                'district': listing.get('district'),
                'direction': listing.get('direction'),
                'city': listing.get('city')
            },
            'create_time_riyadh': listing['create_time_riyadh'],
            'published_at_riyadh': listing['published_at_riyadh'],
            'last_update_riyadh': listing['last_update_riyadh'],
            'full_url': listing['full_url'],
            
            # Additional useful fields
            'id': listing.get('id'),
            'title': listing.get('title'),
            'area': listing.get('area'),
            'beds': listing.get('beds'),
            'wc': listing.get('wc'),
            'furnished': listing.get('furnished'),
            'ac': listing.get('ac'),
            'lift': listing.get('lift'),
            'age': listing.get('age'),
            'fl': listing.get('fl'),
            'livings': listing.get('livings'),
            'ketchen': listing.get('ketchen'),
            'backyard': listing.get('backyard'),
            'stairs': listing.get('stairs'),
            'stores': listing.get('stores'),
            'men_place': listing.get('men_place'),
            'women_place': listing.get('women_place'),
            'family': listing.get('family'),
            'rent_period': listing.get('rent_period'),
            'street_direction': listing.get('street_direction'),
            'status': listing.get('status'),
            'published': listing.get('published'),
            'content': listing.get('content'),
            'rnpl_monthly_price': listing.get('rnpl_monthly_price'),
            'price_2_payments': listing.get('price_2_payments'),
            'price_4_payments': listing.get('price_4_payments'),
            'price_12_payments': listing.get('price_12_payments'),
            'range_price': listing.get('range_price'),
            'original_range_price': listing.get('original_range_price'),
            'plan_no': listing.get('plan_no'),
            'parcel_no': listing.get('parcel_no'),
            'extra_unit': listing.get('extra_unit'),
            'family_section': listing.get('family_section'),
            'fb': listing.get('fb'),
            'refresh': listing.get('refresh'),
            'user': listing.get('user'),
            'path': listing.get('path'),
            'uri': listing.get('uri'),
            
            # Original timestamp fields (kept for reference)
            'create_time': listing.get('create_time'),
            'published_at': listing.get('published_at'),
            'last_update': listing.get('last_update'),
            'refresh': listing.get('refresh'),
            
            # Other fields
            'category': listing.get('category'),
            'city_id': listing.get('city_id'),
            'direction_id': listing.get('direction_id'),
            'district_id': listing.get('district_id'),
            'province_id': listing.get('province_id'),
            'apts': listing.get('apts'),
            '__typename': listing.get('__typename')
        }
        
        filtered_listings.append(reordered_listing)
    
    return filtered_listings, filtered_out_count

# =============================================================================
# SCRAPING FUNCTIONS
# =============================================================================

def scrape_district(api_client, district_info, after_date):
    """Scrape a single district for new listings"""
    
    district_id = district_info["id"]
    district_name = district_info["name"]
    direction_id = district_info["direction_id"]
    
    print(f"\n🏠 Scraping: {district_name} (ID: {district_id}, Direction: {direction_id})")
    print("-" * 50)
    
    # Try both family=0 and family=1 to see which gives more results
    print("🔍 Testing different family filters...")
    
    # Try singles first
    data_singles = api_client.get_all_new_listings(district_id, direction_id, after_date, family=0)
    singles_count = len(data_singles.get('data', {}).get('Web', {}).get('find', {}).get('listings', [])) if data_singles else 0
    
    # Try families
    data_families = api_client.get_all_new_listings(district_id, direction_id, after_date, family=1)
    families_count = len(data_families.get('data', {}).get('Web', {}).get('find', {}).get('listings', [])) if data_families else 0
    
    print(f"Singles listings: {singles_count}")
    print(f"Families listings: {families_count}")
    
    # Use the one with more listings
    if families_count > singles_count:
        data = data_families
        family_type = "families"
        print(f"✅ Using families data ({families_count} listings)")
    else:
        data = data_singles
        family_type = "singles"
        print(f"✅ Using singles data ({singles_count} listings)")
    
    if not data:
        print(f"❌ Failed to fetch data for district {district_id}")
        return None
    
    # Extract listings
    listings = data.get('data', {}).get('Web', {}).get('find', {}).get('listings', [])
    total_listings = len(listings)
    
    print(f"📊 Results:")
    print(f"  Total new listings fetched: {total_listings}")
    print(f"  Family type: {family_type}")
    
    if total_listings == 0:
        print("ℹ️ No new listings found for this district")
        return None
    
    # Apply filters
    print(f"🔍 Applying filters:")
    print(f"  Room count: {MIN_ROOMS}-{MAX_ROOMS} rooms")
    print(f"  Max price: {MAX_PRICE:,} SAR")
    
    filtered_listings, filtered_out_count = filter_listings(listings)
    
    print(f"  Filtered out: {filtered_out_count} listings")
    print(f"  Remaining: {len(filtered_listings)} listings")
    
    if not filtered_listings:
        print("❌ No listings match the filtering criteria!")
        return None
    
    # Calculate distances from office using Google Maps API
    print(f"🌍 Calculating distances from office for {len(filtered_listings)} filtered listings...")
    try:
        google_maps = GoogleMapsService()
        filtered_listings = google_maps.add_distance_to_listings(filtered_listings)
        print(f"✅ Distance calculation completed")
    except Exception as e:
        print(f"⚠️ Distance calculation failed: {e}")
        print("Continuing without distance data...")
    
    return {
        "data": data,
        "filtered_listings": filtered_listings,
        "metadata": {
            "district_id": district_id,
            "district_name": district_name,
            "direction_id": direction_id,
            "total_listings": total_listings,
            "filtered_listings": len(filtered_listings),
            "filtered_out_count": filtered_out_count,
            "family_type": family_type,
            "after_date": after_date,
            "scrape_timestamp": datetime.now().isoformat()
        }
    }

def save_district_data(district_data, output_dir):
    """Save district data to JSON file"""
    
    if not district_data:
        return None
    
    metadata = district_data["metadata"]
    district_name = metadata["district_name"]
    
    # Create output data structure
    output_data = {
        "metadata": {
            **metadata,
            "filters": {
                "min_rooms": MIN_ROOMS,
                "max_rooms": MAX_ROOMS,
                "max_price": MAX_PRICE
            },
            "office_location": {
                "coordinates": OFFICE_COORDINATES,
                "description": OFFICE_DESCRIPTION
            },
            "distance_calculation": {
                "service": "Google Maps Distance Matrix API",
                "mode": "driving",
                "units": "metric"
            }
        },
        "data": {
            "Web": {
                "find": {
                    "total": len(district_data["filtered_listings"]),
                    "listings": district_data["filtered_listings"],
                    "__typename": "WebResults"
                },
                "sov": district_data["data"].get('data', {}).get('Web', {}).get('sov', {"total": 0, "listings": [], "__typename": "WebResults"}),
                "__typename": "WebQryOps"
            }
        }
    }
    
    # Save to JSON file
    filename = f"{district_name}_listings.json"
    filepath = output_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved to: {filename}")
    print(f"📁 File size: {filepath.stat().st_size / 1024 / 1024:.1f} MB")
    
    return filepath

def convert_to_excel(json_dir, excel_dir):
    """Convert JSON files to Excel format"""
    
    print(f"\n📊 Converting JSON files to Excel...")
    print("=" * 50)
    
    # Create a custom converter for the date-based directory
    converter = ExcelConverterService(input_dir=str(json_dir), output_dir=str(excel_dir))
    converter.convert_all_listings()

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main function - scrape new listings for selected districts"""
    
    print("🏠 DISTRICTS SCRAPER")
    print("=" * 60)
    
    # Get configuration
    after_date = AFTER_DATE
    target_districts = DistrictsConfig.get_target_districts()
    json_dir, excel_dir = DistrictsConfig.get_output_dirs()
    scrape_date = DistrictsConfig.get_scrape_date()
    
    print(f"📅 Scraping date: {scrape_date}")
    print(f"📅 After date filter: {after_date}")
    print(f"🏘️ Target districts: {len(target_districts)}")
    print(f"📂 JSON output: {json_dir}")
    print(f"📂 Excel output: {excel_dir}")
    print("-" * 60)
    
    # Create output directories
    json_dir.mkdir(parents=True, exist_ok=True)
    excel_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize API client
    api_client = DistrictsAPIClient()
    
    # Scrape each district
    successful_districts = []
    failed_districts = []
    
    for district_info in target_districts:
        try:
            district_data = scrape_district(api_client, district_info, after_date)
            
            if district_data:
                json_file = save_district_data(district_data, json_dir)
                if json_file:
                    successful_districts.append(district_info)
                    print(f"✅ Successfully scraped: {district_info['name']}")
                else:
                    failed_districts.append(district_info)
            else:
                failed_districts.append(district_info)
                
        except Exception as e:
            print(f"❌ Error scraping {district_info['name']}: {e}")
            failed_districts.append(district_info)
    
    # Convert to Excel if we have successful scrapes
    if successful_districts:
        convert_to_excel(json_dir, excel_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SCRAPING SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully scraped: {len(successful_districts)} districts")
    print(f"❌ Failed districts: {len(failed_districts)}")
    print(f"📅 Scrape date: {scrape_date}")
    print(f"📅 After date filter: {after_date}")
    
    if successful_districts:
        print(f"\n✅ Successful districts:")
        for district in successful_districts:
            print(f"  - {district['name']} (ID: {district['id']})")
    
    if failed_districts:
        print(f"\n❌ Failed districts:")
        for district in failed_districts:
            print(f"  - {district['name']} (ID: {district['id']})")
    
    print(f"\n📁 Output directories:")
    print(f"  JSON: {json_dir}")
    print(f"  Excel: {excel_dir}")
    
    print("\n✅ Districts scraping completed!")

if __name__ == "__main__":
    main()
