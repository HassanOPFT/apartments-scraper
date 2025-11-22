# Apartments Scraper

Simple apartment listing scraper with pagination support.

## Quick Start

1. **Install dependencies:**

```bash
pip install -r requirements.txt
```

2. **Configure environment variables:**

Copy `.env.example` to `.env` and fill in your configuration values.

3. **Run the scraper:**

```bash
python scrape.py
```

4. **Verify results:**

```bash
python verify_results.py
```

## What it does

- **Scrapes districts** from district data file
- **Uses pagination** with 20 records per request
- **Creates JSON files** with all results organized by date
- **Tests both family types** and uses the one with more listings
- **Includes metadata** about the scraping process
- **Converts to Excel/CSV** formats automatically

## Files

- `scrape.py` - Main scraper script
- `convert_to_excel.py` - Convert JSON to Excel/CSV
- `verify_results.py` - Verify the results
- `services/` - Service modules (Google Maps, Excel converter)
- `output/` - Generated JSON files
- `excel_output/` - Generated Excel/CSV files

## Output

The scraper creates files in `output/{date}/` with format:
`{district_name}_listings.json`

**File contains:**

- Metadata (district info, pagination details, family type)
- All listings with complete API response data
- Distance calculations from office location

## Pagination Details

- **Page size**: 20 records per request
- **Rate limiting**: 1 second between requests
- **Automatic**: Handles all pagination automatically

## Results Example

```
🏠 DISTRICTS SCRAPER
==================================================
Target district: District Name (ID: 123)
Expected listings: 500
Family filter: Auto-detect
--------------------------------------------------

🔍 Testing different family filters...
Singles listings: 50
Families listings: 450
✅ Using families data (450 listings)

📊 Results:
  Total listings fetched: 450
  Expected from file: 500
  Family type: families
💾 Saved to: district_name_listings.json
📁 File size: 2.5 MB

✅ Scraping completed successfully!
```
