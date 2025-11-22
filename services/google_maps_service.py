#!/usr/bin/env python3
"""
Google Maps Distance Matrix API Service
Calculates driving distances and travel times from office to apartment listings
"""

import os
import time
import requests
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class GoogleMapsService:
    """Service for calculating distances using Google Maps Distance Matrix API"""
    
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/distancematrix/json"
        office_lat = os.getenv('OFFICE_LAT', '24.785698')
        office_lng = os.getenv('OFFICE_LNG', '46.613715')
        self.office_coords = f"{office_lat},{office_lng}"
        self.timeout = 30
        self.delay = 2  # 2 second delay between API calls
        
        print(f"🔑 API Key loaded: {'Yes' if self.api_key else 'No'}")
        
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is required")
    
    def calculate_distances(self, apartment_coords: List[Tuple[float, float]]) -> List[Dict]:
        """
        Calculate distances from office to multiple apartment locations
        Uses batch processing to handle Google Maps API limits (25 destinations per request)
        
        Args:
            apartment_coords: List of (lat, lng) tuples for apartment locations
            
        Returns:
            List of dictionaries with distance and duration info
        """
        if not apartment_coords:
            return []
        
        # Google Maps API limit: 25 destinations per request
        batch_size = 25
        all_results = []
        
        print(f"🌍 Calculating distances for {len(apartment_coords)} apartments in batches of {batch_size}...")
        
        # Process in batches
        for i in range(0, len(apartment_coords), batch_size):
            batch_coords = apartment_coords[i:i + batch_size]
            batch_results = self._calculate_batch_distances(batch_coords)
            all_results.extend(batch_results)
            
            # Add delay between batches to respect rate limits
            if i + batch_size < len(apartment_coords):
                print(f"⏳ Waiting {self.delay} seconds before next batch...")
                time.sleep(self.delay)
        
        print(f"✅ Successfully calculated distances for {len(all_results)} apartments")
        return all_results
    
    def _calculate_batch_distances(self, apartment_coords: List[Tuple[float, float]]) -> List[Dict]:
        """
        Calculate distances for a single batch of apartment coordinates
        
        Args:
            apartment_coords: List of (lat, lng) tuples for apartment locations (max 25)
            
        Returns:
            List of dictionaries with distance and duration info
        """
        # Convert coordinates to string format for API
        destinations = "|".join([f"{lat},{lng}" for lat, lng in apartment_coords])
        
        # Prepare API request parameters
        params = {
            'origins': self.office_coords,
            'destinations': destinations,
            'mode': 'driving',
            'language': 'ar',
            'units': 'metric',
            'key': self.api_key
        }
        
        try:
            # Make API request
            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                print(f"❌ API request failed with status {response.status_code}")
                return [self._create_error_result() for _ in apartment_coords]
            
            data = response.json()
            
            if data.get('status') != 'OK':
                print(f"❌ API returned error: {data.get('status')}")
                if 'error_message' in data:
                    print(f"Error message: {data['error_message']}")
                return [self._create_error_result() for _ in apartment_coords]
            
            # Process results
            results = []
            elements = data.get('rows', [{}])[0].get('elements', [])
            
            for element in elements:
                if element.get('status') == 'OK':
                    distance_info = element.get('distance', {})
                    duration_info = element.get('duration', {})
                    
                    result = {
                        'distance_km': self._parse_distance(distance_info.get('text', '')),
                        'distance_meters': distance_info.get('value', 0),
                        'duration_text': duration_info.get('text', ''),
                        'duration_seconds': duration_info.get('value', 0),
                        'status': 'OK'
                    }
                else:
                    result = {
                        'distance_km': 0,
                        'distance_meters': 0,
                        'duration_text': 'N/A',
                        'duration_seconds': 0,
                        'status': element.get('status', 'UNKNOWN_ERROR')
                    }
                
                results.append(result)
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            return [self._create_error_result() for _ in apartment_coords]
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return [self._create_error_result() for _ in apartment_coords]
    
    def _create_error_result(self) -> Dict:
        """Create an error result dictionary"""
        return {
            'distance_km': 0,
            'distance_meters': 0,
            'duration_text': 'N/A',
            'duration_seconds': 0,
            'status': 'API_ERROR'
        }
    
    def _parse_distance(self, distance_text: str) -> float:
        """
        Parse distance text from Google API (e.g., "8.2 km" -> 8.2)
        
        Args:
            distance_text: Distance string from API (e.g., "8.2 km", "1.2 mi")
            
        Returns:
            Distance in kilometers as float
        """
        if not distance_text:
            return 0.0
        
        try:
            # Extract number from text like "8.2 km" or "1.2 mi"
            import re
            match = re.search(r'([\d,]+\.?\d*)', distance_text.replace(',', ''))
            if match:
                distance_value = float(match.group(1))
                
                # Convert to km if in miles
                if 'mi' in distance_text.lower():
                    distance_value *= 1.60934
                
                return round(distance_value, 2)
        except (ValueError, AttributeError):
            pass
        
        return 0.0
    
    def add_distance_to_listings(self, listings: List[Dict]) -> List[Dict]:
        """
        Add distance information to apartment listings
        
        Args:
            listings: List of apartment listing dictionaries
            
        Returns:
            Updated listings with distance information
        """
        if not listings:
            return listings
        
        # Extract coordinates from listings
        coords = []
        valid_indices = []
        
        for i, listing in enumerate(listings):
            location = listing.get('location', {})
            lat = location.get('lat')
            lng = location.get('lng')
            
            if lat and lng:
                coords.append((lat, lng))
                valid_indices.append(i)
            else:
                coords.append((None, None))
                valid_indices.append(i)
        
        # Calculate distances
        distances = self.calculate_distances([(lat, lng) for lat, lng in coords if lat and lng])
        
        # Add distance info to listings
        distance_index = 0
        for i, listing in enumerate(listings):
            location = listing.get('location', {})
            lat = location.get('lat')
            lng = location.get('lng')
            
            if lat and lng and distance_index < len(distances):
                distance_info = distances[distance_index]
                
                # Add distance information to location
                listing['location']['distance_from_office'] = {
                    'distance_km': distance_info['distance_km'],
                    'distance_meters': distance_info['distance_meters'],
                    'duration_text': distance_info['duration_text'],
                    'duration_seconds': distance_info['duration_seconds'],
                    'status': distance_info['status']
                }
                distance_index += 1
            else:
                # No coordinates or API error
                listing['location']['distance_from_office'] = {
                    'distance_km': 0,
                    'distance_meters': 0,
                    'duration_text': 'N/A',
                    'duration_seconds': 0,
                    'status': 'NO_COORDINATES' if not (lat and lng) else 'API_ERROR'
                }
        
        # Add delay between API calls
        time.sleep(self.delay)
        
        return listings
