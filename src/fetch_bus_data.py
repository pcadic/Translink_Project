import os
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from supabase import create_client

# 1. Setup Connections
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_bus_data():
    print("Log: Fetching real-time bus data from Translink...")
    url = f"https://api.translink.ca/rttiapi/v1/buses?apikey={TRANSLINK_API_KEY}"
    headers = {'Accept': 'application/json'}
    
    # Try up to 3 times if there is a network error
    for attempt in range(3):
        try:
            print(f"Log: Fetching data (Attempt {attempt + 1})...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status() # Check for HTTP errors
            buses = response.json()
            print(f"Log: {len(buses)} buses retrieved.")
            break # Success! Exit the loop
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Log Warning: Connection failed. Retrying in 5s... ({e})")
            time.sleep(5)
    else:
        print("Log Error: All attempts failed. Skipping this run.")
        return
    

    # 2. Load our GeoJSON map for Spatial Join
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')

    bus_records = []
    
    for bus in buses:
        lat, lon = bus['Latitude'], bus['Longitude']
        point = Point(lon, lat)
        
        # Spatial Join: Find which area the bus is in
        # We look for the first match in our GeoJSON
        containing_area = gdf_map[gdf_map.contains(point)]
        
        area_name = "Unknown"
        area_type = "Outside"
        
        if not containing_area.empty:
            # If multiple (e.g., City + Neighborhood), we take the Neighborhood first
            match = containing_area.sort_values(by='area_type', ascending=False).iloc[0]
            area_name = match['name']
            area_type = match['area_type']

        # Prepare record for Supabase
        bus_records.append({
            "vehicle_no": bus['VehicleNo'],
            "route_no": bus['RouteNo'],
            "direction": bus['Direction'],
            "destination": bus['Destination'],
            "pattern": bus['Pattern'],
            "recorded_time": bus['RecordedTime'],
            "latitude": lat,
            "longitude": lon,
            "location": f"POINT({lon} {lat})", # PostGIS format
            "area_name": area_name,
            "area_type": area_type
        })

    # 3. Insert into Supabase
    if bus_records:
        data, count = supabase.table("bus_positions").insert(bus_records).execute()
        print(f"Log: Successfully inserted {len(bus_records)} records into Supabase.")

if __name__ == "__main__":
    fetch_bus_data()
