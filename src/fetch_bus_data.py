import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Environment Variables
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_bus_gtfs():
    # THE CORRECT V3 ENDPOINT
    url = f"https://gtfsapi.translink.ca/v3/gtfsposition?apikey={TRANSLINK_API_KEY}"
    
    headers = {
        'User-Agent': 'TranslinkProject/1.0',
        'Accept': 'application/x-google-protobuf'
    }
    
    print(f"Log: Fetching GTFS-RT V3 from {url.split('?')[0]}...")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Log Error: {e}")
        return

    # Decode binary Protobuf
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Load local GeoJSON map
    print("Log: Performing spatial enrichment...")
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')

    bus_records = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pos = v.position
            
            # Spatial Match
            point = Point(pos.longitude, pos.latitude)
            match = gdf_map[gdf_map.contains(point)]
            
            area_name = match.iloc[0]['name'] if not match.empty else "Outside / Unknown"
            area_type = match.iloc[0]['area_type'] if not match.empty else "Unknown"

            bus_records.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": pos.latitude,
                "longitude": pos.longitude,
                "location": f"POINT({pos.longitude} {pos.latitude})",
                "area_name": area_name,
                "area_type": area_type,
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    if bus_records:
        try:
            supabase.table("bus_positions").insert(bus_records).execute()
            print(f"Log: Successfully pushed {len(bus_records)} buses to Supabase.")
        except Exception as e:
            print(f"Log Error: Supabase failed: {e}")

if __name__ == "__main__":
    fetch_bus_gtfs()
