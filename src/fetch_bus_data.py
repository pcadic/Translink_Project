import os
import requests
from google.transit import gtfs_realtime_pb2
import geopandas as gpd
from shapely.geometry import Point
from supabase import create_client

# Setup
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_bus_gtfs():
    # New GTFS-RT URL for Translink
    url = f"https://gtfs.translink.ca/gtfsrealtime?apikey={TRANSLINK_API_KEY}"
    
    print("Log: Fetching GTFS-RT V3 data...")
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Log Error: Could not fetch GTFS data. Code: {response.status_code}")
        return

    # Decode the Protocol Buffer
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Load your map
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
    bus_records = []

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            lat = v.position.latitude
            lon = v.position.longitude
            
            # Spatial Join logic
            point = Point(lon, lat)
            match = gdf_map[gdf_map.contains(point)]
            
            area_name = "Unknown"
            if not match.empty:
                area_name = match.iloc[0]['name']

            bus_records.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": lat,
                "longitude": lon,
                "location": f"POINT({lon} {lat})",
                "area_name": area_name,
                "recorded_time": str(pd.to_datetime(v.timestamp, unit='s'))
            })

    # Insert to Supabase
    if bus_records:
        supabase.table("bus_positions").insert(bus_records).execute()
        print(f"Log: Inserted {len(bus_records)} buses via GTFS-RT.")

if __name__ == "__main__":
    fetch_bus_gtfs()
