import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# --- CONFIGURATION ---
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_feed(endpoint):
    """Fetches and parses GTFS-Realtime Protobuf feeds from TransLink."""
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def run_pipeline():
    print("Log: Starting enhanced pipeline...")
    
    try:
        # 1. FETCH REAL-TIME FEEDS
        # gtfsposition: Live Lat/Long and TripIDs
        # gtfsrealtime: Trip Updates (delays, stop sequences)
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Create a dictionary of delays by TripID for fast lookup
        delays = {}
        for entity in rt_feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                # Get delay from the first available stop update
                if tu.stop_time_update:
                    delays[tu.trip.trip_id] = tu.stop_time_update[0].arrival.delay

        # 2. SPATIAL DATA PREPARATION
        # Load GeoJSON and separate into two layers for the "Nested" search
        gdf = gpd.read_file('metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        bus_batch = []
        
        # 3. PROCESS EACH VEHICLE
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # --- NEIGHBORHOOD SEARCH ---
                match_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = match_neigh.iloc[0]['name'] if not match_neigh.empty else None
                
                # --- MUNICIPALITY SEARCH ---
                match_city = municipalities[municipalities.contains(p)]
                city_name = match_city.iloc[0]['name'] if not match_city.empty else "Off-Map"
                
                # --- LOGIC: AREA_NAME VS MUNICIPALITY ---
                # area_name = Neighborhood name (e.g. Kitsilano)
                # If no neighborhood is found, fallback to Municipality name
                final_area_name = neigh_name if neigh_name else city_name
                
                # --- DIRECTION LOGIC ---
                # Convert 0/1 ID to descriptive English labels
                direction_label = "Inbound" if v.trip.direction_id == 0 else "Outbound"

                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": direction_label,
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": final_area_name,
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat(),
                    "destination": "N/A" # Place holder for Static GTFS integration
                })

        # 4. DATABASE UPSERT
        if bus_batch:
            # We use .insert() to keep historical data for the "Time of Day" analysis
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: Processed {len(bus_batch)} vehicles across Metro Vancouver.")

    except Exception as e:
        print(f"Pipeline Error: {e}")

if __name__ == "__main__":
    run_pipeline()
