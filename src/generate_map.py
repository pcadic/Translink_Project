import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Config
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_gtfs_data(endpoint):
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def fetch_bus_enriched_data():
    print("Log: Fetching Positions and Trip Updates...")
    try:
        positions_feed = get_gtfs_data("gtfsposition")
        updates_feed = get_gtfs_data("gtfstripupdate")
    except Exception as e:
        print(f"Log Error: {e}")
        return

    # 1. Map delays to Trip IDs
    delays = {}
    for entity in updates_feed.entity:
        if entity.HasField('trip_update'):
            trip_id = entity.trip_update.trip.trip_id
            # On prend le délai du dernier arrêt rapporté
            if entity.trip_update.stop_time_update:
                last_update = entity.trip_update.stop_time_update[-1]
                delays[trip_id] = last_update.arrival.delay if last_update.HasField('arrival') else 0

    # 2. Load GeoJSON and Sort (Prioritize Neighborhoods)
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
    # On trie pour que 'neighborhood' soit traité avant 'municipality'
    gdf_map['priority'] = gdf_map['area_type'].map({'neighborhood': 1, 'special_zone': 2, 'municipality': 3})
    gdf_map = gdf_map.sort_values('priority')

    bus_records = []
    for entity in positions_feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pos = v.position
            trip_id = v.trip.trip_id
            
            # Spatial Enrichment
            point = Point(pos.longitude, pos.latitude)
            match = gdf_map[gdf_map.contains(point)]
            
            # Si match, on prend le premier (le plus précis grâce au tri 'priority')
            area_name = match.iloc[0]['name'] if not match.empty else "Unknown"
            area_type = match.iloc[0]['area_type'] if not match.empty else "Unknown"

            bus_records.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": pos.latitude,
                "longitude": pos.longitude,
                "location": f"POINT({pos.longitude} {pos.latitude})",
                "area_name": area_name,
                "area_type": area_type,
                "delay_seconds": delays.get(trip_id, 0), # Ajout du retard !
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    if bus_records:
        supabase.table("bus_positions").insert(bus_records).execute()
        print(f"Log: Successfully pushed {len(bus_records)} enriched records.")

if __name__ == "__main__":
    fetch_bus_enriched_data()
