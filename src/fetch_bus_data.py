import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Environment Config
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_gtfs_feed(endpoint):
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def run_pipeline():
    print("Log: Starting GTFS-RT V3 Pipeline...")
    
    # 1. Fetch all feeds
    try:
        pos_feed = get_gtfs_feed("gtfsposition")
        upd_feed = get_gtfs_feed("gtfstripupdate")
        alt_feed = get_gtfs_feed("gtfsalert")
    except Exception as e:
        print(f"Error fetching feeds: {e}")
        return

    # 2. Extract Delays from TripUpdates
    delays = {}
    for entity in upd_feed.entity:
        if entity.HasField('trip_update'):
            tid = entity.trip_update.trip.trip_id
            if entity.trip_update.stop_time_update:
                last_upd = entity.trip_update.stop_time_update[-1]
                delays[tid] = last_upd.arrival.delay if last_upd.HasField('arrival') else 0

    # 3. Handle Service Alerts (Upsert to avoid duplicates)
    alerts = []
    for entity in alt_feed.entity:
        if entity.HasField('alert'):
            a = entity.alert
            alerts.append({
                "alert_id": entity.id,
                "route_id": a.informed_entity[0].route_id if a.informed_entity else "Global",
                "header_text": a.header_text.translation[0].text if a.header_text.translation else "No Header",
                "cause": str(a.cause),
                "start_time": pd.to_datetime(a.active_period[0].start, unit='s').isoformat() if a.active_period else None
            })
    if alerts:
        supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()

    # 4. Process Positions with Spatial Priority
    print("Log: Processing positions with spatial priority...")
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
    # Set priority: Neighborhood (1) > Special Zone (2) > Municipality (3)
    priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
    gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
    gdf_map = gdf_map.sort_values('priority')

    bus_batch = []
    for entity in pos_feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pt = Point(v.position.longitude, v.position.latitude)
            
            # Find matching area
            match = gdf_map[gdf_map.contains(pt)]
            area_info = match.iloc[0] if not match.empty else None

            bus_batch.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "area_name": area_info['name'] if area_info is not None else "Unknown",
                "area_type": area_info['area_type'] if area_info is not None else "Unknown",
                "delay_seconds": delays.get(v.trip.trip_id, 0),
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    # 5. Push to Supabase
    if bus_batch:
        supabase.table("bus_positions").insert(bus_batch).execute()
        print(f"Success: Processed {len(bus_batch)} buses and {len(alerts)} alerts.")

if __name__ == "__main__":
    run_pipeline()
