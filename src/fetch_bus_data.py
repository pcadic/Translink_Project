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
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def run_pipeline():
    print("Log: Starting synchronized pipeline...")
    log_data = {"status": "Success", "bus_count": 0, "alert_count": 0}
    bus_batch = []
    alerts = []

    try:
        # 1. FETCH DATA
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Map delays and search for trip metadata
        delays = {}
        trip_meta = {}
        for entity in rt_feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                t_id = tu.trip.trip_id
                if tu.stop_time_update:
                    delays[t_id] = tu.stop_time_update[0].arrival.delay
                # Store extra info if available
                trip_meta[t_id] = {"route_id": tu.trip.route_id}
            
            if entity.HasField('alert'):
                alerts.append({
                    "alert_id": entity.id,
                    "header": str(entity.alert.header_text.translation[0].text)[:255]
                })

        # 2. GEOGRAPHIC PROCESSING
        gdf = gpd.read_file('metro_vancouver_map(1).geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                lat, lon = v.position.latitude, v.position.longitude
                p = Point(lon, lat)
                
                # Double spatial join
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = m_neigh.iloc[0]['name'] if not m_neigh.empty else None
                
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # Logic for columns
                direction_txt = "Inbound" if v.trip.direction_id == 0 else "Outbound"
                
                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": direction_txt,
                    "latitude": lat,
                    "longitude": lon,
                    "area_name": neigh_name if neigh_name else city_name,
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat(),
                    # These match your new SQL columns:
                    "route_name": f"Route {v.trip.route_id}", # Placeholder until static GTFS
                    "destination": "Check Terminal",
                    "pattern": v.trip.trip_id # Using TripID as pattern
                })

        # 3. UPLOAD TO SUPABASE
        if alerts:
            supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
        
        if bus_batch:
            # We use the correct table name and confirm columns match your SQL
            result = supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} vehicles pushed to Supabase.")
            log_data["bus_count"] = len(bus_batch)

    except Exception as e:
        print(f"Pipeline Error: {e}")
        log_data["status"] = "Error"
        log_data["error_message"] = str(e)

    # 4. LOGGING
    try:
        supabase.table("pipeline_logs").insert(log_data).execute()
    except:
        pass

if __name__ == "__main__":
    run_pipeline()
