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
    print("Log: Starting enhanced pipeline...")
    # Initialisation des logs pour éviter les erreurs "not defined"
    log_data = {"status": "Starting", "bus_count": 0, "alert_count": 0}
    alerts = [] 
    bus_batch = []

    try:
        # 1. FETCH DATA
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Mapping delays
        delays = {}
        for entity in rt_feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                if tu.stop_time_update:
                    delays[tu.trip.trip_id] = tu.stop_time_update[0].arrival.delay
            
            # OPTIONNEL: Récupérer les alertes si présentes
            if entity.HasField('alert'):
                alerts.append({
                    "alert_id": entity.id,
                    "header": str(entity.alert.header_text.translation[0].text)[:255]
                })

        # 2. GEOGRAPHIC PROCESSING
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # Neighborhood & Municipality Search
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = m_neigh.iloc[0]['name'] if not m_neigh.empty else None
                
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # Final logic for area_name (Priority Neighborhood)
                final_area = neigh_name if neigh_name else city_name

                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": final_area,
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
                })

        # 3. SUPABASE UPLOAD
        if alerts:
            supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
            print(f"Alerts: {len(alerts)} uploaded.")

        if bus_batch:
            # On stocke le résultat pour confirmer l'envoi
            response = supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} vehicles uploaded to Supabase.")
            log_data["status"] = "Success"
            log_data["bus_count"] = len(bus_batch)

    except Exception as e:
        print(f"Pipeline Error: {e}")
        log_data["status"] = "Error"
        log_data["error_message"] = str(e)

    # 4. FINAL LOGGING
    try:
        supabase.table("pipeline_logs").insert(log_data).execute()
    except:
        print("Could not save log to database.")

if __name__ == "__main__":
    run_pipeline()
