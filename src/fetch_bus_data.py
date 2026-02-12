import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Configuration
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
    print("Log: Démarrage du pipeline...")
    log_data = {"status": "Success", "error_message": None, "entities_received": 0, "bus_count": 0, "alert_count": 0}
    
    try:
        # Récupération
        pos_feed = get_feed("gtfsposition")
        bundle_feed = get_feed("gtfsrealtime")
        log_data["entities_received"] = len(pos_feed.entity) + len(bundle_feed.entity)
        
        # Map
        gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
        priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
        gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
        gdf_map = gdf_map.sort_values('priority')

        # Extraction Alertes/Retards
        delays = {}
        alerts = []
        for entity in bundle_feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                if tu.stop_time_update:
                    last = tu.stop_time_update[-1]
                    delays[tu.trip.trip_id] = last.arrival.delay if last.HasField('arrival') else 0
            
            if entity.HasField('alert'):
                al = entity.alert
                rid = "NETWORK"
                if al.informed_entity:
                    for item in al.informed_entity:
                        if item.route_id:
                            rid = item.route_id
                            break
                alerts.append({
                    "alert_id": entity.id, "route_id": rid,
                    "header_text": al.header_text.translation[0].text if al.header_text.translation else "No Title",
                    "cause": str(al.cause),
                    "start_time": pd.to_datetime(al.active_period[0].start, unit='s').isoformat() if al.active_period else None
                })
        log_data["alert_count"] = len(alerts)

        # Extraction Positions
        bus_batch = []
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                pt = Point(v.position.longitude, v.position.latitude)
                match = gdf_map[gdf_map.contains(pt)]
                area_name = match.iloc[0]['name'] if not match.empty else "Off-Map"
                area_type = match.iloc[0]['area_type'] if not match.empty else "Unknown"

                bus_batch.append({
                    "vehicle_no": v.vehicle.id, "route_no": v.trip.route_id,
                    "latitude": v.position.latitude, "longitude": v.position.longitude,
                    "location": f"POINT({v.position.longitude} {v.position.latitude})",
                    "area_name": area_name, "area_type": area_type,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
                })
        log_data["bus_count"] = len(bus_batch)

        # Envois Supabase
        if alerts:
            supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
        
        print(f"Final Success: {len(bus_batch)} bus et {len(alerts)} alertes.")

    except Exception as e:
        log_data["status"] = "Error"
        log_data["error_message"] = str(e)
        print(f"Pipeline Error: {e}")

    # --- ÉTAPE FINALE : Sauvegarde du Log ---
    try:
        supabase.table("pipeline_logs").insert(log_data).execute()
        print("Log: Exécution enregistrée dans pipeline_logs.")
    except Exception as log_err:
        print(f"Failed to save log: {log_err}")

if __name__ == "__main__":
    run_pipeline()
