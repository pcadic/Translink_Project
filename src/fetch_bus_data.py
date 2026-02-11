import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Configuration
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
supabase = create_client(os.environ.get('SUPABASE_URL'), os.environ.get('SUPABASE_KEY'))

def run_pipeline():
    # L'endpoint "Bundle" de la V3
    url = f"https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}

    print(f"Log: Tentative sur l'URL bundle: {url.split('?')[0]}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        # Si 404, on tente le fallback sur gtfsposition
        if response.status_code == 404:
            print("Log: 404 sur gtfsrealtime, tentative sur gtfsposition...")
            url = f"https://gtfsapi.translink.ca/v3/gtfsposition?apikey={TRANSLINK_API_KEY}"
            response = requests.get(url, headers=headers, timeout=30)
        
        response.raise_for_status()
    except Exception as e:
        print(f"Log Error: Impossible d'accéder à l'API : {e}")
        return

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Conteneurs pour le tri
    delays = {}
    bus_batch = []
    alerts = []

    # --- ÉTAPE 1 : On parcourt le flux UNIQUE pour tout extraire ---
    for entity in feed.entity:
        # A. On extrait les retards
        if entity.HasField('trip_update'):
            tu = entity.trip_update
            if tu.stop_time_update:
                last = tu.stop_time_update[-1]
                delays[tu.trip.trip_id] = last.arrival.delay if last.HasField('arrival') else 0
        
        # B. On extrait les alertes
        if entity.HasField('alert'):
            al = entity.alert
            alerts.append({
                "alert_id": entity.id,
                "route_id": al.informed_entity[0].route_id if al.informed_entity else "Global",
                "header_text": al.header_text.translation[0].text if al.header_text.translation else "",
                "cause": str(al.cause),
                "start_time": pd.to_datetime(al.active_period[0].start, unit='s').isoformat() if al.active_period else None
            })

    # --- ÉTAPE 2 : Analyse Spatiale Prioritaire ---
    gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
    priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
    gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
    gdf_map = gdf_map.sort_values('priority')

    # --- ÉTAPE 3 : On associe les positions aux retards trouvés ---
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pt = Point(v.position.longitude, v.position.latitude)
            match = gdf_map[gdf_map.contains(pt)]
            area = match.iloc[0] if not match.empty else None

            bus_batch.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "location": f"POINT({v.position.longitude} {v.position.latitude})",
                "area_name": area['name'] if area is not None else "Unknown",
                "area_type": area['area_type'] if area is not None else "Unknown",
                "delay_seconds": delays.get(v.trip.trip_id, 0),
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    # --- ÉTAPE 4 : Envois Supabase ---
    if alerts:
        supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
    if bus_batch:
        supabase.table("bus_positions").insert(bus_batch).execute()
        print(f"Success: {len(bus_batch)} positions et {len(alerts)} alertes traitées depuis un flux unique.")

if __name__ == "__main__":
    run_pipeline()
