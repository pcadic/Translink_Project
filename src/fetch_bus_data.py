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
    print("Log: Démarrage du pipeline Dual-Stream (Positions + Updates)...")
    
    try:
        # 1. On récupère les positions (car ton bundle ne les a pas)
        pos_feed = get_feed("gtfsposition")
        # 2. On récupère les updates/alerts (ton bundle actuel)
        bundle_feed = get_feed("gtfsrealtime")
        print(f"Log: Reçu {len(pos_feed.entity)} positions et {len(bundle_feed.entity)} updates/alerts.")
    except Exception as e:
        print(f"Log Error: Échec récupération API : {e}")
        return

    # --- ÉTAPE 1 : Chargement de la Map ---
    try:
        gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
        priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
        gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
        gdf_map = gdf_map.sort_values('priority')
    except Exception as e:
        print(f"Log Error GeoJSON: {e}")
        return

    # --- ÉTAPE 2 : Extraction des Retards et Alertes (depuis bundle) ---
    delays = {}
    alerts = []
    for entity in bundle_feed.entity:
        if entity.HasField('trip_update'):
            tu = entity.trip_update
            if tu.stop_time_update:
                last = tu.stop_time_update[-1]
                delays[tu.trip.trip_id] = last.arrival.delay if last.HasField('arrival') else 0
        
        # --- Extraction des Alertes (Version robuste) ---
        if entity.HasField('alert'):
            al = entity.alert
            
            # On cherche le route_id s'il existe, sinon on met "NETWORK"
            rid = "NETWORK"
            if al.informed_entity:
                # On prend le premier qui a un route_id
                for item in al.informed_entity:
                    if item.route_id:
                        rid = item.route_id
                         break

             alerts.append({
                "alert_id": entity.id,
                "route_id": rid,
                "header_text": al.header_text.translation[0].text if al.header_text.translation else "No Title",
                 "cause": str(al.cause),
                "start_time": pd.to_datetime(al.active_period[0].start, unit='s').isoformat() if al.active_period else None
            })
    
     print(f"Log Discovery: {len(alerts)} alertes prêtes pour Supabase.")

    # --- ÉTAPE 3 : Traitement des Positions (depuis pos_feed) ---
    bus_batch = []
    for entity in pos_feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pt = Point(v.position.longitude, v.position.latitude)
            match = gdf_map[gdf_map.contains(pt)]
            
            area_name = match.iloc[0]['name'] if not match.empty else "Off-Map"
            area_type = match.iloc[0]['area_type'] if not match.empty else "Unknown"

            bus_batch.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "location": f"POINT({v.position.longitude} {v.position.latitude})",
                "area_name": area_name,
                "area_type": area_type,
                "delay_seconds": delays.get(v.trip.trip_id, 0),
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    print(f"Log Statistics: {len(bus_batch)} bus et {len(alerts)} alertes prêts.")

    # --- ÉTAPE 4 : Envois Supabase ---
    if alerts:
        supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
    if bus_batch:
        try:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Final Success: {len(bus_batch)} bus insérés.")
        except Exception as e:
            print(f"Log Error Supabase: {e}")

if __name__ == "__main__":
    run_pipeline()
