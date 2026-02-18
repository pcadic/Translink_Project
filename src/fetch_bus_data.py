import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client
from datetime import datetime

# --- CONFIGURATION ---
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_feed(endpoint):
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {'Accept': 'application/x-google-protobuf'}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def run_pipeline():
    print("--- DEBUT DU PIPELINE ---")
    bus_batch = []
    alerts_batch = []

    try:
        # 1. FETCH API
        print("Récupération des flux TransLink...")
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # 2. TRAITEMENT DES ALERTES
        print(f"Analyse de {len(rt_feed.entity)} entités pour les alertes...")
        alert_count_found = 0 # Compteur de diagnostic
        for entity in rt_feed.entity:
            if entity.HasField('alert'):
                alert_count_found += 1
                a = entity.alert
                
                # Extraction sécurisée des textes
                h_text = a.header_text.translation[0].text if a.header_text.translation else "No Header"
                d_text = a.description_text.translation[0].text if a.description_text.translation else "No Description"
                
                # Extraction RouteID
                r_id = a.informed_entity[0].route_id if a.informed_entity else None
                
                # Dates
                s_time = datetime.fromtimestamp(a.active_period[0].start).isoformat() if a.active_period else None
                e_time = datetime.fromtimestamp(a.active_period[0].end).isoformat() if a.active_period and a.active_period[0].end < 2147483647 else None

                alerts_batch.append({
                    "alert_id": str(entity.id),
                    "route_id": r_id,
                    "header_text": h_text,
                    "description_text": d_text,
                    "cause": str(a.cause) if a.cause else "UNKNOWN_CAUSE",
                    "effect": str(a.effect) if a.effect else "UNKNOWN_EFFECT",
                    "start_time": s_time,
                    "end_time": e_time,
                    "created_at": datetime.now().isoformat()
                })
        print(f"DEBUG: Nombre d'alertes réelles détectées dans le flux: {alert_count_found}")
        
        # 3. TRAITEMENT DES BUS (GEOSPATIAL)
        print("Chargement du GeoJSON et traitement des bus...")
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        # Mapping des délais
        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = None
                if not m_neigh.empty:
                    for name in m_neigh['name']:
                        if name != city_name:
                            neigh_name = name
                            break
                
                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": neigh_name if neigh_name else city_name,
                    "area_type": "neighborhood" if neigh_name else "municipality",
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
                })

        # 4. ENVOI SUPABASE
        if alerts_batch:
            print(f"Envoi de {len(alerts_batch)} alertes...")
            supabase.table("service_alerts").upsert(alerts_batch, on_conflict="alert_id").execute()
        
        if bus_batch:
            print(f"Envoi de {len(bus_batch)} positions de bus...")
            supabase.table("bus_positions").insert(bus_batch).execute()

        print("--- PIPELINE TERMINE AVEC SUCCÈS ---")

    except Exception as e:
        print(f"❌ ERREUR CRITIQUE : {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()
