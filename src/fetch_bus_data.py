import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client
from datetime import datetime, timezone

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
    
    fetch_time_utc = datetime.now(timezone.utc).isoformat()
    
    bus_batch = []
    alerts_batch = []

    try:
        # 1. FETCH API
        print("Récupération des flux TransLink...")
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")

        # Extraction des délais (trip_id -> delay)
        delays = {}
        for entity in rt_feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                if tu.stop_time_update:
                    # On prend le délai du dernier arrêt mis à jour
                    last_update = tu.stop_time_update[-1]
                    if last_update.HasField('arrival'):
                        delays[tu.trip.trip_id] = last_update.arrival.delay
                    elif last_update.HasField('departure'):
                        delays[tu.trip.trip_id] = last_update.departure.delay

        # 2. CHARGEMENT GEOJSON (Vancouver)
        print("Chargement des zones géographiques...")
        municipalities = gpd.read_file("data\vancouver_areas.geojson")
        neighborhoods = gpd.read_file("data\neighborhoods.geojson")

        # 3. TRAITEMENT DES BUS
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # --- LOGIQUE DE NETTOYAGE DU NUMÉRO DE ROUTE ---
                raw_route = v.trip.route_id
                # On retire les zéros inutiles (ex: '099' -> '99')
                # Mais on garde les lettres intactes (ex: 'R4')
                clean_route = str(raw_route).lstrip('0') if str(raw_route).isdigit() else str(raw_route)
                if not clean_route: clean_route = "0"
                
                # Identification de la zone
                city_name = "Unknown"
                neigh_name = None
                
                m_city = municipalities[municipalities.contains(p)]
                if not m_city.empty:
                    city_name = m_city.iloc[0]['name']
                    
                    n_neigh = neighborhoods[neighborhoods.contains(p)]
                    if not n_neigh.empty:
                        for _, row in n_neigh.iterrows():
                            name = row['name']
                            if name != city_name:
                                neigh_name = name
                                break
                
                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id, # Inchangé (champ original)
                    "route_short_name": clean_route, # NOUVELLE COLONNE PROPRE
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": neigh_name if neigh_name else city_name,
                    "area_type": "neighborhood" if neigh_name else "municipality",
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": fetch_time_utc
                })

        # 4. ENVOI SUPABASE
        if alerts_batch:
            print(f"Envoi de {len(alerts_batch)} alertes...")
            supabase.table("service_alerts").upsert(alerts_batch, on_conflict="alert_id").execute()
        
        if bus_batch:
            print(f"Envoi de {len(bus_batch)} positions de bus...")
            supabase.table("bus_positions").insert(bus_batch).execute()

        print(f"--- PIPELINE TERMINE : {len(bus_batch)} positions enregistrées à {fetch_time_utc} ---")

    except Exception as e:
        print(f"❌ ERREUR CRITIQUE : {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()
