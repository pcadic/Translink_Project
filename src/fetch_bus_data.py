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
        # 1. CHARGEMENT DES RÉFÉRENCES (JOINTS)
        print("Chargement des tables de référence Supabase...")
        # On récupère les données de mapping pour éviter les requêtes SQL dans la boucle
        routes_data = supabase.table("routes").select("route_id, route_short_name, route_long_name").execute().data
        routes_ref = pd.DataFrame(routes_data)
        
        dirs_data = supabase.table("Directions").select("route_name, direction_id, direction_name").execute().data
        dirs_ref = pd.DataFrame(dirs_data)

        # 2. FETCH API
        print("Récupération des flux TransLink...")
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # 3. TRAITEMENT DES ALERTES
        print(f"Analyse de {len(rt_feed.entity)} entités pour les alertes...")
        for entity in rt_feed.entity:
            if entity.HasField('alert'):
                a = entity.alert
                h_text = a.header_text.translation[0].text if a.header_text.translation else "No Header"
                d_text = a.description_text.translation[0].text if a.description_text.translation else "No Description"
                r_id = a.informed_entity[0].route_id if a.informed_entity else None
                
                alerts_batch.append({
                    "alert_id": str(entity.id),
                    "route_id": r_id,
                    "header_text": h_text,
                    "description_text": d_text,
                    "cause": str(a.cause) if a.cause else "UNKNOWN_CAUSE",
                    "effect": str(a.effect) if a.effect else "UNKNOWN_EFFECT",
                    "created_at": fetch_time_utc 
                })
        
        # 4. TRAITEMENT DES BUS (GEOSPATIAL & ENRICHISSEMENT)
        print("Chargement du GeoJSON et traitement des bus...")
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # Identifiants API
                raw_route_id = v.trip.route_id
                raw_direction_id = str(v.trip.direction_id) # '0' ou '1'
                
                # A. Recherche dans la table 'routes'
                r_match = routes_ref[routes_ref['route_id'] == raw_route_id]
                r_short = r_match['route_short_name'].values[0] if not r_match.empty else None
                r_long = r_match['route_long_name'].values[0] if not r_match.empty else None
                
                # B. Recherche dans la table 'Directions'
                # On utilise r_short car votre table Directions se base sur route_name (ex: '99')
                d_match = dirs_ref[(dirs_ref['route_name'] == r_short) & (dirs_ref['direction_id'] == raw_direction_id)]
                d_name = d_match['direction_name'].values[0] if not d_match.empty else None

                # C. Géo-localisation
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
                    "route_no": raw_route_id,
                    "route_short_name": r_short, # Issu de la table routes
                    "route_long_name": r_long,   # Issu de la table routes
                    "direction": raw_direction_id, # Stocke '0' ou '1'
                    "direction_name": d_name,      # Issu de la table Directions
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": neigh_name if neigh_name else city_name,
                    "area_type": "neighborhood" if neigh_name else "municipality",
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": fetch_time_utc
                })

        # 5. ENVOI SUPABASE
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
