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
    print("Log: Starting pipeline with proper City/Neighborhood mapping...")
    bus_batch = []

    try:
        # 1. RÉCUPÉRATION DES DONNÉES TEMPS RÉEL
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Dictionnaire des retards
        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        # 2. CHARGEMENT DU GEOJSON (Ton chemin spécifique)
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        
        # On sépare les couches pour la détection double
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        # 3. TRAITEMENT GÉOSPATIAL
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # Étape A: Trouver le quartier (ex: Kitsilano, Metrotown)
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = m_neigh.iloc[0]['name'] if not m_neigh.empty else None
                
                # Étape B: Trouver la ville (ex: Vancouver, Burnaby)
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # Étape C: Déterminer area_name et area_type pour la table
                # Si on a trouvé un quartier, on le met en priorité
                if neigh_name:
                    final_area_name = neigh_name
                    final_area_type = "neighborhood"
                else:
                    final_area_name = city_name
                    final_area_type = "municipality"

                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": final_area_name,
                    "area_type": final_area_type,
                    "municipality": city_name, # Toujours la ville parente
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat(),
                    "route_name": f"Line {v.trip.route_id}"
                })

        # 4. ENVOI À SUPABASE
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} vehicles processed with City/Neighborhood distinction.")

    except Exception as e:
        print(f"Error in pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
