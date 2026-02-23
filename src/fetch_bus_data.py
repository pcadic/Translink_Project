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

    try:
        # 1. CHARGEMENT DES RÉFÉRENCES
        print("Chargement des tables de référence Supabase...")
        
        # Récupération des routes
        r_res = supabase.table("routes").select("*").limit(10000).execute()
        routes_ref = pd.DataFrame(r_res.data)
        if not routes_ref.empty:
            routes_ref.columns = routes_ref.columns.str.lower() # Sécurité casse
        
        # Récupération des directions
        d_res = supabase.table("Directions").select("*").limit(10000).execute()
        dirs_ref = pd.DataFrame(d_res.data)
        if not dirs_ref.empty:
            dirs_ref.columns = dirs_ref.columns.str.lower() # Sécurité casse

        print(f"Routes chargées: {len(routes_ref)} | Directions chargées: {len(dirs_ref)}")

        # 2. FETCH API
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")

        # 3. GEOSPATIAL & DELAYS
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        # 4. TRAITEMENT DES BUS
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                raw_route_id = v.trip.route_id
                raw_dir_id = str(v.trip.direction_id)

                # LOOKUP : Route info
                r_short, r_long = None, None
                if not routes_ref.empty and 'route_id' in routes_ref.columns:
                    match = routes_ref[routes_ref['route_id'] == raw_route_id]
                    if not match.empty:
                        r_short = match.iloc[0]['route_short_name']
                        r_long = match.iloc[0]['route_long_name']

                # LOOKUP : Direction Name
                d_name = None
                if not dirs_ref.empty and 'route_name' in dirs_ref.columns:
                    # On compare avec r_short (ex: '99')
                    d_match = dirs_ref[(dirs_ref['route_name'] == r_short) & (dirs_ref['direction_id'] == raw_dir_id)]
                    if not d_match.empty:
                        d_name = d_match.iloc[0]['direction_name']

                # Géo-localisation
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = m_neigh.iloc[0]['name'] if not m_neigh.empty else None

                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": raw_route_id,
                    "direction": raw_dir_id,
                    "direction_name": d_name,
                    "route_short_name": r_short,
                    "route_long_name": r_long,
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": neigh_name if neigh_name else city_name,
                    "area_type": "neighborhood" if neigh_name else "municipality",
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": fetch_time_utc
                })

        # 5. ENVOI
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"--- TERMINE : {len(bus_batch)} positions insérées ---")

    except Exception as e:
        print(f"❌ ERREUR : {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()
