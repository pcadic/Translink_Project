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
    print("Log: Starting clean pipeline (City/Neighborhood focus)...")
    bus_batch = []

    try:
        # 1. DATA FETCH
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        # 2. GEO DATA
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        
        # Séparation stricte
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        # 3. PROCESSING
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # RECHERCHE VILLE (Obligatoire)
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # RECHERCHE QUARTIER
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = None
                if not m_neigh.empty:
                    # On prend le premier quartier qui n'est pas juste le nom de la ville
                    for name in m_neigh['name']:
                        if name != city_name:
                            neigh_name = name
                            break
                
                # LOGIQUE FINALE
                final_area_name = neigh_name if neigh_name else city_name
                final_area_type = "neighborhood" if neigh_name else "municipality"

                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": final_area_name,
                    "area_type": final_area_type,
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
                })

        # 4. UPLOAD
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} clean rows uploaded.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_pipeline()
