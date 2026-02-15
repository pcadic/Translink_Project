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
    print("Log: Starting synchronized pipeline...")
    log_data = {"status": "Success", "bus_count": 0}
    bus_batch = []

    try:
        # 1. RÉCUPÉRATION DES FLUX
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Mapping des délais par TripID
        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        # 2. CHARGEMENT GEOJSON
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        # 3. TRAITEMENT DES BUS
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # Double détection spatiale
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = m_neigh.iloc[0]['name'] if not m_neigh.empty else None
                
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # Attribution des champs pour correspondre à ton SQL
                bus_batch.append({
                    "vehicle_no": v.vehicle.id,
                    "route_no": v.trip.route_id,
                    "route_name": f"Line {v.trip.route_id}", # Nom par défaut
                    "direction": "Inbound" if v.trip.direction_id == 0 else "Outbound",
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "area_name": neigh_name if neigh_name else city_name,
                    "municipality": city_name,
                    "delay_seconds": delays.get(v.trip.trip_id, 0),
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat(),
                    "destination": "See Schedule"
                })

        # 4. ENVOI SUPABASE
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} buses pushed to database.")
            log_data["bus_count"] = len(bus_batch)

    except Exception as e:
        print(f"Pipeline Error: {e}")
        log_data["status"] = "Error"

    # Logging final
    try:
        supabase.table("pipeline_logs").insert(log_data).execute()
    except:
        pass

if __name__ == "__main__":
    run_pipeline()
