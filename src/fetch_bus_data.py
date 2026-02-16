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
    print("Log: Démarrage du pipeline complet (Bus + Alertes)...")
    bus_batch = []
    alerts_batch = []

    try:
        # 1. RÉCUPÉRATION DES FLUX API
        pos_feed = get_feed("gtfsposition")
        rt_feed = get_feed("gtfsrealtime")
        
        # Mapping des délais par TripID
        delays = {ent.trip_update.trip.trip_id: ent.trip_update.stop_time_update[0].arrival.delay 
                  for ent in rt_feed.entity if ent.HasField('trip_update') and ent.trip_update.stop_time_update}

        # 2. GESTION DES ALERTES (SERVICE ALERTS)
        # --- VERSION DE TEST ROBUSTE POUR LES ALERTES ---
        for entity in rt_feed.entity:
            if entity.HasField('alert'):
                # Initialisation par défaut
                header_text = "Alerte sans titre"
                
                # Tentative d'extraction du texte
                if entity.alert.header_text.translation:
                    header_text = entity.alert.header_text.translation[0].text
                elif entity.alert.description_text.translation:
                    header_text = entity.alert.description_text.translation[0].text
                    
                alerts_batch.append({
                    "alert_id": str(entity.id), # On s'assure que c'est un string
                    "header": header_text[:255],
                    "recorded_time": pd.Timestamp.now(tz='UTC').isoformat()
                })

        # 3. CHARGEMENT DES DONNÉES GÉOSPATIALES (GEOJSON)
        gdf = gpd.read_file('data/metro_vancouver_map.geojson')
        neighborhoods = gdf[gdf['area_type'] == 'neighborhood']
        municipalities = gdf[gdf['area_type'] == 'municipality']

        # 4. TRAITEMENT DES POSITIONS DE BUS
        for entity in pos_feed.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle
                p = Point(v.position.longitude, v.position.latitude)
                
                # A. Trouver la Ville (Municipality)
                m_city = municipalities[municipalities.contains(p)]
                city_name = m_city.iloc[0]['name'] if not m_city.empty else "Off-Map"
                
                # B. Trouver le Quartier (Neighborhood)
                m_neigh = neighborhoods[neighborhoods.contains(p)]
                neigh_name = None
                if not m_neigh.empty:
                    # On s'assure de ne pas prendre le nom de la ville comme quartier
                    for name in m_neigh['name']:
                        if name != city_name:
                            neigh_name = name
                            break
                
                # C. Définition des étiquettes finales
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
                    "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat(),
                    "route_name": f"Line {v.trip.route_id}"
                })

        # 5. ENVOI DES DONNÉES À SUPABASE
        # Envoi des Alertes
        if alerts_batch:
            supabase.table("service_alerts").upsert(alerts_batch, on_conflict="alert_id").execute()
            print(f"✅ {len(alerts_batch)} alertes mises à jour.")
        else:
            print("ℹ️ Aucune alerte de service détectée actuellement.")

        # Envoi des Bus
        if bus_batch:
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"✅ {len(bus_batch)} positions de bus envoyées.")

    except Exception as e:
        print(f"❌ Erreur durant le pipeline : {e}")

if __name__ == "__main__":
    run_pipeline()
