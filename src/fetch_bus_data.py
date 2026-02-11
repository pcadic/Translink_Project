import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Configuration des accès
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_gtfs_feed(endpoint):
    """Récupère et décode un flux GTFS-RT binaire de TransLink"""
    url = f"https://gtfsapi.translink.ca/v3/{endpoint}?apikey={TRANSLINK_API_KEY}"
    headers = {
        'User-Agent': 'TranslinkProject/1.0',
        'Accept': 'application/x-google-protobuf'
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        raise Exception(f"Erreur {response.status_code} sur l'URL: {url}")
        
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed

def run_pipeline():
    print("Log: Démarrage du Pipeline GTFS-RT V3...")
    
    # 1. RÉCUPÉRATION DES FLUX (Note les pluriels sur updates et alerts)
    try:
        print("Log: Récupération des données en cours...")
        pos_feed = get_gtfs_feed("gtfsposition")
        upd_feed = get_gtfs_feed("gtfstripupdates")  # Standard V3
        alt_feed = get_gtfs_feed("gtfsalerts")       # Standard V3
        print("Log: Tous les flux ont été récupérés avec succès.")
    except Exception as e:
        print(f"Log Error: Échec de la récupération : {e}")
        return

    # 2. EXTRACTION DES RETARDS (Delays)
    delays = {}
    for entity in upd_feed.entity:
        if entity.HasField('trip_update'):
            tid = entity.trip_update.trip.trip_id
            if entity.trip_update.stop_time_update:
                # On prend le délai du dernier point de contrôle connu
                last_upd = entity.trip_update.stop_time_update[-1]
                delays[tid] = last_upd.arrival.delay if last_upd.HasField('arrival') else 0

    # 3. GESTION DES ALERTES (Upsert pour éviter les doublons)
    alerts = []
    for entity in alt_feed.entity:
        if entity.HasField('alert'):
            a = entity.alert
            alerts.append({
                "alert_id": entity.id,
                "route_id": a.informed_entity[0].route_id if a.informed_entity else "Global",
                "header_text": a.header_text.translation[0].text if a.header_text.translation else "Sans titre",
                "cause": str(a.cause),
                "start_time": pd.to_datetime(a.active_period[0].start, unit='s').isoformat() if a.active_period else None
            })
    
    if alerts:
        try:
            supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
            print(f"Log: {len(alerts)} alertes mises à jour dans Supabase.")
        except Exception as e:
            print(f"Log Warning: Erreur insertion alertes : {e}")

    # 4. TRAITEMENT DES POSITIONS ET PRIORITÉ SPATIALE
    print("Log: Analyse spatiale des positions (Priorité Quartiers)...")
    try:
        gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
    except Exception as e:
        print(f"Log Error: Impossible de charger le GeoJSON : {e}")
        return

    # Tri par priorité : Neighborhood (1) > Special Zone (2) > Municipality (3)
    priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
    gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
    gdf_map = gdf_map.sort_values('priority')

    bus_batch = []
    for entity in pos_feed.entity:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pt = Point(v.position.longitude, v.position.latitude)
            
            # Recherche de la zone (le tri garantit qu'on trouve le quartier avant la ville)
            match = gdf_map[gdf_map.contains(pt)]
            area_info = match.iloc[0] if not match.empty else None

            bus_batch.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "location": f"POINT({v.position.longitude} {v.position.latitude})",
                "area_name": area_info['name'] if area_info is not None else "Unknown",
                "area_type": area_info['area_type'] if area_info is not None else "Unknown",
                "delay_seconds": delays.get(v.trip.trip_id, 0),
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    # 5. ENVOI FINAL VERS SUPABASE
    if bus_batch:
        try:
            # Note : Tu peux vider la table ici si tu es encore en phase de test
            # supabase.table("bus_positions").delete().neq("id", 0).execute() 
            
            supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Success: {len(bus_batch)} positions de bus insérées.")
        except Exception as e:
            print(f"Log Error: Erreur lors de l'insertion Supabase : {e}")

if __name__ == "__main__":
    run_pipeline()
