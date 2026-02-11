import os
import requests
import pandas as pd
import geopandas as gpd
from google.transit import gtfs_realtime_pb2
from shapely.geometry import Point
from supabase import create_client

# Config
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
supabase = create_client(os.environ.get('SUPABASE_URL'), os.environ.get('SUPABASE_KEY'))

def run_pipeline():
    # Test de l'URL Bundle
    url = f"https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey={TRANSLINK_API_KEY}"
    headers = {'User-Agent': 'TranslinkProject/1.0', 'Accept': 'application/x-google-protobuf'}

    print(f"Log: Requête sur {url.split('?')[0]}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        total_entities = len(feed.entity)
        print(f"Log: {total_entities} entités brutes reçues de TransLink.")
        
        if total_entities == 0:
            print("Log Warning: Flux vide. Tentative de secours sur l'ancienne URL...")
            url = f"https://gtfsapi.translink.ca/v3/gtfsposition?apikey={TRANSLINK_API_KEY}"
            response = requests.get(url, headers=headers, timeout=30)
            feed.ParseFromString(response.content)
            print(f"Log: {len(feed.entity)} entités reçues via fallback.")

    except Exception as e:
        print(f"Log Error: Échec réseau/parsing : {e}")
        return

    # Extraction
    delays = {}
    bus_batch = []
    alerts = []

    # Chargement Map
    try:
        gdf_map = gpd.read_file('data/metro_vancouver_map.geojson')
        # On s'assure que le GeoJSON n'est pas vide
        if gdf_map.empty:
            print("Log Error: Le fichier GeoJSON est vide !")
            return
        
        priority_map = {'neighborhood': 1, 'special_zone': 2, 'municipality': 3}
        gdf_map['priority'] = gdf_map['area_type'].map(priority_map)
        gdf_map = gdf_map.sort_values('priority')
    except Exception as e:
        print(f"Log Error GeoJSON: {e}")
        return

    # Tri des données
    for entity in feed.entity:
        # Delays
        if entity.HasField('trip_update'):
            tu = entity.trip_update
            if tu.stop_time_update:
                last = tu.stop_time_update[-1]
                delays[tu.trip.trip_id] = last.arrival.delay if last.HasField('arrival') else 0
        
        # Alerts
        if entity.HasField('alert'):
            al = entity.alert
            alerts.append({
                "alert_id": entity.id,
                "route_id": al.informed_entity[0].route_id if al.informed_entity else "Global",
                "header_text": al.header_text.translation[0].text if al.header_text.translation else "",
                "cause": str(al.cause),
                "start_time": pd.to_datetime(al.active_period[0].start, unit='s').isoformat() if al.active_period else None
            })

        # Positions
        if entity.HasField('vehicle'):
            v = entity.vehicle
            pt = Point(v.position.longitude, v.position.latitude)
            
            # Spatial Match
            match = gdf_map[gdf_map.contains(pt)]
            area = match.iloc[0] if not match.empty else None

            bus_batch.append({
                "vehicle_no": v.vehicle.id,
                "route_no": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "location": f"POINT({v.position.longitude} {v.position.latitude})",
                "area_name": area['name'] if area is not None else "Unknown",
                "area_type": area['area_type'] if area is not None else "Unknown",
                "delay_seconds": delays.get(v.trip.trip_id, 0),
                "recorded_time": pd.to_datetime(v.timestamp, unit='s').isoformat()
            })

    print(f"Log Statistics: {len(bus_batch)} bus prêts, {len(alerts)} alertes prêtes.")

    # Envois Supabase
    if alerts:
        supabase.table("service_alerts").upsert(alerts, on_conflict="alert_id").execute()
    
    if bus_batch:
        try:
            res = supabase.table("bus_positions").insert(bus_batch).execute()
            print(f"Final Success: Données envoyées à Supabase.")
        except Exception as e:
            print(f"Log Error Supabase: {e}")

if __name__ == "__main__":
    run_pipeline()
