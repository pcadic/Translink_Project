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

def get_text(entity_attr):
    """Extraction sécurisée des textes GTFS-RT"""
    try:
        return entity_attr.translation[0].text
    except:
        return None

def run_pipeline():
    print("Log: Démarrage du pipeline...")
    alerts_batch = []

    try:
        rt_feed = get_feed("gtfsrealtime")
        
        # --- 1. AJOUT D'UNE ALERTE DE TEST (SIMULATION) ---
        # Cette ligne permet de vérifier si la table accepte l'insertion
        alerts_batch.append({
            "alert_id": "TEST_STATION_CLOSED_001",
            "route_id": "999",
            "header_text": "TEST: Station Simulation",
            "description_text": "Ceci est une alerte de test pour vérifier Supabase.",
            "cause": "TECHNICAL_PROBLEM",
            "effect": "DETOUR",
            "start_time": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
        })

        # --- 2. TRAITEMENT DES VRAIES ALERTES ---
        for entity in rt_feed.entity:
            if entity.HasField('alert'):
                a = entity.alert
                
                # Récupération des dates (format Unix timestamp dans GTFS)
                s_time = None
                if a.active_period:
                    s_time = datetime.fromtimestamp(a.active_period[0].start).isoformat()
                
                # Récupération du RouteID (si disponible dans Informed Entity)
                r_id = None
                if a.informed_entity:
                    r_id = a.informed_entity[0].route_id

                alerts_batch.append({
                    "alert_id": str(entity.id),
                    "route_id": r_id,
                    "header_text": get_text(a.header_text),
                    "description_text": get_text(a.description_text),
                    "cause": str(a.cause), # Convertit l'enum en string
                    "effect": str(a.effect), # Convertit l'enum en string
                    "start_time": s_time,
                    "created_at": datetime.now().isoformat()
                })

        # --- 3. ENVOI À SUPABASE ---
        if alerts_batch:
            # Utilisation de upsert pour éviter les erreurs sur alert_id unique
            result = supabase.table("service_alerts").upsert(alerts_batch, on_conflict="alert_id").execute()
            print(f"✅ {len(alerts_batch)} alertes traitées (incluant le test).")
        else:
            print("ℹ️ Aucune alerte réelle à traiter.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

# (Reste du code get_feed et get_bus identique...)
