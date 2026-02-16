import os
from supabase import create_client
from datetime import datetime

# Forcer l'affichage immédiat dans la console
import sys

print("--- DEBUG START ---")
sys.stdout.flush()

# Vérification des variables d'environnement
url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

if not url or not key:
    print("ERREUR: Variables d'environnement manquantes !")
    sys.exit(1)

print(f"Connexion à : {url}")
sys.stdout.flush()

try:
    supabase = create_client(url, key)
    
    test_data = {
        "alert_id": f"DEBUG_{int(datetime.now().timestamp())}",
        "header_text": "TEST MANUEL",
        "description_text": "Test de connexion directe",
        "created_at": datetime.now().isoformat()
    }

    print("Tentative d'insertion...")
    sys.stdout.flush()
    
    # On utilise .insert() car c'est le plus simple
    response = supabase.table("service_alerts").insert(test_data).execute()
    
    print("SUCCÈS !")
    print("Réponse Supabase :", response)

except Exception as e:
    print(f"ÉCHEC CRITIQUE : {str(e)}")
    import traceback
    traceback.print_exc()

print("--- DEBUG END ---")
sys.stdout.flush()
