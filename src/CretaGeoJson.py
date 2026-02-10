import osmnx as ox
import pandas as pd

# 1. Récupérer toutes les municipalités de Metro Vancouver (Niveau 8)
print("Récupération des municipalités...")
metro_vanc = ox.features_from_place("Metro Vancouver, Canada", tags={"boundary": "administrative", "admin_level": "8"})
metro_vanc = metro_vanc[['name', 'geometry']].copy()
metro_vanc['type'] = 'City'

# 2. Récupérer spécifiquement les quartiers de Vancouver (Niveau 10)
print("Récupération des quartiers de Vancouver...")
van_neighborhoods = ox.features_from_place("Vancouver, BC, Canada", tags={"boundary": "administrative", "admin_level": "10"})
van_neighborhoods = van_neighborhoods[['name', 'geometry']].copy()
van_neighborhoods['type'] = 'Neighborhood'

# 3. Ajouter UBC et Stanley Park (qui sont souvent à part)
print("Ajout des zones spéciales...")
specials = ox.geocode_to_gdf(["University Endowment Lands, BC", "Stanley Park, Vancouver, BC"])
specials = specials[['name', 'geometry']].copy()
specials['type'] = 'Special Zone'

# 4. Fusionner le tout
final_map = pd.concat([metro_vanc, van_neighborhoods, specials])

# 5. Sauvegarder
final_map.to_file("data/metro_vancouver_map.geojson", driver='GeoJSON')
print("Fichier data/metro_vancouver_map.geojson créé avec succès !")
