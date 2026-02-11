import osmnx as ox
import pandas as pd
import geopandas as gpd
import os

# Set timeout for OSM downloads
ox.settings.timeout = 300

def generate_map():
    print("Log: Generating strict Metro Vancouver Map...")
    if not os.path.exists('data'):
        os.makedirs('data')

    # 1. Precise list of Municipalities
    municipalities = [
        "Burnaby", "Coquitlam", "Delta", "Langley City", "Maple Ridge", 
        "New Westminster", "North Vancouver", "Pitt Meadows", "Port Coquitlam", 
        "Port Moody", "Richmond", "Surrey", "Vancouver", "White Rock",
        "District of Langley", "District of North Vancouver", "District of West Vancouver",
        "Anmore", "Belcarra", "Lions Bay", "Bowen Island", "Tsawwassen First Nation"
    ]
    
    geodf_list = []

    # Fetch Cities/Districts
    for place in municipalities:
        print(f"Log: Fetching {place}...")
        try:
            query = f"{place}, British Columbia, Canada"
            area = ox.geocode_to_gdf(query)
            area = area[['geometry']].copy()
            area['name'] = place
            area['area_type'] = 'municipality'
            geodf_list.append(area)
        except:
            print(f"Warning: Could not find {place}")

    # 2. Special Zones (UBC & Stanley Park)
    specials = [
        {"q": "University Endowment Lands, BC, Canada", "n": "UBC"},
        {"q": "Stanley Park, Vancouver, BC, Canada", "n": "Stanley Park"}
    ]
    for s in specials:
        try:
            area = ox.geocode_to_gdf(s['q'])
            area = area[['geometry']].copy()
            area['name'] = s['n']
            area['area_type'] = 'special_zone'
            geodf_list.append(area)
        except: pass

    # 3. Vancouver Neighborhoods (For high detail)
    print("Log: Adding Vancouver Neighborhoods...")
    try:
        neighborhoods = ox.features_from_place(
            "Vancouver, British Columbia, Canada", 
            tags={"boundary": "administrative", "admin_level": "10"}
        )
        neighborhoods = neighborhoods[['name', 'geometry']].copy()
        neighborhoods['area_type'] = 'neighborhood'
        geodf_list.append(neighborhoods)
    except: pass

    # Merge and Save
    if geodf_list:
        final_map = pd.concat(geodf_list, ignore_index=True)
        final_map = final_map.to_crs(epsg=4326)
        final_map.to_file("data/metro_vancouver_map.geojson", driver='GeoJSON')
        print("Log: Map saved successfully to data/metro_vancouver_map.geojson")

if __name__ == "__main__":
    generate_map()
