import osmnx as ox
import pandas as pd
import geopandas as gpd
import os

# Increase timeout for stability
ox.settings.timeout = 300

def generate_map():
    print("Log: Starting strict extraction for Metro Vancouver...")
    
    if not os.path.exists('data'):
        os.makedirs('data')

    # 1. Define your exact list
    cities = [
        "Burnaby", "Coquitlam", "Delta", "Langley City", "Maple Ridge", 
        "New Westminster", "North Vancouver", "Pitt Meadows", "Port Coquitlam", 
        "Port Moody", "Richmond", "Surrey", "Vancouver", "White Rock"
    ]
    districts = ["District of Langley", "District of North Vancouver", "District of West Vancouver"]
    villages = ["Anmore", "Belcarra", "Lions Bay"]
    others = ["Bowen Island", "Tsawwassen First Nation", "University Endowment Lands"]

    all_places = cities + districts + villages + others
    
    geodf_list = []

    for place in all_places:
        print(f"Log: Fetching {place}...")
        try:
            # We search for the administrative boundary of each specific place
            # Adding 'British Columbia, Canada' ensures we don't get a city in the USA
            query = f"{place}, British Columbia, Canada"
            area = ox.geocode_to_gdf(query)
            
            # Keep only name and geometry
            area = area[['display_name', 'geometry']].copy()
            area['name'] = place
            area['area_type'] = 'municipality'
            geodf_list.append(area)
        except Exception as e:
            print(f"Log Warning: Could not find {place}. Error: {e}")

    # 2. Add Vancouver Neighborhoods (Level 10) specifically
    print("Log: Fetching Vancouver neighborhoods (detailed)...")
    try:
        van_neighborhoods = ox.features_from_place(
            "Vancouver, British Columbia, Canada", 
            tags={"boundary": "administrative", "admin_level": "10"}
        )
        van_neighborhoods = van_neighborhoods[['name', 'geometry']].copy()
        van_neighborhoods['area_type'] = 'neighborhood'
        geodf_list.append(van_neighborhoods)
    except Exception as e:
        print(f"Log Warning: Could not fetch neighborhoods. Error: {e}")

    # 3. Merge everything
    if geodf_list:
        final_map = pd.concat(geodf_list, ignore_index=True)
        
        # Ensure we are using standard GPS coordinates
        final_map = final_map.to_crs(epsg=4326)
        
        # Final export
        final_map.to_file("data/metro_vancouver_map.geojson", driver='GeoJSON')
        print(f"Log: Success! Created map with {len(final_map)} areas.")
    else:
        print("Log Error: No data was collected.")

if __name__ == "__main__":
    generate_map()
