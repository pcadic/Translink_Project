import osmnx as ox
import pandas as pd
import geopandas as gpd
import os

# Set timeout to 5 minutes for stable downloads
ox.settings.timeout = 300

def generate_map():
    print("Log: Starting strict geospatial extraction for Metro Vancouver...")
    
    if not os.path.exists('data'):
        os.makedirs('data')

    # 1. Exact list of Municipalities (Cities, Districts, Villages)
    cities_and_towns = [
        "Burnaby", "Coquitlam", "Delta", "Langley City", "Maple Ridge", 
        "New Westminster", "North Vancouver", "Pitt Meadows", "Port Coquitlam", 
        "Port Moody", "Richmond", "Surrey", "Vancouver", "White Rock",
        "District of Langley", "District of North Vancouver", "District of West Vancouver",
        "Anmore", "Belcarra", "Lions Bay", "Bowen Island", "Tsawwassen First Nation"
    ]
    
    # 2. Specific Special Areas (Critical for Translink analysis)
    special_areas = [
        {"query": "University Endowment Lands, BC, Canada", "label": "UBC"},
        {"query": "Stanley Park, Vancouver, BC, Canada", "label": "Stanley Park"}
    ]

    geodf_list = []

    # Fetch Municipalities
    for place in cities_and_towns:
        print(f"Log: Fetching municipality: {place}...")
        try:
            query = f"{place}, British Columbia, Canada"
            area = ox.geocode_to_gdf(query)
            area = area[['geometry']].copy()
            area['name'] = place
            area['area_type'] = 'municipality'
            geodf_list.append(area)
        except Exception as e:
            print(f"Log Warning: Skip {place}. Reason: {e}")

    # Fetch Special Zones (UBC & Stanley Park)
    for area_info in special_areas:
        print(f"Log: Fetching special zone: {area_info['label']}...")
        try:
            gdf = ox.geocode_to_gdf(area_info['query'])
            gdf = gdf[['geometry']].copy()
            gdf['name'] = area_info['label']
            gdf['area_type'] = 'special_zone'
            geodf_list.append(gdf)
        except Exception as e:
            print(f"Log Warning: Skip {area_info['label']}. Reason: {e}")

    # 3. Fetch Vancouver Neighborhoods (For inner-city granularity)
    print("Log: Fetching detailed Vancouver neighborhoods...")
    try:
        van_neighborhoods = ox.features_from_place(
            "Vancouver, British Columbia, Canada", 
            tags={"boundary": "administrative", "admin_level": "10"}
        )
        van_neighborhoods = van_neighborhoods[['name', 'geometry']].copy()
        van_neighborhoods['area_type'] = 'neighborhood'
        geodf_list.append(van_neighborhoods)
    except Exception as e:
        print(f"Log Warning: Could not fetch neighborhoods. Reason: {e}")

    # 4. Merge and Export
    if geodf_list:
        final_map = pd.concat(geodf_list, ignore_index=True)
        
        # Standardize Coordinate Reference System to WGS84
        final_map = final_map.to_crs(epsg=4326)
        
        output_file = "data/metro_vancouver_map.geojson"
        final_map.to_file(output_file, driver='GeoJSON')
        print(f"Log: Success! Created map with {len(final_map)} features.")
        print(f"Log: File saved at {output_file}")
    else:
        print("Log Error: No data collected. Check internet connection or OSM status.")

if __name__ == "__main__":
    generate_map()
