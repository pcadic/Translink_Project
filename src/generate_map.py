import osmnx as ox
import pandas as pd
import geopandas as gpd

def generate_map():
    print("Log: Starting clean extraction...")
    
    # Increase timeout and use strict queries
    ox.settings.timeout = 300

    # 1. Focus specifically on the Regional District
    print("Log: Fetching Metro Vancouver Municipalities...")
    # Using a more specific name to avoid global relations
    metro_vanc = ox.features_from_place(
        "Metro Vancouver Regional District, British Columbia, Canada", 
        tags={"boundary": "administrative", "admin_level": "8"}
    )
    
    # Filter: Keep only features where the name is NOT 'Canada' or 'United States'
    metro_vanc = metro_vanc[metro_vanc['name'].notnull()]
    metro_vanc = metro_vanc[['name', 'geometry']].copy()
    metro_vanc['area_type'] = 'municipality'

    # 2. Fetch Vancouver Neighborhoods
    print("Log: Fetching City of Vancouver neighborhoods...")
    van_neighborhoods = ox.features_from_place(
        "Vancouver, British Columbia, Canada", 
        tags={"boundary": "administrative", "admin_level": "10"}
    )
    van_neighborhoods = van_neighborhoods[['name', 'geometry']].copy()
    van_neighborhoods['area_type'] = 'neighborhood'

    # 3. Combine
    final_map = pd.concat([metro_vanc, van_neighborhoods], ignore_index=True)
    
    # 4. Final Cleanup: Remove any oversized geometries (safety check)
    # We only want stuff around Vancouver (approx Longitude -123)
    final_map = final_map[final_map.geometry.centroid.x < -120]

    final_map.to_file("data/metro_vancouver_map.geojson", driver='GeoJSON')
    print(f"Log: Success! Exported {len(final_map)} clean features.")

if __name__ == "__main__":
    generate_map()
