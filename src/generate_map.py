import osmnx as ox
import pandas as pd
import geopandas as gpd

def generate_map():
    print("Log: Starting geospatial data extraction from OpenStreetMap...")

    # 1. Fetch Metro Vancouver Municipalities (Admin Level 8)
    print("Log: Fetching Metro Vancouver municipalities...")
    metro_vanc = ox.features_from_place(
        "Metro Vancouver, Canada", 
        tags={"boundary": "administrative", "admin_level": "8"}
    )
    metro_vanc = metro_vanc[['name', 'geometry']].copy()
    metro_vanc['area_type'] = 'municipality'

    # 2. Fetch Vancouver Neighborhoods (Admin Level 10)
    print("Log: Fetching City of Vancouver neighborhoods...")
    van_neighborhoods = ox.features_from_place(
        "Vancouver, BC, Canada", 
        tags={"boundary": "administrative", "admin_level": "10"}
    )
    van_neighborhoods = van_neighborhoods[['name', 'geometry']].copy()
    van_neighborhoods['area_type'] = 'neighborhood'

    # 3. Add Special Interest Zones (UBC and Stanley Park)
    print("Log: Fetching special zones (UBC & Stanley Park)...")
    specials = ox.geocode_to_gdf([
        "University Endowment Lands, BC", 
        "Stanley Park, Vancouver, BC"
    ])
    specials = specials[['name', 'geometry']].copy()
    specials['area_type'] = 'special_zone'

    # 4. Combine all layers into one GeoDataFrame
    print("Log: Merging layers...")
    final_map = pd.concat([metro_vanc, van_neighborhoods, specials], ignore_index=True)

    # 5. Ensure CRS is WGS84 (Standard GPS coordinates)
    final_map = final_map.to_crs(epsg=4326)

    # 6. Export to GeoJSON
    output_path = "data/metro_vancouver_map.geojson"
    final_map.to_file(output_path, driver='GeoJSON')
    
    print(f"Log: Success! Map saved at {output_path}")
    print(f"Log: Total features exported: {len(final_map)}")

if __name__ == "__main__":
    generate_map()
