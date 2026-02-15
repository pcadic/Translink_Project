import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- CONNEXION ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=300)
def load_data():
    all_data = []
    chunk_size = 1000
    offset = 0
    while True:
        response = supabase.table("bus_positions").select("*").range(offset, offset + chunk_size - 1).execute()
        data = response.data
        all_data.extend(data)
        if len(data) < chunk_size:
            break
        offset += chunk_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df[df['area_name'] != 'Off-Map'].copy()
        df = df.drop_duplicates(subset=['vehicle_no', 'recorded_time'])
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
    return df

raw_df = load_data()

# --- SIDEBAR (Tes anciens filtres) ---
st.sidebar.header("⚙️ Global Filters")

if not raw_df.empty:
    # Filtre par Ville d'abord
    all_cities = sorted(raw_df['municipality'].unique())
    selected_cities = st.sidebar.multiselect("Cities", options=all_cities, default=all_cities)
    
    df_city = raw_df[raw_df['municipality'].isin(selected_cities)]

    # Filtre par Quartier (Ancien style)
    all_areas = sorted(df_city['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Neighborhoods", options=all_areas, default=all_areas)
    
    df = df_city[df_city['area_name'].isin(selected_areas)].copy()

    st.title("📊 TransLink Performance Dashboard")

    # --- SECTION 1: TES ANCIENS KPIS ---
    col1, col2, col3, col4, col5 = st.columns(5)
    
    unique_buses = df['vehicle_no'].nunique()
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100 if not df.empty else 0
    avg_delay = df['delay_min'].mean() if not df.empty else 0
    
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    
    area_stats = df.groupby('area_name')['delay_min'].mean()
    worst_area = area_stats.idxmax() if not area_stats.empty else "N/A"

    col1.metric("Buses On-Grid", unique_buses)
    col2.metric("Punctuality", f"{on_time:.1f}%")
    col3.metric("Avg Delay", f"{avg_delay:.2f} min")
    col4.metric("Slowest Route", f"R.{slowest_route}")
    col5.metric("Critical Zone", worst_area)

    st.divider()

    # --- SECTION 2: TES ANCIENS GRAPHES (STYLE ORIGINAL) ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        if not route_stats.empty:
            top_10_routes = route_stats.head(10).reset_index()
            fig_route, ax_route = plt.subplots(figsize=(10, 7))
            # Ton ancien dégradé de rouge
            colors_route = [plt.cm.Reds(0.4 + 0.5 * (i/10)) for i in range(len(top_10_routes))][::-1]
            ax_route.barh(top_10_routes['route_no'].astype(str), top_10_routes['delay_min'], color=colors_route)
            ax_route.set_xlabel("Average Delay (min)")
            ax_route.invert_yaxis()
            st.pyplot(fig_route)

    with c2:
        st.subheader("🏙️ Delay by City (Municipality)")
        city_avg = df.groupby('municipality')['delay_min'].mean().sort_values().reset_index()
        fig_city, ax_city = plt.subplots(figsize=(10, 7))
        ax_city.barh(city_avg['municipality'], city_avg['delay_min'], color='skyblue')
        ax_city.set_xlabel("Average Delay (min)")
        st.pyplot(fig_city)

    # --- SECTION 3: LES CARTES (L'UNE EN DESSOUS DE L'AUTRE) ---
    st.divider()
    
    # Carte 1: Positions
    st.subheader("📍 Real-Time Bus Positions")
    st.map(df[['latitude', 'longitude']])

    st.write("") # Espace

    # Carte 2: Heatmap GIS
    st.subheader("🔥 Congestion Heatmap (Spatial Delay)")
    fig_heat = px.density_mapbox(df, 
                                 lat='latitude', lon='longitude', z='delay_min', 
                                 radius=15, center=dict(lat=49.25, lon=-123.12), zoom=9,
                                 mapbox_style="carto-positron", height=600)
    st.plotly_chart(fig_heat, use_container_width=True)

    # --- SECTION 4: QUARTIERS (Ancien bar chart) ---
    st.divider()
    st.subheader("🏘️ Delay by Neighborhood")
    if not area_stats.empty:
        plot_data = area_stats.sort_values().reset_index()
        def get_color(val, min_val, max_val):
            if val < 0: return mcolors.to_hex(plt.cm.Greens(0.6))
            mag = val / max_val if max_val > 0 else 0
            return mcolors.to_hex(plt.cm.Reds(0.3 + 0.6 * mag))
        
        bar_colors = [get_color(x, plot_data['delay_min'].min(), plot_data['delay_min'].max()) for x in plot_data['delay_min']]
        fig_area, ax_area = plt.subplots(figsize=(12, 8))
        ax_area.barh(plot_data['area_name'], plot_data['delay_min'], color=bar_colors)
        ax_area.axvline(0, color='black', linewidth=1)
        st.pyplot(fig_area)

else:
    st.error("No data found in Supabase.")
