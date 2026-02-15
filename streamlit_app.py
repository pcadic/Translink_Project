import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- SUPABASE CONNECTION ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=300)
def load_data():
    all_data = []
    chunk_size = 1000
    offset = 0
    while True:
        # On s'assure de récupérer les nouvelles colonnes
        response = supabase.table("bus_positions").select("*").range(offset, offset + chunk_size - 1).execute()
        data = response.data
        all_data.extend(data)
        if len(data) < chunk_size:
            break
        offset += chunk_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        # Nettoyage
        df = df.drop_duplicates(subset=['vehicle_no', 'recorded_time'])
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
    return df

raw_df = load_data()

# --- SIDEBAR: FILTRES AVANCÉS ---
st.sidebar.header("⚙️ Global Filters")
mode = st.sidebar.radio("Display Mode:", ["Real-Time (Last Run)", "Historical (Global)"])

if not raw_df.empty:
    if mode == "Real-Time (Last Run)":
        latest_ts = raw_df['recorded_time'].max()
        df_working = raw_df[raw_df['recorded_time'] == latest_ts].copy()
    else:
        df_working = raw_df.copy()

    # --- FILTRE 1: VILLE (MUNICIPALITY) ---
    all_cities = sorted(df_working['municipality'].unique())
    selected_cities = st.sidebar.multiselect("Cities", options=all_cities, default=all_cities)
    
    # On filtre par ville d'abord pour mettre à jour la liste des quartiers
    df_filtered_city = df_working[df_working['municipality'].isin(selected_cities)]

    # --- FILTRE 2: QUARTIER (AREA_NAME) ---
    # Ne montre que les quartiers présents dans les villes sélectionnées
    all_areas = sorted(df_filtered_city['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Neighborhoods", options=all_areas, default=all_areas)
    
    # --- FILTRE 3: DIRECTION ---
    all_dirs = sorted(df_filtered_city['direction'].unique())
    selected_dirs = st.sidebar.multiselect("Direction", options=all_dirs, default=all_dirs)

    # Application finale des filtres
    df = df_filtered_city[
        (df_filtered_city['area_name'].isin(selected_areas)) & 
        (df_filtered_city['direction'].isin(selected_dirs))
    ].copy()

    st.title(f"📊 TransLink Analytics - {mode}")

    # --- SECTION 1: KPIs ---
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

    # --- SECTION 2: CHARTS ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        if not route_stats.empty:
            top_10_routes = route_stats.head(10).reset_index()
            fig_route, ax_route = plt.subplots(figsize=(10, 7))
            colors_route = [plt.cm.Reds(0.4 + 0.5 * (i/10)) for i in range(len(top_10_routes))][::-1]
            ax_route.barh(top_10_routes['route_no'].astype(str), top_10_routes['delay_min'], color=colors_route)
            ax_route.set_xlabel("Average Delay (min)")
            ax_route.invert_yaxis()
            st.pyplot(fig_route)
        else:
            st.write("No route data.")

    with c2:
        st.subheader("🏘️ Delay by Neighborhood")
        if not area_stats.empty:
            plot_data = area_stats.sort_values().reset_index()
            def get_color(val, min_val, max_val):
                if val < 0:
                    mag = val / min_val if min_val < 0 else 0
                    return mcolors.to_hex(plt.cm.Greens(0.3 + 0.6 * mag))
                mag = val / max_val if max_val > 0 else 0
                return mcolors.to_hex(plt.cm.Reds(0.3 + 0.6 * mag))
            
            min_d, max_d = plot_data['delay_min'].min(), plot_data['delay_min'].max()
            bar_colors = [get_color(x, min_d, max_d) for x in plot_data['delay_min']]
            fig_area, ax_area = plt.subplots(figsize=(10, 7))
            ax_area.barh(plot_data['area_name'], plot_data['delay_min'], color=bar_colors)
            ax_area.axvline(0, color='black', linewidth=1.5)
            ax_area.set_xlabel("Delay (min)")
            st.pyplot(fig_area)

    # --- SECTION 3: MAP ---
    st.divider()
    st.subheader("📍 Live Bus Map")
    if not df.empty:
        # On affiche le dernier point connu pour chaque bus
        df_map = df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
        st.map(df_map)
    else:
        st.warning("No data for the selected filters.")

else:
    st.error("No data found in Supabase. Check your scraper logs.")
