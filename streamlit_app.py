import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import plotly.express as px # Pour la heatmap
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="Metro Vancouver Transit GIS", page_icon="🚌", layout="wide")

@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

@st.cache_data(ttl=300)
def load_data():
    # Récupération globale
    response = supabase.table("bus_positions").select("*").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
        # On filtre les données aberrantes ou hors carte
        df = df[df['municipality'] != 'Off-Map'].copy()
    return df

df = load_data()

st.title("🚌 Metro Vancouver Transit Performance (GIS Analysis)")

if not df.empty:
    # --- FILTRES ---
    st.sidebar.header("🔍 Filters")
    selected_cities = st.sidebar.multiselect("Select Cities", options=sorted(df['municipality'].unique()), default=df['municipality'].unique())
    df_filtered = df[df['municipality'].isin(selected_cities)]

    # --- SECTION 1: KPIs ---
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Total Observations", len(df_filtered))
    kpi2.metric("Avg Delay (Global)", f"{df_filtered['delay_min'].mean():.2f} min")
    kpi3.metric("Most Delayed City", df_filtered.groupby('municipality')['delay_min'].mean().idxmax())
    kpi4.metric("Active Routes", df_filtered['route_no'].nunique())

    st.divider()

    # --- SECTION 2: ANALYSE PAR VILLE ET QUARTIER ---
    col_v, col_q = st.columns(2)

    with col_v:
        st.subheader("🏙️ Average Delay by City")
        city_stats = df_filtered.groupby('municipality')['delay_min'].mean().sort_values()
        fig_city, ax_city = plt.subplots()
        city_stats.plot(kind='barh', color='skyblue', ax=ax_city)
        ax_city.set_xlabel("Minutes")
        st.pyplot(fig_city)

    with col_q:
        st.subheader("🏘️ Average Delay by Neighborhood")
        # Top 15 quartiers les plus lents pour la lisibilité
        neigh_stats = df_filtered.groupby('area_name')['delay_min'].mean().sort_values(ascending=False).head(15)
        fig_q, ax_q = plt.subplots()
        neigh_stats.plot(kind='barh', color='salmon', ax=ax_q)
        ax_q.invert_yaxis()
        st.pyplot(fig_q)

    # --- SECTION 3: TOP 10 ROUTES ---
    st.divider()
    st.subheader("🚩 Top 10 Most Delayed Routes")
    route_stats = df_filtered.groupby('route_no')['delay_min'].mean().sort_values(ascending=False).head(10)
    st.bar_chart(route_stats)

    # --- SECTION 4: MAPS (GIS FOCUS) ---
    st.divider()
    m1, m2 = st.columns(2)

    with m1:
        st.subheader("📍 Live/Recent Bus Positions")
        # On affiche les points réels
        st.map(df_filtered[['latitude', 'longitude']])

    with m2:
        st.subheader("🔥 Congestion Heatmap (Delay Zones)")
        # Heatmap basée sur l'intensité des retards
        # Plus le point est rouge/foncé, plus le retard cumulé est important
        fig_heat = px.density_mapbox(df_filtered, 
                                     lat='latitude', lon='longitude', z='delay_min', 
                                     radius=15, center=dict(lat=49.25, lon=-123.12), zoom=9,
                                     mapbox_style="carto-positron",
                                     title="Spatial Delay Intensity")
        st.plotly_chart(fig_heat, use_container_width=True)

else:
    st.warning("No data available.")
