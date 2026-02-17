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
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- FONCTION DE COLORATION HARMONISÉE ---
def get_color_gradient(values):
    """Génère un dégradé Vert (avance) -> Jaune (neutre) -> Rouge (retard)"""
    if len(values) == 0: return []
    norm = mcolors.TwoSlopeNorm(vcenter=0, vmin=min(min(values), -0.1), vmax=max(max(values), 2.0))
    return [plt.cm.RdYlGn_r(norm(val)) for val in values]

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=300)
def load_data():
    try:
        response = supabase.table("bus_positions").select("*").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            df['delay_min'] = df['delay_seconds'] / 60
            df['recorded_time_local'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
            return df
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()

raw_df = load_data()

# --- INTERFACE ---
if not raw_df.empty:
    # --- FILTRES (SIDEBAR) ---
    st.sidebar.header("⚙️ Filters")
    cities = sorted(raw_df['municipality'].unique())
    sel_cities = st.sidebar.multiselect("Cities", cities, default=cities)
    
    df_temp = raw_df[raw_df['municipality'].isin(sel_cities)]
    areas = sorted(df_temp['area_name'].unique())
    sel_areas = st.sidebar.multiselect("Neighborhoods", areas, default=areas)
    
    df = df_temp[df_temp['area_name'].isin(sel_areas)].copy()

    st.title("📊 TransLink Performance Dashboard")

    # --- SECTION 1: KPIs ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Buses On-Grid", df['vehicle_no'].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean()*100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"R.{route_stats.idxmax()}" if not route_stats.empty else "N/A")
    
    area_stats = df.groupby('area_name')['delay_min'].mean().sort_values(ascending=False)
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    st.markdown("---")

    # --- SECTION 2: TOP 10 ROUTES (LARGEUR TOTALE) ---
    st.subheader("🏆 Top 10 Most Delayed Routes")
    if not route_stats.empty:
        top_routes = route_stats.head(10).sort_values(ascending=True)
        fig_r, ax_r = plt.subplots(figsize=(15, 5))
        ax_r.barh(top_routes.index.astype(str), top_routes.values, color=get_color_gradient(top_routes.values))
        ax_r.set_xlabel("Delay (min)")
        st.pyplot(fig_r)

    st.markdown("---")

    # --- SECTION 3: CITIES & NEIGHBORHOODS (CÔTE À CÔTE) ---
    col_v, col_n = st.columns(2)

    with col_v:
        st.subheader("🏙️ Delay by City")
        city_avg = df.groupby('municipality')['delay_min'].mean().sort_values(ascending=True)
        if not city_avg.empty:
            fig_v, ax_v = plt.subplots(figsize=(10, 8))
            ax_v.barh(city_avg.index, city_avg.values, color=get_color_gradient(city_avg.values))
            ax_v.set_xlabel("Delay (min)")
            st.pyplot(fig_v)

    with col_n:
        st.subheader("🏘️ Delay by Neighborhood")
        top_neighborhoods = area_stats.head(20).sort_values(ascending=True)
        if not top_neighborhoods.empty:
            fig_n, ax_n = plt.subplots(figsize=(10, 8))
            ax_n.barh(top_neighborhoods.index, top_neighborhoods.values, color=get_color_gradient(top_neighborhoods.values))
            ax_n.axvline(0, color='black', linewidth=0.8)
            ax_n.set_xlabel("Delay (min)")
            st.pyplot(fig_n)

    # --- SECTION 4: TEMPOREL ---
    st.markdown("---")
    st.subheader("⏰ Hourly Delay Trends (Vancouver Time)")
    df['hour'] = df['recorded_time_local'].dt.hour
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    fig_t, ax_t = plt.subplots(figsize=(15, 4))
    ax_t.plot(hourly_trend['hour'], hourly_trend['delay_min'], marker='o', color='#1f77b4', linewidth=2)
    ax_t.set_xticks(range(0, 24))
    ax_t.grid(True, alpha=0.2)
    st.pyplot(fig_t)

    # --- SECTION 5: CARTES ---
    st.markdown("---")
    st.subheader("📍 Real-Time Bus Positions")
    st.map(df[['latitude', 'longitude']])

    st.subheader("🔥 Congestion Heatmap")
    fig_heat = px.density_mapbox(df, lat='latitude', lon='longitude', z='delay_min', 
                                 radius=15, center=dict(lat=49.25, lon=-123.12), zoom=9,
                                 mapbox_style="carto-positron", height=600)
    st.plotly_chart(fig_heat, use_container_width=True)

else:
    st.warning("No data found. Check your database or run the scraper.")
