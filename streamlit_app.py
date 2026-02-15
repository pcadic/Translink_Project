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

# --- CHARGEMENT ---
@st.cache_data(ttl=300)
def load_data():
    response = supabase.table("bus_positions").select("*").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
        # Conversion Heure Locale (Vancouver)
        df['recorded_time_local'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
    return df

raw_df = load_data()

# --- SIDEBAR & FILTRES ---
st.sidebar.header("⚙️ Filters")
if not raw_df.empty:
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
    
    area_stats = df.groupby('area_name')['delay_min'].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    st.markdown("---")

    # --- SECTION 2: ROUTES & CITIES ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        top_routes = route_stats.head(10).reset_index()
        fig1, ax1 = plt.subplots(figsize=(10, 6))
        colors = [plt.cm.Reds(0.4 + 0.5 * (i/10)) for i in range(len(top_routes))][::-1]
        ax1.barh(top_routes['route_no'].astype(str), top_routes['delay_min'], color=colors)
        ax1.invert_yaxis()
        st.pyplot(fig1)

    with col_b:
        st.subheader("🏙️ Delay by City")
        city_avg = df.groupby('municipality')['delay_min'].mean().sort_values().reset_index()
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        ax2.barh(city_avg['municipality'], city_avg['delay_min'], color='skyblue')
        st.pyplot(fig2)

    # --- SECTION 3: TEMPOREL (HEURE LOCALE) ---
    st.markdown("---")
    st.subheader("⏰ Hourly Delay Trends (Vancouver Time)")
    df['hour'] = df['recorded_time_local'].dt.hour
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig3, ax3 = plt.subplots(figsize=(12, 4))
    ax3.plot(hourly_trend['hour'], hourly_trend['delay_min'], marker='o', color='#1f77b4')
    ax3.set_xticks(range(0, 24))
    ax3.set_xlim(-0.5, 23.5)
    ax3.grid(True, alpha=0.2)
    st.pyplot(fig3)

    # --- SECTION 4: MAPS ---
    st.markdown("---")
    st.subheader("📍 Real-Time Bus Positions")
    st.map(df[['latitude', 'longitude']])

    st.subheader("🔥 Congestion Heatmap")
    fig_heat = px.density_mapbox(df, lat='latitude', lon='longitude', z='delay_min', 
                                 radius=15, center=dict(lat=49.25, lon=-123.12), zoom=9,
                                 mapbox_style="carto-positron", height=600)
    st.plotly_chart(fig_heat, use_container_width=True)

    # --- SECTION 5: NEIGHBORHOODS ---
    st.markdown("---")
    st.subheader("🏘️ Delay by Neighborhood")
    plot_data = area_stats.sort_values().reset_index()
    fig4, ax4 = plt.subplots(figsize=(12, 8))
    ax4.barh(plot_data['area_name'], plot_data['delay_min'], color='salmon')
    st.pyplot(fig4)

else:
    st.warning("No data found.")
