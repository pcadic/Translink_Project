import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=0) # ttl=0 ensures every refresh shows both your 17h and 19h batches
def load_data():
    try:
        # Fetching all records (No limits)
        response = supabase.table("bus_positions").select("*").execute()
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # Standardized Timezone Logic (UTC -> Vancouver)
            if df['recorded_time'].dt.tz is None:
                df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC')
            df['recorded_time_local'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            
            df['hour'] = df['recorded_time_local'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            
            # FIX: Geofence - Only keep buses in the Vancouver area (Removes South Africa)
            df = df[(df['latitude'] > 48.0) & (df['latitude'] < 50.0) & 
                    (df['longitude'] > -124.0) & (df['longitude'] < -122.0)]
            
            return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
    return pd.DataFrame()

st.title("🚌 TransLink Real-Time Bus Performance")

df = load_data()

if not df.empty:
    # --- METRICS BAR ---
    avg_delay = df['delay_min'].mean()
    total_records = len(df)
    
    # Calculate Worst Route
    route_stats = df.groupby('route_no')['delay_min'].mean()
    worst_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    worst_delay = route_stats.max() if not route_stats.empty else 0

    m1, m2, m3 = st.columns(3)
    m1.metric("Buses Positions Tracked", total_records)
    m2.metric("Avg System Delay", f"{avg_delay:.2f} min")
    m3.metric("Worst Delay Route", f"Route {worst_route} ({worst_delay:.1f} min)")

    # --- MAP (NO TITLE AS REQUESTED) ---
    # We display the real locations but without the "Real-Time" title
    fig_map = px.scatter_mapbox(
        df, lat="latitude", lon="longitude", color="delay_min",
        hover_name="area_name", size_max=12, zoom=10,
        mapbox_style="carto-positron",
        color_continuous_scale="RdYlGn_r",
        range_color=[-2, 5] 
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

    # --- BAR CHARTS (CITIES & NEIGHBORHOODS) ---
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏙️ Delay by City")
        # Municipality filter
        city_df = df[df['area_type'] == 'municipality']
        if not city_df.empty:
            city_avg = city_df.groupby('area_name')['delay_min'].mean().sort_values(ascending=False)
            st.bar_chart(city_avg)
        else:
            st.info("No city data available.")

    with col2:
        st.subheader("🏘️ Top Delays by Neighborhood")
        # Neighborhood filter
        neigh_df = df[df['area_type'] == 'neighborhood']
        if not neigh_df.empty:
            neigh_avg = neigh_df.groupby('area_name')['delay_min'].mean().sort_values(ascending=False).head(15)
            st.bar_chart(neigh_avg)
        else:
            st.info("No neighborhood data available.")

    # --- HOURLY TRENDS ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")
    
    # Aggregating by hour to show your 17h and 19h batches
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_line = px.line(
        hourly_trend, x='hour', y='delay_min', markers=True,
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    fig_line.update_xaxes(range=[0, 23], dtick=1)
    st.plotly_chart(fig_line, use_container_width=True)

    # FOOTER WITH TIME RANGE
    start_time = df['recorded_time_local'].min().strftime('%H:%M')
    end_time = df['recorded_time_local'].max().strftime('%H:%M')
    st.info(f"📊 Displaying historical performance data from {start_time} to {end_time} local time.")

else:
    st.warning("No data found. Please run your fetcher script to populate the database.")
