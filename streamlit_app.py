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
@st.cache_data(ttl=0) 
def load_dashboard_data():
    try:
        # FIX: Increased limit to 10,000 so the 19:00 batch (and all others) are visible
        response = supabase.table("bus_positions").select("*").limit(10000).execute()
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # Standardized Timezone Logic (UTC -> Vancouver)
            if df['recorded_time'].dt.tz is None:
                df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC')
            df['recorded_time_local'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            
            df['hour'] = df['recorded_time_local'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            
            # GEOFENCE: Keep only buses inside the Vancouver Frame
            df = df[(df['latitude'] > 48.0) & (df['latitude'] < 50.0) & 
                    (df['longitude'] > -124.0) & (df['longitude'] < -122.0)]
            
            return df
    except Exception as e:
        st.error(f"Error: {e}")
    return pd.DataFrame()

st.title("🚌 TransLink Performance Dashboard")

df = load_dashboard_data()

if not df.empty:
    # --- THE 5 KPIs (EXACTLY AS YOU COPIED THEM) ---
    c1, c2, c3, c4, c5 = st.columns(5)
    
    # KPI 1: Buses On-Grid
    c1.metric("Buses On-Grid", df['vehicle_no'].nunique())
    
    # KPI 2: Punctuality (Between -1 and 3 mins)
    punctuality = (df['delay_min'].between(-1, 3)).mean() * 100
    c2.metric("Punctuality", f"{punctuality:.1f}%")
    
    # KPI 3: Avg Delay
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    
    # KPI 4: Slowest Route
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"R.{route_stats.idxmax()}" if not route_stats.empty else "N/A")
    
    # KPI 5: Critical Zone
    area_stats = df.groupby('area_name')['delay_min'].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    # --- MAP (HISTORICAL POSITIONS - NO TITLE) ---
    fig_map = px.scatter_mapbox(
        df, lat="latitude", lon="longitude", color="delay_min",
        hover_name="area_name", size_max=10, zoom=10.5,
        mapbox_style="carto-positron",
        color_continuous_scale="RdYlGn_r",
        range_color=[-2, 5] 
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- HORIZONTAL GRADIENT BARS (RED/GREEN) ---
    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🏙️ Top Delays by City")
        city_avg = df[df['area_type'] == 'municipality'].groupby('area_name')['delay_min'].mean().sort_values(ascending=True)
        fig_city = px.bar(
            city_avg, orientation='h', color=city_avg.values,
            color_continuous_scale="RdYlGn_r", color_continuous_midpoint=0,
            labels={'value': 'Avg Delay (min)', 'area_name': 'City'}
        )
        fig_city.update_layout(showlegend=False, coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_city, use_container_width=True)

    with col_right:
        st.subheader("🏘️ Top Delays by Neighborhood")
        # Tail 15 used to show the highest delays (the "worst" neighborhoods)
        neigh_avg = df[df['area_type'] == 'neighborhood'].groupby('area_name')['delay_min'].mean().sort_values(ascending=True).tail(15)
        fig_neigh = px.bar(
            neigh_avg, orientation='h', color=neigh_avg.values,
            color_continuous_scale="RdYlGn_r", color_continuous_midpoint=0,
            labels={'value': 'Avg Delay (min)', 'area_name': 'Neighborhood'}
        )
        fig_neigh.update_layout(showlegend=False, coloraxis_showscale=False, yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HOURLY TRENDS (FIXED FOR 17:00 AND 19:00 BATCHES) ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_line = px.line(
        hourly_trend, x='hour', y='delay_min', markers=True,
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    fig_line.update_xaxes(range=[0, 23], dtick=1)
    fig_line.update_traces(line_color='#ef4444', line_width=3)
    st.plotly_chart(fig_line, use_container_width=True)

    # FOOTER
    st.caption(f"Historical analysis of {len(df)} positions captured between {df['recorded_time_local'].min().strftime('%H:%M')} and {df['recorded_time_local'].max().strftime('%H:%M')}")

else:
    st.warning("Database empty. Run the fetcher script to load your batches.")
