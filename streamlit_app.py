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
        # MANDATORY FIX: Increased limit to 10,000 to ensure 17h and 19h batches are both loaded.
        # This is why 19h was disappearing: Supabase defaults to 1,000 rows.
        response = supabase.table("bus_positions").select("*").limit(10000).execute()
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # Vancouver Timezone Logic
            if df['recorded_time'].dt.tz is None:
                df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC')
            df['recorded_time_local'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            
            df['hour'] = df['recorded_time_local'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            
            # GEOFENCE: Keep only Vancouver Frame
            df = df[(df['latitude'] > 48.0) & (df['latitude'] < 50.0) & 
                    (df['longitude'] > -124.0) & (df['longitude'] < -122.0)]
            
            return df
    except Exception as e:
        st.error(f"Error: {e}")
    return pd.DataFrame()

st.title("🚌 TransLink Performance Dashboard")

df = load_dashboard_data()

if not df.empty:
    # --- 5 KPIs (RESTORED EXACTLY) ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Buses On-Grid", df['vehicle_no'].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean()*100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"R.{route_stats.idxmax()}" if not route_stats.empty else "N/A")
    
    area_stats = df.groupby('area_name')['delay_min'].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    # --- MAP ---
    fig_map = px.scatter_mapbox(
        df, lat="latitude", lon="longitude", color="delay_min",
        hover_name="area_name", size_max=10, zoom=10,
        mapbox_style="carto-positron",
        color_continuous_scale="RdYlGn_r",
        color_continuous_midpoint=0, # FORCE GREEN FOR NEGATIVE
        range_color=[-3, 5] 
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- HORIZONTAL GRADIENT BARS (FIXED COLORS) ---
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏙️ Top Delays by City")
        city_avg = df[df['area_type'] == 'municipality'].groupby('area_name')['delay_min'].mean().sort_values(ascending=True)
        fig_city = px.bar(
            city_avg, orientation='h', color=city_avg.values,
            color_continuous_scale="RdYlGn_r", 
            color_continuous_midpoint=0, # FIXED: ANYTHING BELOW 0 IS GREEN
            labels={'value': 'Avg Delay (min)', 'area_name': 'City'}
        )
        fig_city.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_city, use_container_width=True)

    with col2:
        st.subheader("🏘️ Top Delays by Neighborhood")
        neigh_avg = df[df['area_type'] == 'neighborhood'].groupby('area_name')['delay_min'].mean().sort_values(ascending=True).tail(15)
        fig_neigh = px.bar(
            neigh_avg, orientation='h', color=neigh_avg.values,
            color_continuous_scale="RdYlGn_r", 
            color_continuous_midpoint=0, # FIXED: ANYTHING BELOW 0 IS GREEN
            labels={'value': 'Avg Delay (min)', 'area_name': 'Neighborhood'}
        )
        fig_neigh.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HOURLY TRENDS (FIXED: SHOWS 17H AND 19H) ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")
    
    # We aggregate by hour to ensure we see the progression from 17:00 to 19:00
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_line = px.line(
        hourly_trend, x='hour', y='delay_min', markers=True,
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    # Ensure the X axis shows the full day so the points aren't squashed
    fig_line.update_xaxes(range=[0, 23], dtick=1)
    fig_line.update_traces(line_color='#ef4444', line_width=3)
    st.plotly_chart(fig_line, use_container_width=True)

else:
    st.warning("No data found. Ensure your fetcher script is uploading to Supabase.")
