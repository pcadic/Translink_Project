import st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="TransLink Live Dashboard", page_icon="🚌", layout="wide")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=0) # Set to 0 to see your manual runs immediately
def load_data():
    try:
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
            
            # FIX: Remove "South Africa" / Invalid coordinates
            # Vancouver is roughly Lat 49, Lon -123
            df = df[(df['latitude'] > 45) & (df['latitude'] < 55) & 
                    (df['longitude'] > -130) & (df['longitude'] < -120)]
            
            # FIX: Filter out the "City" level if a "Neighborhood" level exists 
            # This prevents 'Vancouver' from being compared to 'Marpole'
            df = df[df['area_type'] == 'neighborhood']
            
            return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()
    return pd.DataFrame()

st.title("🚌 TransLink Real-Time Performance")

df = load_data()

if not df.empty:
    # --- METRICS BAR ---
    avg_delay = df['delay_min'].mean()
    total_buses = len(df)
    worst_area = df.groupby('area_name')['delay_min'].mean().idxmax()

    m1, m2, m3 = st.columns(3)
    m1.metric("Buses Tracked", total_buses)
    m2.metric("Avg System Delay", f"{avg_delay:.1f} min")
    m3.metric("Worst Neighborhood", worst_area)

    # --- MAP & CHART ---
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📍 Live Bus Positions")
        fig_map = px.scatter_mapbox(
            df, lat="latitude", lon="longitude", color="delay_min",
            hover_name="area_name", size_max=15, zoom=10,
            mapbox_style="carto-positron",
            color_continuous_scale="RdYlGn_r",
            title="Real-time Delay Distribution"
        )
        st.plotly_chart(fig_map, use_container_width=True)

    with col2:
        st.subheader("📊 Top Delays by Neighborhood")
        # Aggregating by neighborhood
        neigh_avg = df.groupby('area_name')['delay_min'].mean().sort_values(ascending=False).head(15)
        st.bar_chart(neigh_avg)

    # --- HOURLY TRENDS ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_line = px.line(
        hourly_trend, x='hour', y='delay_min', markers=True,
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    fig_line.update_xaxes(range=[0, 23], dtick=1)
    st.plotly_chart(fig_line, use_container_width=True)
    
    st.write(f"ℹ️ Currently showing data from {df['recorded_time_local'].min().strftime('%H:%M')} to {df['recorded_time_local'].max().strftime('%H:%M')}")

else:
    st.warning("No data found. Please run your fetch script and clear the database rows first if starting over.")
