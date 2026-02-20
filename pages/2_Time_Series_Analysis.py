import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="Time Analysis - TransLink", layout="wide", page_icon="⏳")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
# Setting TTL to 0 to force a refresh every time while we debug
@st.cache_data(ttl=0) 
def load_temporal_data():
    try:
        # We fetch the latest data points first
        response = supabase.table("bus_positions") \
            .select("recorded_time, delay_seconds, area_name") \
            .order("recorded_time", desc=True) \
            .limit(2000) \
            .execute()
        
        df = pd.DataFrame(response.data)
        if not df.empty:
            # 1. Convert string to datetime
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # 2. Convert UTC (0:00 AM) to Vancouver (4:00 PM)
            # This handles the 8-hour gap automatically
            df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
            
            # 3. Extract correct local hour
            df['hour'] = df['recorded_time'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()

# --- MAIN LOGIC ---
st.title("⏳ Temporal Performance Trends")

df = load_temporal_data()

if not df.empty:
    # --- DEBUG SECTION (You can delete this once fixed) ---
    with st.expander("🛠️ Debug: Raw Data Check"):
        st.write("Last 5 entries fetched from Database (Vancouver Time):")
        st.write(df[['recorded_time', 'hour', 'area_name', 'delay_min']].head())

    # 1. GLOBAL TREND
    st.subheader("🏙️ City-Wide Hourly Delay")
    
    # We group by hour to see the 16h (4 PM) point
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_global = px.line(
        hourly_avg, x='hour', y='delay_min',
        labels={'hour': 'Hour of Day (Local Vancouver Time)', 'delay_min': 'Avg Delay (min)'},
        markers=True,
        template="plotly_white"
    )
    
    # Force the X-axis to show 0 to 23
    fig_global.update_layout(xaxis=dict(tickmode='linear', range=[0, 23]))
    st.plotly_chart(fig_global, use_container_width=True)

    # 2. NEIGHBORHOOD COMPARISON
    st.markdown("---")
    st.subheader("🏘️ Neighborhood Deep Dive")
    
    selected_areas = st.multiselect(
        "Compare neighborhoods:", 
        options=sorted(df['area_name'].unique()), 
        default=df['area_name'].unique()[:3]
    )

    if selected_areas:
        neigh_df = df[df['area_name'].isin(selected_areas)]
        neigh_hourly = neigh_df.groupby(['hour', 'area_name'])['delay_min'].mean().reset_index()
        
        fig_neigh = px.line(
            neigh_hourly, x='hour', y='delay_min', color='area_name',
            markers=True, template="plotly_white"
        )
        st.plotly_chart(fig_neigh, use_container_width=True)

else:
    st.warning("No data found. Check your Supabase connection.")
