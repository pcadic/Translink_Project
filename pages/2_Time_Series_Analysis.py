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
@st.cache_data(ttl=0) # No cache while we build the timeline
def load_temporal_data():
    try:
        response = supabase.table("bus_positions").select("recorded_time, delay_seconds, area_name").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            # Convert to Local Vancouver Time
            df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
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
    # 1. GLOBAL TREND
    st.subheader("🏙️ City-Wide Hourly Delay")
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_global = px.line(
        hourly_avg, x='hour', y='delay_min',
        markers=True, title="Average System Delay (Vancouver Time)",
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    fig_global.update_xaxes(range=[0, 23], dtick=1) # Keeps the 24h perspective
    st.plotly_chart(fig_global, use_container_width=True)

    # 2. HEATMAP (The "Intensity" Grid)
    st.markdown("---")
    st.subheader("🔥 Delay Intensity Heatmap")
    
    # We group by area and hour
    heatmap_data = df.groupby(['area_name', 'hour'])['delay_min'].mean().reset_index()
    
    # Pivot to create the grid
    pivot_df = heatmap_data.pivot(index='area_name', columns='hour', values='delay_min')
    
    # Fill missing hours with 0 so the heatmap stays visible
    for h in range(24):
        if h not in pivot_df.columns:
            pivot_df[h] = None # Use None so it shows as empty, not 0
            
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

    fig_heat = px.imshow(
        pivot_df,
        labels=dict(x="Hour of Day", y="Neighborhood", color="Delay (min)"),
        color_continuous_scale="Reds",
        title="Spatial-Temporal Delay Distribution"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    if len(hourly_avg) < 2:
        st.info("💡 **Tip:** You only have data for one specific hour. Run the scraper at different times (e.g., morning vs evening) to see the trend line connect!")

else:
    st.warning("No data found.")
