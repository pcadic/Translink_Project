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
@st.cache_data(ttl=0) # Disabled cache to ensure your 4 PM run shows up immediately
def load_temporal_data():
    try:
        # Fetching all records to reconstruct the full history
        response = supabase.table("bus_positions") \
            .select("recorded_time, delay_seconds, area_name") \
            .execute()
        
        df = pd.DataFrame(response.data)
        if not df.empty:
            # 1. Parse timestamps
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # 2. Convert UTC to Vancouver Time (Fixes the 8-hour offset)
            # This ensures 00:00 UTC becomes 16:00 (4 PM) PST
            df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
            
            # 3. Create helper columns
            df['hour'] = df['recorded_time'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            
            # 4. Clean outliers (delays over 1 hour are usually GPS errors)
            df = df[df['delay_min'] < 60]
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()

# --- MAIN INTERFACE ---
st.title("⏳ Temporal Performance Trends")
st.markdown("This page tracks how bus delays change across different hours in Vancouver.")

df = load_temporal_data()
st.write("### 🚨 Latest 10 Records in Database")
if not df.empty:
    st.dataframe(df[['recorded_time', 'hour', 'area_name']].sort_values('recorded_time', ascending=False).head(10))
else:
    st.write("The dataframe is empty!")

if not df.empty:
    # --- GLOBAL SYSTEM TREND ---
    st.subheader("🏙️ City-Wide Hourly Delay")
    
    # Calculate average delay per hour
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_global = px.line(
        hourly_avg, x='hour', y='delay_min',
        markers=True,
        title="Average System Delay (Local Vancouver Time)",
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white"
    )
    # Force X-axis to show the full 24h day
    fig_global.update_xaxes(range=[0, 23], dtick=1)
    st.plotly_chart(fig_global, use_container_width=True)

    # --- NEIGHBORHOOD COMPARISON ---
    st.markdown("---")
    st.subheader("🏘️ Neighborhood Deep Dive")
    
    all_areas = sorted(df['area_name'].unique())
    selected_areas = st.multiselect(
        "Select neighborhoods to compare:", 
        options=all_areas, 
        default=all_areas[:3] if len(all_areas) > 3 else all_areas
    )

    if selected_areas:
        neigh_df = df[df['area_name'].isin(selected_areas)]
        neigh_hourly = neigh_df.groupby(['hour', 'area_name'])['delay_min'].mean().reset_index()
        
        fig_neigh = px.line(
            neigh_hourly, x='hour', y='delay_min', color='area_name',
            markers=True,
            title="Hourly Comparison",
            labels={'hour': 'Hour of Day', 'delay_min': 'Avg Delay (min)'},
            template="plotly_white"
        )
        fig_neigh.update_xaxes(range=[0, 23], dtick=1)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HEATMAP INTENSITY ---
    st.markdown("---")
    st.subheader("🔥 Delay Intensity Heatmap")
    
    # Create the matrix: Neighborhoods vs Hours
    pivot_df = df.pivot_table(index='area_name', columns='hour', values='delay_min', aggfunc='mean')
    
    # Ensure all 24 hours are represented as columns
    for h in range(24):
        if h not in pivot_df.columns:
            pivot_df[h] = None
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

    fig_heat = px.imshow(
        pivot_df,
        labels=dict(x="Hour of Day", y="Neighborhood", color="Delay (min)"),
        color_continuous_scale="Reds",
        aspect="auto"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # --- FOOTER INFO ---
    st.info(f"📊 **Data Status:** Currently analyzing {len(df):,} records across {len(hourly_avg)} distinct hours.")

else:
    st.warning("No data found. Please run your fetch script to populate the database.")
