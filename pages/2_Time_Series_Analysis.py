import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="Time Analysis - TransLink", layout="wide", page_icon="⏳")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=0) # TTL=0 forces a fresh pull every refresh to see your manual runs
def load_temporal_data():
    try:
        # Fetching all records without limit to ensure historical and new data mix
        response = supabase.table("bus_positions").select("*").execute()
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            # 1. Convert to datetime objects
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # 2. THE CRITICAL FIX: Robust Timezone Conversion
            # We first ensure Pandas knows it's UTC, then convert to Vancouver (PST/PDT)
            if df['recorded_time'].dt.tz is None:
                df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC')
            
            df['recorded_time'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            
            # 3. Create helper columns for visualization
            df['hour'] = df['recorded_time'].dt.hour
            df['delay_min'] = df['delay_seconds'] / 60
            
            # Clean outliers (optional but recommended for clean graphs)
            df = df[df['delay_min'] < 60] 
            
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()

# --- MAIN INTERFACE ---
st.title("⏳ Temporal Performance Trends")
st.markdown("Analyze transit reliability by comparing delays across different hours of the day.")

df = load_temporal_data()

# --- 🚨 DIAGNOSTIC SECTION ---
if not df.empty:
    with st.expander("🛠️ Debug: Latest Database Records"):
        # Sort by actual timestamp to see if your 16h run exists
        st.dataframe(
            df[['recorded_time', 'hour', 'area_name', 'delay_min']]
            .sort_values('recorded_time', ascending=False)
            .head(10)
        )

# --- VISUALIZATIONS ---
if not df.empty:
    # 1. GLOBAL SYSTEM TREND
    st.subheader("🏙️ City-Wide Hourly Delay")
    
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_global = px.line(
        hourly_avg, x='hour', y='delay_min',
        markers=True,
        title="Average System Delay (Vancouver Local Time)",
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        template="plotly_white",
        color_discrete_sequence=['#ff4b4b']
    )
    # Force X-axis to show the full day 0-23
    fig_global.update_xaxes(range=[0, 23], dtick=1)
    st.plotly_chart(fig_global, use_container_width=True)

    # 2. NEIGHBORHOOD COMPARISON
    st.markdown("---")
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("🏘️ Filter Areas")
        all_areas = sorted(df['area_name'].unique())
        selected_areas = st.multiselect(
            "Select neighborhoods:", 
            options=all_areas, 
            default=all_areas[:3] if len(all_areas) > 3 else all_areas
        )

    with col2:
        if selected_areas:
            neigh_df = df[df['area_name'].isin(selected_areas)]
            neigh_hourly = neigh_df.groupby(['hour', 'area_name'])['delay_min'].mean().reset_index()
            
            fig_neigh = px.line(
                neigh_hourly, x='hour', y='delay_min', color='area_name',
                markers=True,
                title="Neighborhood Comparison",
                labels={'hour': 'Hour of Day', 'delay_min': 'Avg Delay (min)'},
                template="plotly_white"
            )
            fig_neigh.update_xaxes(range=[0, 23], dtick=1)
            st.plotly_chart(fig_neigh, use_container_width=True)

    # 3. HEATMAP INTENSITY
    st.markdown("---")
    st.subheader("🔥 Delay Intensity Heatmap")
    
    # Pivot for Heatmap
    pivot_df = df.pivot_table(index='area_name', columns='hour', values='delay_min', aggfunc='mean')
    
    # Ensure all 24 hours exist in columns to prevent axis jumping
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

    # FOOTER
    st.info(f"📊 Analyzing {len(df):,} positions across {len(hourly_avg)} unique hours.")

else:
    st.warning("No data found. Ensure your fetcher script is running correctly.")
