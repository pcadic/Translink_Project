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
@st.cache_data(ttl=600)
def load_temporal_data():
    try:
        response = supabase.table("bus_positions").select("recorded_time, delay_seconds, area_name").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            # 1. Ensure we are working with datetime objects
            df['recorded_time'] = pd.to_datetime(df['recorded_time'])
            
            # 2. Most databases store in UTC. 
            # We localize to UTC first, then convert to Vancouver (PST/PDT)
            try:
                # If the data already has timezone info
                df['recorded_time'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            except TypeError:
                # If the data is "timezone naive" (common with some DB drivers)
                df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
            
            df['delay_min'] = df['delay_seconds'] / 60
            
            # 3. Extract the hour from the CONVERTED time
            df['hour'] = df['recorded_time'].dt.hour
            
            # Optional: Add a 'Time Label' for the hover tooltips (e.g., "5:00 PM")
            df['time_label'] = df['recorded_time'].dt.strftime('%I:%M %p')
            
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()



# --- MAIN LOGIC ---
st.title("⏳ Temporal Performance Trends")
st.markdown("Analyze how transit delays evolve throughout the day based on collected snapshots.")

df = load_temporal_data()

with st.expander("🛠️ Raw Data Check"):
    st.write(df[['area_name', 'recorded_time', 'hour']].tail(10))

if not df.empty:
    # 1. GLOBAL TREND (Line Chart)
    st.subheader("🏙️ City-Wide Hourly Delay")
    
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    
    fig_global = px.line(
        hourly_avg, x='hour', y='delay_min',
        title="Average System Delay by Hour",
        labels={'hour': 'Hour of Day (24h)', 'delay_min': 'Avg Delay (min)'},
        markers=True,
        template="plotly_white"
    )
    fig_global.update_traces(line_color='#1f77b4', line_width=3)
    st.plotly_chart(fig_global, use_container_width=True)

    # 2. NEIGHBORHOOD COMPARISON
    st.markdown("---")
    st.subheader("🏘️ Neighborhood Deep Dive")
    
    all_neighborhoods = sorted(df['area_name'].unique())
    selected_areas = st.multiselect(
        "Select neighborhoods to compare:", 
        options=all_neighborhoods, 
        default=all_neighborhoods[:3] if len(all_neighborhoods) > 3 else all_neighborhoods
    )

    if selected_areas:
        neigh_df = df[df['area_name'].isin(selected_areas)]
        # Group by hour and area
        neigh_hourly = neigh_df.groupby(['hour', 'area_name'])['delay_min'].mean().reset_index()
        
        fig_neigh = px.line(
            neigh_hourly, x='hour', y='delay_min', color='area_name',
            title="Hourly Comparison: Selected Areas",
            labels={'hour': 'Hour of Day', 'delay_min': 'Avg Delay (min)'},
            markers=True,
            template="plotly_white"
        )
        st.plotly_chart(fig_neigh, use_container_width=True)
    
    # 3. HEATMAP SUMMARY
    st.markdown("---")
    st.subheader("🔥 Delay Intensity Heatmap")
    
    # Pivot data for heatmap: Rows=Neighborhoods, Cols=Hours
    pivot_df = df.groupby(['area_name', 'hour'])['delay_min'].mean().unstack().fillna(0)
    
    # We only show top 15 most active neighborhoods to keep it readable
    top_areas = df['area_name'].value_counts().nlargest(15).index
    pivot_df = pivot_df.loc[pivot_df.index.isin(top_areas)]

    fig_heat = px.imshow(
        pivot_df,
        labels=dict(x="Hour of Day", y="Neighborhood", color="Delay (min)"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="Reds",
        aspect="auto"
    )
    st.plotly_chart(fig_heat, use_container_width=True)
    st.info("💡 **Insight:** Heatmaps help identify which neighborhoods 'wake up' first with traffic congestion.")

else:
    st.warning("No data points found. Please run the scraper at different times of the day.")
