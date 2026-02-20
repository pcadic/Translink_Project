import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIG ---
st.set_page_config(page_title="Time Analysis", layout="wide")

@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING (FORCED REFRESH) ---
@st.cache_data(ttl=0) 
def load_temporal_data():
    # We fetch more rows and sort by newest to ensure we see the 4 PM run
    response = supabase.table("bus_positions") \
        .select("recorded_time, delay_seconds, area_name") \
        .order("recorded_time", desc=True) \
        .limit(5000) \
        .execute()
    
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        
        # Explicit conversion: UTC -> Vancouver
        # We use 'coerce' and 'shift' logic to be safe
        try:
            df['recorded_time'] = df['recorded_time'].dt.tz_localize('UTC').dt.tz_convert('America/Vancouver')
        except:
            # If already localized, just convert
            df['recorded_time'] = df['recorded_time'].dt.tz_convert('America/Vancouver')
            
        df['hour'] = df['recorded_time'].dt.hour
        df['delay_min'] = df['delay_seconds'] / 60
        return df
    return pd.DataFrame()

st.title("⏳ Temporal Performance Trends")
df = load_temporal_data()

if not df.empty:
    # --- DIAGNOSTIC TABLE ---
    st.write("### 🔍 Data Check (Latest 3 records)")
    st.table(df[['recorded_time', 'hour', 'area_name']].head(3))

    # 1. GLOBAL TREND
    st.subheader("🏙️ City-Wide Hourly Delay")
    hourly_avg = df.groupby('hour')['delay_min'].mean().reset_index()
    fig1 = px.line(hourly_avg, x='hour', y='delay_min', markers=True, template="plotly_white")
    fig1.update_xaxes(range=[0, 23], dtick=1)
    st.plotly_chart(fig1, use_container_width=True)

    # 2. NEIGHBORHOOD COMPARISON (FIXED)
    st.markdown("---")
    st.subheader("🏘️ Neighborhood Deep Dive")
    
    selected_areas = st.multiselect("Select areas:", options=sorted(df['area_name'].unique()), default=sorted(df['area_name'].unique())[:3])
    
    if selected_areas:
        neigh_df = df[df['area_name'].isin(selected_areas)]
        # We group by hour AND area
        neigh_hourly = neigh_df.groupby(['hour', 'area_name'])['delay_min'].mean().reset_index()
        
        # We use markers=True so that single dots are visible even if no line connects them
        fig2 = px.line(
            neigh_hourly, x='hour', y='delay_min', color='area_name',
            markers=True, 
            title="Hourly Comparison",
            template="plotly_white"
        )
        fig2.update_xaxes(range=[0, 23], dtick=1)
        st.plotly_chart(fig2, use_container_width=True)

    # 3. HEATMAP
    st.markdown("---")
    st.subheader("🔥 Intensity Heatmap")
    pivot_df = df.pivot_table(index='area_name', columns='hour', values='delay_min', aggfunc='mean')
    fig3 = px.imshow(pivot_df, color_continuous_scale="Reds", aspect="auto")
    st.plotly_chart(fig3, use_container_width=True)

else:
    st.warning("No data found.")
