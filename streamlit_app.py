import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- SUPABASE CONNECTION ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=300)
def load_data():
    # Increase limit to bypass Supabase default 1000 rows
    response = supabase.table("bus_positions").select("*").limit(5000).execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
    return df

raw_df = load_data()

# --- SIDEBAR ---
st.sidebar.header("⚙️ Configuration")
mode = st.sidebar.radio("Display Mode:", ["Real-Time (Last Run)", "Historical (Global)"])

if not raw_df.empty:
    if mode == "Real-Time (Last Run)":
        latest_ts = raw_df['recorded_time'].max()
        df_working = raw_df[raw_df['recorded_time'] == latest_ts].copy()
    else:
        df_working = raw_df.copy()

    all_areas = sorted(df_working['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Neighborhoods", options=all_areas, default=all_areas)
    df = df_working[df_working['area_name'].isin(selected_areas)].copy()

    # --- MAIN TITLE ---
    st.title(f"📊 TransLink Analytics - {mode}")
    st.caption(f"Based on {len(df)} records")

    # --- SECTION 1: KPIs ---
    col1, col2, col3, col4, col5 = st.columns(5)
    
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100
    avg_delay = df['delay_min'].mean()
    
    route_stats = df.groupby('route_no')['delay_min'].mean()
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    
    area_stats = df[df['area_name'] != 'Off-Map'].groupby('area_name')['delay_min'].mean()
    worst_area = area_stats.idxmax() if not area_stats.empty else "N/A"

    col1.metric("Active Buses", len(df))
    col2.metric("Punctuality Rate", f"{on_time:.1f}%")
    col3.metric("Avg Delay", f"{avg_delay:.2f} min")
    col4.metric("Slowest Route", f"R.{slowest_route}")
    col5.metric("Critical Zone", worst_area)

    st.divider()

    # --- SECTION 2: CHARTS ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏘️ Average Delay by Neighborhood")
        plot_data = area_stats.sort_values().reset_index()
        
        # Color Gradient Logic
        def get_color(val, min_val, max_val):
            if val < 0: # Green Gradient for early/on-time
                # Scale between 0 and 1 based on negative values
                mag = val / min_val if min_val < 0 else 0
                return mcolors.to_hex(plt.cm.Greens(0.3 + 0.6 * mag))
            else: # Red Gradient for delays
                mag = val / max_val if max_val > 0 else 0
                return mcolors.to_hex(plt.cm.Reds(0.3 + 0.6 * mag))

        min_d, max_d = plot_data['delay_min'].min(), plot_data['delay_min'].max()
        bar_colors = [get_color(x, min_d, max_d) for x in plot_data['delay_min']]
        
        fig, ax = plt.subplots(figsize=(10, 7))
        ax.barh(plot_data['area_name'], plot_data['delay_min'], color=bar_colors)
        ax.axvline(0, color='black', linewidth=1.5)
        ax.set_xlabel("Minutes (Green = Early | Red = Delayed)")
        st.pyplot(fig)

    with c2:
        st.subheader("📈 Delay Distribution")
        fig2, ax2 = plt.subplots(figsize=(10, 7))
        # Purple bars and Orange line
        sns.histplot(df['delay_min'], bins=20, kde=True, color="#6a1b9a", ax=ax2, 
                     line_kws={"color": "#ff9100", "lw": 3})
        ax2.axvline(0, color='red', linestyle='--')
        ax2.set_xlabel("Delay (min)")
        st.pyplot(fig2)

    # --- SECTION 3: MAP ---
    st.divider()
    st.subheader("📍 Live Bus Map")
    
    map_filter = st.radio(
        "Map Filter:", 
        ["All Buses", "Delayed (> 3 min)", "Early (> 1 min)"], 
        horizontal=True
    )
    
    df_map = df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
    
    if map_filter == "Delayed (> 3 min)":
        df_map = df_map[df_map['delay_min'] > 3]
    elif map_filter == "Early (> 1 min)":
        df_map = df_map[df_map['delay_min'] < -1]
    
    st.map(df_map)

else:
    st.error("No data available.")

st.caption("Data source: TransLink Open Data • Real-time analysis with gradient mapping.")
