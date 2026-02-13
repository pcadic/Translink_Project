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
    all_data = []
    chunk_size = 1000
    offset = 0
    while True:
        response = supabase.table("bus_positions").select("*").range(offset, offset + chunk_size - 1).execute()
        data = response.data
        all_data.extend(data)
        if len(data) < chunk_size:
            break
        offset += chunk_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df[df['area_name'] != 'Off-Map'].copy()
        df = df.drop_duplicates(subset=['vehicle_no', 'recorded_time'])
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

    st.title(f"📊 TransLink Analytics - {mode}")

    # --- SECTION 1: KPIs ---
    col1, col2, col3, col4, col5 = st.columns(5)
    
    unique_buses = df['vehicle_no'].nunique()
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100
    avg_delay = df['delay_min'].mean()
    
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    area_stats = df.groupby('area_name')['delay_min'].mean()
    worst_area = area_stats.idxmax() if not area_stats.empty else "N/A"

    col1.metric("Buses On-Grid", unique_buses)
    col2.metric("Punctuality", f"{on_time:.1f}%")
    col3.metric("Avg Delay", f"{avg_delay:.2f} min")
    col4.metric("Slowest Route", f"R.{slowest_route}")
    col5.metric("Critical Zone", worst_area)

    st.divider()

    # --- SECTION 2: DELAY CHARTS ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        top_10_routes = route_stats.head(10).reset_index()
        fig_route, ax_route = plt.subplots(figsize=(10, 7))
        # Gradient colors: more delay = darker red
        colors_route = [plt.cm.Reds(0.4 + 0.5 * (i/10)) for i in range(10)][::-1]
        ax_route.barh(top_10_routes['route_no'].astype(str), top_10_routes['delay_min'], color=colors_route)
        ax_route.set_xlabel("Average Delay (min)")
        ax_route.invert_yaxis() # Highest delay on top
        st.pyplot(fig_route)

    with c2:
        st.subheader("🏘️ Delay by Neighborhood")
        plot_data = area_stats.sort_values().reset_index()
        def get_color(val, min_val, max_val):
            if val < 0:
                mag = val / min_val if min_val < 0 else 0
                return mcolors.to_hex(plt.cm.Greens(0.3 + 0.6 * mag))
            else:
                mag = val / max_val if max_val > 0 else 0
                return mcolors.to_hex(plt.cm.Reds(0.3 + 0.6 * mag))
        min_d, max_d = plot_data['delay_min'].min(), plot_data['delay_min'].max()
        bar_colors = [get_color(x, min_d, max_d) for x in plot_data['delay_min']]
        fig_area, ax_area = plt.subplots(figsize=(10, 7))
        ax_area.barh(plot_data['area_name'], plot_data['delay_min'], color=bar_colors)
        ax_area.axvline(0, color='black', linewidth=1.5)
        ax_area.set_xlabel("Delay (min)")
        st.pyplot(fig_area)

    st.divider()

    # --- SECTION 3: OCCUPANCY (CROWDING) ---
    st.subheader("👥 Fleet Occupancy Status")
    
    # Check if occupancy data exists (TransLink uses 'LoadQuotas' or occupancy labels in GTFS-RT)
    if 'occupancy' in df.columns and not df['occupancy'].isnull().all():
        occ_counts = df['occupancy'].value_counts()
        fig_occ, ax_occ = plt.subplots(figsize=(8, 4))
        sns.barplot(x=occ_counts.index, y=occ_counts.values, palette="viridis", ax=ax_occ)
        ax_occ.set_ylabel("Number of Buses")
        st.pyplot(fig_occ)
    else:
        st.info("💡 Live occupancy data is not currently available in the feed. Most TransLink buses transmit this via APC (Automatic Passenger Counters) which is often filtered in basic API tiers.")

    # --- SECTION 4: MAP ---
    st.divider()
    st.subheader("📍 Live Bus Map")
    df_map = df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
    st.map(df_map)

else:
    st.error("No data available.")
