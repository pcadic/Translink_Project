import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TransLink Network Intelligence", page_icon="🚌", layout="wide")

# --- SUPABASE CONNECTION ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- TIME OF DAY LOGIC ---
def get_time_category(dt):
    hour = dt.hour
    if 5 <= hour < 7: return "Early Morning"
    if 7 <= hour < 10: return "Morning Rush"
    if 10 <= hour < 15: return "Mid-Day"
    if 15 <= hour < 19: return "Evening Rush"
    if 19 <= hour < 23: return "Evening"
    return "Night"

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
        if len(data) < chunk_size: break
        offset += chunk_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        # Exclude off-map
        df = df[df['area_name'] != 'Off-Map'].copy()
        # Dédoublonnage
        df = df.drop_duplicates(subset=['vehicle_no', 'recorded_time'])
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
        df['time_of_day'] = df['recorded_time'].apply(get_time_category)
        
        # Fill nulls for display
        df['destination'] = df['destination'].fillna("Unknown")
        df['direction'] = df['direction'].fillna("N/A")
    return df

raw_df = load_data()

# --- SIDEBAR ---
st.sidebar.header("⚙️ Settings")
mode = st.sidebar.radio("Data Scope:", ["Real-Time (Last Run)", "Historical (Global)"])

if not raw_df.empty:
    if mode == "Real-Time (Last Run)":
        latest_ts = raw_df['recorded_time'].max()
        df_working = raw_df[raw_df['recorded_time'] == latest_ts].copy()
    else:
        df_working = raw_df.copy()

    # Neighborhoods from your GeoJSON logic
    all_areas = sorted(df_working['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Neighborhoods", options=all_areas, default=all_areas)
    df = df_working[df_working['area_name'].isin(selected_areas)].copy()

    st.title(f"📊 TransLink Analytics - {mode}")

    # --- RESTORED ALL KPIS ---
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    unique_buses = df['vehicle_no'].nunique()
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100
    avg_delay = df['delay_min'].mean()
    
    route_stats = df.groupby('route_no')['delay_min'].mean().sort_values(ascending=False)
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    
    area_stats = df.groupby('area_name')['delay_min'].mean()
    worst_area = area_stats.idxmax() if not area_stats.empty else "N/A"
    
    busiest_line = df['route_no'].value_counts().idxmax() if not df.empty else "N/A"

    col1.metric("Active Buses", unique_buses)
    col2.metric("Punctuality", f"{on_time:.1f}%")
    col3.metric("Avg Delay", f"{avg_delay:.1f} min")
    col4.metric("Slowest Route", f"R.{slowest_route}")
    col5.metric("Critical Zone", worst_area)
    col6.metric("Busiest Line", busiest_line)

    st.divider()

    # --- GRAPHS SECTION ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        top_10 = route_stats.head(10).reset_index()
        fig_r, ax_r = plt.subplots(figsize=(10, 6))
        # Gradient Reds
        pal = sns.color_palette("Reds_r", len(top_10))
        sns.barplot(data=top_10, x='delay_min', y='route_no', palette=pal, ax=ax_r)
        ax_r.set_xlabel("Avg Delay (min)")
        st.pyplot(fig_r)

    with c2:
        st.subheader("📈 Delay Trend by Time of Day")
        time_order = ["Early Morning", "Morning Rush", "Mid-Day", "Evening Rush", "Evening", "Night"]
        trend = df.groupby('time_of_day')['delay_min'].mean().reindex(time_order).reset_index()
        fig_t, ax_t = plt.subplots(figsize=(10, 6))
        # Use a line plot for trends
        sns.lineplot(data=trend, x='time_of_day', y='delay_min', marker="o", color="purple", ax=ax_t)
        ax_t.set_ylim(bottom=0)
        plt.xticks(rotation=45)
        st.pyplot(fig_t)

    # --- NEIGHBORHOOD PERFORMANCE (RESTORED) ---
    st.subheader("🏘️ Neighborhood Performance")
    plot_area = area_stats.sort_values().reset_index()
    fig_a, ax_a = plt.subplots(figsize=(12, 5))
    # Green to Red manual logic
    colors = ['#2ecc71' if x < 2 else '#e74c3c' for x in plot_area['delay_min']]
    ax_a.bar(plot_area['area_name'], plot_area['delay_min'], color=colors)
    plt.xticks(rotation=90)
    ax_a.set_ylabel("Delay (min)")
    st.pyplot(fig_a)

    # --- DATA TABLE (With Direction/Destination) ---
    st.subheader("📋 Detailed Route Logs")
    st.dataframe(df[['recorded_time', 'route_no', 'direction', 'destination', 'delay_min']].sort_values('delay_min', ascending=False).head(20), use_container_width=True)

    # --- MAP ---
    st.divider()
    st.subheader("📍 Live Network Map")
    st.map(df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no'))

else:
    st.error("No data available.")
