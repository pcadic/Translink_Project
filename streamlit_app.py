import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TransLink Route Intelligence", page_icon="🚌", layout="wide")

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
        # Fetching all available fields: Direction, Destination, Pattern, etc.
        response = supabase.table("bus_positions").select("*").range(offset, offset + chunk_size - 1).execute()
        data = response.data
        all_data.extend(data)
        if len(data) < chunk_size: break
        offset += chunk_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df[df['area_name'] != 'Off-Map'].copy()
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
        # Create Time Category
        df['time_of_day'] = df['recorded_time'].apply(get_time_category)
    return df

raw_df = load_data()

# --- SIDEBAR FILTERS ---
st.sidebar.header("🔍 Route & Time Filters")
if not raw_df.empty:
    time_order = ["Early Morning", "Morning Rush", "Mid-Day", "Evening Rush", "Evening", "Night"]
    selected_times = st.sidebar.multiselect("Time of Day", options=time_order, default=time_order)
    
    df = raw_df[raw_df['time_of_day'].isin(selected_times)].copy()
    
    # KPIs
    st.title("🚌 TransLink Network Intelligence")
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Busiest Line", df['route_no'].value_counts().idxmax())
    k2.metric("Avg System Delay", f"{df['delay_min'].mean():.1f} min")
    k3.metric("Fleet Density", f"{len(df)} Active Units")
    k4.metric("Punctuality", f"{(df['delay_min'] < 3).mean()*100:.1f}%")

    st.divider()

    # --- SECTION 1: TRENDS ---
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("📈 Delay Trend by Time of Day")
        trend_data = df.groupby('time_of_day')['delay_min'].mean().reindex(time_order).dropna().reset_index()
        fig_trend, ax_trend = plt.subplots(figsize=(10, 6))
        sns.lineplot(data=trend_data, x='time_of_day', y='delay_min', marker="o", color="#ff9100", ax=ax_trend)
        ax_trend.set_ylabel("Avg Delay (min)")
        st.pyplot(fig_trend)

    with c2:
        st.subheader("🏆 Top 10 Busiest Lines (Density)")
        # Busiest = Most buses active at once
        busiest = df['route_no'].value_counts().head(10).reset_index()
        busiest.columns = ['Route', 'Number of Buses']
        fig_bus, ax_bus = plt.subplots(figsize=(10, 6))
        sns.barplot(data=busiest, x='Number of Buses', y='Route', palette="Blues_r", ax=ax_bus)
        st.pyplot(fig_bus)

    # --- SECTION 2: ROUTE PERFORMANCE DETAILS ---
    st.divider()
    st.subheader("📋 Route Performance Deep Dive")
    # Grouping by route to see destination and pattern
    route_perf = df.groupby(['route_no', 'destination']).agg({
        'delay_min': 'mean',
        'vehicle_no': 'count'
    }).reset_index().sort_values('delay_min', ascending=False).head(15)
    
    route_perf.columns = ['Route', 'Destination', 'Avg Delay (min)', 'Active Buses']
    st.dataframe(route_perf, use_container_width=True)

    # --- SECTION 3: MAP ---
    st.divider()
    st.subheader("📍 Live Network Map")
    # Show direction on map labels if possible
    st.map(df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no'))

else:
    st.error("Data connection failed.")
