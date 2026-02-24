import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="TransLink Real-Time", page_icon="🚌", layout="wide")

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING (LATEST RUN VIA VIEW) ---
@st.cache_data(ttl=60)
def load_latest_data():
    """Fetch only the most recent position for each vehicle using the SQL View."""
    try:
        response = supabase.table("v_latest_bus_locations").select("*").limit(2000).execute()
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df["recorded_time"] = pd.to_datetime(df["recorded_time"])
            df["delay_min"] = df["delay_seconds"] / 60
            # Coordinates filter for Greater Vancouver
            df = df[(df["latitude"] > 48.0) & (df["latitude"] < 50.0) & 
                    (df["longitude"] > -124.0) & (df["longitude"] < -122.0)]
            return df
    except Exception as e:
        st.error(f"Error loading live data: {e}")
    return pd.DataFrame()

# --- HEADER SECTION ---
st.title("⏱️ TransLink - Latest Run Status")
st.markdown("This dashboard reflects the **current state** of the network from the most recent data fetch.")

df = load_latest_data()

if not df.empty:
    # --- TIME METADATA ---
    last_update = df["recorded_time"].max().strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"Last data synchronization: **{last_update}** (Vancouver Time)")

    # --- TOP KPIs SECTION (5 COLUMNS) ---
    st.markdown("### 📊 System Performance Overview")
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    
    kpi1.metric("Buses On-Grid", df["vehicle_no"].nunique())
    
    punctuality = (df["delay_min"].between(-1, 3)).mean() * 100
    kpi2.metric("Punctuality", f"{punctuality:.1f}%")
    
    kpi3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    
    route_delays = df.groupby("route_short_name")["delay_min"].mean()
    if not route_delays.empty:
        kpi4.metric("Slowest Route", f"Line {route_delays.idxmax()}")
    
    area_delays = df.groupby("area_name")["delay_min"].mean()
    if not area_delays.empty:
        kpi5.metric("Critical Zone", area_delays.idxmax())

    # --- GEOGRAPHICAL FILTERS ---
    st.markdown("---")
    st.subheader("📍 Live Vehicle Map")
    
    all_routes = sorted(df["route_short_name"].dropna().unique(),
                        key=lambda x: int(x) if str(x).isdigit() else 999)
    
    selected_route = st.selectbox("Select Bus Line (Short Name)", ["All Routes"] + list(all_routes))

    df_map = df if selected_route == "All Routes" else df[df["route_short_name"] == selected_route]

    # --- MAP VISUALIZATION ---
    delay_color_scale = [
        [0.0, "#006400"], [0.2, "#00cc00"], [0.4, "#ffff00"], 
        [0.6, "#ff9900"], [1.0, "#cc0000"]
    ]

    fig_map = px.scatter_mapbox(
        df_map, lat="latitude", lon="longitude", color="delay_min",
        hover_name="route_short_name", 
        hover_data={"route_long_name": True, "direction_name": True, "delay_min": ":.2f"},
        color_continuous_scale=delay_color_scale,
        range_color=[-2, 10],
        zoom=10, mapbox_style="open-street-map"
    )
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- GEOGRAPHICAL ANALYSIS (AS IN ORIGINAL FILE) ---
    st.markdown("---")
    st.subheader("🏙️ Local Delay Analysis")
    col_city, col_neigh = st.columns(2)

    with col_city:
        st.markdown("**Avg Delay by Municipality**")
        # Keep original format: grouping by municipality, calculating mean, sorting
        city_stats = df_map.groupby("municipality")["delay_min"].mean().sort_values(ascending=True)
        fig_city = px.bar(
            city_stats, orientation='h',
            color=city_stats.values, # Dynamic color based on delay
            color_continuous_scale="Reds",
            labels={"value": "Avg Delay (min)", "municipality": "City"}
        )
        fig_city.update_layout(showlegend=False, height=400, coloraxis_showscale=False)
        st.plotly_chart(fig_city, use_container_width=True)

    with col_neigh:
        st.markdown("**Avg Delay by Neighborhood**")
        # Keep original format: filtering neighborhoods, grouping by area_name
        neigh_stats = df_map[df_map["area_type"] == "neighborhood"].groupby("area_name")["delay_min"].mean().sort_values(ascending=True).tail(10)
        fig_neigh = px.bar(
            neigh_stats, orientation='h',
            color=neigh_stats.values,
            color_continuous_scale="Oranges",
            labels={"value": "Avg Delay (min)", "area_name": "Neighborhood"}
        )
        fig_neigh.update_layout(showlegend=False, height=400, coloraxis_showscale=False)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- DISTRIBUTION ---
    st.markdown("---")
    st.subheader("📈 Current Delay Distribution")
    fig_hist = px.histogram(df_map, x="delay_min", nbins=50, color_discrete_sequence=["#00cc00"])
    st.plotly_chart(fig_hist, use_container_width=True)

else:
    st.error("No data found. Check your SQL View and fetch script.")
