import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_ROLE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=60)
def load_latest_data():
    try:
        response = supabase.table("v_latest_bus_locations").select("*").limit(2000).execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df["recorded_time"] = pd.to_datetime(df["recorded_time"])
            df["delay_min"] = df["delay_seconds"] / 60
            df = df[(df["latitude"] > 48.0) & (df["latitude"] < 50.0) & 
                    (df["longitude"] > -124.0) & (df["longitude"] < -122.0)]
            return df
    except Exception as e:
        st.error(f"Error: {e}")
    return pd.DataFrame()

# --- EXACT ZERO-CENTERED COLOR SCALE ---
# 0.0 -> Negative (Green)
# 0.5 -> Exactly 0 min delay (Yellow)
# 1.0 -> Positive (Red)
color_scale = [
    [0.0, "green"],
    [0.5, "yellow"],
    [1.0, "red"]
]

# Symmetric range ensures 0.0 is placed exactly at scale index 0.5
symmetric_range = [-10, 10]

st.title("🚌 TransLink Real-Time Status")

df = load_latest_data()

if not df.empty:
    # --- KPIs ---
    st.markdown("### Global Metrics")
    c1, c2, c3, c4, c5 = st.columns(5)
    
    c1.metric("Buses On-Grid", df["vehicle_no"].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean()*100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    
    route_stats = df.groupby("route_short_name")["delay_min"].mean()
    c4.metric("Slowest Route", f"Line {route_stats.idxmax()}")
    
    area_stats = df.groupby("area_name")["delay_min"].mean()
    c5.metric("Critical Zone", area_stats.idxmax())

    # --- FILTRE ET CARTE ---
    st.markdown("---")
    routes = sorted(df["route_short_name"].unique(), key=lambda x: str(x))
    sel_route = st.selectbox("Select Route (Short Name)", ["All Routes"] + list(routes))

    df_map = df if sel_route == "All Routes" else df[df["route_short_name"] == sel_route]

    fig_map = px.scatter_map(
        df_map, lat="latitude", lon="longitude", color="delay_min",
        hover_name="route_short_name", 
        hover_data=["route_long_name", "delay_min"],
        color_continuous_scale=color_scale,
        range_color=symmetric_range,
        zoom=9.2, 
        center={"lat": 49.2200, "lon": -122.9500},
        map_style="open-street-map"
    )
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- ANALYSE GEOGRAPHIQUE ---
    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Delays by Municipality")
        city_res = df_map.groupby("municipality")["delay_min"].mean().sort_values()
        fig_city = px.bar(
            city_res, orientation='h', 
            color=city_res.values,
            color_continuous_scale=color_scale,
            range_color=symmetric_range
        )
        fig_city.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_city, use_container_width=True)

    with col_b:
        st.subheader("Delays by Neighborhood")
        neigh_res = df_map[df_map["area_type"] == "neighborhood"].groupby("area_name")["delay_min"].mean().sort_values().tail(10)
        fig_neigh = px.bar(
            neigh_res, orientation='h',
            color=neigh_res.values,
            color_continuous_scale=color_scale,
            range_color=symmetric_range
        )
        fig_neigh.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HISTOGRAMME ---
    st.subheader("Delay Distribution")
    fig_hist = px.histogram(df_map, x="delay_min", nbins=50, color_discrete_sequence=["blue"])
    st.plotly_chart(fig_hist, use_container_width=True)

else:
    st.warning("No data available.")
