import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(
    page_title="TransLink Performance Dashboard",
    page_icon="🚌",
    layout="wide"
)

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"]
    )

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=0)
def load_dashboard_data():
    try:
        response = supabase.rpc("get_all_bus_positions").execute()

        df = pd.DataFrame(response.data)

        if not df.empty:
            df["recorded_time"] = pd.to_datetime(df["recorded_time"])

            if df["recorded_time"].dt.tz is None:
                df["recorded_time"] = df["recorded_time"].dt.tz_localize("UTC")

            df["recorded_time_local"] = df["recorded_time"].dt.tz_convert("America/Vancouver")

            df["hour_bucket"] = df["recorded_time_local"].dt.strftime("%Y-%m-%d %H:00")

            df["delay_min"] = df["delay_seconds"] / 60

            df = df[
                (df["latitude"] > 48.0) & (df["latitude"] < 50.0) &
                (df["longitude"] > -124.0) & (df["longitude"] < -122.0)
            ]

            return df

    except Exception as e:
        st.error(f"Error: {e}")

    return pd.DataFrame()


st.title("🚌 TransLink Performance Dashboard")

df = load_dashboard_data()

if not df.empty:

    # --- KPIs ---
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Buses On-Grid", df["vehicle_no"].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean() * 100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")

    route_stats = df.groupby("route_no")["delay_min"].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"R.{route_stats.idxmax()}" if not route_stats.empty else "N/A")

    area_stats = df.groupby("area_name")["delay_min"].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    custom_scale = [
        [0.0, "#006400"],   # dark green
        [0.25, "#00cc00"],  # green
        [0.5, "#ffffcc"],   # near zero
        [0.75, "#ff9900"],  # orange
        [1.0, "#cc0000"]    # dark red
    ]

    # --- MAP ---
    fig_map = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="delay_min",
        hover_name="area_name",
        zoom=10,
        mapbox_style="carto-positron",
        color_continuous_scale=custom_scale,
        color_continuous_midpoint=0
    )
    
    fig_map.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=500
    )

    st.plotly_chart(fig_map, use_container_width=True)

    # --- COLOR RANGE ---
    max_delay = df["delay_min"].max()
    min_delay = df["delay_min"].min()
    range_max = max(abs(max_delay), abs(min_delay))



    st.markdown("---")
    col1, col2 = st.columns(2)

    # --- CITY DELAYS ---
    with col1:
        st.subheader("🏙️ Top Delays by City")

        city_avg = (
            df[df["area_type"] == "municipality"]
            .groupby("area_name")["delay_min"]
            .mean()
            .reset_index()
            .sort_values("delay_min", ascending=True)
        )

        fig_city = px.bar(
            city_avg,
            x="delay_min",
            y="area_name",
            orientation="h",
            color="delay_min",
            color_continuous_scale=custom_scale,
            range_color=[-range_max, range_max],
            labels={
                "delay_min": "Avg Delay (min)",
                "area_name": "City"
            }
        )

        fig_city.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_city, use_container_width=True)

    # --- NEIGHBORHOOD DELAYS ---
    with col2:
        st.subheader("🏘️ Top Delays by Neighborhood")

        neigh_avg = (
            df[df["area_type"] == "neighborhood"]
            .groupby("area_name")["delay_min"]
            .mean()
            .reset_index()
            .sort_values("delay_min", ascending=True)
            .tail(15)
        )

        fig_neigh = px.bar(
            neigh_avg,
            x="delay_min",
            y="area_name",
            orientation="h",
            color="delay_min",
            color_continuous_scale=custom_scale,
            range_color=[-range_max, range_max],
            labels={
                "delay_min": "Avg Delay (min)",
                "area_name": "Neighborhood"
            }
        )

        fig_neigh.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HOURLY TREND ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")

    hourly_response = supabase.table("v_hourly_delay").select("*").execute()
    hourly_df = pd.DataFrame(hourly_response.data)
    
    if not hourly_df.empty:
        hourly_df["hour_vancouver"] = pd.to_datetime(hourly_df["hour_vancouver"])
    
    fig_line = px.line(
        hourly_df,
        x="hour_vancouver",
        y="avg_delay_min",
        markers=True,
        labels={
            "hour_vancouver": "Time (Vancouver)",
            "avg_delay_min": "Avg Delay (min)"
        },
        template="plotly_white"
    )
    
    fig_line.update_traces(line_width=3)
    
    fig_line.update_xaxes(
        dtick=3600000,
        tickformat="%H:%M"
    )
    
    st.plotly_chart(fig_line, use_container_width=True)
