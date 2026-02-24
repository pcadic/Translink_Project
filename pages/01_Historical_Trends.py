import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Historical Trends", page_icon="📈", layout="wide")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- YOUR ORIGINAL COLOR SCALE ---
color_scale = [
    [0.0, "green"],
    [0.15, "green"],
    [0.2, "yellow"], # On-time at 0
    [0.25, "red"],
    [1.0, "red"]
]

st.title("📈 Global Performance Analytics")
st.markdown("This page analyzes historical patterns across all recorded runs to identify recurring bottlenecks.")

# --- 1. HOURLY TRENDS BY ROUTE ---
st.subheader("🚌 Route Performance over Time")

# Fetching from the hourly route view
r_res = supabase.table("v_route_hourly_delay").select("*").execute()
r_df = pd.DataFrame(r_res.data)

if not r_df.empty:
    r_df["hour_vancouver"] = pd.to_datetime(r_df["hour_vancouver"])
    
    # Filter by route_short_name (the improvement we made)
    routes = sorted(r_df["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Select Routes to Compare", routes, default=routes[:3])
    
    f_route = r_df[r_df["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="hour_vancouver", y="avg_delay_min", color="route_short_name",
        markers=True, title="Evolution of Average Delay",
        labels={"avg_delay_min": "Delay (min)", "hour_vancouver": "Time (Vancouver)"}
    )
    fig_line.update_xaxes(dtick=3600000, tickformat="%H:%M")
    st.plotly_chart(fig_line, use_container_width=True)

# --- 2. CITY ANALYSIS & HEATMAP ---
st.markdown("---")
st.subheader("🔥 Hourly Delay Intensity Heatmap")

# Fetching from the city/area hourly view
c_res = supabase.table("v_city_hourly_delay").select("*").execute()
c_df = pd.DataFrame(c_res.data)

if not c_df.empty:
    c_df["hour_vancouver"] = pd.to_datetime(c_df["hour_vancouver"])
    
    # Pivot data for the Heatmap
    # We use area_name (Municipality) vs Time
    heat_df = c_df.pivot(index="area_name", columns="hour_vancouver", values="avg_delay_min")
    
    fig_heat = px.imshow(
        heat_df,
        labels=dict(x="Time of Day", y="Municipality", color="Avg Delay (min)"),
        x=heat_df.columns,
        y=heat_df.index,
        color_continuous_scale=color_scale, # USING YOUR SCALE
        range_color=[-2, 10],
        aspect="auto"
    )
    fig_heat.update_xaxes(side="top", tickformat="%H:%M")
    st.plotly_chart(fig_heat, use_container_width=True)

    # --- 3. COMPARATIVE BAR CHART (LONG TERM) ---
    st.markdown("---")
    st.subheader("🏙️ Cumulative City Performance")
    
    # Total average per city across all time
    city_perf = c_df.groupby("area_name")["avg_delay_min"].mean().sort_values()
    
    fig_city_total = px.bar(
        city_perf, orientation='h',
        color=city_perf.values,
        color_continuous_scale=color_scale, # USING YOUR SCALE
        range_color=[-2, 8],
        title="Overall Average Delay by Municipality (All Runs)"
    )
    fig_city_total.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig_city_total, use_container_width=True)

else:
    st.info("No historical data available yet. Keep the fetch script running to accumulate data.")
