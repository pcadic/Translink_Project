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

# --- VOTRE ECHELLE DE COULEURS ORIGINALE ---
color_scale = [
    [0.0, "green"],
    [0.15, "green"],
    [0.2, "yellow"], # On-time (0)
    [0.25, "red"],
    [1.0, "red"]
]

st.title("📈 Global Performance Analytics")
st.markdown("Analysis of historical patterns using pre-aggregated database views.")

# --- 1. PERFORMANCE PAR LIGNE (Vue: v_route_hourly_delay) ---
st.subheader("🚌 Route Delay Trends (Hourly)")

# Fetch from your specific view
r_res = supabase.table("v_route_hourly_delay").select("*").execute()
r_df = pd.DataFrame(r_res.data)

if not r_df.empty:
    r_df["hour_vancouver"] = pd.to_datetime(r_df["hour_vancouver"])
    
    # Selector for routes (using route_short_name as requested)
    routes = sorted(r_df["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Select Routes to Compare", routes, default=routes[:3])
    
    f_route = r_df[r_df["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="hour_vancouver", y="avg_delay_min", color="route_short_name",
        markers=True,
        labels={"avg_delay_min": "Avg Delay (min)", "hour_vancouver": "Time"}
    )
    fig_line.update_xaxes(dtick=3600000, tickformat="%H:%M")
    st.plotly_chart(fig_line, use_container_width=True)

# --- 2. PERFORMANCE PAR QUARTIER (Vue: v_neighborhood_hourly_delay) ---
st.markdown("---")
st.subheader("🏘️ Neighborhood Reliability Analysis")

# Fetch from your neighborhood view
n_res = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
n_df = pd.DataFrame(n_res.data)

if not n_df.empty:
    n_df["hour_vancouver"] = pd.to_datetime(n_df["hour_vancouver"])
    
    # We replace the heatmap with a faceted line chart or a clean comparison
    neighborhoods = sorted(n_df["neighborhood"].unique())
    selected_neigh = st.multiselect("Select Neighborhoods", neighborhoods, default=neighborhoods[:2])
    
    f_neigh = n_df[n_df["neighborhood"].isin(selected_neigh)]
    
    fig_neigh = px.area(
        f_neigh, x="hour_vancouver", y="avg_delay_min", color="neighborhood",
        line_group="neighborhood",
        labels={"avg_delay_min": "Avg Delay (min)", "hour_vancouver": "Time"}
    )
    fig_neigh.update_xaxes(dtick=3600000, tickformat="%H:%M")
    st.plotly_chart(fig_neigh, use_container_width=True)

    # --- 3. GLOBAL RANKING (Bar Chart with your colors) ---
    st.markdown("---")
    st.subheader("🏆 Worst Performing Areas (Overall)")
    
    # Aggregate data to show the most problematic areas over time
    overall_neigh = n_df.groupby("neighborhood")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_bar = px.bar(
        overall_neigh, orientation='h',
        color=overall_neigh.values,
        color_continuous_scale=color_scale, # VOTRE ECHELLE
        range_color=[-2, 8]
    )
    fig_bar.update_layout(coloraxis_showscale=False, showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

else:
    st.info("Historical views are empty. Ensure the aggregation script or views are correctly populated.")
