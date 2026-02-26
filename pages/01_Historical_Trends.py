import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(page_title="Historical Trends", page_icon="📈", layout="wide")

# --- 2. DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_ROLE_KEY"])

supabase = init_connection()

# --- 3. COLOR SCALE ---
color_scale = [
    [0.0, "green"],
    [0.15, "green"],
    [0.2, "yellow"],
    [0.25, "red"],
    [1.0, "red"]
]

st.title("📈 Global Performance Analytics")
st.markdown("Analysis of historical patterns based on hourly aggregated data.")

# --- 4. GLOBAL NETWORK TREND ---
st.subheader("🌐 Global Network Delay (Hourly Cycle)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')
    # Aggregation to avoid criss-crossing lines between days
    df_global_plot = df_global.groupby("display_hour")["avg_delay_min"].mean().reset_index()
    df_global_plot = df_global_plot.sort_values("display_hour")

    fig_global = px.area(
        df_global_plot, x="display_hour", y="avg_delay_min",
        labels={"avg_delay_min": "Delay (min)", "display_hour": "Hour"},
        color_discrete_sequence=["#3366CC"]
    )
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 5. ROUTE PERFORMANCE TRENDS ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    df_route_plot = df_route.groupby(["route_short_name", "display_hour"])["avg_delay_min"].mean().reset_index()
    
    routes_list = sorted(df_route_plot["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Select Routes to Analyze", routes_list, default=routes_list[:3])
    
    f_route = df_route_plot[df_route_plot["route_short_name"].isin(selected_routes)]
    
    fig_route = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Hourly Delay per Route"
    )
    fig_route.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_route, use_container_width=True)

# --- 6. GEOGRAPHIC ANALYSIS ---
st.markdown("---")
col1, col2 = st.columns(2)

with col1:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        df_city_plot = df_city.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        city_list = sorted(df_city_plot["area_name"].unique())
        sel_cities = st.multiselect("Select Municipalities", city_list, default=city_list[:2])
        f_city = df_city_plot[df_city_plot["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col2:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)
    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_neigh["hour_vancouver"])
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        df_neigh_plot = df_neigh.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        neigh_list = sorted(df_neigh_plot["area_name"].unique())
        sel_neighs = st.multiselect("Select Neighborhoods", neigh_list, default=neigh_list[:2])
        f_neigh = df_neigh_plot[df_neigh_plot["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 7. OVERALL RANKING ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        ranking, orientation='h',
        color=ranking.values,
        color_continuous_scale=color_scale,
        range_color=[-2, 8]
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)", yaxis_title="Area")
    st.plotly_chart(fig_rank, use_container_width=True)
