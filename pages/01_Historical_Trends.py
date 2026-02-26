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
st.markdown("Analysis of historical patterns based on hourly aggregated data.")

# --- 1. GLOBAL NETWORK TREND (v_hourly_delay) ---
st.subheader("🌐 Global Network Delay (Hourly Cycle)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    # On extrait l'heure et on moyenne pour fusionner les différents jours
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')
    df_global_plot = df_global.groupby("display_hour")["avg_delay_min"].mean().reset_index()
    df_global_plot = df_global_plot.sort_values("display_hour")

    fig_global = px.area(
        df_global_plot, x="display_hour", y="avg_delay_min",
        title="Average System-Wide Delay by Hour",
        labels={"avg_delay_min": "Delay (min)", "display_hour": "Hour"},
        color_discrete_sequence=["#3366CC"]
    )
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 2. ROUTE PERFORMANCE TRENDS (v_route_hourly_delay) ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    
    # Agrégation pour n'avoir qu'une ligne continue par route
    df_route_plot = df_route.groupby(["route_short_name", "display_hour"])["avg_delay_min"].mean().reset_index()
    
    routes = sorted(df_route_plot["route_short_name"].unique(), key=lambda x: str(x))
    # Label unique pour éviter DuplicateElementId
    selected_routes = st.multiselect("Filter by Route Number", routes, default=routes[:3])
    
    f_route = df_route_plot[df_route_plot["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Hourly Delay per Route"
    )
    fig_line.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_line, use_container_width=True)

# --- 3. GEOGRAPHIC ANALYSIS (Cities & Neighborhoods) ---
st.markdown("---")
col_city, col_neigh = st.columns(2)

with col_city:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        df_city_plot = df_city.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        cities = sorted(df_city_plot["area_name"].unique())
        # Label unique pour éviter DuplicateElementId
        sel_cities = st.multiselect("Filter by Municipality", cities, default=cities[:2])
        f_city = df_city_plot[df_city_plot["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col_neigh:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)
    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_neigh["hour_vancouver"])
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        df_neigh_plot = df_neigh.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        neighs = sorted(df_neigh_plot["area_name"].unique())
        # Label unique pour éviter DuplicateElementId
        sel_neighs = st.multiselect("Filter by Neighborhood", neighs, default=neighs[:2])
        f_neigh = df_neigh_plot[df_neigh_plot["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 4. OVERALL RANKING ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    overall_ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        overall_ranking, orientation='h',
        color=overall_ranking.values,
        color_continuous_scale=color_scale,
        range_color=[-2, 8]
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)", yaxis_title="Neighborhood")
    st.plotly_chart(fig_rank, use_container_width=True)import streamlit as st
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
    [0.2, "yellow"], # On-time (0)
    [0.25, "red"],
    [1.0, "red"]
]

st.title("📈 Global Performance Analytics")
st.markdown("Analysis of historical patterns based on hourly aggregated data.")

# --- 1. GLOBAL NETWORK TREND ---
st.subheader("🌐 Global Network Delay (Hourly Cycle)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')
    df_global_plot = df_global.groupby("display_hour")["avg_delay_min"].mean().reset_index()
    df_global_plot = df_global_plot.sort_values("display_hour")

    fig_global = px.area(
        df_global_plot, x="display_hour", y="avg_delay_min",
        title="Average System-Wide Delay by Hour",
        labels={"avg_delay_min": "Delay (min)", "display_hour": "Hour"},
        color_discrete_sequence=["#3366CC"]
    )
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 2. ROUTE PERFORMANCE TRENDS ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    df_route_plot = df_route.groupby(["route_short_name", "display_hour"])["avg_delay_min"].mean().reset_index()
    
    routes = sorted(df_route_plot["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Compare Specific Routes", routes, default=routes[:3])
    
    f_route = df_route_plot[df_route_plot["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Hourly Delay per Route"
    )
    fig_line.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_line, use_container_width=True)

# --- 3. GEOGRAPHIC ANALYSIS ---
st.markdown("---")
col_city, col_neigh = st.columns(2)

with col_city:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        df_city_plot = df_city.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        cities = sorted(df_city_plot["area_name"].unique())
        sel_cities = st.multiselect("Compare Selected Cities", cities, default=cities[:2])
        f_city = df_city_plot[df_city_plot["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col_neigh:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)
    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_neigh["hour_vancouver"])
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        df_neigh_plot = df_neigh.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        neighs = sorted(df_neigh_plot["area_name"].unique())
        sel_neighs = st.multiselect("Compare Selected Neighborhoods", neighs, default=neighs[:2])
        f_neigh = df_neigh_plot[df_neigh_plot["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 4. OVERALL RANKING ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    overall_ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        overall_ranking, orientation='h',
        color=overall_ranking.values,
        color_continuous_scale=color_scale,
        range_color=[-2, 8]
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)", yaxis_title="Neighborhood")
    st.plotly_chart(fig_rank, use_container_width=True)import streamlit as st
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
st.markdown("Analysis of historical patterns based on hourly aggregated data.")

# --- 1. GLOBAL NETWORK TREND (View: v_hourly_delay) ---
st.subheader("🌐 Global Network Delay (Hourly Cycle)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    # Extract hour and aggregate to merge different days into one 24h cycle
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')
    df_global_plot = df_global.groupby("display_hour")["avg_delay_min"].mean().reset_index()
    df_global_plot = df_global_plot.sort_values("display_hour")

    fig_global = px.area(
        df_global_plot, x="display_hour", y="avg_delay_min",
        title="Average System-Wide Delay by Hour",
        labels={"avg_delay_min": "Delay (min)", "display_hour": "Hour"},
        color_discrete_sequence=["#3366CC"]
    )
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 2. ROUTE PERFORMANCE TRENDS (View: v_route_hourly_delay) ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    
    # Aggregate by route and hour
    df_route_plot = df_route.groupby(["route_short_name", "display_hour"])["avg_delay_min"].mean().reset_index()
    
    routes = sorted(df_route_plot["route_short_name"].unique(), key=lambda x: str(x))
    # Unique label to avoid DuplicateElementId
    selected_routes = st.multiselect("Compare Specific Routes", routes, default=routes[:3])
    
    f_route = df_route_plot[df_route_plot["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Hourly Delay per Route"
    )
    fig_line.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_line, use_container_width=True)

# --- 3. GEOGRAPHIC ANALYSIS (Cities & Neighborhoods) ---
st.markdown("---")
col_city, col_neigh = st.columns(2)

with col_city:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        
        # Aggregate by city
        df_city_plot = df_city.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        cities = sorted(df_city_plot["area_name"].unique())
        # Unique label to avoid DuplicateElementId
        sel_cities = st.multiselect("Compare Selected Cities", cities, default=cities[:2])
        f_city = df_city_plot[df_city_plot["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col_neigh:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)
    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_neigh["hour_vancouver"])
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        
        # Aggregate by neighborhood
        df_neigh_plot = df_neigh.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        neighs = sorted(df_neigh_plot["area_name"].unique())
        # Unique label to avoid DuplicateElementId
        sel_neighs = st.multiselect("Compare Selected Neighborhoods", neighs, default=neighs[:2])
        f_neigh = df_neigh_plot[df_neigh_plot["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 4. OVERALL RANKING ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    # Use the raw neighborhood data for overall historical average
    overall_ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        overall_ranking, orientation='h',
        color=overall_ranking.values,
        color_continuous_scale=color_scale,
        range_color=[-2, 8]
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)", yaxis_title="Neighborhood")
    st.plotly_chart(fig_rank, use_container_width=True)import streamlit as st
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
st.markdown("Historical patterns extracted from pre-aggregated bus data.")

# --- 1. TENDANCE GLOBALE (Vue: v_hourly_delay) ---
st.subheader("🌐 Global Network Delay (Hourly)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    # Tri par heure pour que l'axe X soit dans l'ordre chronologique
    df_global = df_global.sort_values(by="hour_vancouver")
    # Extraction de l'heure formatée
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')

    fig_global = px.area(
        df_global, x="display_hour", y="avg_delay_min",
        title="Average System-Wide Delay by Hour",
        labels={"avg_delay_min": "Delay (min)", "display_hour": "Hour of Day"},
        color_discrete_sequence=["#3366CC"]
    )
    # On force l'ordre des catégories pour éviter que Plotly ne les mélange
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 2. TENDANCE PAR LIGNE (Vue: v_route_hourly_delay) ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route = df_route.sort_values(by="hour_vancouver")
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    
    routes = sorted(df_route["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Select Routes to Compare", routes, default=routes[:3])
    
    f_route = df_route[df_route["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Hourly Delay by Route (Daily Cycle)"
    )
    fig_line.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_line, use_container_width=True)

# --- 3. ANALYSE PAR VILLE ET QUARTIER (Vues: v_city_hourly_delay & v_neighborhood_hourly_delay) ---
st.markdown("---")
col_city, col_neigh = st.columns(2)

with col_city:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
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
    [0.2, "yellow"], # Pile sur 0
    [0.25, "red"],
    [1.0, "red"]
]

st.title("📈 Global Performance Analytics")
st.markdown("Analyse des cycles horaires historiques basée sur les données agrégées.")

# --- 1. TENDANCE GLOBALE (Vue: v_hourly_delay) ---
st.subheader("🌐 Global Network Delay (Hourly Cycle)")
res_global = supabase.table("v_hourly_delay").select("*").execute()
df_global = pd.DataFrame(res_global.data)

if not df_global.empty:
    df_global["hour_vancouver"] = pd.to_datetime(df_global["hour_vancouver"])
    # Extraction de l'heure et agrégation pour fusionner les jours
    df_global["display_hour"] = df_global["hour_vancouver"].dt.strftime('%H:00')
    df_global_plot = df_global.groupby("display_hour")["avg_delay_min"].mean().reset_index()
    df_global_plot = df_global_plot.sort_values("display_hour")

    fig_global = px.area(
        df_global_plot, x="display_hour", y="avg_delay_min",
        title="Système Complet : Retard Moyen par Heure",
        labels={"avg_delay_min": "Retard (min)", "display_hour": "Heure"},
        color_discrete_sequence=["#3366CC"]
    )
    fig_global.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_global, use_container_width=True)

# --- 2. TENDANCE PAR LIGNE (Vue: v_route_hourly_delay) ---
st.markdown("---")
st.subheader("🚌 Route Performance Trends")
res_route = supabase.table("v_route_hourly_delay").select("*").execute()
df_route = pd.DataFrame(res_route.data)

if not df_route.empty:
    df_route["hour_vancouver"] = pd.to_datetime(df_route["hour_vancouver"])
    df_route["display_hour"] = df_route["hour_vancouver"].dt.strftime('%H:00')
    
    # Agrégation par ligne et par heure
    df_route_plot = df_route.groupby(["route_short_name", "display_hour"])["avg_delay_min"].mean().reset_index()
    
    routes = sorted(df_route_plot["route_short_name"].unique(), key=lambda x: str(x))
    selected_routes = st.multiselect("Select Routes to Compare", routes, default=routes[:3])
    
    f_route = df_route_plot[df_route_plot["route_short_name"].isin(selected_routes)]
    
    fig_line = px.line(
        f_route, x="display_hour", y="avg_delay_min", color="route_short_name",
        markers=True, title="Retards Horaires par Ligne"
    )
    fig_line.update_xaxes(categoryorder='category ascending')
    st.plotly_chart(fig_line, use_container_width=True)

# --- 3. ANALYSE PAR VILLE ET QUARTIER ---
st.markdown("---")
col_city, col_neigh = st.columns(2)

with col_city:
    st.subheader("🏙️ City Trends")
    res_city = supabase.table("v_city_hourly_delay").select("*").execute()
    df_city = pd.DataFrame(res_city.data)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        
        # Agrégation par ville
        df_city_plot = df_city.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        cities = sorted(df_city_plot["area_name"].unique())
        sel_cities = st.multiselect("Select Cities", cities, default=cities[:2])
        f_city = df_city_plot[df_city_plot["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col_neigh:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)
    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_neigh["hour_vancouver"])
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        
        # Agrégation par quartier
        df_neigh_plot = df_neigh.groupby(["area_name", "display_hour"])["avg_delay_min"].mean().reset_index()
        
        neighs = sorted(df_neigh_plot["area_name"].unique())
        sel_neighs = st.multiselect("Select Neighborhoods", neighs, default=neighs[:2])
        f_neigh = df_neigh_plot[df_neigh_plot["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 4. CLASSEMENT GLOBAL (Barres - Ne change pas) ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    overall_ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        overall_ranking, orientation='h',
        color=overall_ranking.values,
        color_continuous_scale=color_scale,
        range_color=[-2, 8]
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)")
    st.plotly_chart(fig_rank, use_container_width=True)
    if not df_city.empty:
        df_city["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_city = df_city.sort_values(by="hour_vancouver")
        df_city["display_hour"] = df_city["hour_vancouver"].dt.strftime('%H:00')
        
        cities = sorted(df_city["area_name"].unique())
        sel_cities = st.multiselect("Select Cities", cities, default=cities[:2])
        f_city = df_city[df_city["area_name"].isin(sel_cities)]
        
        fig_city = px.line(f_city, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_city.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_city, use_container_width=True)

with col_neigh:
    st.subheader("🏘️ Neighborhood Trends")
    res_neigh = supabase.table("v_neighborhood_hourly_delay").select("*").execute()
    df_neigh = pd.DataFrame(res_neigh.data)

    if not df_neigh.empty:
        df_neigh["hour_vancouver"] = pd.to_datetime(df_city["hour_vancouver"])
        df_neigh = df_neigh.sort_values(by="hour_vancouver")
        df_neigh["display_hour"] = df_neigh["hour_vancouver"].dt.strftime('%H:00')
        
        neighs = sorted(df_neigh["area_name"].unique())
        sel_neighs = st.multiselect("Select Cities", neighs, default=neighs[:2])
        f_neigh = df_neigh[df_neigh["area_name"].isin(sel_neighs)]
        
        fig_neigh = px.line(f_neigh, x="display_hour", y="avg_delay_min", color="area_name", markers=True)
        fig_neigh.update_xaxes(categoryorder='category ascending')
        st.plotly_chart(fig_neigh, use_container_width=True)

# --- 4. CUMULATIVE RANKING (Worst Areas) ---
st.markdown("---")
st.subheader("🏆 Overall Reliability Ranking")
if not df_neigh.empty:
    # On agrège toutes les observations historiques de la vue quartier
    overall_ranking = df_neigh.groupby("area_name")["avg_delay_min"].mean().sort_values(ascending=True).tail(15)
    
    fig_rank = px.bar(
        overall_ranking, orientation='h',
        color=overall_ranking.values,
        color_continuous_scale=color_scale, # VOTRE ECHELLE
        range_color=[-2, 8],
        title="Top 15 Neighborhoods with Highest Historical Delay"
    )
    fig_rank.update_layout(coloraxis_showscale=False, xaxis_title="Average Delay (min)", yaxis_title="Neighborhood")
    st.plotly_chart(fig_rank, use_container_width=True)
