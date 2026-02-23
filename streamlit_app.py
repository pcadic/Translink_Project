import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

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
            df["delay_min"] = df["delay_seconds"] / 60
            df = df[(df["latitude"] > 48.0) & (df["latitude"] < 50.0) & 
                    (df["longitude"] > -124.0) & (df["longitude"] < -122.0)]
            return df
    except Exception as e:
        st.error(f"Error: {e}")
    return pd.DataFrame()

st.title("🚌 TransLink Performance Dashboard")
df = load_dashboard_data()

if not df.empty:
    # --- KPIs (Inchangés) ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Buses On-Grid", df["vehicle_no"].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean() * 100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    route_stats = df.groupby("route_no")["delay_min"].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"R.{route_stats.idxmax()}" if not route_stats.empty else "N/A")
    area_stats = df.groupby("area_name")["delay_min"].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    # --- ÉCHELLE DE COULEUR ---
    custom_scale = [
        [0.0, "#006400"], [0.25, "#00cc00"], [0.5, "#ffffcc"], 
        [0.75, "#ff9900"], [1.0, "#cc0000"]
    ]

    # --- CARTE (STYLE INTERMÉDIAIRE + BORDURES) ---
    fig_map = px.scatter_mapbox(
        df, lat="latitude", lon="longitude", color="delay_min",
        hover_name="area_name", zoom=10,
        mapbox_style="open-street-map", # Style gris/couleur équilibré
        color_continuous_scale=custom_scale,
        color_continuous_midpoint=0
    )
    
    # ASTUCE : Ajouter une bordure noire très fine autour des points pour faire ressortir le JAUNE
    fig_map.update_traces(marker=dict(size=9, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
    
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- HISTOGRAMME DES DÉLAIS (RAJOUTÉ) ---
    st.markdown("---")
    st.subheader("📊 Distribution des Délais")
    fig_hist = px.histogram(
        df, x="delay_min", nbins=50,
        labels={"delay_min": "Délai (minutes)"},
        color_discrete_sequence=["#00cc00"],
        template="plotly_white"
    )
    fig_hist.update_layout(bargap=0.1)
    st.plotly_chart(fig_hist, use_container_width=True)

    # --- LE RESTE DU CODE (Vues SQL, Heatmap, etc. - Inchangé) ---
    # ... (Copier la suite de votre fichier original ici)

    # --- COLOR RANGE ---
    max_delay = df["delay_min"].max()
    min_delay = df["delay_min"].min()
    range_max = max(abs(max_delay), abs(min_delay))

    st.markdown("---")
    col1, col2 = st.columns(2)

    # --- CITY DELAYS ---
    with col1:
        st.subheader("🏙️ Top Delays by City")
        city_avg = df[df["area_type"] == "municipality"].groupby("area_name")["delay_min"].mean().reset_index().sort_values("delay_min", ascending=True)
        fig_city = px.bar(
            city_avg, x="delay_min", y="area_name", orientation="h", color="delay_min",
            color_continuous_scale=custom_scale, range_color=[-range_max, range_max],
            labels={"delay_min": "Avg Delay (min)", "area_name": "City"}
        )
        fig_city.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_city, use_container_width=True)

    # --- NEIGHBORHOOD DELAYS ---
    with col2:
        st.subheader("🏘️ Top Delays by Neighborhood")
        neigh_avg = df[df["area_type"] == "neighborhood"].groupby("area_name")["delay_min"].mean().reset_index().sort_values("delay_min", ascending=True).tail(15)
        fig_neigh = px.bar(
            neigh_avg, x="delay_min", y="area_name", orientation="h", color="delay_min",
            color_continuous_scale=custom_scale, range_color=[-range_max, range_max],
            labels={"delay_min": "Avg Delay (min)", "area_name": "Neighborhood"}
        )
        fig_neigh.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HOURLY TREND - General ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends (Vancouver Time)")
    hourly_response = supabase.table("v_hourly_delay").select("*").execute()
    hourly_df = pd.DataFrame(hourly_response.data)
    if not hourly_df.empty:
        hourly_df["hour_vancouver"] = pd.to_datetime(hourly_df["hour_vancouver"])
        fig_line = px.line(
            hourly_df, x="hour_vancouver", y="avg_delay_min", markers=True,
            labels={"hour_vancouver": "Time (Vancouver)", "avg_delay_min": "Avg Delay (min)"},
            template="plotly_white"
        )
        fig_line.update_traces(line_width=3)
        fig_line.update_xaxes(dtick=3600000, tickformat="%H:%M")
        st.plotly_chart(fig_line, use_container_width=True)

    # --- HOURLY TREND - City ---
    city_response = supabase.table("v_city_hourly_delay").select("*").execute()
    city_df = pd.DataFrame(city_response.data)
    if not city_df.empty:
        city_df["hour_vancouver"] = pd.to_datetime(city_df["hour_vancouver"])
        cities = sorted(city_df["area_name"].unique())
        selected_cities = st.multiselect("Select City/Cities", cities, default=cities[:1], key="city_selector_multi")
        filtered_city = city_df[city_df["area_name"].isin(selected_cities)]
        fig_city_trend = px.line(
            filtered_city, x="hour_vancouver", y="avg_delay_min", color="area_name", markers=True,
            labels={"hour_vancouver": "Time (Vancouver)", "avg_delay_min": "Avg Delay (min)", "area_name": "City"},
            template="plotly_white"
        )
        fig_city_trend.update_xaxes(dtick=3600000, tickformat="%H:%M")
        fig_city_trend.update_traces(line_width=3)
        st.plotly_chart(fig_city_trend, use_container_width=True)

    # --- HOURLY TREND - Route ---
    route_response = supabase.table("v_route_hourly_delay").select("*").execute()
    route_df = pd.DataFrame(route_response.data)
    if not route_df.empty:
        route_df["hour_vancouver"] = pd.to_datetime(route_df["hour_vancouver"])
        routes = sorted(route_df["route_no"].unique(), key=lambda x: int(x) if str(x).isdigit() else str(x))
        selected_routes = st.multiselect("Select Route(s)", routes, placeholder="Choose one or more routes", key="route_selector_multi")
        filtered_route = route_df[route_df["route_no"].isin(selected_routes)]
        fig_route = px.line(filtered_route, x="hour_vancouver", y="avg_delay_min", color="route_no", markers=True)
        fig_route.update_xaxes(dtick=3600000, tickformat="%H:%M")
        st.plotly_chart(fig_route, use_container_width=True)

    # --- HOURLY HEATMAP ---
    st.markdown("---")
    st.subheader("🔥 Hourly Delay Intensity by City")
    if not city_df.empty:
        heatmap_df = city_df.pivot(index="area_name", columns="hour_vancouver", values="avg_delay_min")
        heatmap_df.columns = pd.to_datetime(heatmap_df.columns)
        heatmap_df = heatmap_df.sort_index(axis=1)
        max_val = heatmap_df.max().max()
        min_val = heatmap_df.min().min()
        range_max_heat = max(abs(max_val), abs(min_val))
        fig_heatmap = px.imshow(
            heatmap_df, aspect="auto", color_continuous_scale=custom_scale, zmin=-range_max_heat, zmax=range_max_heat,
            labels=dict(x="Hour (Vancouver Time)", y="City", color="Avg Delay (min)")
        )
        fig_heatmap.update_xaxes(dtick=3600000, tickformat="%H:%M")
        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("Not enough hourly city data available.")
