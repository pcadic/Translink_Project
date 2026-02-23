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

st.title("🚌 TransLink Real-Time Performance Dashboard")
df = load_dashboard_data()

if not df.empty:
    # --- KPIs (English) ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Buses On-Grid", df["vehicle_no"].nunique())
    c2.metric("Punctuality", f"{(df['delay_min'].between(-1, 3)).mean() * 100:.1f}%")
    c3.metric("Avg Delay", f"{df['delay_min'].mean():.2f} min")
    route_stats = df.groupby("route_no")["delay_min"].mean().sort_values(ascending=False)
    c4.metric("Slowest Route", f"Line {route_stats.idxmax()}" if not route_stats.empty else "N/A")
    area_stats = df.groupby("area_name")["delay_min"].mean()
    c5.metric("Critical Zone", area_stats.idxmax() if not area_stats.empty else "N/A")

    # --- FILTRE DE ROUTE (NOUVEAU) ---
    st.markdown("---")
    # On trie les routes numériquement si possible pour le menu
    available_routes = sorted(df["route_no"].unique(), key=lambda x: int(x) if str(x).isdigit() else 999)
    
    # "All Routes" par défaut pour ne pas perdre l'utilisateur
    selected_route = st.selectbox("🔍 Filter Map by Bus Route", ["All Routes"] + list(available_routes))

    # Application du filtre au DataFrame de la carte
    df_map = df.copy()
    if selected_route != "All Routes":
        df_map = df[df["route_no"] == selected_route]

    # --- ÉCHELLE DE COULEUR ---
    custom_scale = [
        [0.0, "#006400"], [0.25, "#00cc00"], [0.5, "#ffff00"], 
        [0.75, "#ff9900"], [1.0, "#cc0000"]
    ]

    # --- MAP ---
    fig_map = px.scatter_mapbox(
        df_map, lat="latitude", lon="longitude", color="delay_min",
        hover_name="route_no", hover_data=["area_name", "delay_min"],
        zoom=10,
        mapbox_style="open-street-map",
        color_continuous_scale=custom_scale,
        color_continuous_midpoint=0,
        labels={"delay_min": "Delay (min)"}
    )
    fig_map.update_traces(marker=dict(size=12 if selected_route != "All Routes" else 10, opacity=0.8))
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    # --- DISTRIBUTION DES DÉLAIS ---
    st.markdown("---")
    st.subheader("📊 Delay Distribution")
    # On utilise aussi le DataFrame filtré pour que l'histogramme réagisse au filtre
    fig_hist = px.histogram(
        df_map, x="delay_min", nbins=50,
        labels={"delay_min": "Delay (minutes)"},
        color_discrete_sequence=["#00cc00"],
        template="plotly_white"
    )
    fig_hist.update_layout(xaxis_title="Delay (min)", yaxis_title="Number of Buses", bargap=0.1)
    st.plotly_chart(fig_hist, use_container_width=True)

    # ... Le reste du code (Bar charts, Trends, Heatmap) reste identique

    # Calculate dynamic range for bar charts
    max_d = df["delay_min"].max()
    min_d = df["delay_min"].min()
    r_max = max(abs(max_d), abs(min_d))

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏙️ Top Delays by City")
        city_avg = df[df["area_type"] == "municipality"].groupby("area_name")["delay_min"].mean().reset_index().sort_values("delay_min")
        fig_city = px.bar(
            city_avg, x="delay_min", y="area_name", orientation="h", color="delay_min",
            color_continuous_scale=custom_scale, range_color=[-r_max, r_max]
        )
        fig_city.update_layout(coloraxis_showscale=False, xaxis_title="Avg Delay (min)", yaxis_title=None)
        st.plotly_chart(fig_city, use_container_width=True)

    with col2:
        st.subheader("🏘️ Top Delays by Neighborhood")
        neigh_avg = df[df["area_type"] == "neighborhood"].groupby("area_name")["delay_min"].mean().reset_index().sort_values("delay_min").tail(15)
        fig_neigh = px.bar(
            neigh_avg, x="delay_min", y="area_name", orientation="h", color="delay_min",
            color_continuous_scale=custom_scale, range_color=[-r_max, r_max]
        )
        fig_neigh.update_layout(coloraxis_showscale=False, xaxis_title="Avg Delay (min)", yaxis_title=None)
        st.plotly_chart(fig_neigh, use_container_width=True)

    # --- HOURLY TRENDS (English) ---
    st.markdown("---")
    st.subheader("⏳ Hourly Delay Trends")
    
    h_res = supabase.table("v_hourly_delay").select("*").execute()
    h_df = pd.DataFrame(h_res.data)
    if not h_df.empty:
        h_df["hour_vancouver"] = pd.to_datetime(h_df["hour_vancouver"])
        fig_line = px.line(h_df, x="hour_vancouver", y="avg_delay_min", markers=True, template="plotly_white")
        fig_line.update_xaxes(dtick=3600000, tickformat="%H:%M", title="Time")
        fig_line.update_yaxes(title="Avg Delay (min)")
        st.plotly_chart(fig_line, use_container_width=True)

    # City filter trend
    c_res = supabase.table("v_city_hourly_delay").select("*").execute()
    c_df = pd.DataFrame(c_res.data)
    if not c_df.empty:
        c_df["hour_vancouver"] = pd.to_datetime(c_df["hour_vancouver"])
        cities = sorted(c_df["area_name"].unique())
        sel_cities = st.multiselect("Select Cities to Compare", cities, default=cities[:1])
        f_city = c_df[c_df["area_name"].isin(sel_cities)]
        fig_c_trend = px.line(f_city, x="hour_vancouver", y="avg_delay_min", color="area_name", markers=True)
        fig_c_trend.update_xaxes(dtick=3600000, tickformat="%H:%M")
        st.plotly_chart(fig_c_trend, use_container_width=True)

    # --- HEATMAP (English) ---
    st.markdown("---")
    st.subheader("🔥 Hourly Delay Intensity Heatmap")
    if not c_df.empty:
        heat_df = c_df.pivot(index="area_name", columns="hour_vancouver", values="avg_delay_min")
        heat_df.columns = pd.to_datetime(heat_df.columns)
        heat_df = heat_df.sort_index(axis=1)
        fig_heat = px.imshow(
            heat_df, aspect="auto", color_continuous_scale=custom_scale, 
            zmin=-r_max, zmax=r_max, labels=dict(x="Time", y="City", color="Delay")
        )
        fig_heat.update_xaxes(dtick=3600000, tickformat="%H:%M")
        st.plotly_chart(fig_heat, use_container_width=True)
