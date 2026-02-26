import streamlit as st
import pandas as pd
import geopandas as gpd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="AI Clustering - TransLink", layout="wide", page_icon="🤖")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_ROLE_KEY"])

supabase = init_connection()

# --- VOTRE ECHELLE DE COULEURS (Pour cohérence visuelle) ---
color_scale = [[0.0, "green"], [0.2, "yellow"], [1.0, "red"]]

# --- DATA LOADING ---
@st.cache_data(ttl=600)
def load_clustering_data():
    """
    On charge les données historiques pour calculer les profils de performance.
    """
    try:
        # On récupère les données de base pour le calcul des statistiques par quartier
        response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df["delay_min"] = df["delay_seconds"] / 60
            df = df[df["delay_min"] < 60] # Nettoyage des anomalies extrêmes
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()

@st.cache_data
def load_geojson():
    # Charge la carte pour la visualisation spatiale des clusters
    return gpd.read_file("data/metro_vancouver_map.geojson")

st.title("🤖 Intelligent Neighborhood Segmentation")
st.markdown("""
This AI module uses **K-Means Clustering** to group neighborhoods by their transit reliability.
Instead of looking at just one run, it analyzes the 'DNA' of delays in each area.
""")

df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:
    # --- FEATURE ENGINEERING ---
    # On crée les indicateurs pour l'IA : Moyenne, Volatilité (StD), et Taux de retard important
    stats = df_all.groupby("area_name").agg(
        mean_delay=("delay_min", "mean"),
        volatility=("delay_min", "std"),
        count=("delay_min", "count"),
        late_trip_pct=("delay_min", lambda x: (x > 3).mean() * 100)
    ).reset_index().fillna(0)

    # On ne garde que les zones avec assez de données (ex: min 5 observations)
    stats = stats[stats["count"] >= 5]

    if len(stats) > 3:
        # --- MACHINE LEARNING PIPELINE ---
        scaler = StandardScaler()
        # On normalise les données pour que la volatilité pèse autant que le délai moyen
        features = ["mean_delay", "volatility", "late_trip_pct"]
        X_scaled = scaler.fit_transform(stats[features])

        # Sidebar control for AI sensitivity
        n_clusters = st.sidebar.slider("Number of Profiles to Detect", 2, 5, 3)
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        stats["cluster"] = kmeans.fit_predict(X_scaled).astype(str)

        # --- VISUALIZATION: STATISTICAL PROFILES ---
        st.subheader("🎯 Performance Profiles (Statistical View)")
        
        # Ce graphique montre comment l'IA a groupé les quartiers
        fig_scatter = px.scatter(
            stats, x="mean_delay", y="volatility",
            color="cluster", size="late_trip_pct",
            hover_name="area_name",
            title="Neighborhoods grouped by Delay vs. Predictability",
            labels={"mean_delay": "Average Delay (min)", "volatility": "Volatility (Predictability)"},
            template="plotly_white",
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # --- GEOGRAPHIC VIEW ---
        st.markdown("---")
        st.subheader("🗺️ Geographic Cluster Distribution")
        
        # Préparation de la carte
        geojson_data["name_upper"] = geojson_data["name"].str.upper().str.strip()
        stats["area_name_upper"] = stats["area_name"].str.upper().str.strip()
        map_df = geojson_data.merge(stats, left_on="name_upper", right_on="area_name_upper")

        fig_map = px.choropleth_mapbox(
            map_df, geojson=map_df.__geo_interface__, locations=map_df.index,
            color="cluster", hover_name="name",
            mapbox_style="carto-positron",
            center={"lat": 49.25, "lon": -123.12}, zoom=9, opacity=0.7,
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)

        # --- INTERPRETATION DES CLUSTERS ---
        st.markdown("---")
        st.subheader("📋 Profile Characteristics (Ordered by Performance)")
        
        # We sort by mean_delay so Profile with lowest delay is always on the left
        c_summary = stats.groupby("cluster")[features].mean().sort_values("mean_delay")
        
        # Create columns based on the number of clusters found
        cols = st.columns(len(c_summary))

        for i, (c_id, row) in enumerate(c_summary.iterrows()):
            with cols[i]:
                # Dynamic Logic based on the sorted values
                if row["mean_delay"] < 1.0:
                    status = "🟢 Efficient"
                    border_color = "green"
                elif row["volatility"] > 1.5:
                    status = "🟡 Unpredictable"
                    border_color = "orange"
                else:
                    status = "🔴 Critical"
                    border_color = "red"
                
                # Displaying the cluster info
                st.markdown(f"### {status}")
                st.write(f"**Cluster ID:** {c_id}")
                st.metric("Avg Delay", f"{row['mean_delay']:.2f} min")
                st.metric("Volatility", f"{row['volatility']:.2f}")
                st.metric("Late Trip %", f"{row['late_trip_pct']:.1f}%")
                
                # Show representative neighborhoods
                examples = stats[stats["cluster"] == c_id]["area_name"].head(3).tolist()
                st.info(f"**Locations:** \n{', '.join(examples)}")

    else:
        st.info("Gathering more data to identify distinct clusters...")
else:
    st.warning("No data available for clustering analysis.")
