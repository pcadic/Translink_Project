import streamlit as st
import pandas as pd
import geopandas as gpd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="IA Clustering - TransLink", layout="wide")

# --- CONNEXION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- CHARGEMENT ---
@st.cache_data(ttl=600)
def load_clustering_data():
    # On récupère l'historique pour l'analyse
    response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['delay_min'] = df['delay_seconds'] / 60
        return df
    return pd.DataFrame()

@st.cache_data
def load_geojson():
    # On charge le GeoJSON pour la carte
    gdf = gpd.read_file('data/metro_vancouver_map.geojson')
    return gdf[gdf['area_type'] == 'neighborhood']

# --- LOGIQUE PRINCIPALE ---
st.title("🤖 Neighborhood & City Clustering Analysis")
df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:
    # 1. PRÉPARATION DES STATS (On baisse le seuil à 2 relevés)
    stats = df_all.groupby('area_name')['delay_min'].agg(['mean', 'std', 'count']).reset_index()
    
    # On remplace les NaN dans 'std' par 0 (si un seul relevé, pas de variation)
    stats['std'] = stats['std'].fillna(0)
    
    # On garde presque tout pour voir les villes périphériques
    stats = stats[stats['count'] >= 2] 

    if len(stats) > 3:
        # 2. K-MEANS ML
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(stats[['mean', 'std']])
        
        n_clusters = st.sidebar.slider("Nombre de profils", 2, 5, 3)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        stats['cluster'] = kmeans.fit_predict(X_scaled).astype(str)

        # 3. JOINTURE ROBUSTE
        # On s'assure que les noms sont en majuscules/minuscules identiques
        geojson_data['name_clean'] = geojson_data['name'].str.strip()
        stats['area_name_clean'] = stats['area_name'].str.strip()
        
        map_df = geojson_data.merge(stats, left_on='name_clean', right_on='area_name_clean', how='inner')

        # Diagnostic pour toi dans Streamlit
        if len(map_df) < len(stats):
            missing = set(stats['area_name_clean']) - set(geojson_data['name_clean'])
            st.warning(f"Note: {len(missing)} zones n'ont pas de correspondance géométrique (ex: {list(missing)[:3]})")

        # --- LE RESTE DU CODE (PLOTS) RESTE IDENTIQUE ---

        # --- AFFICHAGE ---
        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.subheader("🎯 Clustering Scatter Plot")
            fig_scatter = px.scatter(
                stats, x="mean", y="std", color="cluster",
                size="count", hover_name="area_name",
                labels={"mean": "Avg Delay (min)", "std": "Volatility"},
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        with col_right:
            st.subheader("🗺️ Geographic Cluster Map")
            fig_map = px.choropleth_mapbox(
                map_df,
                geojson=map_df.__geo_interface__,
                locations=map_df.index,
                color="cluster",
                hover_name="name",
                mapbox_style="carto-positron",
                center={"lat": 49.25, "lon": -123.12},
                zoom=9,
                opacity=0.6,
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)

        # 4. INSIGHTS
        st.markdown("---")
        st.subheader("📋 Cluster Characteristics")
        for i in sorted(stats['cluster'].unique()):
            c_data = stats[stats['cluster'] == i]
            with st.expander(f"PROFIL CLUSTER {i} ({len(c_data)} quartiers)"):
                st.write(f"**Retard Moyen du groupe :** {c_data['mean'].mean():.2f} min")
                st.write(f"**Quartiers inclus :** {', '.join(c_data['area_name'].tolist())}")

    else:
        st.info("Continue à collecter des données pour activer le clustering (besoin de plus de quartiers avec >5 relevés).")
