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
    try:
        response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['delay_min'] = df['delay_seconds'] / 60
            return df
    except Exception as e:
        st.error(f"Erreur Supabase : {e}")
    return pd.DataFrame()

@st.cache_data
def load_geojson():
    return gpd.read_file('data/metro_vancouver_map.geojson')

# --- LOGIQUE ---
st.title("🤖 Segmentation des Quartiers")

df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:
    # 1. PREPARATION
    stats = df_all.groupby('area_name')['delay_min'].agg(['mean', 'std', 'count']).reset_index()
    stats['std'] = stats['std'].fillna(0)
    stats = stats[stats['count'] >= 2]

    if len(stats) > 3:
        # 2. ML
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(stats[['mean', 'std']])
        
        n_clusters = st.sidebar.slider("Nombre de clusters", 2, 5, 3)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        stats['cluster'] = kmeans.fit_predict(X_scaled).astype(str)

        # 3. JOINTURE
        geojson_data['name_clean'] = geojson_data['name'].str.upper().str.strip()
        stats['area_name_clean'] = stats['area_name'].str.upper().str.strip()
        
        map_df = geojson_data.merge(stats, left_on='name_clean', right_on='area_name_clean', how='inner')

        # --- DIAGNOSTIC ---
        missing_count = len(stats) - len(map_df)
        if missing_count > 0:
            with st.expander(f"🔍 Diagnostic : {missing_count} zones manquantes"):
                missing_names = set(stats['area_name_clean']) - set(geojson_data['name_clean'])
                st.write(list(missing_names))

        # --- GRAPHIQUES ---
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("🎯 Statistique")
            fig_s = px.scatter(stats, x="mean", y="std", color="cluster", size="count", 
                               hover_name="area_name", template="plotly_white")
            st.plotly_chart(fig_s, use_container_width=True)

        with c2:
            st.subheader("🗺️ Géographie")
            fig_m = px.choropleth_mapbox(
                map_df, geojson=map_df.__geo_interface__, locations=map_df.index,
                color="cluster", hover_name="name",
                mapbox_style="carto-positron", center={"lat": 49.25, "lon": -123.12},
                zoom=9, opacity=0.7
            )
            fig_m.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_m, use_container_width=True)

    else:
        st.info("Besoin de plus de données pour le clustering.")
else:
    st.warning("Base de données vide.")
