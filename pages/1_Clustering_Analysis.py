import streamlit as st
import pandas as pd
import geopandas as gpd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="IA Clustering - TransLink", layout="wide", page_icon="🤖")

# --- CONNEXION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=600)
def load_clustering_data():
    try:
        # On récupère l'historique global pour le Machine Learning
        response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['delay_min'] = df['delay_seconds'] / 60
            return df
    except Exception as e:
        st.error(f"Erreur de connexion base de données : {e}")
    return pd.DataFrame()

@st.cache_data
def load_geojson():
    # Chargement sans filtre pour inclure toutes les zones de l'agglo
    return gpd.read_file('data/metro_vancouver_map.geojson')

# --- LOGIQUE PRINCIPALE ---
st.title("🤖 Segmentation des Quartiers par Performance")
st.markdown("""
Cette analyse utilise un algorithme de **Machine Learning (K-Means)** pour regrouper les quartiers qui partagent des comportements de trafic similaires.
""")

df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:
    # 1. PRÉPARATION DES STATISTIQUES (Aggrégation par quartier)
    stats = df_all.groupby('area_name')['delay_min'].agg(['mean', 'std', 'count']).reset_index()
    stats['std'] = stats['std'].fillna(0) # Si 1 seul bus, pas de variation
    stats = stats[stats['count'] >= 2] # Seuil minimal pour la crédibilité

    if len(stats) > 3:
        # 2. MACHINE LEARNING : CLUSTERING K-MEANS
        scaler = StandardScaler()
        # On normalise le retard (mean) et la stabilité (std)
        X_scaled = scaler.fit_transform(stats[['mean', 'std']])
        
        n_clusters = st.sidebar.slider("Nombre de profils à détecter", 2, 5, 3)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        stats['cluster'] = kmeans.fit_predict(X_scaled).astype(str)

        # --- SYNCHRONISATION DES COULEURS ---
        unique_clusters = sorted(stats['cluster'].unique())
        color_palette = px.colors.qualitative.Prism 
        cluster_color_map = {c_id: color_palette[i] for i, c_id in enumerate(unique_clusters)}

        # 3. JOINTURE GÉOGRAPHIQUE
        # Nettoyage pour maximiser les correspondances (Surrey, Coquitlam, etc.)
        geojson_data['name_upper'] = geojson_data['name'].str.upper().str.strip()
        stats['area_name_upper'] = stats['area_name'].str.upper().str.strip()
        
        map_df = geojson_data.merge(stats, left_on='name_upper', right_on='area_name_upper', how='inner')

        # 4. DIAGNOSTIC DES DONNÉES MANQUANTES
        missing_count = len(stats) - len(map_df)
        if missing_count > 0:
            with st.expander(f"🔍 Diagnostic : {missing_count} zones non cartographiées"):
                st.write("Ces quartiers sont analysés mais leurs noms ne correspondent pas au fichier GeoJSON :")
                missing_names = set(stats['area_name_upper']) - set(geojson_data['name_upper'])
                st.write(list(missing_names))

        # --- SECTION VISUALISATION ---
        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.subheader("🎯 Profils Statistiques")
            fig_s = px.scatter(
                stats, x="mean", y="std", color="cluster", 
                size="count", hover_name="area_name",
                color_discrete_map=cluster_color_map,
                labels={
                    "mean": "Retard Moyen (min)", 
                    "std": "Instabilité / Volatilité (min)",
                    "cluster": "Groupe IA"
                },
                template="plotly_white"
            )
            fig_s.update_layout(
                xaxis_title="<b>RETARD MOYEN</b><br><sup>(Plus on est à droite, plus c'est lent)</sup>",
                yaxis_title="<b>INSTABILITÉ</b><br><sup>(Plus on est haut, plus c'est imprévisible)</sup>"
            )
            st.plotly_chart(fig_s, use_container_width=True)

        with col_right:
            st.subheader("🗺️ Carte des Groupes")
            fig_m = px.choropleth_mapbox(
                map_df, geojson=map_df.__geo_interface__, locations=map_df.index,
                color="cluster", color_discrete_map=cluster_color_map,
                hover_name="name",
                mapbox_style="carto-positron", 
                center={"lat": 49.25, "lon": -123.12}, zoom=9, opacity=0.7
            )
            fig_m.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_m, use_container_width=True)

        # 5. INTERPRÉTATION DES RÉSULTATS
        st.markdown("---")
        st.subheader("📋 Décryptage des Clusters")
        
        # Calcul des moyennes par cluster pour l'explication
        c_summary = stats.groupby('cluster')[['mean', 'std']].mean().sort_values('mean')
        cols = st.columns(len(c_summary))
        
        for i, (c_id, row) in enumerate(c_summary.iterrows()):
            with cols[i]:
                # Identification simplifiée du type de cluster
                if row['mean'] < 1.0 and row['std'] < 1.0:
                    title, color = "🟢 Zone Fiable", "green"
                elif row['mean'] > 2.0:
                    title, color = "🔴 Point Critique", "red"
                elif row['std'] > 1.2:
                    title, color = "🟡 Zone Instable", "orange"
                else:
                    title, color = f"🔵 Groupe {c_id}", "blue"
                
                st.markdown(f"### {title}")
                st.metric("Retard Moyen", f"{row['mean']:.2f} min")
                st.metric("Volatilité", f"{row['std']:.2f}")
                
                q_list = stats[stats['cluster'] == c_id]['area_name'].tolist()
                st.caption(f"**Exemples :** {', '.join(q_list[:3])}...")

    else:
        st.info("💡 **Pas assez de données.** Continuez vos relevés pour que l'IA puisse identifier des groupes (besoin de plus de quartiers avec au moins 2 bus observés).")
else:
    st.warning("⚠️ Aucune donnée disponible. Veuillez lancer le script de récupération.")
