import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="IA Clustering - TransLink", layout="wide")

# --- CONNEXION (Reprends tes secrets) ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- CHARGEMENT DE TOUTE LA BASE ---
@st.cache_data(ttl=600)
def load_all_history():
    response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['delay_min'] = df['delay_seconds'] / 60
    return df

st.title("🤖 Neighborhood Clustering (Machine Learning)")
st.write("Cette analyse regroupe les quartiers de Vancouver selon leur profil de performance (Retard moyen vs Stabilité).")

df_all = load_all_history()

if not df_all.empty:
    # 1. PRÉPARATION DES DONNÉES PAR QUARTIER
    # On calcule la moyenne et l'écart-type (volatilité) pour chaque quartier
    stats = df_all.groupby('area_name')['delay_min'].agg(['mean', 'std', 'count']).reset_index()
    stats = stats[stats['count'] > 5].dropna() # On filtre les quartiers avec peu de données

    if len(stats) > 3: # Il faut au moins quelques quartiers pour cluuster
        # 2. NORMALISATION (Indispensable pour le K-Means)
        scaler = StandardScaler()
        X = stats[['mean', 'std']]
        X_scaled = scaler.fit_transform(X)

        # 3. K-MEANS
        n_clusters = st.sidebar.slider("Nombre de clusters", 2, 5, 3)
        kmeans = KMeans(n_clusters=n_clusters, random_seconds=42, n_init=10)
        stats['cluster'] = kmeans.fit_predict(X_scaled)
        stats['cluster'] = stats['cluster'].astype(str) # Pour la légende Plotly

        # 4. VISUALISATION DES CLUSTERS
        fig = px.scatter(
            stats, x="mean", y="std", color="cluster",
            hover_name="area_name", size="count",
            title="Segmentation des Quartiers par Performance",
            labels={"mean": "Retard Moyen (min)", "std": "Volatilité (Instabilité)"},
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        st.plotly_chart(fig, use_container_width=True)

        # 5. EXPLICATION DES CLUSTERS
        st.subheader("📋 Profils des Clusters")
        cols = st.columns(n_clusters)
        for i in range(n_clusters):
            cluster_data = stats[stats['cluster'] == str(i)]
            with cols[i]:
                st.markdown(f"**Cluster {i}**")
                st.write(f"Quartiers : {len(cluster_data)}")
                st.write(f"Retard moyen : {cluster_data['mean'].mean():.2f} min")
                st.info(", ".join(cluster_data['area_name'].head(3).tolist()) + "...")

    else:
        st.warning("Pas assez de données historiques pour lancer l'algorithme. Continue tes relevés !")
