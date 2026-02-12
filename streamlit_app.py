import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# Configuration de la page
st.set_page_config(page_title="Vancouver Bus Tracker", layout="wide")

# Connexion Supabase (Utilise les secrets de Streamlit)
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

st.title("🚌 Analyse du Trafic TransLink Vancouver")

# Fonction pour charger les données
@st.cache_data(ttl=300) # Rafraîchir toutes les 5 minutes
def get_data():
    response = supabase.table("bus_positions").select("*").execute()
    return pd.DataFrame(response.data)

df = get_data()

if not df.empty:
    # --- Métriques rapides ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Bus en circulation", len(df))
    col2.metric("Retard moyen (min)", f"{round(df['delay_seconds'].mean() / 60, 2)}")
    col3.metric("Quartiers couverts", df['area_name'].nunique())

    # --- Graphique des Retards ---
    st.subheader("Retard moyen par quartier")
    
    # Préparation des données
    df_plot = df[df['area_name'] != 'Off-Map'].copy()
    df_plot['delay_min'] = df_plot['delay_seconds'] / 60
    stats = df_plot.groupby('area_name')['delay_min'].mean().sort_values(ascending=False).reset_index()

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='delay_min', y='area_name', data=stats, palette="RdYlGn_r", ax=ax)
    ax.set_xlabel("Minutes (Positif = Retard, Négatif = Avance)")
    ax.set_ylabel("Quartier")
    ax.axvline(0, color='black', lw=1)
    st.pyplot(fig)

    # --- Tableau de données ---
    # with st.expander("Voir les données brutes"):
    #    st.write(df)
else:
    st.warning("Aucune donnée trouvée dans Supabase. Lancez votre script GitHub Action !")


# --- Carte Interactive des Bus ---
st.subheader("📍 Position des bus en temps réel")

# On crée une colonne pour la couleur basée sur le retard
# Les bus très en retard (ex: > 5 min) apparaîtront différemment si on utilisait Pydeck, 
# mais avec st.map on affiche déjà tous les points.

if not df.empty:
    # Filtrage optionnel : ne montrer que les bus avec un certain retard
    show_only_delayed = st.checkbox("Montrer uniquement les bus en retard (> 2 min)")
    
    map_data = df.copy()
    if show_only_delayed:
        map_data = map_data[map_data['delay_seconds'] > 120]

    # Streamlit cherche automatiquement les colonnes 'latitude' et 'longitude'
    st.map(map_data)
else:
    st.write("Aucune donnée géographique disponible.")
