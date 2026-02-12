import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="TransLink Performance Dashboard", page_icon="🚌", layout="wide")

# --- CONNEXION SUPABASE ---
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=300)
def load_data():
    # On monte la limite à 5000 pour voir tous les bus
    response = supabase.table("bus_positions").select("*").limit(5000).execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
    return df

raw_df = load_data()

# --- SIDEBAR ---
st.sidebar.header("⚙️ Configuration")
mode = st.sidebar.radio("Mode d'affichage :", ["Temps Réel (Dernier Run)", "Historique (Global)"])

if not raw_df.empty:
    # Choix du jeu de données selon le mode
    if mode == "Temps Réel (Dernier Run)":
        dernier_timestamp = raw_df['recorded_time'].max()
        df_working = raw_df[raw_df['recorded_time'] == dernier_timestamp].copy()
    else:
        df_working = raw_df.copy()

    # Filtre de quartier
    all_areas = sorted(df_working['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Quartiers", options=all_areas, default=all_areas)
    df = df_working[df_working['area_name'].isin(selected_areas)].copy()

    # --- TITRE ---
    st.title(f"📊 TransLink Analytics - {mode}")
    st.caption(f"Données basées sur {len(df)} enregistrements")

    # --- SECTION 1 : KPIs ---
    col1, col2, col3, col4, col5 = st.columns(5)
    
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100
    avg_delay = df['delay_min'].mean()
    
    route_stats = df.groupby('route_no')['delay_min'].mean()
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    
    area_stats = df[df['area_name'] != 'Off-Map'].groupby('area_name')['delay_min'].mean()
    worst_area = area_stats.idxmax() if not area_stats.empty else "N/A"

    col1.metric("🚌 Bus", len(df))
    col2.metric("✅ Ponctualité", f"{on_time:.1f}%")
    col3.metric("⏳ Retard Moyen", f"{avg_delay:.2f} m")
    col4.metric("🚩 Ligne Lente", f"L.{slowest_route}")
    col5.metric("📍 Zone Critique", worst_area)

    st.divider()

    # --- SECTION 2 : GRAPHIQUES ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏘️ Retard par quartier")
        plot_data = area_stats.sort_values().reset_index()
        
        # Attribution des couleurs : Vert si < 0 (avance/ponctuel), Rouge si > 0 (retard)
        # On utilise des dégradés simples basés sur la valeur
        colors = ['#2ecc71' if x < 0 else '#e74c3c' for x in plot_data['delay_min']]
        
        fig, ax = plt.subplots(figsize=(10, 7))
        ax.barh(plot_data['area_name'], plot_data['delay_min'], color=colors)
        ax.axvline(0, color='black', linewidth=1.5)
        ax.set_xlabel("Minutes (Vert = Avance | Rouge = Retard)")
        st.pyplot(fig)

    with c2:
        st.subheader("📈 Distribution des écarts")
        fig2, ax2 = plt.subplots(figsize=(10, 7))
        # Retour au vert clair pour l'histogramme
        sns.histplot(df['delay_min'], bins=20, kde=True, color="#90be6d", ax=ax2)
        ax2.axvline(0, color='red', linestyle='--')
        ax2.set_xlabel("Minutes d'écart")
        st.pyplot(fig2)

    # --- SECTION 3 : CARTE ---
    st.divider()
    st.subheader("📍 Carte des positions")
    
    # Rétablissement du filtre de retard pour la carte
    map_filter = st.radio(
        "Filtrer la carte :", 
        ["Tous les bus", "Bus en retard (> 3 min)", "Bus en avance (> 1 min)"], 
        horizontal=True
    )
    
    df_map = df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
    
    if map_filter == "Bus en retard (> 3 min)":
        df_map = df_map[df_map['delay_min'] > 3]
    elif map_filter == "Bus en avance (> 1 min)":
        df_map = df_map[df_map['delay_min'] < -1]
    
    st.map(df_map)

else:
    st.error("Aucune donnée disponible.")
