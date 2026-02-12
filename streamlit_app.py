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
    # .limit(5000) pour casser la barrière des 1000 lignes par défaut de Supabase
    response = supabase.table("bus_positions").select("*").limit(5000).execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        df['delay_min'] = df['delay_seconds'] / 60
    return df

raw_df = load_data()

# --- SÉLECTEUR DE MODE (TEMPS RÉEL VS HISTORIQUE) ---
st.sidebar.header("⚙️ Configuration")
mode = st.sidebar.radio("Mode d'affichage :", ["Temps Réel (Dernier Run)", "Historique (Global)"])

if not raw_df.empty:
    if mode == "Temps Réel (Dernier Run)":
        # On prend uniquement les bus du dernier timestamp enregistré
        dernier_timestamp = raw_df['recorded_time'].max()
        df_working = raw_df[raw_df['recorded_time'] == dernier_timestamp].copy()
    else:
        # On garde tout, mais on dédoublonne par bus pour les stats de base
        df_working = raw_df.copy()

    # --- FILTRES DE QUARTIER ---
    all_areas = sorted(df_working['area_name'].unique())
    selected_areas = st.sidebar.multiselect("Quartiers", options=all_areas, default=all_areas)
    df = df_working[df_working['area_name'].isin(selected_areas)].copy()

    # --- TITRE ---
    st.title(f"📊 TransLink Analytics - {mode}")
    st.caption(f"Données basées sur {len(df)} enregistrements")

    # --- SECTION 1 : KPIs ---
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Calculs
    on_time = (df['delay_min'].between(-1, 3)).mean() * 100
    avg_delay = df['delay_min'].mean()
    
    # Route la plus lente
    route_stats = df.groupby('route_no')['delay_min'].mean()
    slowest_route = route_stats.idxmax() if not route_stats.empty else "N/A"
    
    # Zone critique
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
        
        fig, ax = plt.subplots(figsize=(10, 7))
        # Correction de l'erreur : On gère les couleurs manuellement pour l'effet bleu/rouge
        colors = plt.cm.coolwarm((plot_data['delay_min'] - plot_data['delay_min'].min()) / 
                                (plot_data['delay_min'].max() - plot_data['delay_min'].min()))
        
        ax.barh(plot_data['area_name'], plot_data['delay_min'], color=colors)
        ax.axvline(0, color='black', linewidth=1)
        ax.set_xlabel("Minutes (Bleu=Avance, Rouge=Retard)")
        st.pyplot(fig)

    with c2:
        st.subheader("📈 Distribution")
        fig2, ax2 = plt.subplots(figsize=(10, 7))
        sns.histplot(df['delay_min'], bins=20, kde=True, color="#4A90E2", ax=ax2)
        ax2.axvline(0, color='red', linestyle='--')
        st.pyplot(fig2)

    # --- SECTION 3 : CARTE ---
    st.divider()
    st.subheader("📍 Carte")
    # Pour la carte, on dédoublonne toujours par véhicule pour éviter les points superposés
    df_map = df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
    st.map(df_map)

else:
    st.error("Aucune donnée disponible.")
