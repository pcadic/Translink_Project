import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="TransLink Performance Dashboard",
    page_icon="🚌",
    layout="wide"
)

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
    # Récupère les positions les plus récentes
    response = supabase.table("bus_positions").select("*").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        # Conversion en minutes pour plus de clarté
        df['delay_min'] = df['delay_seconds'] / 60
        # Conversion du timestamp en objet datetime
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
    return df

df_raw = load_data()

# --- BARRE LATÉRALE (FILTRES) ---
st.sidebar.header("🔍 Filtres")
if not df_raw.empty:
    all_areas = sorted(df_raw['area_name'].unique())
    selected_areas = st.sidebar.multiselect(
        "Choisir les quartiers", 
        options=all_areas, 
        default=all_areas
    )
    
    # Filtrage du dataframe
    df = df_raw[df_raw['area_name'].isin(selected_areas)].copy()
else:
    df = df_raw

# --- TITRE PRINCIPAL ---
st.title("📊 TransLink Real-Time Analytics")
st.markdown(f"**Dernière mise à jour :** {df['recorded_time'].max() if not df.empty else 'N/A'}")

if not df.empty:
    # --- SECTION 1 : KPIs (Key Performance Indicators) ---
    # Un bus est considéré "On Time" s'il a moins de 3 min de retard et moins de 1 min d'avance
    on_time_count = len(df[(df['delay_min'] <= 3) & (df['delay_min'] >= -1)])
    punctuality_rate = (on_time_count / len(df)) * 100
    avg_delay = df['delay_min'].mean()
    most_delayed_route = df.groupby('route_no')['delay_min'].mean().idxmax()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🚌 Bus Actifs", len(df))
    col2.metric("✅ Taux de Ponctualité", f"{punctuality_rate:.1f}%", help="Retard < 3min et Avance < 1min")
    col3.metric("⏳ Retard Moyen", f"{avg_delay:.2f} min")
    col4.metric("🚩 Ligne la plus lente", f"Route {most_delayed_route}")

    st.divider()

    # --- SECTION 2 : GRAPHIQUES ---
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("🏘️ Retard moyen par quartier")
        df_area = df[df['area_name'] != 'Off-Map'].copy()
        stats_area = df_area.groupby('area_name')['delay_min'].mean().sort_values(ascending=False).reset_index()
        
        fig_bar, ax_bar = plt.subplots(figsize=(10, 6))
        sns.barplot(data=stats_area, x='delay_min', y='area_name', palette="RdYlGn_r", ax=ax_bar)
        ax_bar.axvline(0, color='black', linestyle='-', linewidth=1)
        ax_bar.set_xlabel("Minutes (Retard > 0)")
        ax_bar.set_ylabel("")
        st.pyplot(fig_bar)

    with right_col:
        st.subheader("📈 Distribution des écarts")
        fig_hist, ax_hist = plt.subplots(figsize=(10, 6))
        sns.histplot(df['delay_min'], bins=25, kde=True, color="#2e7d32", ax=ax_hist)
        ax_hist.axvline(0, color='red', linestyle='--', label="Heure théorique")
        ax_hist.set_xlabel("Minutes d'écart")
        ax_hist.set_ylabel("Nombre de bus")
        st.pyplot(fig_hist)

    # --- SECTION 3 : CARTE ---
    st.divider()
    st.subheader("📍 Localisation des bus")
    # Filtre rapide pour la carte
    map_filter = st.radio("Afficher :", ["Tous les bus", "Bus en retard (> 3 min)"], horizontal=True)
    
    df_map = df.copy()
    if map_filter == "Bus en retard (> 3 min)":
        df_map = df_map[df_map['delay_min'] > 3]
    
    st.map(df_map)

else:
    st.error("⚠️ Aucune donnée disponible. Vérifiez la connexion Supabase ou lancez le pipeline.")

# --- FOOTER ---
st.caption("Données fournies par TransLink Open API • Développé pour analyse de performance.")
