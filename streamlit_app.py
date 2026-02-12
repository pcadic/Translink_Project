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

# --- CHARGEMENT ET NETTOYAGE DES DONNÉES ---
@st.cache_data(ttl=300)
def load_and_clean_data():
    # 1. Récupération de toute la table
    response = supabase.table("bus_positions").select("*").execute()
    full_df = pd.DataFrame(response.data)
    
    if full_df.empty:
        return full_df

    # 2. Préparation des types
    full_df['recorded_time'] = pd.to_datetime(full_df['recorded_time'])
    full_df['delay_min'] = full_df['delay_seconds'] / 60
    
    # 3. DÉDOUBLONNAGE : On ne garde que la position la plus récente pour chaque véhicule
    # Cela permet d'avoir le nombre exact de bus en circulation au dernier relevé
    df_latest = full_df.sort_values('recorded_time', ascending=False).drop_duplicates('vehicle_no')
    
    return df_latest

# On récupère le dataframe "propre"
df = load_and_clean_data()

# --- BARRE LATÉRALE (FILTRES) ---
st.sidebar.header("🔍 Filtres")
if not df.empty:
    all_areas = sorted(df['area_name'].unique())
    selected_areas = st.sidebar.multiselect(
        "Choisir les quartiers", 
        options=all_areas, 
        default=all_areas
    )
    # Application du filtre
    df_filtered = df[df['area_name'].isin(selected_areas)].copy()
else:
    df_filtered = df

# --- TITRE PRINCIPAL ---
st.title("📊 TransLink Real-Time Analytics")
if not df.empty:
    last_update = df['recorded_time'].max().strftime('%d/%m/%Y %H:%M:%S')
    st.markdown(f"**Dernier relevé détecté :** {last_update} (UTC)")

st.divider()

if not df_filtered.empty:
    # --- SECTION 1 : KPIs ---
    # Ponctualité : écart entre -1 min (avance légère) et +3 min (retard acceptable)
    on_time_count = len(df_filtered[(df_filtered['delay_min'] <= 3) & (df_filtered['delay_min'] >= -1)])
    punctuality_rate = (on_time_count / len(df_filtered)) * 100
    avg_delay = df_filtered['delay_min'].mean()
    
    # Identification du quartier le plus en retard
    stats_area_kpi = df_filtered[df_filtered['area_name'] != 'Off-Map'].groupby('area_name')['delay_min'].mean()
    worst_area = stats_area_kpi.idxmax() if not stats_area_kpi.empty else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🚌 Bus Actifs", len(df_filtered))
    col2.metric("✅ Taux de Ponctualité", f"{punctuality_rate:.1f}%")
    col3.metric("⏳ Retard Moyen", f"{avg_delay:.2f} min")
    col4.metric("🚩 Zone Critique", worst_area)

    st.divider()

    # --- SECTION 2 : GRAPHIQUES ---
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("🏘️ Performance par quartier")
        # Préparation des données du graphique
        plot_data = df_filtered[df_filtered['area_name'] != 'Off-Map'].groupby('area_name')['delay_min'].mean().sort_values().reset_index()
        
        fig_bar, ax_bar = plt.subplots(figsize=(10, 7))
        # Utilisation de la palette divergente 'coolwarm'
        # Bleu = Avance | Blanc = Ponctuel | Rouge = Retard
        sns.barplot(
            data=plot_data, 
            x='delay_min', 
            y='area_name', 
            palette="coolwarm", 
            ax=ax_bar,
            center=0
        )
        ax_bar.axvline(0, color='black', linestyle='-', linewidth=1.5)
        ax_bar.set_xlabel("Minutes d'écart (Négatif = Avance | Positif = Retard)")
        ax_bar.set_ylabel("")
        st.pyplot(fig_bar)

    with right_col:
        st.subheader("📈 Distribution des écarts")
        fig_hist, ax_hist = plt.subplots(figsize=(10, 7))
        sns.histplot(df_filtered['delay_min'], bins=20, kde=True, color="#4A90E2", ax=ax_hist)
        ax_hist.axvline(0, color='red', linestyle='--', label="Théorique")
        ax_hist.set_xlabel("Minutes d'écart")
        ax_hist.set_ylabel("Nombre de bus")
        st.pyplot(fig_hist)

    # --- SECTION 3 : CARTE ---
    st.divider()
    st.subheader("📍 Carte des bus en circulation")
    
    # Sélecteur de mode pour la carte
    map_mode = st.radio(
        "Visualiser :", 
        ["Tous les bus", "Bus en retard (> 3 min)", "Bus en avance (> 1 min)"], 
        horizontal=True
    )
    
    df_map = df_filtered.copy()
    if map_mode == "Bus en retard (> 3 min)":
        df_map = df_map[df_map['delay_min'] > 3]
    elif map_mode == "Bus en avance (> 1 min)":
        df_map = df_map[df_map['delay_min'] < -1]
    
    st.map(df_map)

else:
    st.warning("⚠️ Aucune donnée ne correspond aux filtres sélectionnés ou la table est vide.")

st.caption("Données TransLink Open Data • Analyse temps réel dédoublonnée.")
