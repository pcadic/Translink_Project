import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION & CONNEXION ---
# (Garder ton code d'initialisation Supabase ici)

# --- FONCTION DE COLORATION HARMONISÉE ---
def get_color_gradient(values):
    """Génère une liste de couleurs du vert au rouge selon les valeurs"""
    colors = []
    # On définit les limites pour le dégradé (ex: de -2 min à 10 min)
    norm = mcolors.TwoSlopeNorm(vcenter=0, vmin=min(min(values), -1), vmax=max(max(values), 5))
    
    for val in values:
        # RdYlGn_r est le colormap Red-Yellow-Green inversé (donc Vert -> Rouge)
        color = plt.cm.RdYlGn_r(norm(val))
        colors.append(color)
    return colors

# --- CHARGEMENT DES DONNÉES ---
# (Garder ton code load_data() ici)

if not raw_df.empty:
    # ... (Tes filtres et KPIs restent identiques)

    st.markdown("---")

    # --- SECTION 2: ROUTES & CITIES (HARMONISÉES) ---
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("🏆 Top 10 Most Delayed Routes")
        top_routes = route_stats.head(10).sort_values(ascending=True).reset_index()
        if not top_routes.empty:
            fig1, ax1 = plt.subplots(figsize=(10, 6))
            route_colors = get_color_gradient(top_routes['delay_min'])
            ax1.barh(top_routes['route_no'].astype(str), top_routes['delay_min'], color=route_colors)
            ax1.set_xlabel("Average Delay (min)")
            st.pyplot(fig1)

    with col_b:
        st.subheader("🏙️ Delay by City")
        city_avg = df.groupby('municipality')['delay_min'].mean().sort_values().reset_index()
        if not city_avg.empty:
            fig2, ax2 = plt.subplots(figsize=(10, 6))
            city_colors = get_color_gradient(city_avg['delay_min'])
            ax2.barh(city_avg['municipality'], city_avg['delay_min'], color=city_colors)
            ax2.set_xlabel("Average Delay (min)")
            st.pyplot(fig2)

    # --- SECTION 3: TEMPOREL ---
    # (Garder ton code pour la courbe temporelle ici)

    # --- SECTION 4: MAPS ---
    # (Garder tes cartes ici)

    # --- SECTION 5: NEIGHBORHOODS (HARMONISÉ) ---
    st.markdown("---")
    st.subheader("🏘️ Delay by Neighborhood")
    # On affiche les 20 plus importants pour la lisibilité
    plot_data = area_stats.sort_values(ascending=True).tail(20).reset_index()
    if not plot_data.empty:
        fig4, ax4 = plt.subplots(figsize=(12, 10))
        neigh_colors = get_color_gradient(plot_data['delay_min'])
        ax4.barh(plot_data['area_name'], plot_data['delay_min'], color=neigh_colors)
        ax4.axvline(0, color='black', linewidth=0.8, linestyle='--')
        ax4.set_xlabel("Average Delay (min)")
        st.pyplot(fig4)
