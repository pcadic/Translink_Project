# ... (Gardez le début du script identique jusqu'à la Section 2)

    st.markdown("---")

    # --- SECTION 2: ROUTES (PLEINE LARGEUR) ---
    st.subheader("🏆 Top 10 Most Delayed Routes")
    if not route_stats.empty:
        top_routes = route_stats.head(10).sort_values(ascending=True)
        # On augmente un peu la largeur du graph (figsize) pour occuper l'espace horizontal
        fig1, ax1 = plt.subplots(figsize=(15, 5)) 
        ax1.barh(top_routes.index.astype(str), top_routes.values, color=get_color_gradient(top_routes.values))
        ax1.set_xlabel("Average Delay (min)")
        st.pyplot(fig1)

    st.markdown("---")

    # --- SECTION 3: CITIES & NEIGHBORHOODS (CÔTE À CÔTE) ---
    col_v, col_n = st.columns(2)

    with col_v:
        st.subheader("🏙️ Delay by City")
        city_avg = df.groupby('municipality')['delay_min'].mean().sort_values(ascending=True)
        if not city_avg.empty:
            fig2, ax2 = plt.subplots(figsize=(10, 8)) # Plus haut pour compenser la largeur réduite
            ax2.barh(city_avg.index, city_avg.values, color=get_color_gradient(city_avg.values))
            ax2.set_xlabel("Average Delay (min)")
            st.pyplot(fig2)

    with col_n:
        st.subheader("🏘️ Delay by Neighborhood")
        # Top 15 ou 20 pour ne pas écraser le graph des villes à côté
        top_neighborhoods = area_stats.head(20).sort_values(ascending=True)
        if not top_neighborhoods.empty:
            fig4, ax4 = plt.subplots(figsize=(10, 8))
            ax4.barh(top_neighborhoods.index, top_neighborhoods.values, color=get_color_gradient(top_neighborhoods.values))
            ax4.axvline(0, color='black', linewidth=0.8)
            ax4.set_xlabel("Average Delay (min)")
            st.pyplot(fig4)

    # --- SECTION 4: TEMPOREL ---
    st.markdown("---")
    # ... (Le reste du code pour les courbes et les cartes reste identique)
