# --- SECTION NOUVELLE: TIME-SERIES ANALYSIS (CORRIGÉE) ---
st.divider()
st.subheader("⏰ Hourly Delay Trends (Vancouver Time)")

if not df.empty:
    # 1. Conversion en heure locale de Vancouver
    # On s'assure que c'est bien du datetime, on localise en UTC puis on convertit
    df['recorded_time_local'] = df['recorded_time'].dt.tz_localize('UTC', Simmons=True).dt.tz_convert('America/Vancouver')
    df['hour'] = df['recorded_time_local'].dt.hour
    
    # 2. Moyenne par heure
    hourly_trend = df.groupby('hour')['delay_min'].mean().reset_index()
    
    if len(hourly_trend) > 0:
        fig_time, ax_time = plt.subplots(figsize=(12, 4))
        
        # On dessine la ligne
        ax_time.plot(hourly_trend['hour'], hourly_trend['delay_min'], 
                     marker='o', linestyle='-', color='#1f77b4', linewidth=2, label='Avg Delay')
        
        # On force l'affichage de 0 à 23h même s'il n'y a pas de données
        ax_time.set_xlim(-0.5, 23.5)
        ax_time.set_xticks(range(0, 24))
        ax_time.set_xlabel("Hour of the Day (Local Time)")
        ax_time.set_ylabel("Min")
        ax_time.grid(True, alpha=0.2)
        
        # Zones de pointe
        ax_time.axvspan(7, 9, color='orange', alpha=0.1, label='Morning Peak')
        ax_time.axvspan(15, 18, color='red', alpha=0.1, label='Evening Peak')
        ax_time.legend()
        
        st.pyplot(fig_time)
        
        if len(hourly_trend) == 1:
            st.info("💡 Un seul point affiché car les données actuelles ne couvrent qu'une seule tranche horaire.")
    else:
        st.write("Pas assez de données pour l'analyse temporelle.")
