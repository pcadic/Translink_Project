import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client

# Configuration Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_plot():
    print("Log: Récupération des données pour visualisation...")
    
    # 1. Requête SQL via Supabase
    # On récupère les bus qui ne sont pas "Off-Map"
    response = supabase.table("bus_positions") \
        .select("area_name, delay_seconds") \
        .neq("area_name", "Off-Map") \
        .execute()
    
    data = response.data
    if not data:
        print("Erreur: Aucune donnée trouvée dans la table.")
        return

    # 2. Transformation en DataFrame
    df = pd.DataFrame(data)
    
    # Conversion des secondes en minutes
    df['delay_minutes'] = df['delay_seconds'] / 60

    # 3. Calcul de la moyenne par quartier
    stats = df.groupby('area_name')['delay_minutes'].mean().sort_values(ascending=False).reset_index()

    # 4. Création du graphique
    plt.figure(figsize=(12, 8))
    sns.set_theme(style="whitegrid")
    
    # Création du barplot
    plot = sns.barplot(
        x='delay_minutes', 
        y='area_name', 
        data=stats, 
        palette="viridis"
    )

    # Personnalisation
    plt.title('Retard Moyen des Bus par Quartier (Vancouver)', fontsize=16)
    plt.xlabel('Retard Moyen (Minutes)', fontsize=12)
    plt.ylabel('Quartier', fontsize=12)
    plt.axvline(0, color='red', linestyle='--') # Ligne à 0 (ponctualité parfaite)

    # Sauvegarde
    plt.tight_layout()
    plt.savefig('retard_par_quartier.png')
    print("Succès: Le graphique 'retard_par_quartier.png' a été généré !")

if __name__ == "__main__":
    fetch_and_plot()
