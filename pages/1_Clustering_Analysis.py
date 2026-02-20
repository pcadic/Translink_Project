import streamlit as st
import pandas as pd
import geopandas as gpd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(page_title="AI Clustering - TransLink", layout="wide", page_icon="🤖")

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=600)
def load_clustering_data():
    try:
        # We now fetch all rows to give the AI more history to learn from
        response = supabase.table("bus_positions").select("area_name, delay_seconds").execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df['delay_min'] = df['delay_seconds'] / 60
            # Remove extreme outliers that break K-Means (e.g., GPS glitches > 1hr)
            df = df[df['delay_min'] < 60]
            return df
    except Exception as e:
        st.error(f"Database error: {e}")
    return pd.DataFrame()

@st.cache_data
def load_geojson():
    # Loading GeoJSON without filters to include all cities (Surrey, Burnaby, etc.)
    return gpd.read_file('data/metro_vancouver_map.geojson')

# --- MAIN LOGIC ---
st.title("🤖 Intelligent Neighborhood Segmentation")
st.markdown("""
This analysis uses a **K-Means Clustering** algorithm to group neighborhoods that share similar transit performance patterns.
""")

df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:
    # 1. STATISTICS PREPARATION
    stats = df_all.groupby('area_name')['delay_min'].agg(['mean', 'std', 'count']).reset_index()
    stats['std'] = stats['std'].fillna(0) # Standard deviation is 0 if only 1 record
    stats = stats[stats['count'] >= 2] # Minimum threshold for reliability

    if len(stats) > 3:
        # 2. MACHINE LEARNING: K-MEANS CLUSTERING
        scaler = StandardScaler()
        # Normalizing Delay (mean) and Volatility (std) for equal weighting
        X_scaled = scaler.fit_transform(stats[['mean', 'std']])
        
        n_clusters = st.sidebar.slider("Number of Profiles to Detect", 2, 5, 3)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        stats['cluster'] = kmeans.fit_predict(X_scaled).astype(str)

        # --- COLOR SYNCHRONIZATION ---
        unique_clusters = sorted(stats['cluster'].unique())
        color_palette = px.colors.qualitative.Prism 
        cluster_color_map = {c_id: color_palette[i] for i, c_id in enumerate(unique_clusters)}

        # 3. GEOGRAPHIC JOIN
        # Cleaning names to maximize matches (handling case sensitivity and spaces)
        geojson_data['name_upper'] = geojson_data['name'].str.upper().str.strip()
        stats['area_name_upper'] = stats['area_name'].str.upper().str.strip()
        
        map_df = geojson_data.merge(stats, left_on='name_upper', right_on='area_name_upper', how='inner')

        # 4. DATA QUALITY DIAGNOSTIC
        missing_count = len(stats) - len(map_df)
        if missing_count > 0:
            with st.expander(f"🔍 Diagnostic: {missing_count} Unmapped Zones"):
                st.write("These neighborhoods are calculated by the AI but their names don't match the GeoJSON file:")
                missing_names = set(stats['area_name_upper']) - set(geojson_data['name_upper'])
                st.write(list(missing_names))

        # --- VISUALIZATION SECTION ---
        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.subheader("🎯 Statistical Profiles")
            fig_s = px.scatter(
                stats, x="mean", y="std", color="cluster", 
                size="count", hover_name="area_name",
                color_discrete_map=cluster_color_map,
                labels={
                    "mean": "Average Delay (min)", 
                    "std": "Volatility / Instability (min)",
                    "cluster": "AI Profile"
                },
                template="plotly_white"
            )
            fig_s.update_layout(
                xaxis_title="<b>AVERAGE DELAY</b><br><sup>(Right = Slower)</sup>",
                yaxis_title="<b>VOLATILITY</b><br><sup>(Higher = Less Predictable)</sup>"
            )
            st.plotly_chart(fig_s, use_container_width=True)

        with col_right:
            st.subheader("🗺️ Geographic Cluster Map")
            fig_map = px.choropleth_mapbox(
                map_df, geojson=map_df.__geo_interface__, locations=map_df.index,
                color="cluster", color_discrete_map=cluster_color_map,
                hover_name="name",
                mapbox_style="carto-positron", 
                center={"lat": 49.25, "lon": -123.12}, zoom=9, opacity=0.7
            )
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)

        # 5. RESULTS INTERPRETATION
        st.markdown("---")
        st.subheader("📋 Cluster Characteristics")
        
        # Summary by cluster for explanation
        c_summary = stats.groupby('cluster')[['mean', 'std']].mean().sort_values('mean')
        cols = st.columns(len(c_summary))
        
        for i, (c_id, row) in enumerate(c_summary.iterrows()):
            with cols[i]:
                # Dynamic naming based on performance
                if row['mean'] < 1.0 and row['std'] < 1.0:
                    title, color = "🟢 Efficient Zones", "green"
                elif row['mean'] > 2.0:
                    title, color = "🔴 Critical Hotspots", "red"
                elif row['std'] > 1.2:
                    title, color = "🟡 Unpredictable Zones", "orange"
                else:
                    title, color = f"🔵 Group {c_id}", "blue"
                
                st.markdown(f"### {title}")
                st.metric("Avg Delay", f"{row['mean']:.2f} min")
                st.metric("Volatility", f"{row['std']:.2f}")
                
                q_list = stats[stats['cluster'] == c_id]['area_name'].tolist()
                st.caption(f"**Examples:** {', '.join(q_list[:3])}...")

    else:
        st.info("💡 **Need more data.** Keep running the scraper to help the AI identify distinct groups (requires more neighborhoods with at least 2 observations).")
else:
    st.warning("⚠️ No data available. Please run the data scraper first.")
