import streamlit as st
import pandas as pd
import geopandas as gpd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(
    page_title="AI Clustering - TransLink",
    layout="wide",
    page_icon="🤖"
)

# --- CONNECTION ---
@st.cache_resource
def init_connection():
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"]
    )

supabase = init_connection()

# --- DATA LOADING ---
@st.cache_data(ttl=600)
def load_clustering_data():
    try:
        response = supabase.table("bus_positions") \
            .select("area_name, delay_seconds") \
            .execute()

        df = pd.DataFrame(response.data)

        if not df.empty:
            df["delay_min"] = df["delay_seconds"] / 60

            # Remove extreme GPS glitches
            df = df[df["delay_min"] < 60]

            return df

    except Exception as e:
        st.error(f"Database error: {e}")

    return pd.DataFrame()


@st.cache_data
def load_geojson():
    return gpd.read_file("data/metro_vancouver_map.geojson")


# --- MAIN PAGE ---
st.title("🤖 Intelligent Neighborhood Segmentation")

st.markdown("""
This analysis applies **K-Means clustering** to segment neighborhoods 
based on transit performance patterns.

The model considers:

- Average delay  
- Delay volatility  
- Percentage of significantly late trips (> 3 min)
""")

df_all = load_clustering_data()
geojson_data = load_geojson()

if not df_all.empty:

    # --- FEATURE ENGINEERING ---
    late_threshold = 3  # minutes considered significantly late

    stats = df_all.groupby("area_name").agg(
        mean=("delay_min", "mean"),
        std=("delay_min", "std"),
        count=("delay_min", "count"),
        pct_late=("delay_min", lambda x: (x > late_threshold).mean())
    ).reset_index()

    stats["std"] = stats["std"].fillna(0)

    # Minimum data threshold
    stats = stats[stats["count"] >= 2]

    if len(stats) > 3:

        # --- MACHINE LEARNING ---
        scaler = StandardScaler()

        X_scaled = scaler.fit_transform(
            stats[["mean", "std", "pct_late"]]
        )

        n_clusters = st.sidebar.slider(
            "Number of Profiles to Detect",
            2,
            5,
            3
        )

        kmeans = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10
        )

        stats["cluster"] = kmeans.fit_predict(X_scaled).astype(str)

        # --- COLOR MAP ---
        unique_clusters = sorted(stats["cluster"].unique())
        color_palette = px.colors.qualitative.Prism
        cluster_color_map = {
            c_id: color_palette[i]
            for i, c_id in enumerate(unique_clusters)
        }

        # --- GEOGRAPHIC JOIN ---
        geojson_data["name_upper"] = geojson_data["name"].str.upper().str.strip()
        stats["area_name_upper"] = stats["area_name"].str.upper().str.strip()

        map_df = geojson_data.merge(
            stats,
            left_on="name_upper",
            right_on="area_name_upper",
            how="inner"
        )

        # --- DIAGNOSTIC ---
        missing_count = len(stats) - len(map_df)

        if missing_count > 0:
            with st.expander(f"🔍 Diagnostic: {missing_count} Unmapped Zones"):
                missing_names = set(stats["area_name_upper"]) - set(geojson_data["name_upper"])
                st.write(list(missing_names))

        # --- VISUALIZATION ---
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("🎯 Statistical Profiles")

            fig_scatter = px.scatter(
                stats,
                x="mean",
                y="std",
                color="cluster",
                size="count",
                hover_name="area_name",
                color_discrete_map=cluster_color_map,
                labels={
                    "mean": "Average Delay (min)",
                    "std": "Volatility (min)",
                    "cluster": "AI Profile"
                },
                template="plotly_white"
            )

            fig_scatter.update_layout(
                xaxis_title="<b>AVERAGE DELAY</b><br><sup>(Right = Slower)</sup>",
                yaxis_title="<b>VOLATILITY</b><br><sup>(Higher = Less Predictable)</sup>"
            )

            st.plotly_chart(fig_scatter, use_container_width=True)

        with col_right:
            st.subheader("🗺️ Geographic Cluster Map")

            fig_map = px.choropleth_mapbox(
                map_df,
                geojson=map_df.__geo_interface__,
                locations=map_df.index,
                color="cluster",
                color_discrete_map=cluster_color_map,
                hover_name="name",
                mapbox_style="carto-positron",
                center={"lat": 49.25, "lon": -123.12},
                zoom=9,
                opacity=0.7
            )

            fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

            st.plotly_chart(fig_map, use_container_width=True)

        # --- INTERPRETATION ---
        st.markdown("---")
        st.subheader("📋 Cluster Characteristics")

        c_summary = stats.groupby("cluster")[["mean", "std"]].mean().sort_values("mean")

        cols = st.columns(len(c_summary))

        for i, (c_id, row) in enumerate(c_summary.iterrows()):
            with cols[i]:

                if row["mean"] < 1.0 and row["std"] < 1.0:
                    title = "🟢 Efficient Zones"
                elif row["mean"] > 2.0:
                    title = "🔴 Critical Hotspots"
                elif row["std"] > 1.2:
                    title = "🟡 Unpredictable Zones"
                else:
                    title = f"🔵 Group {c_id}"

                st.markdown(f"### {title}")
                st.metric("Avg Delay", f"{row['mean']:.2f} min")
                st.metric("Volatility", f"{row['std']:.2f}")

                examples = stats[stats["cluster"] == c_id]["area_name"].tolist()
                st.caption(f"Examples: {', '.join(examples[:3])}...")

        # --- KEY INSIGHT ---
        st.markdown("### 🔎 Key Insight")

        worst_cluster = c_summary.sort_values("mean", ascending=False).index[0]
        worst_data = c_summary.loc[worst_cluster]

        st.write(
            f"Cluster {worst_cluster} represents the highest average delay "
            f"({worst_data['mean']:.2f} min) and elevated instability "
            f"({worst_data['std']:.2f}). "
            "These neighborhoods may require operational attention."
        )

    else:
        st.info(
            "💡 Need more data. Keep running the scraper to help the AI "
            "identify distinct performance groups."
        )

else:
    st.warning("⚠️ No data available. Please run the data scraper first.")
