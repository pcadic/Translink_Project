import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(
    page_title="System Behavior - TransLink",
    layout="wide",
    page_icon="🔎"
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
@st.cache_data(ttl=0)
def load_data():
    try:
        response = supabase.table("bus_positions").select(
            "area_name, delay_seconds, recorded_time"
        ).execute()

        df = pd.DataFrame(response.data)

        if not df.empty:
            df["recorded_time"] = pd.to_datetime(df["recorded_time"])

            if df["recorded_time"].dt.tz is None:
                df["recorded_time"] = df["recorded_time"].dt.tz_localize("UTC")

            df["recorded_time"] = df["recorded_time"].dt.tz_convert("America/Vancouver")

            df["delay_min"] = df["delay_seconds"] / 60
            df["hour"] = df["recorded_time"].dt.hour

            # Clean extreme GPS outliers
            df = df[df["delay_min"] < 60]

            return df

    except Exception as e:
        st.error(f"Database error: {e}")

    return pd.DataFrame()


# --- MAIN PAGE ---
st.title("🔎 System Behavior Snapshot")

st.markdown("""
This page explores the structural behavior of the transit system.

Instead of focusing on hourly averages, it analyzes:

- The overall distribution of delays  
- Variability patterns across hours  
- Delay intensity by neighborhood  
""")

df = load_data()

if not df.empty:

    # ----------------------------------------------------
    # 1️⃣ DISTRIBUTION OF DELAYS
    # ----------------------------------------------------
    st.subheader("📊 Distribution of Delays")

    fig_hist = px.histogram(
        df,
        x="delay_min",
        nbins=40,
        labels={"delay_min": "Delay (minutes)"},
        template="plotly_white"
    )

    fig_hist.update_layout(
        xaxis_title="Delay (minutes)",
        yaxis_title="Number of Observations"
    )

    st.plotly_chart(fig_hist, use_container_width=True)

    # ----------------------------------------------------
    # 2️⃣ VARIABILITY BY HOUR (BOXPLOT)
    # ----------------------------------------------------
    st.markdown("---")
    st.subheader("⏱️ Delay Variability by Hour")

    fig_box = px.box(
        df,
        x="hour",
        y="delay_min",
        labels={
            "hour": "Hour of Day (Vancouver Time)",
            "delay_min": "Delay (minutes)"
        },
        template="plotly_white"
    )

    fig_box.update_xaxes(range=[0, 23], dtick=1)

    st.plotly_chart(fig_box, use_container_width=True)

    # ----------------------------------------------------
    # 3️⃣ HEATMAP (INTENSITY)
    # ----------------------------------------------------
    st.markdown("---")
    st.subheader("🔥 Delay Intensity by Area and Hour")

    pivot_df = df.pivot_table(
        index="area_name",
        columns="hour",
        values="delay_min",
        aggfunc="mean"
    )

    # Ensure all 24 hours exist (prevents shifting axes)
    for h in range(24):
        if h not in pivot_df.columns:
            pivot_df[h] = None

    pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

    fig_heat = px.imshow(
        pivot_df,
        labels=dict(
            x="Hour of Day",
            y="Neighborhood",
            color="Avg Delay (min)"
        ),
        color_continuous_scale="RdYlGn_r",
        aspect="auto"
    )

    st.plotly_chart(fig_heat, use_container_width=True)

else:
    st.warning("No data found. Please run the data fetcher.")
