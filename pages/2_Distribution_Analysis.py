import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Distribution Analysis - TransLink",
    layout="wide",
    page_icon="📊"
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

            # Remove extreme GPS outliers
            df = df[df["delay_min"] < 60]

            return df

    except Exception as e:
        st.error(f"Database error: {e}")

    return pd.DataFrame()


# --- MAIN PAGE ---
st.title("📊 Network Performance Distribution")

st.markdown("""
This page analyzes the structural distribution of delays 
based on the latest available system snapshot.

It focuses on:

- Delay distribution shape  
- Hour-based intensity patterns  
""")

df = load_data()

if not df.empty:

    # --------------------------------------------------
    # 1️⃣ GLOBAL DISTRIBUTION
    # --------------------------------------------------
    st.subheader("📈 Distribution of Delays")

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

    # --------------------------------------------------
    # 2️⃣ HEATMAP (ROBUST 0–23 HOURS)
    # --------------------------------------------------
    st.markdown("---")
    st.subheader("🔥 Delay Intensity by Area and Hour")

    pivot_df = df.pivot_table(
        index="area_name",
        columns="hour",
        values="delay_min",
        aggfunc="mean"
    )

    # Force all 24 hours
    full_hours = list(range(24))
    pivot_df = pivot_df.reindex(columns=full_hours)

    fig_heat = px.imshow(
        pivot_df,
        labels=dict(
            x="Hour of Day (Vancouver Time)",
            y="Neighborhood",
            color="Avg Delay (min)"
        ),
        color_continuous_scale="RdYlGn_r",
        aspect="auto"
    )

    fig_heat.update_xaxes(
        tickmode="linear",
        dtick=1
    )

    st.plotly_chart(fig_heat, use_container_width=True)

else:
    st.warning("No data found. Please run the data fetcher.")
