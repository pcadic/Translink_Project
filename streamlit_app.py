import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client
import pytz

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(layout="wide")
st.title("🚍 TransLink Delay Monitoring Dashboard")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

VAN_TZ = pytz.timezone("America/Vancouver")

# ----------------------------
# LOAD DATA
# ----------------------------
@st.cache_data(ttl=600)
def load_data():
    response = supabase.table("bus_positions").select("*").execute()
    return pd.DataFrame(response.data)

df = load_data()

if df.empty:
    st.warning("No data available.")
    st.stop()

# ----------------------------
# PREPARE DATA
# ----------------------------

# Convert timestamps
df["recorded_time"] = pd.to_datetime(df["recorded_time"], utc=True)
df["recorded_time_van"] = df["recorded_time"].dt.tz_convert(VAN_TZ)

# Convert delay to minutes
df["delay_min"] = df["delay_seconds"] / 60

# Floor to full hour
df["hour_vancouver"] = df["recorded_time_van"].dt.floor("H")

# ----------------------------
# KPIs
# ----------------------------
col1, col2, col3 = st.columns(3)

col1.metric("Total Observations", len(df))
col2.metric("Average Delay (min)", round(df["delay_min"].mean(), 2))
col3.metric("Max Delay (min)", round(df["delay_min"].max(), 2))

# ----------------------------
# COLOR SCALE (Green ↔ Yellow ↔ Red)
# ----------------------------
max_abs_delay = max(
    abs(df["delay_min"].max()),
    abs(df["delay_min"].min())
)

custom_scale = [
    [0.0, "#004d00"],   # dark green
    [0.25, "#00cc00"],
    [0.5, "#ffffcc"],   # yellow center
    [0.75, "#ff9900"],
    [1.0, "#cc0000"]    # dark red
]

# ----------------------------
# MAP
# ----------------------------
st.markdown("---")
st.subheader("🗺️ Live Bus Delays")

fig_map = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="delay_min",
    color_continuous_scale=custom_scale,
    range_color=(-max_abs_delay, max_abs_delay),
    zoom=10,
    height=600
)

fig_map.update_layout(
    mapbox_style="carto-darkmatter",  # navy but readable
    margin=dict(l=0, r=0, t=0, b=0),
    coloraxis_colorbar=dict(title="Delay (min)")
)

st.plotly_chart(fig_map, use_container_width=True)

# ----------------------------
# HISTOGRAM
# ----------------------------
st.markdown("---")
st.subheader("📊 Delay Distribution")

fig_hist = px.histogram(
    df,
    x="delay_min",
    nbins=40,
    template="plotly_white"
)

fig_hist.update_layout(
    xaxis_title="Delay (minutes)",
    yaxis_title="Count"
)

st.plotly_chart(fig_hist, use_container_width=True)

# ----------------------------
# HOURLY TREND
# ----------------------------
st.markdown("---")
st.subheader("⏱ Hourly Delay Trends (Vancouver Time)")

hourly_df = (
    df.groupby("hour_vancouver")["delay_min"]
    .mean()
    .reset_index()
)

fig_line = px.line(
    hourly_df,
    x="hour_vancouver",
    y="delay_min",
    markers=True,
    template="plotly_white"
)

fig_line.update_xaxes(
    dtick=3600000,
    tickformat="%H:%M"
)

fig_line.update_layout(
    xaxis_title="Hour",
    yaxis_title="Average Delay (min)"
)

st.plotly_chart(fig_line, use_container_width=True)

# ----------------------------
# HEATMAP
# ----------------------------
st.markdown("---")
st.subheader("🔥 Hourly Delay Intensity by Municipality")

if "municipality" in df.columns:

    city_hour = (
        df.groupby(["municipality", "hour_vancouver"])["delay_min"]
        .mean()
        .reset_index()
    )

    heatmap_df = city_hour.pivot(
        index="municipality",
        columns="hour_vancouver",
        values="delay_min"
    )

    if not heatmap_df.empty:

        heatmap_df = heatmap_df.sort_index(axis=1)

        max_val = heatmap_df.max().max()
        min_val = heatmap_df.min().min()
        range_max = max(abs(max_val), abs(min_val))

        fig_heatmap = px.imshow(
            heatmap_df,
            aspect="auto",
            color_continuous_scale=custom_scale,
            zmin=-range_max,
            zmax=range_max,
            labels=dict(
                x="Hour (Vancouver Time)",
                y="Municipality",
                color="Avg Delay (min)"
            )
        )

        fig_heatmap.update_xaxes(
            dtick=3600000,
            tickformat="%H:%M"
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)

    else:
        st.info("Not enough data for heatmap.")
