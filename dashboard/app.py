import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="GitHub Events Pipeline",
    page_icon="🔧",
    layout="wide"
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

@st.cache_data(ttl=60)
def load_summary():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM mart_event_summary ORDER BY summary_date DESC", conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_recent_events():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT event_type, actor, repo_name, created_at 
        FROM stg_github_events 
        ORDER BY created_at DESC 
        LIMIT 50
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_top_actors():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT actor, COUNT(*) as event_count
        FROM stg_github_events
        GROUP BY actor
        ORDER BY event_count DESC
        LIMIT 10
    """, conn)
    conn.close()
    return df

# Header
st.title("🔧 GitHub Events Pipeline Dashboard")
st.markdown("Real-time GitHub public events — ingested, transformed, and visualized")

# Metrics row
summary = load_summary()
recent = load_recent_events()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Events", summary["total_events"].sum())
with col2:
    st.metric("Unique Actors", summary["unique_actors"].sum())
with col3:
    st.metric("Unique Repos", summary["unique_repos"].sum())
with col4:
    st.metric("Event Types", summary["event_type"].nunique())

st.divider()

# Charts row
col1, col2 = st.columns(2)

with col1:
    st.subheader("Events by Type")
    fig = px.bar(
        summary,
        x="event_type",
        y="total_events",
        color="event_type",
        title="GitHub Event Type Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 Most Active Users")
    top_actors = load_top_actors()
    fig2 = px.bar(
        top_actors,
        x="event_count",
        y="actor",
        orientation="h",
        title="Most Active GitHub Users"
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Recent events table
st.subheader("Recent Events")
st.dataframe(recent, use_container_width=True)

# Pipeline info
st.divider()
st.markdown("**Pipeline:** GitHub API → Raw JSON → PostgreSQL → dbt transforms → Dashboard")
st.markdown("**Stack:** Python · PostgreSQL · Docker · Streamlit · Plotly")
