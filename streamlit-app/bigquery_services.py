import streamlit as st
from google.cloud import bigquery

client = bigquery.Client()

@st.cache_data
def load_stations():
    query = f"""
    SELECT short_name, name, lat, lon
    FROM `is3107-491906.citibike.static_stations`
    ORDER BY name
    """
    return client.query(query).to_dataframe()