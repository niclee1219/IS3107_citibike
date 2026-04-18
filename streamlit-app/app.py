import streamlit as st
import pandas as pd
import pydeck as pdk
from datetime import datetime
from bigquery_services import load_stations
from trip_duration_predictor.feature_builder import build_feature_row
from trip_duration_predictor.predict_trip import predict_trip_duration
from trip_duration_predictor.weather_features import build_weather_features
from trip_duration_predictor.dist_features import build_distance_features
from ui_components.map_view import render_map

st.title("🚲 CitiBike Dashboard")

tab1, tab2 = st.tabs(["⏱️Citibike Trip Duration Predictor", "Others"])


with tab1:
    # ROW 1: Date & Time, Profile
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🗓️ Date & Time")
        date_input = st.date_input("Select date", min_value=datetime.today().date())
        time_input = st.time_input("Select time")

    with col2:
        st.subheader("🚴 Profile")
        is_ebike = st.toggle("Electric Bike", value=False)
        is_member = st.toggle("Member", value=False)

    selected_time = pd.Timestamp(datetime.combine(date_input, time_input))


    # Load stations
    stations_df = load_stations()

    station_map = {
        row["name"]: (row["lat"], row["lon"])
        for _, row in stations_df.iterrows()
    }

    station_names = sorted(station_map.keys())


    # ROW 2: Station selection
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("📍 Start Station")
        start_station = st.selectbox("Start station", station_names)

    with col4:
        st.subheader("🏁 End Station")
        end_station = st.selectbox("End station", station_names)


    start_lat, start_lon = station_map[start_station]
    end_lat, end_lon = station_map[end_station]


    # ROW 3: Map
    st.subheader("🗺️ Route Preview")

    if start_station and end_station and start_station != end_station:
        render_map(start_lat, start_lon, end_lat, end_lon, start_station, end_station)
    else:
        st.info("Select two different stations to preview the route 🗺️")
        st.stop()

    # ROW 4: Predict button + results
    feature_row = build_feature_row(start_lat, start_lon, end_lat, end_lon, selected_time, is_ebike, is_member)
    disable_predict = (start_station == end_station)

    if start_station == end_station:
        st.warning("Start and end stations cannot be the same")

    # Predict button and display results
    if st.button("Predict Trip Duration", disable_predict):
        duration = predict_trip_duration(feature_row) / 60  # convert seconds to minutes

        weather_info = build_weather_features(start_lat, start_lon, selected_time)
        actual_temp = weather_info["actual_temp"]
        precipitation = weather_info["precipitation_mm"]
        windspeed = weather_info["windspeed"]
        snowfall = weather_info["snowfall"]

        dist_info = build_distance_features(start_lat, start_lon, end_lat, end_lon)
        estimated_dist = dist_info["euclidean_dist_m"] / 1000  # convert to km


        st.success(f"Successful: Estimated based on route distance, time and weather conditions.")

        st.subheader("Prediction Result")
        col1, col2 = st.columns(2)

        col1.metric(label="🚲 Predicted Trip Duration", value=f"{duration:.2f} min")
        col2.metric(label="Estimated Distance", value=f"{estimated_dist:.2f} km")

        st.subheader("🌦️ Weather Conditions at Trip Time")
        col1, col2, col3, col4 = st.columns(4)

        col1.metric("🌡️ Temperature", f"{actual_temp} °C")
        col2.metric("🌧️ Rainfall", f"{precipitation} mm")
        col3.metric("💨 Wind", f"{windspeed} m/s")
        col4.metric("❄️ Snowfall", f"{snowfall} mm")

