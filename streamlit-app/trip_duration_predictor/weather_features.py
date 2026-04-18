import requests
import pandas as pd
import numpy as np
from datetime import timedelta

def add_precip_last_3h(df):
    df = df.sort_values("time").copy()

    df["precip_last_3h"] = (
        df["precipitation"]
        .shift(1)                 
        .rolling(window=3, min_periods=1)
        .sum()
        .fillna(0)
    )

    return df

def add_mins_since_rain(df):
    df = df.sort_values("time").copy()

    rain_flag = df["precipitation"] > 0.1

    last_rain_idx = np.where(rain_flag, np.arange(len(df)), np.nan)
    last_rain_idx = pd.Series(last_rain_idx).ffill().to_numpy()

    mins_since = (np.arange(len(df)) - last_rain_idx) * 60

    df["mins_since_rain"] = np.where(
        np.isnan(last_rain_idx),
        0,
        mins_since
    )

    return df


def get_weather(lat, lon, selected_time):
    url = "https://api.open-meteo.com/v1/forecast"

    start_date = (selected_time - pd.Timedelta(days=2)).date()
    end_date = selected_time.date()

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,snowfall,snow_depth",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "auto"
    }

    res = requests.get(url, params=params).json()

    if "hourly" not in res:
        raise ValueError(res)

    df = pd.DataFrame(res["hourly"])
    df["time"] = pd.to_datetime(df["time"])

    return df

def get_last_24h(df, selected_time):
    start_time = selected_time - pd.Timedelta(hours=24)

    df = df[
        (df["time"] > start_time) &
        (df["time"] <= selected_time)
    ].copy()

    return df


def build_weather_features(lat, lon, selected_time):
    # 1. get raw weather
    df = get_weather(lat, lon, selected_time)

    # 2. filter last 24h
    df = get_last_24h(df, selected_time)

    # 3. build features
    df = add_precip_last_3h(df)
    df = add_mins_since_rain(df)

    # 4. take the latest row (ie prediction point)
    df = df.sort_values("time")
    row = df.iloc[-1]

    return {
        "actual_temp": row["temperature_2m"],
        "precipitation_mm": row["precipitation"],
        "windspeed": row["wind_speed_10m"],
        "snowfall": row["snowfall"],
        "snow_depth": row["snow_depth"],
        "precip_last_3h": row["precip_last_3h"],
        "mins_since_rain": row["mins_since_rain"],
    }