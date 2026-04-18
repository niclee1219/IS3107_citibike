import pandas as pd
from .weather_features import build_weather_features
from .temporal_features import build_time_features
from .spatial_features import build_spatial_features
from .dist_features import build_distance_features

def build_feature_row(
    start_lat, start_lon,
    end_lat, end_lon,
    selected_time,
    is_ebike,
    is_member
):
    # 1. Time features
    time_features = build_time_features(selected_time)

    # 2. Weather features
    weather_features = build_weather_features(
        start_lat, start_lon, selected_time
    )

    # 3. Spatial features
    od_features = build_spatial_features(
        start_lat, start_lon, end_lat, end_lon
    )

    # 4. Distance features
    dist_features = build_distance_features(
        start_lat, start_lon, end_lat, end_lon
    )

    # 5. Combine everything
    features = {
        **time_features,
        **weather_features,
        **od_features,
        **dist_features,
        "is_ebike": is_ebike,
        "is_member": is_member,
    }

    return features