from math import radians, sin, cos, asin, sqrt

R = 6_371_000.0  # Earth radius in meters

def haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    a = (sin((lat2 - lat1) / 2) ** 2
        + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    )

    return 2 * R * asin(sqrt(a))

def build_distance_features(start_lat, start_lon, end_lat, end_lon):

    euclidean_dist_m = haversine_m(start_lat, start_lon, end_lat, end_lon)

    manhattan_dist_m = (
        haversine_m(start_lat, start_lon, end_lat, start_lon)
        + haversine_m(start_lat, start_lon, start_lat, end_lon)
    )

    dist_ratio = (
        euclidean_dist_m / manhattan_dist_m
        if manhattan_dist_m > 0
        else 1.0
    )

    return {
        "euclidean_dist_m": euclidean_dist_m,
        "dist_ratio": dist_ratio
    }