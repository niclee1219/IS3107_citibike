import h3
import hashlib

def get_h3(lat, lon, res):
    return h3.latlng_to_cell(lat, lon, res)

def encode_od(od_pair):
    return float(
        int(hashlib.md5(od_pair.encode()).hexdigest(), 16) % 1_000_000
    )

def build_spatial_features(start_lat, start_lon, end_lat, end_lon):
    origin_h3_r8 = get_h3(start_lat, start_lon, 8)
    dest_h3_r8   = get_h3(end_lat, end_lon, 8)

    origin_h3_r9 = get_h3(start_lat, start_lon, 9)
    dest_h3_r9   = get_h3(end_lat, end_lon, 9)

    od_pair_r9 = f"{origin_h3_r9}_{dest_h3_r9}"
    od_encoded = encode_od(od_pair_r9)

    return {
        "origin_h3_r8": origin_h3_r8,
        "dest_h3_r8": dest_h3_r8,
        "od_encoded": od_encoded
    }