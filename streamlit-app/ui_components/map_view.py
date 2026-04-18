import streamlit as st
import pydeck as pdk
import pandas as pd

def render_map(start_lat, start_lon, end_lat, end_lon, start_station, end_station):
    
    points_df = pd.DataFrame({
            "lat": [start_lat, end_lat],
            "lon": [start_lon, end_lon],
            "type": ["Start", "End"],
            "name": [start_station, end_station],
            "label": [
                f"Start: {start_station}",
                f"End: {end_station}"
            ]
    })

    line_df = pd.DataFrame({
        "path": [[[start_lon, start_lat], [end_lon, end_lat]]]
    })

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=points_df,
        get_position='[lon, lat]',
        get_radius=100,
        get_fill_color='[255, 0, 0]',
        pickable=True,
    )

    line_layer = pdk.Layer(
        "PathLayer",
        data=line_df,
        get_path="path",
        get_width=8,
        get_color='[0, 150, 255]',
        opacity=0.8,
    )

    text_layer = pdk.Layer(
        "TextLayer",
        data=points_df,
        get_position='[lon, lat]',
        get_text='label',
        get_size=12,
        get_color=[0, 150, 255],
        get_angle=0,
        get_text_anchor='"middle"',
        get_alignment_baseline='"bottom"',
        get_pixel_offset=[0, -10]
    )

    view_state = pdk.ViewState(
        latitude=(start_lat + end_lat) / 2,
        longitude=(start_lon + end_lon) / 2,
        zoom=11,
        pitch=0
    )

    st.pydeck_chart(pdk.Deck(
        layers=[scatter_layer, line_layer, text_layer],
        initial_view_state=view_state,
        tooltip={"text": "{type}: {name}"}
    ))
