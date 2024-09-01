#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 15:22:06 2023

@author: andres_principe
"""

import pandas as pd
from folium.plugins import HeatMap
import folium
import json
import os
import webbrowser
from sklearn.preprocessing import MinMaxScaler


metadata_filename = "deploy_metadata.json"
forecast_data_filename = os.getcwd() + "/Bmore_Crime_Forecast.csv"

## Loading Crime Forecast Data
Bmore_Crime_Forecast_df = pd.read_csv(forecast_data_filename)
Bmore_Crime_Forecast_df.index = pd.to_datetime(
    Bmore_Crime_Forecast_df["Unnamed: 0"].rename("DATETIME")
)
Bmore_Crime_Forecast_df["date"] = Bmore_Crime_Forecast_df.index[0].date()
Bmore_Crime_Forecast_df = Bmore_Crime_Forecast_df.drop(columns="Unnamed: 0")
## Loading Metadata
f = open(metadata_filename)
metadata_dict = json.load(f)

## Keeping only geolocation and scaling weights. Using Day sin as weights.
## Using Day sin as weights gives the heatmap a sense of frequency at each
## predicted crime location
mms = MinMaxScaler()
mms.fit(Bmore_Crime_Forecast_df["Day sin"].values.reshape(-1, 1))
GEO_Crime_Forecast_df = Bmore_Crime_Forecast_df[
    metadata_dict["geo_cols_"] + ["Day sin"]
]
# GEO_Crime_Forecast_df = Bmore_Crime_Forecast_df[metadata_dict['geo_cols_']]
scaled_weights = mms.transform(
    Bmore_Crime_Forecast_df["Day sin"].values.reshape(-1, 1)
)
GEO_Crime_Forecast_df.loc[:, "Day sin"] = scaled_weights

## Formatting data for map visualization
GEO_data = [list(array) for array in list(GEO_Crime_Forecast_df.values)]

start_date = (
    pd.to_datetime(GEO_Crime_Forecast_df.index[0]).date().strftime("%m-%d-%Y")
)
end_date = (
    pd.to_datetime(GEO_Crime_Forecast_df.index[-1]).date().strftime("%m-%d-%Y")
)
timerange_str = start_date + " to " + end_date
layername = "Daily Frequency from " + timerange_str

## Creating the name of the layer for the heatmap
start_date = (
    pd.to_datetime(GEO_Crime_Forecast_df.index[0]).date().strftime("%m-%d-%Y")
)
end_date = (
    pd.to_datetime(GEO_Crime_Forecast_df.index[-1]).date().strftime("%m-%d-%Y")
)
timerange_str = start_date + " to " + end_date
layername = "Daily Frequency from " + timerange_str
## Generating the heat map and saving it externally for later use
crime_forecast_map = folium.Map(
    location=[39.299236, -76.609383], tiles="OpenStreetMap", zoom_start=12
)

HeatMap(GEO_data, max_opacity=0.5, name=layername).add_to(crime_forecast_map)

html_file_str = "ForecastMap.html"
crime_forecast_map.save(html_file_str)
html_browsr_url = "file://" + os.getcwd() + "/" + html_file_str
webbrowser.open(html_browsr_url, new=1)
