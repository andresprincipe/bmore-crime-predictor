#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 15:22:06 2023

@author: andres_principe
"""

import pandas as pd
import folium
import json
import os

metadata_filename = 'deploy_metadata.json'
forecast_data_filename = os.getcwd()+'/Bmore_Crime_Forecast.csv'

# Loading Crime Forecast Data
Bmore_Crime_Forecast_df = pd.read_csv(forecast_data_filename)
Bmore_Crime_Forecast_df.index = Bmore_Crime_Forecast_df['Unnamed: 0']
# Loading Metadata
f = open(metadata_filename)
metadata_dict = json.load(f)

GEO_Crime_Forecast_df = Bmore_Crime_Forecast_df[metadata_dict['geo_cols_']]
geo_forecast_li = GEO_Crime_Forecast_df.values.tolist()

start_date = pd.to_datetime(GEO_Crime_Forecast_df.index[0]).date().strftime('%m-%d-%Y')
end_date = pd.to_datetime(GEO_Crime_Forecast_df.index[-1]).date().strftime('%m-%d-%Y')
timerange_str = start_date + '_to_' + end_date

map_folium = folium.Map(location=[39.299236, -76.609383],
                  tiles="OpenStreetMap",
                  zoom_start=10)

for coord_index in range(len(GEO_Crime_Forecast_df)):
    pred_datetime_str = str(GEO_Crime_Forecast_df.iloc[coord_index].name)
    coordinates_li = geo_forecast_li[coord_index]
    map_folium.add_child(folium.Marker(location=coordinates_li,
                          popup='Prediction Datetime:<br>'
                          +pred_datetime_str))
map_folium.save('ForecastMap'+timerange_str+'.html')

