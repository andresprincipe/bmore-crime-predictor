#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script deploys the current model of the Bmore_Crime_Predictor on the
latest crime data
"""
import os
import numpy as np
import pandas as pd
import tensorflow as tf
import keras
import json
import joblib

model_file_name = 'Bmore_Crime_Predict_Model.keras'
deploy_filename = 'deploy.csv'
metadata_filename = 'deploy_metadata.json'
scaler_filename = 'normalizer.save'

## Loading trained model
multi_lstm_model = keras.models.load_model(model_file_name)
## Loading Deployment Data
deploy_df = pd.read_csv(deploy_filename,index_col='Unnamed: 0')
## Loading Metadata
f = open(metadata_filename)
metadata_dict = json.load(f)
## Loading Normalizer
norm = joblib.load(scaler_filename)

## Displaying metadata about model 
print('Input Shape:',multi_lstm_model.input_shape)
print('Output Shape:',multi_lstm_model.output_shape)
print(multi_lstm_model.summary())

## Preparing deployment data for inference
deploy_shape_tu = deploy_df.shape
deploy_ar = np.reshape(deploy_df.values,
                       newshape=(1,
                                 deploy_shape_tu[0],
                                 deploy_shape_tu[1]))

## Obtaining forecasted datetime range
LATEST_DATETIME = deploy_df.tail(1).index[0]
FORECAST_DATE_RANGE = pd.date_range(start=LATEST_DATETIME,
                                    freq='1H',
                                    periods=metadata_dict['OUT_STEPS']+1)[1:]

## Generating inference and scaling back to original data
pred_ar = multi_lstm_model.predict(deploy_ar)
pred_df = pd.DataFrame(pred_ar[0],
                       columns=metadata_dict['column_indices'],
                       index=FORECAST_DATE_RANGE)

forecast_df = pd.DataFrame(norm.inverse_transform(pred_df),
                           columns=pred_df.columns,
                           index=pred_df.index)
geo_forecast_df = forecast_df[metadata_dict['geo_cols_']]
print(geo_forecast_df)

