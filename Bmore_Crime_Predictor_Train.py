#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Training script for Bmore Crime Predictor
"""

import ipydeps
ipydeps.pip(['requests','tqdm','tensorflow'],verbose=False)
import IPython
import tensorflow as tf
import pandas as pd
import json
import os
import math
from datetime import datetime
from sklearn.preprocessing import QuantileTransformer

from Baltimore_Data_Modules import BaltimoreDataExtraction

from Baltimore_Data_Modules import Bmore_FeatureEngineering

from Baltimore_Data_Modules import Model_Utils

def days_hours_minutes(td):
    return td.days, td.seconds//3600, (td.seconds//60)%60

##########################################
# Initializing Baltimore Data Extraction class
##########################################

Bmore_API_Crime_link_dict = {'FEAT_COLLECT':
                             {#'911 Calls for Service 2022 Through Present':'https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/911_CallsForService_PreviousYear_Present/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              #'BPD Arrests':'https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/Arrest/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              #'Parking and Moving Citations':'https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/Parking_and_Moving_Citations__view/FeatureServer/2/query?outFields=*&where=1%3D1&f=geojson',
                              #'Gun Offenders Registry':'https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/GunOffenders/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Part 1 Crime Data':'https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/Part1_Crime_Beta/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson'},
                             'CSA_DATA':
                             {'Percent of Adult Population on Parole/Probation - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prbprl/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Property Crime Rate per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prop/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Violent Crime Rate per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Viol/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Part 1 Crime Rate per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Crime/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Number of Gun-Related Homicides per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Gunhom/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Number of Arrests per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Arrests/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Domestic Violence Calls For Service per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvarr/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Juvenile Arrest Rate per 1,000 Juveniles - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvarr/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Juvenile Arrest Rate for Violent Offenses per 1,000 Juveniles - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvviol/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Juvenile Arrest Rate for Drug-Related Offenses per 1,000 Juveniles - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvdrug/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
                              'Number of Narcotics Calls for Service per 1,000 Residents - Community Statistical Area':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Narc/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson'},
                             'CITY_DATA':
                             {'Property Crime Rate per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prop/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Violent Crime Rate per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Viol/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Part 1 Crime Rate per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Crime/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Number of Gun-Related Homicides per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Gunhom/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Number of Narcotics Calls for Service per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Narc/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Number of Common Assault Calls for Service per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Caslt/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson',
                             'Number of Common Assault Calls for Service per 1,000 Residents - City':'https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Caslt/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson'}
                            }

with open("Bmore_API_Crime_link.json", "w") as outfile:
    json.dump(Bmore_API_Crime_link_dict, outfile, indent=4)

bde = BaltimoreDataExtraction.BmoreDataExtraction(Bmore_API_Crime_link_dict)

##########################################
# Downloading latest data set by type of data
##########################################

FEAT_COLLECT_Path_dict = bde.DownloadDataType(data_type_str='FEAT_COLLECT',includeAll=True)

def Nested_featCol_Union(nestedPath_dict,chosenKey):
    oneDataset_dict = nestedPath_dict[chosenKey]
    oneDatasetDF_dict = bde.FeatureCollectionToDF(oneDataset_dict)
    return {chosenKey:pd.concat(list(oneDatasetDF_dict.values()))}

FEAT_COLLECT_DF_dict = Nested_featCol_Union(FEAT_COLLECT_Path_dict,'Part 1 Crime Data')

##########################################
# Cleaning "Part 1 Crime" Data Specifically
##########################################

crime_feat_dict = {'DROP':
                        ['RowID',
                         'Post',
                         'Gender',
                         'Age',
                         'Race',
                         'Ethnicity',
                         'GeoLocation',
                         'PremiseType',
                         'Total_Incidents',
                         'New_District'],
                   'CATEGORICAL':
                         ['CCNumber',
                          'CrimeCode',
                          'Description',
                          'Inside_Outside',
                          'Weapon',
                          'Location',
                          'Old_District',
                          'Neighborhood'],
                    'NUMERICAL':
                         []}

# instantiating feature engineering class
bcfe = Bmore_FeatureEngineering.BmoreCrimeFeatureEngineering(geo_cols_li=['Latitude',
                                                                          'Longitude'],
                                                                          time_col_str='CrimeDateTime',
                                                                          feat_col_dict=crime_feat_dict)
# Initial Cleaning of Baltimore crime data
Bmore_Crime_df = bcfe.CreateTimeIndexDF(FEAT_COLLECT_DF_dict['Part 1 Crime Data'])
Bmore_Crime_df = bcfe.DropUnwantedFeatsDF(Bmore_Crime_df)
Bmore_Crime_df = bcfe.GeoStatsDF(Bmore_Crime_df)
# filtering to Datetimes that make sense
Bmore_Crime_df = Bmore_Crime_df[Bmore_Crime_df.CrimeDateTime.astype(int) > 1600000000000]
Bmore_Crime_df.index = pd.DatetimeIndex(list(Bmore_Crime_df.index))
print('Bmore Crime Shape:',Bmore_Crime_df.shape)

# Extracting categorical and periodic features
categorical_df = bcfe.CatFeatEncode(Bmore_Crime_df)
periodicity_df = bcfe.PeriodicityEncode(Bmore_Crime_df)
print('Categorical Feature Shape:',categorical_df.shape)
print('periodicity Feature Shape:',periodicity_df.shape)

# Merging features & scaling
FINAL_X_df = pd.concat([categorical_df,periodicity_df],axis=1)
print('Feature Shape (X):',FINAL_X_df.shape)
# Separating Prediction targets
FINAL_Y_df = Bmore_Crime_df[bcfe.geo_cols_].apply(pd.to_numeric)
print('Target Shape (Y):',FINAL_Y_df.shape)

# Combining everything together again
FINAL_DF = pd.concat([FINAL_X_df,FINAL_Y_df],axis=1).sort_index()
FINAL_DF = bcfe.DropZeroGeos(FINAL_DF)
print('Final Shape Regular (X+Y):',FINAL_DF.shape)
#resampling  for every hour
resampled_FINAL_DF = FINAL_DF.resample(rule='1H').mean(numeric_only=True).interpolate(method='linear',axis=0)
resampled_FINAL_shape = resampled_FINAL_DF.shape
print('Final Shape Resampled (X+Y):',resampled_FINAL_shape)

##########################################
# Splitting Data Into Train, Validation, & Test Sets
##########################################

def DataSplitter(df):
    # Splitting training, validation, and test sets
    column_indices_dict = {name: i for i, name in enumerate(df.columns)}
    n = len(df)
    TRAIN_DF = df[0:int(n*0.7)]
    VAL_DF = df[int(n*0.7):int(n*0.9)]
    TEST_DF = df[int(n*0.9):]
    print('Train Shape:',TRAIN_DF.shape)
    print('Train Datetime Range:',
          str(TRAIN_DF.index.min())+' -> '+str(TRAIN_DF.index.max())+'\n')
    print('Validation Shape:',VAL_DF.shape)
    print('Validation Datetime Range:',
          str(VAL_DF.index.min())+' -> '+str(VAL_DF.index.max())+'\n')
    print('Test Shape:',TEST_DF.shape)
    print('Test Datetime Range:',
          str(TEST_DF.index.min())+' -> '+str(TEST_DF.index.max())+'\n')
    return {'TRAIN':TRAIN_DF,'VAL':VAL_DF,'TEST':TEST_DF}

split_dict = DataSplitter(resampled_FINAL_DF)

def NormalizeSplits(split_dict=split_dict):
    # Normalizing data sets via L2
    # Fitting to the training set
    norm = QuantileTransformer().fit(split_dict['TRAIN'])
    # Transforming all the split sets
    scaled_TRAIN_DF = pd.DataFrame(norm.transform(split_dict['TRAIN']),
                                   columns=norm.get_feature_names_out(),
                                   index=split_dict['TRAIN'].index)
    scaled_VAL_DF = pd.DataFrame(norm.transform(split_dict['VAL']),
                                   columns=norm.get_feature_names_out(),
                                   index=split_dict['VAL'].index)
    scaled_TEST_DF = pd.DataFrame(norm.transform(split_dict['TEST']),
                                    columns=norm.get_feature_names_out(),
                                    index=split_dict['TEST'].index)
    return norm, {'TRAIN':scaled_TRAIN_DF,'VAL':scaled_VAL_DF,'TEST':scaled_TEST_DF}

norm, scaled_split_dict = NormalizeSplits(split_dict)

##########################################
# Data Windowing
##########################################

## Creating a multistep output to predict more that one window at a time
OUT_STEPS = 168*1 # Each time step is 1 hour. 168 hours in a week
INPUT_STEPS = 730*1 # Each time step is 1 hour. 730 hours in a month
num_features = resampled_FINAL_shape[1]

wg = Model_Utils.WindowGenerator(input_width=INPUT_STEPS,
                                 label_width=OUT_STEPS,
                                 label_columns=None, # Originally just latitude
                                 shift=OUT_STEPS,
                                 split_dict=scaled_split_dict)

##########################################
# Training LSTM Model
##########################################

MAX_EPOCHS = 20
multi_val_performance = {}
multi_performance = {}

def compile_and_fit(model, window, patience=2):
    early_stopping = tf.keras.callbacks.EarlyStopping(monitor='val_loss',
                                                    patience=patience,
                                                    mode='min')
    # Created using M1/M2 Mac hence the use of "tf.keras.optimizers.legacy.Adam" instead of "tf.keras.optimizers.Adam"
    model.compile(loss=tf.keras.losses.MeanSquaredError(),
                optimizer=tf.keras.optimizers.legacy.Adam(), 
                metrics=[tf.keras.metrics.MeanAbsoluteError()])

    history = model.fit(window.train, epochs=MAX_EPOCHS,
                      validation_data=window.val,
                      callbacks=[early_stopping])
    return history

multi_lstm_model = tf.keras.Sequential([
    # Shape [batch, time, features] => [batch, lstm_units].
    # Adding more `lstm_units` just overfits more quickly.
    tf.keras.layers.LSTM(32, return_sequences=False),
    # Shape => [batch, out_steps*features].
    tf.keras.layers.Dense(OUT_STEPS*num_features,
                          kernel_initializer=tf.initializers.zeros()),
    # Shape => [batch, out_steps, features].
    tf.keras.layers.Reshape([OUT_STEPS, num_features])
])
train_start = datetime.now()
history = compile_and_fit(multi_lstm_model, wg)
IPython.display.clear_output()
train_finish = datetime.now() - train_start
print('Total Training Time {d}:{h}:{m}'.format(d=days_hours_minutes(train_finish)[0],
                                               h=days_hours_minutes(train_finish)[1],
                                               m=days_hours_minutes(train_finish)[2]))

multi_val_performance['LSTM'] = multi_lstm_model.evaluate(wg.val)
multi_performance['LSTM'] = multi_lstm_model.evaluate(wg.test, verbose=0)
multi_lstm_model.save(os.getcwd() + '/Bmore_Crime_Predict_Model.keras')

##########################################
# Calculating different forms of error
##########################################
geo_sigma_se = FINAL_DF.describe().loc['std']
lat_error = geo_sigma_se[bcfe.geo_cols_[0]]
lon_error = geo_sigma_se[bcfe.geo_cols_[1]]
train_error = multi_val_performance['LSTM'][1]
test_error = multi_performance['LSTM'][1]
geo_uncertainty = math.sqrt((lat_error**2)+(lon_error**2))
model_error = math.sqrt((train_error**2)+(test_error**2))
combined_uncertainty = math.sqrt((lat_error**2)+(lon_error**2)+(train_error**2)+(test_error**2))
print('Geo Uncertainty:',geo_uncertainty)
print('Model Error:',model_error)
print('Combined Uncertainty:',combined_uncertainty)
