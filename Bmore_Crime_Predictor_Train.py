#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Training script for Bmore Crime Predictor
"""
##########################################
# Initializing Baltimore Data Extraction class
##########################################

import ipydeps

ipydeps.pip(
    [
        "IPython",
        "tensorflow",
        "requests",
        "tqdm",
        "pandas",
        "joblib",
        "scikit-learn",
    ],
    verbose=False,
)
import logging
import IPython
import tensorflow as tf
import pandas as pd
import json
import os
import math
from datetime import datetime
import joblib
from sklearn.preprocessing import QuantileTransformer

# importing classes from other files that assist with the downloading and
#  feature engineering of Baltimore's arcgis data
from Baltimore_Data_Modules.BaltimoreDataExtraction import BmoreDataExtraction
from Baltimore_Data_Modules.BaltimoreDataExtraction import TimeRangeBuild
from Baltimore_Data_Modules.Bmore_FeatureEngineering import (
    BmoreCrimeFeatureEngineering,
)
import Baltimore_Data_Modules.Model_Utils as Model_Utils


def days_hours_minutes(td):
    # function that is used for converting time into a readable format
    return td.days, td.seconds // 3600, (td.seconds // 60) % 60


logging.basicConfig(
    filename=os.getcwd() + "/BmoreCrimeForecast.log",
    encoding="utf-8",
    level=logging.DEBUG,
)
Tlog = logging.getLogger(__name__)

##########################################
# Initializing Baltimore Data Extraction class
##########################################

Bmore_API_Crime_link_dict = {
    "FEAT_COLLECT": {  #'911 Calls for Service 2022 Through Present':'https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/911_CallsForService_PreviousYear_Present/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
        #'BPD Arrests':'https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/Arrest/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
        #'Parking and Moving Citations':'https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/Parking_and_Moving_Citations__view/FeatureServer/2/query?outFields=*&where=1%3D1&f=geojson',
        #'Gun Offenders Registry':'https://egis.baltimorecity.gov/egis/rest/services/GeoSpatialized_Tables/GunOffenders/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson',
        "Part 1 Crime Data": "https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/Part1_Crime_Beta/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    },
    "CSA_DATA": {
        "Percent of Adult Population on Parole/Probation - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prbprl/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Property Crime Rate per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prop/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Violent Crime Rate per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Viol/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Part 1 Crime Rate per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Crime/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Gun-Related Homicides per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Gunhom/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Arrests per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Arrests/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Domestic Violence Calls For Service per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvarr/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Juvenile Arrest Rate per 1,000 Juveniles - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvarr/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Juvenile Arrest Rate for Violent Offenses per 1,000 Juveniles - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvviol/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Juvenile Arrest Rate for Drug-Related Offenses per 1,000 Juveniles - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Juvdrug/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Narcotics Calls for Service per 1,000 Residents - Community Statistical Area": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Narc/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
    },
    "CITY_DATA": {
        "Property Crime Rate per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Prop/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Violent Crime Rate per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Viol/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Part 1 Crime Rate per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Crime/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Gun-Related Homicides per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Gunhom/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Narcotics Calls for Service per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Narc/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Common Assault Calls for Service per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Caslt/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
        "Number of Common Assault Calls for Service per 1,000 Residents - City": "https://services1.arcgis.com/mVFRs7NF4iFitgbY/arcgis/rest/services/Caslt/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson",
    },
}

with open("Bmore_API_Crime_link.json", "w") as outfile:
    json.dump(Bmore_API_Crime_link_dict, outfile, indent=4)

bde = BmoreDataExtraction(Bmore_API_Crime_link_dict)

##########################################
# Downloading latest data set by type of data
##########################################

FEAT_COLLECT_Path_dict = bde.DownloadDataType(
    data_type_str="FEAT_COLLECT", includeAll=True
)


def Nested_featCol_Union(nestedPath_dict, chosenKey):
    oneDataset_dict = nestedPath_dict[chosenKey]
    oneDatasetDF_dict = bde.FeatureCollectionToDF(oneDataset_dict)
    return {chosenKey: pd.concat(list(oneDatasetDF_dict.values()))}


FEAT_COLLECT_DF_dict = Nested_featCol_Union(
    FEAT_COLLECT_Path_dict, "Part 1 Crime Data"
)

##########################################
# Cleaning "Part 1 Crime" Data Specifically
##########################################

crime_feat_dict = {
    "DROP": [
        "RowID",
        "Post",
        "Gender",
        "Age",
        "Race",
        "Ethnicity",
        "GeoLocation",
        "PremiseType",
        "Total_Incidents",
        "New_District",
    ],
    "CATEGORICAL": [
        "CCNumber",
        "CrimeCode",
        "Description",
        "Inside_Outside",
        "Weapon",
        "Location",
        "Old_District",
        "Neighborhood",
    ],
    "NUMERICAL": [],
}

# instantiating feature engineering class
bcfe = BmoreCrimeFeatureEngineering(
    geo_cols_li=["Latitude", "Longitude"],
    time_col_str="CrimeDateTime",
    feat_col_dict=crime_feat_dict,
)
# Initial Cleaning of Baltimore crime data
Bmore_Crime_df = bcfe.CreateTimeIndexDF(
    FEAT_COLLECT_DF_dict["Part 1 Crime Data"]
)
Bmore_Crime_df = bcfe.DropUnwantedFeatsDF(Bmore_Crime_df)
Bmore_Crime_df = bcfe.GeoStatsDF(Bmore_Crime_df)
# filtering to Datetimes that make sense
# calculating an epoch time 20 years back from the current datetime
trb = TimeRangeBuild(yearsBack=20, monthsBack=0, daysBack=0)
dtEpochNumfltr = trb.CreateMinMaxDict()["MIN"][0]
Bmore_Crime_df = Bmore_Crime_df[
    Bmore_Crime_df.CrimeDateTime.astype(int) > dtEpochNumfltr
]
Bmore_Crime_df.index = pd.DatetimeIndex(list(Bmore_Crime_df.index))
print("Bmore Crime Shape = ", Bmore_Crime_df.shape)
Tlog.info("Bmore Crime Shape = " + str(Bmore_Crime_df.shape))

# Extracting categorical and periodic features
categorical_df, oe = bcfe.CatFeatEncode(Bmore_Crime_df)
periodicity_df = bcfe.PeriodicityEncode(Bmore_Crime_df)
print("Categorical Feature Shape = ", categorical_df.shape)
print("periodicity Feature Shape = ", periodicity_df.shape)
Tlog.info("Categorical Feature Shape = " + str(categorical_df.shape))
Tlog.info("periodicity Feature Shape = " + str(periodicity_df.shape))

# Merging features & scaling
FINAL_X_df = pd.concat([categorical_df, periodicity_df], axis=1)
print("Feature Shape (X) = ", FINAL_X_df.shape)
Tlog.info("Feature Shape (X) = " + str(FINAL_X_df.shape))
# Separating Prediction targets
FINAL_Y_df = Bmore_Crime_df[bcfe.geo_cols_].apply(pd.to_numeric)
print("Target Shape (Y) = ", FINAL_Y_df.shape)
Tlog.info("Target Shape (Y) = " + str(FINAL_Y_df.shape))

# Combining everything together again
FINAL_DF = pd.concat([FINAL_X_df, FINAL_Y_df], axis=1).sort_index()
FINAL_DF = bcfe.DropZeroGeos(FINAL_DF)
print("Final Shape Regular (X+Y) = ", FINAL_DF.shape)
Tlog.info("Final Shape Regular (X+Y) = " + str(FINAL_DF.shape))
# resampling  for every hour
RULE_str = "1H"  # 1H for every hour and 1D for every day
resampled_FINAL_DF = (
    FINAL_DF.resample(rule=RULE_str)
    .mean(numeric_only=True)
    .interpolate(method="akima")
)
resampled_FINAL_shape = resampled_FINAL_DF.shape
print("Final Shape Resampled (X+Y) = ", resampled_FINAL_shape)
Tlog.info("Final Shape Resampled (X+Y) = " + str(resampled_FINAL_shape))

##########################################
# Splitting Data Into Train, Validation, & Test Sets
##########################################


def DataSplitter(df):
    # Splitting training, validation, and test sets
    n = len(df)
    TRAIN_DF = df[0 : int(n * 0.7)]
    VAL_DF = df[int(n * 0.7) : int(n * 0.9)]
    TEST_DF = df[int(n * 0.9) :]
    print("Train Shape = ", TRAIN_DF.shape)
    print(
        "Train Datetime Range = ",
        str(TRAIN_DF.index.min()) + " -> " + str(TRAIN_DF.index.max()) + "\n",
    )
    Tlog.info("Train Shape = " + str(TRAIN_DF.shape))
    Tlog.info(
        "Train Datetime Range = "
        + str(TRAIN_DF.index.min())
        + " -> "
        + str(TRAIN_DF.index.max())
        + "\n"
    )
    print("Validation Shape = ", VAL_DF.shape)
    print(
        "Validation Datetime Range = ",
        str(VAL_DF.index.min()) + " -> " + str(VAL_DF.index.max()) + "\n",
    )
    Tlog.info("Validation Shape = " + str(VAL_DF.shape))
    Tlog.info(
        "Validation Datetime Range = "
        + str(VAL_DF.index.min())
        + " -> "
        + str(VAL_DF.index.max())
        + "\n"
    )
    print("Test Shape = ", TEST_DF.shape)
    print(
        "Test Datetime Range = ",
        str(TEST_DF.index.min()) + " -> " + str(TEST_DF.index.max()) + "\n",
    )
    Tlog.info("Test Shape = " + str(TEST_DF.shape))
    Tlog.info(
        "Test Datetime Range = "
        + str(TEST_DF.index.min())
        + " -> "
        + str(TEST_DF.index.max())
        + "\n"
    )
    return {"TRAIN": TRAIN_DF, "VAL": VAL_DF, "TEST": TEST_DF}


split_dict = DataSplitter(resampled_FINAL_DF)


def NormalizeSplits(split_dict=split_dict):
    # Normalizing data sets via L2
    # Fitting to the training set
    norm = QuantileTransformer().fit(split_dict["TRAIN"])
    # Transforming all the split sets
    scaled_TRAIN_DF = pd.DataFrame(
        norm.transform(split_dict["TRAIN"]),
        columns=norm.get_feature_names_out(),
        index=split_dict["TRAIN"].index,
    )
    scaled_VAL_DF = pd.DataFrame(
        norm.transform(split_dict["VAL"]),
        columns=norm.get_feature_names_out(),
        index=split_dict["VAL"].index,
    )
    scaled_TEST_DF = pd.DataFrame(
        norm.transform(split_dict["TEST"]),
        columns=norm.get_feature_names_out(),
        index=split_dict["TEST"].index,
    )
    return norm, {
        "TRAIN": scaled_TRAIN_DF,
        "VAL": scaled_VAL_DF,
        "TEST": scaled_TEST_DF,
    }


norm, scaled_split_dict = NormalizeSplits(split_dict)

##########################################
# Data Windowing
##########################################

## Creating a multistep output to predict more that one window at a time
# OUT_STEPS = 7*2 # Each time step is 1 day. 7 days in a week
# INPUT_STEPS = 30*2 # Each time step is 1 day. About 30 days in a month
OUT_STEPS = 168 * 2  # Each time step is 1 hour. 168 hours in a week
INPUT_STEPS = 730 * 2  # Each time step is 1 hour. 730 hours in a month
num_features = resampled_FINAL_shape[1]

wg = Model_Utils.WindowGenerator(
    input_width=INPUT_STEPS,
    label_width=OUT_STEPS,
    label_columns=None,  # Originally just latitude
    shift=OUT_STEPS,
    split_dict=scaled_split_dict,
)

##########################################
# Training LSTM Model
##########################################

MAX_EPOCHS = 10
multi_val_performance = {}
multi_performance = {}


def compile_and_fit(model, window, patience=1):
    # Created using M1/M2 Mac hence the use of 
    # "tf.keras.optimizers.legacy.Adam"
    # instead of "tf.keras.optimizers.Adam"
    early_stopping = tf.keras.callbacks.EarlyStopping(
        monitor="val_loss", patience=patience, mode="min"
    )
    model.compile(
        loss=tf.keras.losses.MeanSquaredError(),
        optimizer=tf.keras.optimizers.Adam(),
        metrics=[tf.keras.metrics.MeanAbsoluteError()],
    )

    history = model.fit(
        window.train,
        epochs=MAX_EPOCHS,
        validation_data=window.val,
        callbacks=[early_stopping],
    )
    return history


multi_lstm_model = tf.keras.Sequential(
    [
        # Shape [batch, time, features] => [batch, lstm_units].
        # Adding more `lstm_units` just overfits more quickly.
        tf.keras.layers.LSTM(32, return_sequences=False),
        # Shape => [batch, out_steps*features].
        tf.keras.layers.Dense(
            OUT_STEPS * num_features,
            kernel_initializer=tf.initializers.zeros(),
        ),
        # Shape => [batch, out_steps, features].
        tf.keras.layers.Reshape([OUT_STEPS, num_features]),
    ]
)
train_start = datetime.now()
history = compile_and_fit(multi_lstm_model, wg)
IPython.display.clear_output()
train_finish = datetime.now() - train_start
print(
    "Total Training Time {d}:{h}:{m}".format(
        d=days_hours_minutes(train_finish)[0],
        h=days_hours_minutes(train_finish)[1],
        m=days_hours_minutes(train_finish)[2],
    )
)
Tlog.info(
    "\nTotal Training Time {d}:{h}:{m}".format(
        d=days_hours_minutes(train_finish)[0],
        h=days_hours_minutes(train_finish)[1],
        m=days_hours_minutes(train_finish)[2],
    )
)
multi_val_performance["LSTM"] = multi_lstm_model.evaluate(wg.val)
multi_performance["LSTM"] = multi_lstm_model.evaluate(wg.test, verbose=0)
multi_lstm_model.save(os.getcwd() + "/Bmore_Crime_Predict_Model.keras")

##########################################
# Calculating different forms of error
##########################################
geo_sigma_se = FINAL_DF.describe().loc["std"]
lat_error = geo_sigma_se[bcfe.geo_cols_[0]]
lon_error = geo_sigma_se[bcfe.geo_cols_[1]]
train_error = multi_val_performance["LSTM"][1]
test_error = multi_performance["LSTM"][1]
geo_uncertainty = math.sqrt((lat_error**2) + (lon_error**2))
model_error = math.sqrt((train_error**2) + (test_error**2))
print("Geo Uncertainty = ", geo_uncertainty)
print("Model Error = ", model_error)
Tlog.info("Geo Uncertainty = " + str(geo_uncertainty))
Tlog.info("Model Error = " + str(model_error))

##########################################
# Getting the latest crime data,
# preparing for inference, and
# exporting files for later use
##########################################
deploy_df = wg.test_df.tail(INPUT_STEPS)
deploy_df.to_csv(os.getcwd() + "/deploy.csv")  # saving deployment data
joblib.dump(norm, "normalizer.save")  # saving quantile transformer
joblib.dump(oe, "categorical_encoder.save")  # saving ordinal encoder
# Creating JSON file of metadata needed to deploy model
metadata_dict = {
    "INPUT_STEPS": INPUT_STEPS,
    "OUT_STEPS": OUT_STEPS,
    "column_indices": wg.column_indices,
    "geo_cols_": bcfe.geo_cols_,
}
with open("deploy_metadata.json", "w") as outfile:
    json.dump(metadata_dict, outfile)
