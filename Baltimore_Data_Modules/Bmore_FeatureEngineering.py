#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  7 10:05:03 2023

@author: andres_principe
"""

import IPython
import tensorflow as tf
import pandas as pd
import numpy as np
from tqdm import tqdm
import requests
import json
import os
import math
import time
from datetime import date
from datetime import datetime
from sklearn.preprocessing import QuantileTransformer, OrdinalEncoder


class BmoreCrimeFeatureEngineering:
    """
    This class is meant to take in the "Part 1 Crime" data set from
    data.baltimorecity.gov. It encodes & formats the different features available
    into a form that will eventually be transformed for use in training
    an artifificial neural net.
    """

    def __init__(self, geo_cols_li, time_col_str, feat_col_dict):
        self.geo_cols_ = geo_cols_li
        self.time_col_ = time_col_str
        self.feature_cols_ = feat_col_dict
        self.day_ = 24 * 60 * 60
        self.year_ = (365.2425) * self.day_

    def CreateTimeIndexDF(self, df):
        new_df = df.copy()
        datetime_se = (
            new_df[self.time_col_]
            .astype(float)
            .div(1000.0)
            .apply(datetime.utcfromtimestamp)
        )
        new_df.index = datetime_se.rename("DATETIME")
        new_df = new_df.drop_duplicates()
        new_df = new_df.sort_index(ascending=False).drop_duplicates()
        return new_df

    def DropUnwantedFeatsDF(self, df):
        new_df = df.copy()
        new_df = new_df.drop(columns=self.feature_cols_["DROP"]).fillna(
            "UKNOWN"
        )
        new_df = new_df[new_df[self.geo_cols_[0]] != "UKNOWN"]
        return new_df

    def GeoStatsDF(self, df):
        df["LAT"] = df[self.geo_cols_[0]].astype("float")
        df["LON"] = df[self.geo_cols_[1]].astype("float")
        df["LAT_STDEV"] = df["LAT"].std()
        df["LON_STDEV"] = df["LON"].std()
        df = df.drop_duplicates()
        return df

    def CatFeatEncode(self, df):
        oe = OrdinalEncoder()
        Cat_X = df[self.feature_cols_["CATEGORICAL"]].values
        oe.fit(Cat_X)
        E_Cat_df = pd.DataFrame(
            oe.transform(Cat_X),
            columns=self.feature_cols_["CATEGORICAL"],
            index=df.index,
        )
        return E_Cat_df, oe

    def PeriodicityEncode(self, df):
        new_df = self.CreateTimeIndexDF(df)
        # turning pandas datetime to seconds
        new_df["seconds"] = new_df.index.map(pd.Timestamp.timestamp)
        # hourly frequency of events in radians
        new_df["Day sin"] = np.sin(new_df["seconds"] * (2 * np.pi / self.day_))
        new_df["Day cos"] = np.cos(new_df["seconds"] * (2 * np.pi / self.day_))
        # yearly frequency of events in radians
        new_df["Year sin"] = np.sin(
            new_df["seconds"] * (2 * np.pi / self.year_)
        )
        new_df["Year cos"] = np.cos(
            new_df["seconds"] * (2 * np.pi / self.year_)
        )
        new_df = new_df[["Day sin", "Day cos", "Year sin", "Year cos"]]
        return new_df

    def DropZeroGeos(self, df):
        float_df = df[self.geo_cols_].astype(float)
        initial_len = len(float_df)
        clean_df = df[float_df[self.geo_cols_[0]] != 0.0]
        clean_len = len(clean_df)
        print(str(initial_len - clean_len), "data points lack geos")
        return clean_df
