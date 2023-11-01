# -*- coding: utf-8 -*-

import os
import sys
import json
import pandas as pd
module_path = r''+os.getcwd()+'/Baltimore_Data_Modules'
sys.path.append(module_path)
from BaltimoreDataExtraction import BmoreDataExtraction

API_filename = 'Bmore_API_Crime_link.json'
f = open(os.getcwd()+'/'+API_filename)
Bmore_API_Crime_link_dict = json.load(f)
bde = BmoreDataExtraction(Bmore_API_Crime_link_dict)
print('Types of Data:',bde.data_types_)

Full_Crime_dict = {}
fullDataNames_li = []
for data_type in bde.data_types_:
    data_path_dict = bde.DownloadDataType(data_type)
    partialDataName_li = bde.CleanDataNames(list(data_path_dict.keys()))
    fullDataNames_li.append(partialDataName_li)
    if data_type == 'TABULAR_DATA':
        data_df_dict = bde.TabularDataToDF(data_path_dict)
        Full_Crime_dict[data_type] = data_df_dict
    else:
        data_df_dict = bde.GeoDataToDF(data_path_dict)
        Full_Crime_dict[data_type] = data_df_dict

fullDataNames_li = [string for sublist in fullDataNames_li for string in sublist]
pd.Series(fullDataNames_li).to_csv('CrimeFileNames.csv')