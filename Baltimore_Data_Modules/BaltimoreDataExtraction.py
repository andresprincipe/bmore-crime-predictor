# -*- coding: utf-8 -*-
import ipydeps
ipydeps.pip(['requests','tqdm'])
import pandas as pd
import numpy as np
from tqdm import tqdm
import requests
import json
import os
from datetime import date



class BmoreDataExtraction:
    """
    This class is meant to download a large number of JSON files based on an 
    input dictionary that contains API links to Baltimore's Open Data.
    
    The input dictionary must be in the following format:
        {'DATA_TYPE_1':
            {'DATA_NAME_A':'API_LINK_A',
             'DATA_NAME_B':'API_LINK_B',
             'DATA_NAME_C':'API_LINK_C'},
         'DATA_TYPE_2':
            {'DATA_NAME_A':'API_LINK_A',
             'DATA_NAME_B':'API_LINK_B',
             'DATA_NAME_C':'API_LINK_C'}
        }
    Example:
        {'TABULAR_DATA':
            {'Part 1 Crime Data':'https://opendata.baltimorecity.gov/egis/rest/ ...so on so forth'}}

    Baltimore's Open Data uses arcgis to store most (if not all) of their 
    data. This class can also help you change multiple storage formats of 
    baltimore data into a pandas DataFrame for other uses.
    """
    def __init__(self,API_dict):
        self.API_dict_ = API_dict
        self.data_types_ = list(API_dict.keys())
    
    def CleanDataNames(self,some_list):
        clean_list = [dataName.replace(' ','_').replace(',','').replace('-','').replace('/','_')
                      for dataName in some_list]
        return clean_list
    
    def LinkCheck(self,data_type_str):
        HTTP_Status_dict = {data_name:requests.get(self.API_dict_[data_type_str][data_name]).status_code 
                            for data_name in self.API_dict_[data_type_str].keys()}
        available_li = [name if HTTP_Status_dict[name] == 200 else None 
                        for name in HTTP_Status_dict.keys()]
        return HTTP_Status_dict, available_li

    def createDataPath(self,existing_dir=None):
        cwd = os.getcwd()
        if existing_dir is None:
            Bmore_data_path = cwd+'/Baltimore_Data/'
            return Bmore_data_path 
        else:
            Bmore_data_path = cwd+'/'+existing_dir+'/Baltimore_Data/'
            return Bmore_data_path
    
    def DownloadDataType(self,data_type_str,chosen_dir=None):
        HTTP_Status_dict, available_li = self.LinkCheck(data_type_str)
        clean_available_li = self.CleanDataNames(available_li)
        OGName_CleanName_dict = dict(zip(available_li,clean_available_li))
        print('Downloading Available Baltimore Data...')
        if chosen_dir is None:
            path_str = self.createDataPath()
        if chosen_dir is not None:
            path_str = self.createDataPath(chosen_dir)
        if os.path.exists(path_str) is False:
            os.mkdir(path_str)
        DataPath_dict = {}
        for data_name in tqdm(available_li):
            filename_str = OGName_CleanName_dict[data_name]+'_'+str(date.today())+'.json'
            full_path_str = path_str+filename_str
            r = requests.get(self.API_dict_[data_type_str][data_name])
            with open(full_path_str, "w+") as f:
                json.dump(r.json(), f)
            DataPath_dict[data_name] = full_path_str
        return DataPath_dict
    
    def FeatureCollectionToDF(self,DataPath_dict):
        print('Turning Feature Collections data Into DataFrames...')
        FEAT_DATA_DF_dict = {}
        for key in tqdm(DataPath_dict.keys()):
            f = open(DataPath_dict[key])
            oneTableData_dict = json.load(f)
            FEAT_DATA_DF_dict[key] = self.ExtractAllProperties(oneTableData_dict)
        return FEAT_DATA_DF_dict
        
    def GenerateMeanGeoDict(self,lon_lat_ar):
        # This function expects an input of a 2d numpy array where the first
        # column is the Longitude and the second column is the Latitude.
        mean_geo_ar = np.mean(lon_lat_ar,axis=0)
        std_geo_ar = np.std(lon_lat_ar,axis=0)
        row_dict = {'MEAN_LAT':[mean_geo_ar[1]],
                    'MEAN_LON':[mean_geo_ar[0]],
                    'STD_LAT':[std_geo_ar[1]],
                    'STD_LON':[std_geo_ar[0]]}
        return row_dict
    
    def ExtractAllProperties(self,oneDataFile_dict):
        try:
            row_li = [sub_dict['properties'] for sub_dict in oneDataFile_dict['features']]
            return pd.DataFrame(row_li)
        except KeyError:
            print('Successful connection, but no data.')
            return pd.DataFrame([0])
        
    
    def ExtractAllCoordinates(self,oneDataFile_dict):
        AREAgeo_ar_li = [np.array(AREA['geometry']['coordinates'][0]) 
                        for AREA in oneDataFile_dict['features']]
        GeoStatFrames_li = []
        for ar in AREAgeo_ar_li:
            # check to see if shape is length of 2 
            # then generate means and standard deviations.
            if len(ar.shape) > 2:
                bad_shape_tu = ar.shape
                new_shape_tu = (bad_shape_tu[1],bad_shape_tu[2])
                new_ar = np.reshape(ar,new_shape_tu)
                new_row_dict = self.GenerateMeanGeoDict(new_ar)
                GeoStatFrames_li.append(pd.DataFrame(new_row_dict))
            else:
                row_dict = self.GenerateMeanGeoDict(ar)
                GeoStatFrames_li.append(pd.DataFrame(row_dict))
        meanGeos_df = pd.concat(GeoStatFrames_li).reset_index(drop=True)
        return meanGeos_df

    def GeoDataToDF(self,DataPath_dict):
        # This function can extract all the "Community Statistical Area" (CSA) data 
        # and City level data specified in your API link dictionary.
        print('Turning Geo JSON data into DataFrames...')
        GEO_DATA_DF_dict = {}
        for key in tqdm(DataPath_dict.keys()):
            f = open(DataPath_dict[key])
            oneGeoData_dict = json.load(f)
            # This DataFrame contains the properties of a given Community Statistical Area (CSA)
            properties_df = self.ExtractAllProperties(oneGeoData_dict)
            # This DataFrame contains the mean latitudes and mean longitudes of a given
            # Community Statistical Area (CSA)
            meanGeos_df = self.ExtractAllCoordinates(oneGeoData_dict)
            GEO_DATA_df = properties_df.merge(meanGeos_df,left_index=True,right_index=True)
            GEO_DATA_DF_dict[key] = GEO_DATA_df
        return GEO_DATA_DF_dict
