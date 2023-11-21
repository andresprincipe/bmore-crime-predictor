#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 16:08:03 2023

@author: andres_principe
"""

from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
import requests
from urllib.parse import urlparse, urlunparse

class TimeRangeBuild:
    """
    This class is meant to take today's datetime convert it into milliseconds.
    Then based on inputs into the class it determines the millisecond time 
    stamp for a date in the past. Ultimately it gives you a datetime range that 
    you can use as bounds for a query where the field is in milliseconds.
    """
    def __init__(self,yearsBack=0,monthsBack=0,daysBack=0):
        self.now_milli_ = int(time.time() * 1000)
        self.now_dt_ = datetime.fromtimestamp(self.now_milli_/1000.0)
        self.years_ = yearsBack
        self.months_ = monthsBack
        self.days_ = daysBack
    
    def nTimeRangeBack(self):
        nBack_dt = self.now_dt_ - relativedelta(years=self.years_,
                                                months=self.months_,
                                                days=self.days_)
        nBack_milli = int(nBack_dt.timestamp() * 1000)
        return nBack_milli, nBack_dt
                
    def CreateMinMaxDict(self):
        MinMax_dict = {}
        MinMax_dict['MAX'] = (self.now_milli_,self.now_dt_)
        MinMax_dict['MIN'] = self.nTimeRangeBack()
        return MinMax_dict
     
# trb = TimeRangeBuild(daysBack=7)
# print(trb.CreateMinMaxDict())

class APIOffset_UrlGather:
    """
    This class takes in an input of an API url taken from a given dataset on 
    the OpenBaltimore website: https://data.baltimorecity.gov/ 
    The functions of this class are able to modify the API urls and check for 
    the number of features in a GET request response. Combining these 
    functions allow you to programatically generate a list of working urls that
    you can use to obtain all the data in a given Open Baltimore dataset.
    """
    def __init__(self,init_url_str):
        self.url_ = init_url_str
        self.url_parsed_ = urlparse(init_url_str)
        self.query_ = self.url_parsed_.query
        self.queriesli_ = self.query_.split('&')
        
    def AddOffsetToURL(self,offset):
        initial_url = self.url_parsed_
        offsetAddition = 'resultOffset={}'.format(offset)
        paginationAddition = 'supportsPagination=true'
        new_queries_li = [offsetAddition]+[paginationAddition]+self.queriesli_
        new_query = '&'.join(new_queries_li)
        new_url_parsed = initial_url._replace(query=new_query)
        new_url = urlunparse(new_url_parsed)
        return new_url
    
    def CheckFeatures(self,url_str):
        response_num = requests.get(url_str).status_code
        if response_num != 200:
            print('HTTP code:',response_num,'no features')
        else:
            response_dict = requests.get(url_str).json()
            if len(response_dict['features']) > 0:
                return len(response_dict['features'])
            else:
                print('Request sucessful, but no features available')
                return None
            
    def GatherOffsetUrls(self,max_start=10000,page_limit=2000):
        """
        This function is used to programatically create all the urls necessary
        to obtain the entirety of a given Open Baltimore dataset. Each request 
        to Open Baltimore is limited to 2000 "rowIDs" as of 11/20/2023.
        """
        AllDataURLs_li = [self.url_]
        feature_num = self.CheckFeatures(self.url_)
        offset_num = 0
        while feature_num is not None:
            offset_num += 2000
            url = self.AddOffsetToURL(offset_num)
            feature_num = self.CheckFeatures(url)
            AllDataURLs_li.append(url)
        return AllDataURLs_li[:-1]

# apio_ug = APIOffset_UrlGather('https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/Part1_Crime_Beta/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson')
# print(apio_ug.GatherOffsetUrls())




