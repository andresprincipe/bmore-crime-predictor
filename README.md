# bmore-crime-predictor
## This is a project that uses Baltimore city's publicly available datasets to predict the geolocation of new crimes. 
- Main source of the data can be found at https://data.baltimorecity.gov/
- Time series forecasting was based off of the tensorflow time series tutorials. Created by modifying the examples at https://www.tensorflow.org/tutorials/structured_data/time_series
- Bmore_Crime_Predictor_Train.py is a python script that trains a keras model on the most recent baltimore crime data
- Bmore_Crime_Predictor_Deploy.py is a python script that generates future geolocations of crime based off the most recent crimes in Baltimore city
- Forecast_Map_Generator.py is a python script that visualizes the predicted geolocation forcast of potential future crimes in Baltimore city
- ForecastMap.html is an html file that can be opened in a browser to visualize the most recent crime location predictions.
- Bmore_Crime_Predictor.ipynb is a notebook that visualizes the functionality of the machine learning model
