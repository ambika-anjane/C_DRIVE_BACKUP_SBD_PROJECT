import gzip
import pandas as pd
import glob
import requests
import requests
from genericAPI import genericAPI as genapi
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
from pytz import timezone
import sys
from datetime import date, datetime
from time import sleep

def get_url_paths(url, ext='', params={}):
    response = requests.get(url, params=params)
    if response.ok:
        response_text = response.text
     #print(response_text)
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    parent = [node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return parent

def main():
  try:
     
      now = datetime.now().strftime('%Y%m%d%H%M%S')
      tz = timezone('EST')
      processName = sys.argv[1]
      #base_url, src_rec_name, snowflakeConnection = genapi().loadFromCsv(processName)
      
      url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
      ext = 'gz'
      result = get_url_paths(url, ext)

      detailsList = []
      locationList = []
      fatalityList = []

      for i in result:
          if 'details' in i:
             detailsList.append(i)
          elif 'location' in i:
             locationList.append(i)
          elif 'fatalities' in i:
             fatalityList.append(i)
          else:
             continue
      
      if processName == 'NoaaStormEventDetailsConnection':
         processList = detailsList
      elif processName == 'NoaaStormLocationsConnection':
         processList = locationList
      elif processName == 'NoaaStormFatalityConnection':
          processList = fatalityList
         
          
      listOfYear = ['d'+str(year) for year in range(2017,2040)]
      processList = [file for file in processList for year in listOfYear  if year in file]

      count = 1
      df_noaa = pd.DataFrame()
      for stormInfo in processList:
         fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + stormInfo
         print(fileName)
         data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False,dtype = str)
         
         df_noaa = df_noaa.append(data)
         #df_noaa.iloc[:,18:19] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
         if count == 2:
           break
         count = count + 1
         df_noaa['SNAPSHOT_DATE'] = datetime.now(tz=None)
         print(df_noaa)
         
         
      '''
      #snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(stormLocationsConnection)
      ##snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().getNoaaSeriesUnits('NoaaSeriesUnitsConnection')
      snowflakeAccount = 'sbd_caspian.us-east-1'
      snowflakeUser = 'DEV_INGEST'
      snowflakePass = 'poonooD9fooBeth9'
      snowflakeWarehouse = 'DEV_INGEST_WH'
      snowflakeDatabase = 'DEV_RAW'
      snowflakeSchema = 'NOAA_WEATHER'
      ##snowflakeTableName = 'IHS_ECONOMIC_US_ECONOMY_HISTORICAL_SERIES_LANDING'
      ##snowflakeTableName = 'IHS_PRICING_AND_PURCHASING_HISTORICAL_SERIES_LANDING'
      snowflakeTableName = 'STORMEVENTS_DETAILS_LANDING'
      #snowflakeTableName = 'STORM_LOCATIONS_LANDING'
      #snowflakeTableName = 'STORM_FATALITIES_LANDING'

      df_noaa = genapi().df_include_landing_audit_columns(df_noaa, 'NOAA')
      print(df_noaa)
      df_noaa = genapi().fix_date_cols(df_noaa)
      print(df_noaa.columns)
      
      ##df_location.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)
      genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_noaa)
      '''
  except Exception as e: 
      #my_logger.error('An Exception occured in main function. Please check the parameter is available', e)
      sys.exit(1)   

if __name__ == '__main__':
    main()
