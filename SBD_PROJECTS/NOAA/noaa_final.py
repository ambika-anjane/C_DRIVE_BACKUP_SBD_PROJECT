
'''
import pandas as pd
count = 1
for year in range(2017,2019):
    string  = 'd'
    string += str(year)
    print(year)
    print(string)
    data = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_'+string+'_c20220217.csv.gz', compression='gzip',error_bad_lines=False)
    for filename in data:
        if filename.endswith('.csv.gz'):
            print(filename)
    print(data.shape[0])
    count = count + 1
'''

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
    base_url, src_rec_name, snowflakeConnection = genapi().loadFromCsv(processName) 
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
      processList = [file for file in processList for year in listOfYear if year in file]
      count = 1
      df_noaa = pd.DataFrame()
      print(df_noaa)
      for stormInfo in processList:
              fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + stormInfo
              print(fileName)
              data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False, dtype = str)
              df_noaa = df_noaa.append(data)
              count = count + 1
              df_noaa['SNAPSHOT_DATE'] = datetime.now(tz=None)
              print(df_noaa)
              snowflakeAccount = 'sbd_caspian.us-east-1'
              snowflakeUser = 'DEV_INGEST'
              snowflakePass = 'poonooD9fooBeth9'
              snowflakeWarehouse = 'DEV_INGEST_WH'
              snowflakeDatabase = 'DEV_RAW' 
              snowflakeSchema = 'NOAA_WEATHER'
              snowflakeTableName = 'STORM_FATALITIES_LANDING'
              df_noaa = genapi().df_include_landing_audit_columns(df_noaa, 'NOAA')
              print(df_noaa)
              df_noaa = genapi().fix_date_cols(df_noaa)
              print(df_noaa.columns)
              ##df_location.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)
              genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_noaa)
  except Exception as e:
        sys.exit(1)
        #my_logger.error('An Exception occured in main function. Please check the parameter is available', e)

if __name__ == '__main__':
  main()

##snowflakeTableName = 'IHS_ECONOMIC_US_ECONOMY_HISTORICAL_SERIES_LANDING'
##snowflakeTableName = 'IHS_PRICING_AND_PURCHASING_HISTORICAL_SERIES_LANDING'
#snowflakeTableName = 'STORMEVENTS_DETAILS_LANDING'
#snowflakeTableName = 'STORM_LOCATIONS_LANDING'

   










#snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(stormLocationsConnection)
##snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().getNoaaSeriesUnits('NoaaSeriesUnitsConnection')


'''
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new = r"C:/ambika/SBD_PROJECT/test2.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORM_LOCATIONS_LANDING")
print("table created:")
cur = conn.cursor()
'''

