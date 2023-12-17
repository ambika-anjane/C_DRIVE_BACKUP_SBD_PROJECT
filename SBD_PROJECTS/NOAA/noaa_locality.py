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
     #print(parent)
     







'''
for i in result:
  if 'details' in i:
     detailsList.append(i)
  elif 'location' in i:
     print('Location')
     locationList.append(i)
  elif 'fatalities' in i:
     fatalityList.append(i)
  else:
     continue
'''



#df_location.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)

def main():
  try:
      #df_location1 = get_url_paths(url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/',
#ext = 'gz')
      now = datetime.now().strftime('%Y%m%d%H%M%S')
      tz = timezone('EST')
      processName = sys.argv[1]
      print("Proces",processName)
      
      url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
      ext = 'gz'
      #base_url, src_rec_name, snowflakeConnection = genapi().loadFromCsv(processName)
      #print("SRC",base_url)
      result = get_url_paths(url, ext)
      print(result)
      
      detailsList = []
      locationList = []
      fatalityList = []
      #for i in result:
       # if 'detailsList' in i:
        #  locationList.append(i)
      for year in range(2017,2019):
         string  = 'd'
         string += str(year)
         print(year)
         year = year + 1
         print(string)
      for i in result:
         if year in i:
          locationList.append(i)
          df_location = pd.DataFrame()
        count = 1
        for loca in locationList:
         print(loca)
         fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2017_c20220318.csv.gz' + loca
         print(fileName)
         data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False)
         print(data)
         df_location = df_location.append(data)
         if count == 5:
           break
         count = count + 1
         df_location["SNAPSHOT_DATE"]  = dt.datetime.today().strftime("%Y%m%d")
         print(df_location)
         
         #snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(stormLocationsConnection)
         #print(snowflakeAccount)
         #
         snowflakeAccount = 'sbd_caspian.us-east-1'
         snowflakeUser = 'DEV_INGEST'
         snowflakePass = 'poonooD9fooBeth9'
         snowflakeWarehouse = 'DEV_INGEST_WH'
         snowflakeDatabase = 'DEV_RAW'
         snowflakeSchema = 'NOAA_WEATHER'
         snowflakeTableName = 'IHS_ECONOMIC_US_ECONOMY_HISTORICAL_SERIES_LANDING'
         snowflakeTableName = 'IHS_PRICING_AND_PURCHASING_HISTORICAL_SERIES_LANDING'
         snowflakeTableName = 'STORM_LOCATIONS_LANDING'
         #snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(stormLocationsConnection)
         #snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().getNoaaSeriesUnits('NoaaSeriesUnitsConnection')
         print(snowflakeAccount)
         df_location = genapi().df_include_landing_audit_columns(df_location, src_rec_name = 'Noaa')
         print(df_location)
         df_location1 = genapi().fix_date_cols(df_location)
         df_location1.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)
         genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_downloaded_csv)

  except Exception as e: 
      #my_logger.error('An Exception occured in main function. Please check the parameter is available', e)
      sys.exit(1)   

if __name__ == '__main__':
    main()

'''
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new =  r"C:/ambika/SBD_PROJECT/test2.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORM_LOCATIONS_LANDING")
print("table created:")
cur = conn.cursor()

'''
