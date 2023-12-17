import requests, json, csv, sys, yaml, logging, sys, requests_cache, urllib, io, requests
import pandas as pd
from datetime import datetime
from generalapi_logger import *
from datetime import date, datetime
from time import sleep
from genericAPI import genericAPI as genapi
import sys

def main():
   
   try:
      processName = sys.argv[1]
      
      now = datetime.now().strftime('%Y%m%d%H%M%S')
      #my_logger = get_logger('COVID_Vaccine_Data.log'.format(now))
      #my_logger.info("Starting COVID Vaccine Data Process..") 

      #github_session = requests.Session()

      # providing raw url to download csv from github
      
      csv_url, src_rec_name, snowflakeConnection = genapi().loadFromCsv(processName)

      download = github_session.get(csv_url).content
      df_downloaded_csv = pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)

      df_downloaded_csv['SNAPSHOT_DATE'] = datetime.now(tz=None)
      
      ##src_rec_name = 'COVID_VACCINE_GLOBAL'
      
      df_downloaded_csv = genapi().df_include_landing_audit_columns(df_downloaded_csv, src_rec_name)      
      df_downloaded_csv = genapi().fix_date_cols(df_downloaded_csv)
      
      snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)
      
      query_text = "TRUNCATE TABLE " + snowflakeDatabase + "." + snowflakeSchema + "." + snowflakeTableName 
      my_logger.info('Truncating Table Table..%s', query_text)
      query_text = (query_text)
      genapi().executeSnowflakeQuery(query_text)

      genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_downloaded_csv)
      print(df_downloaded_csv.shape[0])

   except Exception as e: 
      my_logger.error('An Exception occured in main function. Please check the parameter is available', e)
      sys.exit(1)   

if __name__ == '__main__':
    main()
