import requests, json, csv, sys, yaml, logging, sys, requests_cache, urllib, io, requests
import pandas as pd
from datetime import datetime
#from generalapi_logger import *
from datetime import date, datetime
from time import sleep
from genericAPI import genericAPI as genapi
import sys


def main():
   
   try:
      processName = sys.argv[1]
      print(processName)
      flatFileName, srcRecName, Required_Columns, snowflakeConnection = genapi().loadFromFlatFile(processName)
      print(flatFileName)
      print(snowflakeConnection)
      df_downloaded_csv = pd.read_csv(flatFileName, header = 0,  error_bad_lines=False, dtype=str)
      df_downloaded_csv.columns = Required_Columns
      print(df_downloaded_csv.columns)
      df_downloaded_csv = genapi().df_include_landing_audit_columns(df_downloaded_csv, srcRecName)      
      df_downloaded_csv = genapi().fix_date_cols(df_downloaded_csv)
      #snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config('econMnemonicMappingConnection')
      
      snowflakeAccount = 'sbd_caspian.us-east-1'
      snowflakeUser = 'DEV_INGEST'
      snowflakePass = 'poonooD9fooBeth9'
      snowflakeWarehouse = 'DEV_INGEST_WH'
      snowflakeDatabase = 'DEV_RAW'
      snowflakeSchema = 'SI_MARKETPLACE_REF_DATA'
      snowflakeTableName = 'SI_ECON_MNEMONIC_MAPPING_LANDING'
      
      genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_downloaded_csv)
      print(df_downloaded_csv.shape[0])
      
      
      
     
   except Exception as e: 
      #my_logger.error('An Exception occured in main function. Please check the parameter is available', e)
      sys.exit(1)   

if __name__ == '__main__':
    main()
