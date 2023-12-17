import pandas as pd
import sys
import yaml
import logging
#from generalapi_logger import *
from datetime import date, datetime
import requests_cache
from time import sleep
from genericAPI import genericAPI as genapi
from genericAPI import CustomException


now = datetime.now().strftime('%Y%m%d%H%M%S')
#my_logger = get_logger('ProcessExcelStaticFile_{}.log'.format(now))  

def main():
    try: 
        processName = sys.argv[1]
        ##file_instance = pd.ExcelFile('c:\EON\latlongbycityus.xlsx')
        excelFileName, srcSheet, srcRecName, requiredColumns, snowflakeConnection = genapi().getExcelFileLoadInfo(processName)
        print(requiredColumns)
        #my_logger.info("File Name is %s", excelFileName)
        #snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)  
        
        ##all_dfs = pd.read_excel(excelFileName, sheet_name=None, index_col=0)
        all_dfs = pd.read_excel(excelFileName, sheet_name=None, index_col=0)
        print(excelFileName)
        for key in all_dfs.keys():
           print('testing 2')
           print(key)
           
           print('testing 3')
           ##if key == 'Brand Data':
           if key == srcSheet:
               print('continue the process', srcSheet)
           else:
               print(key+' not required to be processed.')
               continue

           ## data frame for value k
           ##1 for alexa (with k,M string)
           df_sheet = all_dfs[key].reset_index(inplace=False)
           df_sheet.columns = requiredColumns
           df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
           if processName == 'urlListCanada':
             df_sheet['CURRENCY'] = 'CAD'
             df_sheet['SKU'] = df_sheet['SKU'].astype(str)
             df_sheet['ALEXA'] = df_sheet['ALEXA'].astype(str)
           if processName == 'urlListUs':
             df_sheet['CURRENCY'] = 'USD'
             df_sheet['SKU'] = df_sheet['SKU'].astype(str)
             df_sheet['ALEXA'] = df_sheet['ALEXA'].astype(str)
           if processName == 'urlListUs1':
             df_sheet['CURRENCY'] = 'USD'
             df_sheet['SKU'] = df_sheet['SKU'].astype(str)
             df_sheet['ALEXA'] = df_sheet['ALEXA'].astype(str)
           '''
           df_sheet['ALEXA']= df_sheet['ALEXA'].str.replace('k', '00')
           df_sheet['ALEXA'] = df_sheet['ALEXA'].str.replace('.', '')
           df_sheet['ALEXA']= df_sheet['ALEXA'].str.replace('M', '000000')
           print(df_sheet['ALEXA'])
           '''
           df_sheet = genapi().df_include_landing_audit_columns(df_sheet, srcRecName)
           df_sheet = genapi().fix_date_cols(df_sheet)
           df_sheet.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)
           
           snowflakeAccount = 'sbd_caspian.us-east-1'
           snowflakeUser = 'DEV_INGEST'
           snowflakePass = 'poonooD9fooBeth9'
           snowflakeWarehouse = 'DEV_INGEST_WH'
           snowflakeDatabase =  'DEV_RAW'
           snowflakeSchema = 'PROWL'
           snowflakeTableName = 'PROWL_FULL_URL_LIST_US_LANDING'
           genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_sheet)
           
    except Exception as e: 
       #my_logger.info("An exception occured.") 
       print('An Exception occured in main function.', e)
       sys.exit(1)   
       
if __name__ == '__main__':
    main()       
       
