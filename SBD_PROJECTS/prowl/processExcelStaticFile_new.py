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
        
        #my_logger.info("File Name is %s", excelFileName)
        #snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)  
        
        ##all_dfs = pd.read_excel(excelFileName, sheet_name=None, index_col=0)
        all_dfs = pd.read_excel(excelFileName, sheet_name=None, index_col=0)
        for key in all_dfs.keys():
           print('testing 2')
           print(key)
           print('testing 3')
           ##if key == 'Brand Data':
           if key == srcSheet:
               print('continue the process')
           else:
               print(key+' not required to be processed.')
               continue
           df_sheet = all_dfs[key].reset_index(inplace=False)
           df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
           df_sheet.columns = df_sheet.columns.str.replace(' ', '')
           print(df_sheet.head(5))
           df_sheet.columns = requiredColumns
           df_sheet['SKU'] = df_sheet['SKU'].astype(str)
           df_sheet['ALEXA'] = df_sheet['ALEXA'].astype(str)
           df_sheet['ALEXA']= df_sheet['ALEXA'].str.replace('.k', '0')
           print(df_sheet['ALEXA'])
           df_sheet = genapi().df_include_landing_audit_columns(df_sheet, srcRecName)
           df_sheet = genapi().fix_date_cols(df_sheet)
           print(df_sheet.columns)
           snowflakeAccount = 'sbd_caspian.us-east-1'
           snowflakeUser = 'DEV_INGEST'
           snowflakePass = 'poonooD9fooBeth9'
           snowflakeWarehouse = 'DEV_INGEST_WH'
           snowflakeDatabase =  'DEV_RAW'
           #genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_sheet)
    except Exception as e: 
       #my_logger.info("An exception occured.") 
       print('An Exception occured in main function.', e)
       sys.exit(1)   
       
if __name__ == '__main__':
    main()       
       
