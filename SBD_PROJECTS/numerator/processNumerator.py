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
        print('testing 10')
        excelFileName, srcRecName, snowflakeConnection,startingHeaderLine, sheet1ColumnNumber, sheet3ColumnNumber, requiredColumns = genapi().getNumeratorFileLoadInfo(processName)
        print(requiredColumns)
        print('testing 1')
        #my_logger.info("File Name is %s", excelFileName)
        #snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)  
        
        print('testing 2')
        all_dfs = pd.read_excel(excelFileName, header = startingHeaderLine,  sheet_name=None, index_col=0)
        for key in all_dfs.keys():
           ##if key == 'Brand Data':
           print('testing 3')
           df_sheet = all_dfs[key].reset_index(inplace=False)
           df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
           df_sheet.columns = df_sheet.columns.str.replace(' ', '')
           
           df_sheet1 = df_sheet.iloc[:,sheet1ColumnNumber:]
           df_sheet3 = df_sheet.iloc[:,:sheet3ColumnNumber]
           
           print(df_sheet3)
           print('testing 4')
           df_sheet3.columns = requiredColumns

           df_sheet1_columns = df_sheet1.columns
           print(df_sheet3)
           df_sheet2 = pd.DataFrame()
           
           snowflakeAccount = 'sbd_caspian.us-east-1'
           snowflakeUser = 'DEV_INGEST'
           snowflakePass = 'poonooD9fooBeth9'
           snowflakeWarehouse = 'DEV_INGEST_WH'
           snowflakeDatabase =  'DEV_RAW'
           snowflakeSchema =  'NUMERATOR_PHASE2'
           snowflakeTableName = 'TOTAL_TOOLS_OUTDOORS_NO_LIGHTING_BRAND_SHARE_LANDING'
           
           df_sheet4 = pd.DataFrame()
           for iValue in range(0, len(df_sheet1.columns)):
               print('testing 5')
               df_sheet2 = pd.DataFrame()
               columnName = df_sheet1_columns[iValue]
               print(columnName)
               
               timeScale = columnName[0:3]
               print(timeScale)
               timePeriod = columnName[3:13]
               timeCoverage = columnName[14:]
               #timeScale = columnName[0:4]
               #timePeriod = columnName[4:14]
               #timeCoverage = columnName[15:]
               df_sheet2['VALUE'] = df_sheet1[columnName]
               print(df_sheet2)
               
               df_sheet2['VALUE'] = df_sheet2['VALUE'].replace('-',0)
               df_sheet2['TIME_SCALE'] = timeScale
               df_sheet2['TIME_PERIOD'] = timePeriod
               df_sheet2['TIME_COVERAGE'] = timeCoverage
               df_sheet2 = pd.concat([df_sheet3, df_sheet2], axis = 1) 
               df_sheet2 = df_sheet2.reset_index(drop=True)

               df_sheet2 = genapi().df_include_landing_audit_columns(df_sheet2, srcRecName)
               df_sheet2 = genapi().fix_date_cols(df_sheet2)
               
               df_sheet4 = df_sheet4.append(df_sheet2, ignore_index = True)
               df_sheet4.to_csv('C:/ambika/SBD_PROJECT/test.csv',index = False)
               
           genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_sheet4)
           print(df_sheet4.shape[0])
           print(df_sheet4)

          
    except Exception as e: 
       #my_logger.info("An exception occured.") 
       print('An Exception occured in main function.', e)
       sys.exit(1)   
       
if __name__ == '__main__':
    main()       
       
