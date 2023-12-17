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
from datetime import datetime, timedelta, date
import datetime
import time

start_time = time.time()
#now = datetime.now().strftime('%Y%m%d%H%M%S')
#my_logger = get_logger('ProcessExcelStaticFile_{}.log'.format(now))

def main():
    try:
        processName = sys.argv[1]
        print(processName)
        excelFileName, srcSheet, srcRecName, Required_Columns, snowflakeConnection = genapi().getExcelFileLoadInfo(processName)
        euro_dfs = pd.read_excel(excelFileName, srcSheet , index_col=False)
        euro_dfs.columns = Required_Columns
        df_sheet = pd.DataFrame(euro_dfs)
        df_sheet.columns = Required_Columns
        print(df_sheet.columns)
        df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
        df_sheet = genapi().df_include_landing_audit_columns(df_sheet, srcRecName)
        df_sheet = genapi().fix_date_cols(df_sheet)
        print(df_sheet)
        
        snowflakeAccount = 'sbd_caspian.us-east-1'
        snowflakeUser = 'DEV_INGEST'
        snowflakePass = 'poonooD9fooBeth9'
        snowflakeWarehouse = 'DEV_INGEST_WH'
        snowflakeDatabase = 'DEV_RAW' 
        snowflakeSchema = 'EUROMONITOR'
        snowflakeTableName = "MARKET_SIZEIN_VALUE_LANDING"
        
        
        genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse,
                                            snowflakeDatabase, snowflakeSchema, snowflakeTableName, df_sheet)
        
        print("--- %s seconds ---" % (time.time() - start_time))
    except Exception as e:
        #my_logger.info("An exception occured.")
        print('An Exception occured in main function.', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
