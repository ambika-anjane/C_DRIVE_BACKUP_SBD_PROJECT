import pandas as pd
import sys
import yaml
import logging
from generalapi_logger import *
from datetime import date, datetime
import requests_cache
from time import sleep
from genericAPI import genericAPI as genapi
from genericAPI import CustomException

now = datetime.now().strftime('%Y%m%d%H%M%S')
my_logger = get_logger('ProcessExcelStaticFile_{}.log'.format(now))


def main():
    try:
        processName = sys.argv[1]

        excelFileName, srcRecName, snowflakeConnection = genapi().getExcelFileLoadInfo(processName)

        #my_logger.info("File Name is %s", excelFileName)
        snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(
            snowflakeConnection)

        df1 = pd.read_excel (excelFileName,sheet_name='Monthly_Data_amazon.com',index_col=False,dtype=str)
        df2 = pd.read_excel (excelFileName,sheet_name='Monthly_Data_lowes.com',index_col=False,dtype=str)
        df3 = pd.read_excel (excelFileName,sheet_name='Monthly_Data_homedepot.com',index_col=False,dtype=str)
        df4 = pd.read_excel (excelFileName,sheet_name='Monthly_Data_truevalue.com',index_col=False,dtype=str)

        all_dfs = pd.concat([df1, df2, df3, df4])

        for key in all_dfs.keys():
            df_sheet = all_dfs
            df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
            df_sheet.columns = df_sheet.columns.str.replace(' ', '_')
            #Below code is to convert the columns which has special characters like Pages/Visits in Similar Web Excel
            df_sheet.columns = df_sheet.columns.str.replace('\/', '_')
            df_sheet.columns = df_sheet.columns.str.replace('\_+', '_')
            #Below code is for the Channel Traffic column which has values like <5000
            df_sheet['CHANNEL_TRAFFIC'] = df_sheet['CHANNEL_TRAFFIC'].map(lambda x: str(x).lstrip('<')).astype(str)
            df_sheet['CHANNEL_TRAFFIC'] = df_sheet['CHANNEL_TRAFFIC'].str.replace(',', '')
            df_sheet = genapi().df_include_landing_audit_columns(df_sheet, srcRecName)
            df_sheet = genapi().fix_date_cols(df_sheet)

        print(df_sheet['CHANNEL_TRAFFIC'])
        #genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse,
        #                                  snowflakeDatabase, snowflakeSchema, snowflakeTableName, df_sheet)
    except Exception as e:
        #my_logger.info("An exception occured.")
        print('An Exception occured in main function.', e)
        sys.exit(1)


if __name__ == '__main__':
    main()