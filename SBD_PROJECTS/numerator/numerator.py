import pandas as pd
import sys
#import yaml
import logging
#from generalapi_logger import *
from datetime import date, datetime
#import requests_cache
from time import sleep
#from genericAPI import genericAPI as genapi
#from genericAPI import CustomException



#now = datetime.now().strftime('%Y%m%d%H%M%S')
#my_logger = get_logger('ProcessExcelStaticFile_{}.log'.format(now))

def main():
 try:
   processName = sys.argv[1]
##file_instance = pd.ExcelFile('c:\EON\latlongbycityus.xlsx')
   excelFileName, srcRecName, snowflakeConnection,startingHeaderLine = genapi().getExcelFileLoadInfo(processName)
   #my_logger.info("File Name is %s", excelFileName)
   snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)
   all_dfs = pd.read_excel(excelFileName, header = startingHeaderLine, sheet_name=None, index_col=0)
## all_dfs = pd.read_excel(excelFileName, skiprows = startingHeaderLine, sheet_name=None, index_col=0)
   for key in all_dfs.keys():
##if key == 'Brand Data':
      df_sheet = all_dfs[key].reset_index(inplace=False)
      df_sheet.columns = map(lambda x: str(x).upper(), df_sheet.columns)
      df_sheet.columns = df_sheet.columns.str.replace(' ', '')
      df_sheet1 = df_sheet.iloc[:,5:]
      df_sheet3 = df_sheet.iloc[:,:5]
      df_sheet3.columns = ['METRIC','PRODUCT_AND_CATEGORY','CATEGORY_GROUP','QUARTERLY_SCORECARD_STORES','BRAND']
      print(df_sheet3.head(5))
      df_sheet1_columns = df_sheet1.columns
      df_sheet2 = pd.DataFrame()


   for iValue in range(0, len(df_sheet1.columns)):
     df_sheet2 = pd.DataFrame()
     columnName = df_sheet1_columns[iValue]
     timeScale = columnName[0:3]
     timePeriod = columnName[3:13]
     timeCoverage = columnName[14:]
     print(timeScale)
     print(timePeriod)
     df_sheet2['PERCENTAGE_VALUE'] = df_sheet1[columnName]
     df_sheet2['PERCENTAGE_VALUE'] = df_sheet2['PERCENTAGE_VALUE'].replace('-',0)
     df_sheet2['TIME_SCALE'] = timeScale
     df_sheet2['TIME_PERIOD'] = timePeriod
     df_sheet2['TIME_COVERAGE'] = timeCoverage
     df_sheet2 = pd.concat([df_sheet3, df_sheet2], axis = 1)
     df_sheet2 = df_sheet2.reset_index(drop=True)
     print(df_sheet2.columns)
##df_sheet = genapi().df_include_landing_audit_columns(df_sheet, srcRecName)
     df_sheet2 = genapi().fix_date_cols(df_sheet2)
     #genapi().write_snowflake_data(snowflakeAccount, snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_sheet2)
 except Exception as e:
    #my_logger.info("An exception occured.")
    print('An Exception occured in main function.', e)
    sys.exit(1)
if __name__ == '__main__':
 main()
