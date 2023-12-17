import requests
import itertools
import json
from datetime import datetime
import pandas as pd
import yaml
import logging
#from generalapi_logger import *
import sys
from datetime import date, datetime, timedelta
import requests_cache
from time import sleep
from genericAPI import genericAPI as genapi
from genericAPI import CustomException 
from pytz import timezone
from ftfy import fix_text
import asyncio
import aiohttp
import os
import time
import json
import socket

username, password = genapi().getIHSMarkitInfo('ihsMarkitConnection')
login = username
now = datetime.now().strftime('%Y%m%d%H%M%S')
#my_logger = get_logger('IHSMarkit_{}.log'.format(now))
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
#my_logger.info("Starting..")

def retrieve_all_records(url, processType):
    all_records = []

    counter = 1
    while True:
        if not url:
            break
       
        #my_logger.info("processing the url {} ".format(url))
        print(counter)
        
        ##if counter == 2:
           ##break
        
        response = requests.get(url, auth=(login, password))
        if response.status_code == 200:
            json_data = json.loads(response.text)
            if processType == 'observations':
                all_records = json_data['value']
                break
            ##
            all_records = all_records + json_data['value']
            if '@odata.nextLink' in json_data:
               url = json_data['@odata.nextLink']
               counter = counter + 1
            else:
               break
    #my_logger.info("total number of series records is {}".format(len(all_records)))    
    return all_records


##get_tasks is not used.
results = []
def get_tasks(session, urls):
    tasks = []
    ##for symbol in symbols:
        ##tasks.append(asyncio.create_task(session.get(url)))
    tasks = [asyncio.create_task(session.get(url, ssl=False)) for url in urls]
    return tasks

async def get_symbols(vals):
    txt = []
    conn = aiohttp.TCPConnector(
        family=socket.AF_INET,
        ssl=False,
    )
    
    auth = aiohttp.BasicAuth(login=username, password=password, encoding='utf-8')
    urls = [val[0] for val in vals]
    async with aiohttp.ClientSession(connector=conn, auth=auth, trust_env=True) as session:
        ##tasks = get_tasks(session,urls)
        while True:
            try:
               tasks = [asyncio.create_task(session.get(url, ssl=False)) for url in urls]
               responses = await asyncio.gather(*tasks)
               ##results.append(await responses.json())
               for response in responses:
                   txt.append(await response.text())
               for i in range(len(vals)):
                   vals[i].append(txt[i])

               return vals
     
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
               print('CONNECTION ISSUE')
               await asyncio.sleep(15)
               continue
     
def main():
   try: 
      #my_logger.info("Starting IHS Markit Process..")
      
      processName = sys.argv[1]
      json_url, src_rec_name, snowflakeConnection = genapi().loadFromCsv(processName)
      
      json_data = retrieve_all_records(json_url, 'series')  
      
      lenJsonData = len(json_data)

      #snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName = genapi().get_snowflake_config(snowflakeConnection)
      startingCounter = 0
      endOfProcess = False
      df_final = pd.DataFrame()
      while True:
          print('value of startingCounter is ', startingCounter)
          print('value of len_json_data is', lenJsonData)
          endingCounter = startingCounter + 100
          if endingCounter >= lenJsonData:
              endingCounter = lenJsonData
              endOfProcess = True
          print('value of endingCounter is', endingCounter)
          jsonDataToProcess = json_data[startingCounter:endingCounter]
          startingCounter = startingCounter + 100
         
          link = r'{}'.format("observations@odata.navigationLink")
          jsonValue = jsonDataToProcess
          val1 = [[str(val[link]), str(val['source_id']), str(val)] for val in jsonValue]
      
          asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
          txt = asyncio.run(get_symbols(val1))
          
          df = pd.DataFrame(txt, columns = ['OBSERVATIONS_LINK','SOURCE_ID','SERIES_JSON_DATA', 'OBSERVATIONS_JSON_DATA'])
          df = genapi().df_include_landing_audit_columns(df, src_rec_name)
          df = genapi().fix_date_cols(df)
         
          df_final = df_final.append(df)
          print('df final..',df_final)

          if endOfProcess == True:
              break
      snowflakeAccount = 'sbd_caspian.us-east-1'
      snowflakeUser = 'DEV_INGEST'
      snowflakePass = 'poonooD9fooBeth9'
      snowflakeWarehouse = 'DEV_INGEST_WH'
      snowflakeDatabase = 'DEV_RAW'
      snowflakeSchema = 'IHSMARKIT'
      snowflakeTableName = 'IHS_CHINA_REGIONAL_HISTORICAL_SERIES_LANDING'
          
      genapi().write_snowflake_data(snowflakeAccount,  snowflakeUser, snowflakePass, snowflakeWarehouse, snowflakeDatabase, snowflakeSchema, snowflakeTableName,df_final)
      
      query_text = "UPDATE " + snowflakeTableName +  " SET OBSERVATIONS_JSON_DATA ='InvalidData' WHERE OBSERVATIONS_JSON_DATA LIKE '%<%'"  
      #my_logger.info('Updating Invalid data information..%s', query_text)
      query_text = (query_text)
      genapi().executeSnowflakeQueryParm(query_text, snowflakeConnection)   
       
   except Exception as e: 
      #my_logger.info("An exception occured.") 
      print('An Exception occured in main function.', e)
      sys.exit(1)       

if __name__ == '__main__':
    main()
