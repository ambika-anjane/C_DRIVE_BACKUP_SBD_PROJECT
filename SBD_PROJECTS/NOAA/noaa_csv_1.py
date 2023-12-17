import pandas as pd
import glob
import datetime as dt
import logging
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

import requests
from bs4 import BeautifulSoup
import gzip
import csv

import pandas as pd

data = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/',  compression='gzip',
                   error_bad_lines=False)
print(data)

'''
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
     



url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
ext = 'gz'

result = get_url_paths(url, ext)
print(result)

df  = pd.DataFrame(result)
print(df)


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
     



url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
ext = 'gz'


result = get_url_paths(url, ext)
import os
#print(result)
res_1 = []
res_2 = []
res_3 = []
for r in result:
  if 'StormEvents_details' in r:
        res_1.append(r)
print("res1",res_1)



#for r in result:
    #print(r)
    if 'StormEvents_details' in r:
        res_1.append(r)
    print(res_1)
    
    elif 'StormEvents_locations' in r:
        res_2.append(r)
        print(res_2)
    elif 'StormEvents_fatalities' in r:
        res_3.append(r)
        print(res_3)
'''

'''
    root_path = 'C:/ambika/SBD_PROJECT/NOAA/folder1'
    #os.mkdir(path)
    path = os.path.join(root_path, r)
    print(path)
    
#print(res)



#list = ['folder1']
#for items in list:
 #   os.mkdir(items)






dest = 'C:/ambika/SBD_PROJECT/NOAA/folder1/StormEvents_details-ftp_v1.0_d1950_c20210803.csv.gz'
f = gzip.open(dest, 'r')
file_content = f.read()
file_content = file_content.decode('utf-8')
f_out = open('file', 'w+')
f_out.write(file_content)
f.close()
f_out.close()

#df  = pd.DataFrame(result)
#print(df)

import xlrd
 
loc = (r'C:/ambika/SBD_PROJECT/NOAA/out.xls')
 
wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(0)
 
sheet.cell_value(0, 0)
 
print(sheet.row_values(1))
df_row  = pd.DataFrame(sheet.row_values(1))
print(df_row)






import xlrd
 
loc = (r'C:/ambika/SBD_PROJECT/NOAA/out.xls')
 
wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(0)
 
sheet.cell_value(0, 0)
 
print(sheet.row_values(1))
csvFile = gzip.open(sheet.row_values(1))
#csvFile = gzip.open('C:/ambika/SBD_PROJECT/NOAA/out.xls', 'rb')  # Open in text mode, not binary, no line ending translation
reader = csv.reader(csvFile)
print(reader)


    


folder_path = 'C:/ambika/SBD_PROJECT/NOAA/csv'
file_list = glob.glob(folder_path + "/*.csv")
main_dataframe = pd.DataFrame(pd.read_csv(file_list[0]))
for i in range(0,len(file_list)):
    data = pd.read_csv(file_list[i])
    df = pd.DataFrame(data)
    df_finaldata2 = pd.DataFrame(data)
    df_finaldata2["SNAPSHOT_DATE"]  = dt.datetime.today().strftime("%Y%m%d")
    df_finaldata2_columns = df_finaldata2.columns
    #print("FINAL",df_finaldata2_columns)
    df_sheet2 = pd.DataFrame()
    df_sheet3 = pd.DataFrame(df_finaldata2)
    df_sheet3 = df_finaldata2.iloc[:,0:17]
    df_sheet5 = df_finaldata2.iloc[:,21:56]
    df_sheet2 = pd.concat([df_sheet3,df_sheet5], axis = 1)
    print(df_sheet2)
    df_sheet2.to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= False)
    conn = snow.connect(user = 'DEV_INGEST',
    password = 'poonooD9fooBeth9',
    account = 'sbd_caspian.us-east-1',
    warehouse = 'DEV_INGEST_WH',
    database = 'DEV_RAW',
    schema = 'NOAA_WEATHER')
    print("connection success")
    new = r"C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx"    
    total = pd.read_excel(new)
    write_pandas(conn, total, "STORMEVENTS_DETAILS_LANDING")
    print("table created:")
    cur = conn.cursor()
'''   



