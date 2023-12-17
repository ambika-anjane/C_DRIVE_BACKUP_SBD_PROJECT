import gzip
import pandas as pd
import glob
import requests
import requests
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import datetime as dt
import urllib
from urllib import request
#from urllib import urlopen
from urllib.request import urlopen

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
     


#Uri myUri = new Uri(URLInStringFormat, UriKind.Absolute);


#url = urlopen('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/',timeout = 30)
r = request.urlopen('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/')
print(r.url)
ext = 'gz'






result = get_url_paths(r.url, ext)
print(result)
print(type(result))

detailsList = []
locationList = []
fatalityList = []
detailsList1 = []




'''
for i in result:
  if 'details' in i:
     detailsList.append(i)
  elif 'location' in i:
     print('Location')
     locationList.append(i)
  elif 'fatalities' in i:
     fatalityList.append(i)
  else:
     continue
'''



for i in result:
   if 'details' in i:
      detailsList.append(i)
count = 1
df_details = pd.DataFrame()
for detail in detailsList:
   print(detail)
   fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + detail
   print(fileName)
   data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False)
   print(data)
   
   df_details = df_details.append(data)
   df_details["SNAPSHOT_DATE"]  = dt.datetime.today().strftime("%Y%m%d")
   if count == 70:
      break
   count = count + 1

print(df_details)
df_details.to_csv('C:/ambika/SBD_PROJECT/test.csv',index = False)
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new =  r"C:/ambika/SBD_PROJECT/test.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORMEVENTS_DETAILS_LANDING")
print("Table Loaded:")
cur = conn.cursor()

'''
for i in result:
   if 'details-ftp_v1.0_d1976' in i:
      detailsList1.append(i)
count = 26
df_details = pd.DataFrame()
for detail1 in detailsList1:
   print(detail1)
   fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + detail1
   print(fileName)
   data1 = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False)
   print(data1)
   
   df_details1 = df_details1.append(data)
   df_details1["SNAPSHOT_DATE"]  = dt.datetime.today().strftime("%Y%m%d")
   if count == 5:
      break
   count = count + 1

print(df_details1)

df_details.to_csv('C:/ambika/SBD_PROJECT/test.csv',index = False)
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new =  r"C:/ambika/SBD_PROJECT/test.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORMEVENTS_DETAILS_LANDING")
print("Table Loaded:")
cur = conn.cursor()

'''
