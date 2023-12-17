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



result = get_url_paths(url, ext)
print(result)
print(type(result))

detailsList = []
locationList = []
fatalityList = []




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
   if 'fatalities' in i:
     fatalityList.append(i)
count = 1
df_fatality = pd.DataFrame()
for fatal in fatalityList:
   print(fatal)
   fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + fatal
   print(fileName)
   data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False)
   print(data)
   
   df_fatality = df_fatality.append(data)
   df_fatality["SNAPSHOT_DATE"]  = dt.datetime.today().strftime("%Y%m%d")
   if count == 75:
      break
   count = count + 1

print(df_fatality)
df_fatality.to_csv('C:/ambika/SBD_PROJECT/test1.csv',index = False)
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new =  r"C:/ambika/SBD_PROJECT/test1.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORM_FATALITIES_LANDING")
print("table Loaded:")
cur = conn.cursor()

