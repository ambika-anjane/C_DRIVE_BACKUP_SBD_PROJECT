import gzip
import pandas as pd
'''
data = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d1951_c20210803.csv.gz', compression='gzip',
                   error_bad_lines=False)

print(data)
df = pd.DataFrame(data)
df.to_csv(r'C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d1950_c20210803',header = True,index= False)
import gzip
with gzip.open('C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d1950_c20210803.csv.gz') as f:
    header = f.readlines()
    print(header)

#print(df)
#df.to_csv(r'C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d1950_c20210803'),header = True,index= False)
#df.to_csv(r'C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d1951_c20210803',header = True,index= False)


import requests
url = 'https://public.bitmex.com/?prefix=data/trade/20191026.csv.gz'
r = requests.get(url, allow_redirects=True)
open('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d1951_c20210803.csv.gz', 'wb').write(r.content)
print(r.content)
'''
import gzip
import glob
import requests
'''
with gzip.open(r'C:/ambika/SBD_PROJECT/NOAA/StormEvents_details-ftp_v1.0_d1958_c20210803.csv.gz','rt') as f: 
 text = f.readlines()
 df = pd.DataFrame(text)
 print(df)
 df.to_csv(r'C:/ambika/SBD_PROJECT/NOAA/StormEvents_details-ftp_v1.0_d1958_c20210803.csv')

folder_path = 'C:/ambika/SBD_PROJECT/NOAA/gz'
file_list = glob.glob(folder_path + "/*.gz")
for i in range(0,len(file_list)):
    print(file_list[i])
    with gzip.open(file_list[i],'rt') as f:
        text = f.readlines()
        #print(text)
        data = pd.read_csv(file_list[i])
        df = pd.DataFrame(data)
        print(data)
        df.to_csv(r'C:/ambika/SBD_PROJECT/NOAA/StormEvents_details-ftp_v1.0_d1958_c20210803.csv')

    

    

from bs4 import BeautifulSoup
import re

#response = requests.get(url)
#print(response.content)

url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d1951_c20210803.csv.gz'
def getHTMLdocument(url):
      
    # request for HTML document of given url
    response = requests.get(url)
      
    # response will be provided in JSON format
    return response.text
  
# create document
html_document = getHTMLdocument(url)
  
# create soap object
soup = BeautifulSoup(html_document, 'html.parser')
  
  
# find all the anchor tags with "href" 
# attribute starting with "https://"
for link in soup.find_all('S', 
                          attrs={'href': re.compile("^https://")}):
    # display the actual urls
    print(link)  


import requests
from bs4 import BeautifulSoup



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

df = pd.DataFrame(result)
print(df)

for d in df:
    print(d)
'''
import requests
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


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
   if count == 2:
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
print("table created:")
cur = conn.cursor()


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
   if count == 2:
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
print("table created:")
cur = conn.cursor()

for year in range(2017,2019):
    string  = 'd'
    string += str(year)
    print(year)
    year = year + 1
    print(string)
for i in result:
   if year in i:
     locationList.append(i)
count = 1
df_location = pd.DataFrame()
for loca in locationList:
   print(loca)
   fileName = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/' + loca
   print(fileName)
   data = pd.read_csv(fileName, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False,low_memory=False)
   print(data)
   
   df_location = df_location.append(data)
   if count == 2:
      break
   count = count + 1

print(df_location)
df_location.to_csv('C:/ambika/SBD_PROJECT/test2.csv',index = False)
conn = snow.connect(user = 'DEV_INGEST',
password = 'poonooD9fooBeth9',
account = 'sbd_caspian.us-east-1',
warehouse = 'DEV_INGEST_WH',
database = 'DEV_RAW',
schema = 'NOAA_WEATHER')
print("connection success")
new =  r"C:/ambika/SBD_PROJECT/test2.csv"
total = pd.read_csv(new,low_memory=False)
write_pandas(conn, total, "STORM_LOCATIONS_LANDING")
print("table created:")
cur = conn.cursor()

