
import requests
import sys
'''
r = requests.get('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/')
r.status_code
#print(r.status_code)
if r.status_code == 400:
    print(r.status_code)
'''

def retrieve_all_records(url):
    all_records = []

    login = 'SharedStanleyBlack@sbdinc.com'
    password = 'L4fa9733'
    r = requests.get(url, auth=(login, password))
    #r = requests.get(url)
    #while(r.status_code == 200):
    if 'null' in r.text:
         print("Yes Present")
    while(r.status_code == 204):
        print("Content Not Found")
    else:
        print("Content FOund")
    
    
    #response = requests.get(url)
    #if response.status_code == 200:
     #   print(response.status_code)
    #else:
     #   print(response.status_code)
url = 'https://api.connect.ihsmarkit.com/dataplatform/v1/odata/Pricing_and_Purchasing_Historical_API_Data'   
retrieve_all_records(url)

    

# testing with file  
'''       
file1 = open('C:/ambika/SBD_PROJECT/IHS/test_file.txt', 'r')
Lines = file1.readlines()
for l in Lines:
    if 'http Not Found' in l:
      print("Yes present")
      print(l)
'''
