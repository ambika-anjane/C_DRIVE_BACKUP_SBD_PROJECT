# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 16:52:50 2022

@author: ambik
"""

import csv
import pandas as pd
import datetime as dt
#URL
#data1 = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2017_c20220124.csv.gz', compression='gzip',
#                  error_bad_lines=False)

#data2 = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2018_c20220217.csv.gz', compression='gzip',
#                   error_bad_lines=False)

#data3 = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2019_c20220214.csv.gz', compression='gzip',
#                   error_bad_lines=False)

#data4 = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2020_c20220217.csv.gz', compression='gzip',
#                   error_bad_lines=False)

#data5 = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2021_c20220217.csv.gz', compression='gzip',
#                   error_bad_lines=False)
'''
# WITH CSV FILES (FROM GZ)
data1 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2017_c20220124.csv',usecols=(['BEGIN_YEARMONTH']))
data2 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2018_c20220217.csv')
data3 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2019_c20220214.csv')
data4 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2020_c20220217.csv')
data5 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2021_c20220217.csv')
'''
data1 = pd.read_csv('C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d2017_c20220124.csv',usecols=(['BEGIN_YEARMONTH','BEGIN_DAY','BEGIN_TIME','END_YEARMONTH','END_DAY','END_TIME','EPISODE_ID','INJURIES_DIRECT','BEGIN_LAT','BEGIN_LON','END_LAT','END_LON','EVENT_NARRATIVE','DATA_SOURCE']))
#data1['End_Date'] = pd.to_datetime(data1['BEGIN_DATE_TIME']).dt.date
#data1['End_Time'] = pd.to_datetime(data1['BEGIN_DATE_TIME']).dt.time
df_finaldata1 = pd.DataFrame(data1)
#df_finaldata1['End_Date'] = data1['End_Date']
#df_finaldata1['End_Time'] = data1['End_Time']
df_finaldata1["Snapshot_Date"]  = dt.datetime.today().strftime("%Y%m%d")
df_finaldata1.to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data1.xlsx',header = True,index= True)
'''
data2 = pd.read_csv('C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d2018_c20220217.csv',usecols=(['BEGIN_DATE_TIME','DEATHS_INDIRECT','INJURIES_DIRECT','END_DATE_TIME','BEGIN_LAT','BEGIN_LON','END_LAT','END_LON','EVENT_NARRATIVE','DATA_SOURCE']))
data2['End_Date'] = pd.to_datetime(data2['END_DATE_TIME']).dt.date
data2['End_Time'] = pd.to_datetime(data2['END_DATE_TIME']).dt.time
data2['Begin_Date'] = pd.to_datetime(data2['BEGIN_DATE_TIME']).dt.date
data2['Begin_Time'] = pd.to_datetime(data2['BEGIN_DATE_TIME']).dt.time
print(data2.columns)

df_finaldata2 = pd.DataFrame(data2)
df_finaldata2["Snapshot_Date"]  = dt.datetime.today().strftime("%Y%m%d")
#print(data2.columns)
df_finaldata2_columns = df_finaldata2.columns
print("FINAL",df_finaldata2_columns)

df_sheet2 = pd.DataFrame()
df_sheet3 = pd.DataFrame(df_finaldata2)
df_sheet4 = pd.DataFrame(df_finaldata2)
df_sheet1 = df_finaldata2.iloc[:,10:14]
df_sheet3 = df_finaldata2.iloc[:,2:10]
df_sheet4 = df_finaldata2.iloc[:,14:15]
#print("Df_sheet4",df_sheet4)
#print("Df_sheet3",df_sheet3)
print("df_sheet1",df_sheet1)
#df_sheet2 = pd.concat([df_sheet1, df_sheet3], axis = 1)
#df_sheet2.head(5).to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= True)

df_sheet2 = pd.concat([df_sheet1, df_sheet3,df_sheet4], axis = 1)
print(df_sheet2.head(5))
#print(df_sheet2)
df_sheet2.head(5).to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= True)



df_finaldata1 = pd.DataFrame(data1)
df_finaldata2 = pd.DataFrame(data2)
df_finaldata3 = pd.DataFrame(data3)
df_finaldata4 = pd.DataFrame(data4)
df_finaldata5 = pd.DataFrame(data5)
df_finaldata1.append(data1)
df_finaldata2.append(data2)
df_finaldata3.append(data3)
df_finaldata4.append(data4)
df_finaldata5.append(data5)
df_finaldata1.to_csv(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data1.csv', mode='a',header = False,index=False)
df_finaldata2.to_csv(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data2.csv', mode='a',header = False,index=False)
df_finaldata3.to_csv(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data3.csv', mode='a',header = False,index=False)
df_finaldata4.to_csv(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data4.csv', mode='a',header = False,index=False)
df_finaldata5.to_csv(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data5.csv', mode='a',header = False,index=False)
list_of_datasets = [df_finaldata1,df_finaldata2,df_finaldata3,df_finaldata4,df_finaldata5]
for index, dataset in enumerate(list_of_datasets):
    print(index)
    print(dataset)
import csv
f = open('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2017_c20220124.csv', 'r')
reader = csv.reader(f)
column_names = next(reader, None)
print(column_names)
filtered_columns = [BEGIN_YEARMONTH for BEGIN_YEARMONTH in column_names if '202102' in BEGIN_YEARMONTH]
print('filtered_columns',filtered_columns)

for year in range(2018,2019):
    string  = 'd'
    string += str(year)
    print(year)
    print(string)
    data = pd.read_csv('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_'+string+'_c20220217.csv.gz', compression='gzip',error_bad_lines=False)
    for filename in data:
        if filename.endswith('.csv.gz'):
            print(filename)
    print(data.shape[0])
'''
