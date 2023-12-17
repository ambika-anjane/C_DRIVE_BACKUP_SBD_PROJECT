import csv
import pandas as pd
import datetime as dt
from datetime import datetime

data2 = pd.read_csv('C:/ambika/SBD_PROJECT/NOAA/csv/StormEvents_details-ftp_v1.0_d2018_c20220217.csv',usecols=(['BEGIN_DATE_TIME','DEATHS_INDIRECT','INJURIES_DIRECT','END_DATE_TIME','BEGIN_LAT','BEGIN_LON','END_LAT','END_LON','EVENT_NARRATIVE','DATA_SOURCE']))
data2['Begin_Date'] = pd.to_datetime(data2['BEGIN_DATE_TIME']).dt.date
data2['Begin_Time'] = pd.to_datetime(data2['BEGIN_DATE_TIME']).dt.time
data2['End_Date'] = pd.to_datetime(data2['END_DATE_TIME']).dt.date
data2['End_Time'] = pd.to_datetime(data2['END_DATE_TIME']).dt.time
print(data2.columns)
df_finaldata2 = pd.DataFrame(data2)
df_finaldata2["Snapshot_Date"]  = dt.datetime.today().strftime("%Y%m%d")
#df_finaldata2["NEW_TIME"] = df_finaldata2.iloc[:,0:2]
df_finaldata2.iloc[:,0:2] = dt.datetime.now().strftime('%Y-%M-%D %H:%MI:%S')
#YYYY-MM-DD HH24:MI:SS
df_finaldata2_columns = df_finaldata2.columns
print("FINAL",df_finaldata2_columns)
df_sheet2 = pd.DataFrame()
df_sheet3 = df_finaldata2.iloc[:,0:2]
df_sheet4 = df_finaldata2.iloc[:,14:15]
df_sheet1 = df_finaldata2.iloc[:,10:14]
df_sheet2 = pd.concat([df_sheet1, df_sheet3,df_sheet4], axis = 1)
df_sheet2.head(5).to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= False)
#df_sheet3 = df_finaldata2["NEW_TIME"]
'''

df_sheet4 = df_finaldata2.iloc[:,14:15]
df_sheet2 = pd.concat([df_sheet1, df_sheet3,df_sheet4], axis = 1)
#print(df_sheet3.head(5))
#print(df_sheet2.head(5))
df_sheet2.head(5).to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= False)


#print("Df_sheet4",df_sheet4)
#print("Df_sheet3",df_sheet3)
#print("df_sheet1",df_sheet1)
#df_sheet2 = pd.concat([df_sheet1, df_sheet3], axis = 1)
#df_sheet2.head(5).to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= True)
'''
