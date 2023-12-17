import pandas as pd
'''
file_list=['StormEvents_details-ftp_v1.0_d2017_c20220124.csv','StormEvents_details-ftp_v1.0_d2019_c20220214.csv']
  
main_dataframe = pd.DataFrame(pd.read_csv(file_list[0]))
  
for i in range(1,len(file_list)):
    data = pd.read_csv(file_list[i])
    df = pd.DataFrame(data)
    main_dataframe = pd.concat([main_dataframe,df],axis=1)
print(main_dataframe)
'''
import pandas as pd
import glob
import datetime as dt
import logging
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

  
folder_path = 'C:/ambika/SBD_PROJECT/NOAA/csv'
file_list = glob.glob(folder_path + "/*.csv")
main_dataframe = pd.DataFrame(pd.read_csv(file_list[0]))
for i in range(0,len(file_list)):
    data = pd.read_csv(file_list[i])
    df = pd.DataFrame(data)
    #df.reset_index(drop=True, inplace=True)
    #print(data)
    #main_dataframe = pd.concat([main_dataframe,df],axis=1)
    #print(main_dataframe)
    data['Begin_Date'] = pd.to_datetime(data['BEGIN_DATE_TIME']).dt.date
    data['Begin_Time'] = pd.to_datetime(data['BEGIN_DATE_TIME']).dt.time
    data['End_Date'] = pd.to_datetime(data['END_DATE_TIME']).dt.date
    data['End_Time'] = pd.to_datetime(data['END_DATE_TIME']).dt.time
    #print(data.columns)
    df_finaldata2 = pd.DataFrame(data)
    df_finaldata2["Snapshot_Date"]  = dt.datetime.today().strftime("%Y%m%d")
    df_finaldata2_columns = df_finaldata2.columns
    #print("FINAL",df_finaldata2_columns)
    df_sheet2 = pd.DataFrame()
    df_sheet3 = pd.DataFrame(df_finaldata2)
    df_sheet4 = pd.DataFrame(df_finaldata2)
    #df_sheet1 = df_finaldata2.iloc[:,17:18]
    #df_sheet3 = df_finaldata2.iloc[:,19:20]
    df_sheet3 = df_finaldata2.iloc[:,0:17]
    #df_sheet4.columns = ['Begin_Date','Begin_Time','End_Date','End_Time']
    #df_sheet4 = df_finaldata2.iloc[:,51:55]
    df_sheet5 = df_finaldata2.iloc[:,21:51]
    df_sheet6 = df_finaldata2.iloc[:,55:56]
    df_sheet2 = pd.concat([df_sheet3,df_sheet5,df_sheet6], axis = 1)
    print(df_sheet2.head)
    #df_sheet2.to_excel(r'C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx',header = True,index= False)
    #print(df_sheet3,df_sheet4,df_sheet5)
    '''
    logging.basicConfig(filename='app.log',level=logging.INFO)
    try:
      logging.info('Trying to open the file')
      filePointer = open('C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx','r')
      try:
        logging.info('Trying to read the file content')
        content = filePointer.readlines()
      finally:
        filePointer.close()
    except IOError as e:
       logging.error('Error occurred ' + str(e))
    
    conn = snow.connect(user='AMBIKAS',
    password="Ambika#22",
    account = "https://app.snowflake.com/ap-southeast-2/rn43388/w54HqszgL7X2#query",
    warehouse="COMPUTE_WH",
    database="NOAA",
    schema="NOAA_WEATHER")
    '''
    conn = snow.connect(user = 'DEV_INGEST',
    password = 'poonooD9fooBeth9',
    account = 'sbd_caspian.us-east-1',
    warehouse = 'DEV_INGEST_WH',
    database = 'DEV_RAW',
    schema = 'NOAA_WEATHER')
    #TableName = 'FRED_SERIES_OBSERVATIONS_DATA_LANDING1'
    print("connection success")

    new = r"C:/ambika/SBD_PROJECT/NOAA/novaa_data2.xlsx"    
    total = pd.read_excel(new)
    write_pandas(conn, total, "STORMEVENTS_DETAILS_LANDING")
    print("table created:")

    cur = conn.cursor()



'''
data1 = pd.read_csv('D:/SF/SBD_PROJECT_DOCS/NOAA/csv/StormEvents_details-ftp_v1.0_d2017_c20220124.csv',usecols=(['DEATHS_INDIRECT','INJURIES_DIRECT','END_DATE_TIME','BEGIN_LAT','BEGIN_LON','END_LAT','END_LON','EVENT_NARRATIVE','DATA_SOURCE']))
df_finaldata1 = pd.DataFrame(data1)
df_finaldata1["Snapshot_Date"]  = dt.datetime.today().strftime("%Y%m%d")
df_finaldata1.to_excel(r'D:/SF/SBD_PROJECT_DOCS/NOAA/novaa_data1.xlsx',header = True,index= True)
'''
