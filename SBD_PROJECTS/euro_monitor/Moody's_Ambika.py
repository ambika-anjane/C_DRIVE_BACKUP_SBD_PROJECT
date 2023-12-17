####
# Data Buffet API
# Code sample: Python
# 1 December 2017
# (c)2017 Moody's Analytics

import requests
import hashlib
import hmac
import datetime
import json
import numpy as np

from pandas.io.json import json_normalize
import pandas as pd
import pandas as pd
from time import sleep

#####
# Function: Make API request, including a freshly generated signature.
#
# Arguments:
# 1. Part of the endpoint, i.e., the URL after "https://api.economy.com/data/v1/"
# 2. Your access key.
# 3. Your personal encryption key.
# 4. Optional: default GET, but specify POST when requesting action from the API.
#
# Returns:
# HTTP response object.
def api_call(apiCommand, accKey, encKey, call_type="GET"):
 url = "https://api.economy.com/data/v1/" + apiCommand
 timeStamp = datetime.datetime.strftime(
 datetime.datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
 payload = bytes(accKey + timeStamp, "utf-8")
 signature = hmac.new(bytes(encKey, "utf-8"), payload, digestmod=hashlib.sha256)
 head = {"AccessKeyId":accKey,
 "Signature":signature.hexdigest(),
 "TimeStamp":timeStamp}
 sleep(1)
 if call_type == "POST":
  response = requests.post(url, headers=head)
 elif call_type =="DELETE":
  response = requests.delete(url, headers=head)
 else:
  response = requests.get(url, headers=head)

  return(response)


#####
# Setup:
# 1. Store your access key, encryption key, and basket name.
# Get your keys at:
# https://www.economy.com/myeconomy/api-key-info
ACC_KEY = "375E0931-5F22-4861-A179-A3AB15B18827"
ENC_KEY = "63176248-FF12-4684-85D9-A3EED8F047C3"
BASKET_NAME = "Driversall_03232021"
#BASKET_NAME = "all indicators"



#####
# Identify a basket to execute:
# 2. Get list of baskets.

# 3. Extract the basket with a given name, and save its ID for later.
baskets = pd.DataFrame(json.loads(api_call("baskets/", ACC_KEY, ENC_KEY).text))
print(baskets.columns)
print('testing1')
basketInfo = baskets.loc[baskets["name"]==BASKET_NAME]
print(basketInfo)
print(type(basketInfo))
sleep(10)
basketId = baskets.loc[baskets["name"]==BASKET_NAME, 'basketId'].item()
print("Basket ID: " + basketId)
print("Basket Name: " + BASKET_NAME)



# 4. Execute a particular basket using its ID.
# This requires that the optional argument "type" be set to "POST".
call = ("orders?type=baskets&action=run&id=" + basketId)
print(call)
order = api_call(call, ACC_KEY, ENC_KEY, call_type="GET")
print(type(order))
#df = pd.DataFrame(order, columns = ['Mnemonic'])
#print(df)
orderId = order.text[9:19]

    
    
'''    
   
#####
# Download the output:
# 5. Periodically check if the order has completed.
call = "orders/" + orderId
print(call)
processing_check = True
while processing_check:
 #sleep(5)
 status = api_call(call, ACC_KEY, ENC_KEY)
 processing_check = json.loads(status.content.decode('utf-8'))['processing']
 print('processing: ' + str(processing_check))
'''

# 6. Download completed output.
new_call = ("orders?type=baskets&id=" + basketId)
get_basket = api_call(new_call, ACC_KEY, ENC_KEY)
get_basket = (str(get_basket.content).split("\\r\\n"))


print(get_basket)

# 7. Format the data frame.
data_df= pd.DataFrame(get_basket)
data_df = data_df[0].str.split(',', expand=True)
data_df_columns = data_df.iloc[0:5,0]
list_of_columns = data_df_columns.values.flatten().tolist()
print(list_of_columns)
df1 = pd.DataFrame(columns = list_of_columns)
data_df_moodys = df1
print(data_df_moodys)


ivalue = 1



for iValue in range(1, len(data_df.columns)):
 df1 = pd.DataFrame()
 df1 = pd.DataFrame(columns = list_of_columns)

 mnemonic = (data_df.iloc[0:1,iValue]).to_string(index=False)
 geography = (data_df.iloc[1:2,iValue]).to_string(index=False)
 description = (data_df.iloc[2:3,iValue]).to_string(index=False)
 sourceValue = (data_df.iloc[3:4,iValue]).to_string(index=False)
 nativeFrequency = (data_df.iloc[4:5,iValue]).to_string(index=False)

 dateValue = data_df.iloc[5:,[0,iValue]]

 dateValue.columns = ['value','date']

 df1 = pd.concat([df1, dateValue], axis = 1)

 df1['Mnemonic'] = mnemonic
 df1['Geography'] = geography
 df1['Description'] = description
 df1['Source'] = sourceValue
 df1['Native Frequency'] = nativeFrequency

 data_df_moodys = data_df_moodys.append(df1, ignore_index = True)




 print('finally')
 print(data_df_moodys.head(10))

 data_df_moodys.to_csv('D:\SF\SBD_PROJECT_DOCS\moodys\moodys_data.csv', index=False)

''' (my code starts here)
headers0 = data_df.iloc[0]
headers1 = data_df.iloc[1]
headers2 = data_df.iloc[2]
headers3 = data_df.iloc[3]
headers4 = data_df.iloc[4]
data_df.columns = [headers0,headers1,headers2,headers3,headers4]
for d in data_df.columns:
    if 'FGDP$.IUSA'  in d:
     print("d",d)
     df_row1 = pd.DataFrame(d)
     #print("DF0",d,headers0)
     df_row1.style.hide_index()
     df_row1_trans = df_row1.transpose()
    
    
     #headers
     df_mne = pd.DataFrame(headers0)
     df_geo = pd.DataFrame(headers1)
     df_desc = pd.DataFrame(headers2)
     df_source = pd.DataFrame(headers3)
     df_native = pd.DataFrame(headers4)
     
     #print(df)
     #header0
     #indexes_to_drop  = df.index[[1,3]]
     columns_to_keep = [x for x in range(df_mne.shape[0]) if x not in [0]]
     indexes_to_keep = set(range(df_mne.shape[0])) - set(columns_to_keep)
     df_header_mnemonic = df_mne.take(list(indexes_to_keep))                          
     #header1
     columns_to_keep = [x for x in range(df_geo.shape[0]) if x not in [0]]
     indexes_to_keep = set(range(df_geo.shape[0])) - set(columns_to_keep)
     df_header_geo = df_geo.take(list(indexes_to_keep))
     df_header_geo_trans = df_header_geo.transpose()
     #header2
     columns_to_keep = [x for x in range(df_desc.shape[0]) if x not in [0]]
     indexes_to_keep = set(range(df_desc.shape[0])) - set(columns_to_keep)
     df_header_desc = df_desc.take(list(indexes_to_keep))
     df_header_desc_trans = df_header_desc.transpose()
     #header3
     columns_to_keep = [x for x in range(df_source.shape[0]) if x not in [0]]
     indexes_to_keep = set(range(df_source.shape[0])) - set(columns_to_keep)
     df_header_source = df_source.take(list(indexes_to_keep))
     df_header_source_trans = df_header_source.transpose()
     #header4
     columns_to_keep = [x for x in range(df_native.shape[0]) if x not in [0]]
     indexes_to_keep = set(range(df_native.shape[0])) - set(columns_to_keep)
     df_header_native = df_native.take(list(indexes_to_keep))
     df_header_native_trans = df_header_native.transpose()
     #All headers)
     df_header0_header1_header2_header3_header4 = pd.concat([df_header_mnemonic,df_header_geo_trans,df_header_desc_trans,df_header_source_trans,df_header_native_trans])
     df_header0_header1_header2_header3_header4_trans = df_header0_header1_header2_header3_header4.transpose()
     df_header0_header1_header2_header3_header4_trans.dropna()
     print(df_header0_header1_header2_header3_header4_trans)     
     
     #final frmae
     f = r'D:\SF\SBD_PROJECT_DOCS\moodys\moodys_data.csv'
     #pd.concat([dfff,df1, df3],   axis=1).to_excel(f, header=False)
     #frames1 = [dfff,df1, df3]
     frames1 = [df_header0_header1_header2_header3_header4_trans,df_row1_trans]
     with open(f, mode='a+') as f1:
      for dff in frames1:
          dff.to_csv(f1, mode='a',index=False,header = False)
          #f1.write('\n')
'''    
# not needed
data_df = data_df.set_index(data_df["Mnemonic"])

data_df = data_df[:-1]
data_df.dropna(axis=1, how='all')
filter = data_df != ""
data_df = data_df[filter]




#num_rows = str(len(data_df.index))
#print(num_rows)
num_columns = str(data_df.columns)
print(num_columns)
#num_columns = str(len(data_df.columns))
#print(num_columns)
