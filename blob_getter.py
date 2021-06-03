# -*- coding: utf-8 -*-
"""
blob-getter.py
Archivo con datos de conexión al storage account de azure para obtener 
los archivos csv.
"""

import os
from azure.storage.blob import BlobServiceClient, __version__
import sql_server_getter as sg
import json_loader as jl
import mysql_loader as ml
from datetime import datetime

import pandas as pd

dow_dict={
    'Sunday':0,
    'Monday':1,
    'Tuesday':2,
    'Wednesday':3,
    'Thursday':4,
    'Friday':5,
    'Saturday':6
}


def check_isnumeric(to_check):
    return str(to_check).isnumeric()


def dow_convert(value):
    resp=str(value)
    if resp.isnumeric()==False:
        resp=dow_dict.get(value,0)
    
    return resp

def generate_df_to_load():
    dataframe=pd.DataFrame({'ORDER_ID':pd.Series([],dtype='int'),
                            'USER_ID':pd.Series([],dtype='int'),
                            'ORDER_NUMBER':pd.Series([],dtype='int'),
                            'ORDER_DOW':pd.Series([],dtype='int'),
                            'ORDER_HOUR_OF_DAY':pd.Series([],dtype='int'),
                            'PRODUCT':pd.Series([],dtype='object'),
                            'AISLE':pd.Series([],dtype='object'),
                            'ADD_TO_CART_ORDER':pd.Series([],dtype='int'),
                            'DAYS_SINCE_PRIOR_ORDER':pd.Series([],dtype='int')})
    return dataframe

connect_str = 'BlobEndPoint=https://orderstg.blob.core.windows.net/;SharedAccessSignature=?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https&sig=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D'

# Using a local directory to hold blob data
local_path = ".\data"
#os.mkdir(local_path)

try:
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")
    
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    container_client = blob_service_client.get_container_client('ordersdow')
    # Quick start code goes here
    print("\nListing blobs...")
    
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    
    #Creating dataframe
    dataframe_csv = pd.DataFrame(columns=['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY',
       'DAYS_SINCE_PRIOR_ORDER', 'ORDER_DETAIL'])
    
    for blob in blob_list:
        print('Iniciando obtención de los archivos ' + str(datetime.now(tz=None)))

        # Downloading blobs
        download_file_path = os.path.join(local_path, str.replace(blob.name,'.csv','DOWNLOAD.csv'))
        print("\nDownloading blob to \n\t" + download_file_path)
        
        blob_client = blob_service_client.get_blob_client('ordersdow',blob)
        
        with open(download_file_path, "wb") as download_file:
            
            download_file.write(blob_client.download_blob().readall())
        
        #Reading csv file contents into dataframe
        dataframe_csv=dataframe_csv.append(pd.read_csv(download_file_path))
    
except Exception as ex:
    print('Exception:')
    print(ex)

# Reading data from sql server
dataframe_sql = sg.read_orders_from_sqlserver()

# Copying column names
dataframe_sql.columns = dataframe_csv.columns

# Appending the dataframes
dataframe_full = dataframe_csv.append(dataframe_sql)
#Converting data types
dataframe_full['ORDER_ID']=pd.to_numeric(dataframe_full['ORDER_ID'])
dataframe_full['USER_ID']=pd.to_numeric(dataframe_full['USER_ID'])
dataframe_full['ORDER_NUMBER']=pd.to_numeric(dataframe_full['ORDER_NUMBER'])
dataframe_full['ORDER_HOUR_OF_DAY']=pd.to_numeric(dataframe_full['ORDER_HOUR_OF_DAY'])
dataframe_full['DAYS_SINCE_PRIOR_ORDER']=pd.to_numeric(dataframe_full['DAYS_SINCE_PRIOR_ORDER'],downcast='integer')

dow_convert_ls=list(map(dow_convert,dataframe_full['ORDER_DOW']))
dataframe_full['ORDER_DOW']=pd.to_numeric(dow_convert_ls)


# Getting product data from web api
product_catalog=jl.load_json_product_catalog()


#df_to_load = pd.DataFrame(columns=['ORDER_ID','USER_ID','ORDER_NUMBER','ORDER_DOW','ORDER_HOUR_OF_DAY','PRODUCT','AISLE','ADD_TO_CART_ORDER','DAYS_SINCE_PRIOR_ORDER'])
df_to_load = generate_df_to_load()
contador_lin_procs=0

print('Iniciando normalización de detalles de órdenes ' + str(datetime.now(tz=None)))

for data_line in dataframe_full.iterrows():
    #print('bucle')
    
    #Getting order lines
    product_lines = data_line[1][6].split('~')

    #splitting each order line
    order_detail= []
    for line in product_lines:
        order_detail.append(line.split('|'))
    
    #constructing lines to load to user formatted table
    cols=['ORDER_ID','USER_ID','ORDER_NUMBER','ORDER_DOW','ORDER_HOUR_OF_DAY']
    for detail in order_detail:
        line_to_load = [data_line[1][i] for i in cols]
        line_to_load.append(detail[0])
        line_to_load.append(detail[1])
        line_to_load.append(detail[2])
        line_to_load.append(data_line[1]['DAYS_SINCE_PRIOR_ORDER'])
    
        series = pd.Series(line_to_load,index=df_to_load.columns)
        df_to_load=df_to_load.append(series,ignore_index=True)
    
    contador_lin_procs=contador_lin_procs+1
    if contador_lin_procs % 1000 == 0:
        print( str(contador_lin_procs) + ' líneas procesadas. ' + str(datetime.now(tz=None)))
    
    # Sending a batch of processed lines to data warehouse.
    if contador_lin_procs % 2001 == 0:
        print('Enviando batch a base de datos. ' + str(contador_lin_procs) + ' lineas procesadas')
        # Joining with product catalog to get the department
        df_to_load = df_to_load.join(product_catalog, on='PRODUCT', rsuffix='_CAT')

        #Removing redundant AISLE_CAT column
        df_to_load.drop(['AISLE_CAT'],axis='columns', inplace=True)

        #Sending batch to database
        ml.load_orders_to_mysql(df_to_load)      

        #Resetting dataframe
        #df_to_load = pd.DataFrame(columns=['ORDER_ID','USER_ID','ORDER_NUMBER','ORDER_DOW','ORDER_HOUR_OF_DAY','PRODUCT','AISLE','ADD_TO_CART_ORDER','DAYS_SINCE_PRIOR_ORDER'])
        df_to_load = generate_df_to_load()
        continue
    
    # Hard limit for data processing
    if contador_lin_procs == 140000:
        print('Terminando. ' + str(contador_lin_procs) + ' líneas procesadas.  ' + str(datetime.now(tz=None)))
        break

# Joining with product catalog to get the department
df_to_load = df_to_load.join(product_catalog, on='PRODUCT', rsuffix='_CAT')

#Removing redundant AISLE_CAT column
df_to_load.drop(['AISLE_CAT'],axis='columns', inplace=True)

print('Enviando ultima subida de data a mysql. ' + str(contador_lin_procs) + ' lineas procesadas.')

#Last data upload to mysql
ml.load_orders_to_mysql(df_to_load)

print('Finalizado...')

    




