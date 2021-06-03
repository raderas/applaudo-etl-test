# -*- coding: utf-8 -*-
"""
Created on Sun May 30 07:31:12 2021

@author: Rafael Deras
"""

### Reading order data from SQL Server into pandas dataframe
import pandas as pd
import pyodbc

def read_orders_from_sqlserver():
    sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};' + 
                            'SERVER=orderservers.database.windows.net;' +
                            'DATABASE=orderdb;' +
                            'UID=etlguest;' +
                            'PWD=Etltest_2020') 
    query = "SELECT * from dbo.[order_details]"
    
    df = pd.read_sql(query, sql_conn)

    return df
    
    
#dataframe_sql=read_orders_from_sqlserver()