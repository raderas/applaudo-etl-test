# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 05:40:13 2021

@author: Rafael Deras
"""
from sqlalchemy import create_engine
import pymysql
import pandas as pd


def load_orders_to_mysql(df_to_load):
    tableName   = 'STG_ORDERS'

    sqlEngine       = create_engine('mysql+mysqlconnector://root:df030185@35.231.15.41:3306/STAGE', pool_recycle=3600)

    dbConnection    = sqlEngine.connect()

 
    try:

        frame = df_to_load.to_sql(tableName, dbConnection, if_exists='append', index=False);

    except ValueError as vx:

        print(vx)

    except Exception as ex:   

        print(ex)

    else:

        print("Table %s loaded successfully."%tableName);   

    finally:

        dbConnection.close()

