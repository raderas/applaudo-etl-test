# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 00:02:03 2021

@author: Rafael Deras
"""
import json
import requests
import pandas as pd

def load_json_product_catalog():
    response = requests.get("https://etlwebapp.azurewebsites.net/api/products")
    container = json.loads(response.text)
    
    catalog = container['results'][0]
    item_catalog = catalog['items']
    
    df_catalog = pd.DataFrame(data=item_catalog)
    df_catalog.columns=['PRODUCT_NAME','AISLE','DEPARTMENT']
    df_catalog.set_index('PRODUCT_NAME', inplace=True)

    return df_catalog