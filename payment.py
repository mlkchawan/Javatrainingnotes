from datetime import datetime, timedelta
from datetime import timedelta
import requests
import psycopg2
import json
import pandas as pd
import requests
import logging
import csv



## extract data from zuora 
def extract_from_zuora(**kwargs):

    base_url = 'https://rest.apisandbox.zuora.com'
    client_id ='2199b8d5-ca5f-4742-bb54-d20ad018fd6d'
    client_secret ='A6CrnzeUUCctMyeIf/dEAUmVb=yQLiEFGvhGv1h'


    #Generate OAuth token
    auth_url = f'{base_url}/oauth/token'
    auth_payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret 
    }

    auth_response = requests.post(auth_url, data=auth_payload)
    auth_data = auth_response.json()
    access_token = auth_data['access_token']
    logging.info(access_token)

# api lo fetch data 
    query_url = f'{base_url}/v1/action/query'
    query_payload =  {
                     "queryString": "select Createdbyid,PaymentNumber,Accountid,Paymentmethodid,Updateddate,source,Referenceid,Gatewayresponsecode,Status from payment limit 100",
                       
            }        
    headers = {
            'Authorization': f'Bearer {access_token}',  
            'Content-Type': 'application/json'         
        }
    
    api_response = requests.post(query_url, headers=headers, json=query_payload)
    data=api_response.json()
    print(data)
    if 'records' in data:
        records=data['records']
        df=pd.DataFrame(records)
        field_names=list(records[0].keys())

        with open ("dags/paymentData.csv",'w', newline='') as csvfile:
            writer=csv.DictWriter(csvfile,field_names)
            writer.writeheader()
            writer.writerows(records)
    else:
        print("error in generating")

extract_from_zuora()



