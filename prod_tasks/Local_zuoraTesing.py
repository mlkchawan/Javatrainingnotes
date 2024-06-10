
import requests
import psycopg2
import json
import pandas as pd
import requests
import logging
import csv
import logging
import time
import io

def extract_revenueforecasting_from_zuora(**kwargs):
    base_url = 'https://rest.zuora.com'
    client_id ='f520c62a-0a73-4b31-be91-d9f460c280c7'
    client_secret ='zceUZlriX4NT1UCscwbiJXkHeHegVqmwr0ogWF6'
    
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
    # zuora_object='contact'
    try:
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "Rateplancharge",
                        "query": f'''select Account.Id as AccountId,Account.Name as AccountName,Account.AccountNumber as 
                            AccountNumber,RatePlan.Id as RatePlanId, RatePlan.Name as RatePlanName, RatePlan.AmendmentType as 
                            RatePlanAmendmentType, RatePlan.CreatedDate as RatePlanCreatedDate,Product.Name as ProductName, 
                            Product.ProductFamily__c as ProductFamily, ProductRatePlan.ratePlanId__c as ProductRatePlanId,
                            Subscription.Id as SubscriptionId,
                            Subscription.Name as SubscriptionName, Subscription.OriginalId as SubscriptionOriginalId, 
                            Subscription.PreviousSubscriptionId as PreviousSubscriptionId, Subscription.Status as 
                            SubscriptionStatus,Subscription.TermType as SubscriptionTermType,Subscription.RenewalTerm as 
                            SubscriptionRenewalTerm, 
                            Subscription.Version as SubscriptionVersion, Subscription.termStartDate as 
                            SubscriptiontermStartDate, Subscription.termEndDate as SubscriptiontermEndDate, 
                            Subscription.CancelledDate as SubscriptionCancelledDate,
                            Subscription.CreatedDate as SubscriptionCreatedDate,RatePlanCharge.ChargedThroughDate as 
                            ChargedThroughDate,RatePlanCharge.MRR as MRR,RatePlanCharge.TCV as TCV from RatePlanCharge where 
                            RatePlanCharge.ChargedThroughDate  >= '2024-06-01' and 
                            RatePlanCharge.ChargedThroughDate  <= '2024-06-30' and Subscription.Status = 'Active' and Account.Currency = 'USD' order by 
                            Subscription.CreatedDate ASC; ''',
                        "type": "zoqlexport"
                        }
                        ]
            }        
        logging.info(query_payload)

        headers = {
            'Authorization': f'Bearer {access_token}',  
            'Content-Type': 'application/json'         
        }
        
        api_response = requests.post(query_url, headers=headers, json=query_payload)        
        print(api_response)
        if api_response.status_code == 200 :
            logging.info('Success')        
            query_job_id = api_response.json().get('id')   
            print(query_job_id)         
            status_url = base_url + f'/v1/batch-query/jobs/{query_job_id}'
            status_response = requests.get(status_url, headers=headers)
            status=status_response.json().get('status')
            logging.info(f'Status before while:{status}')
            print(status)

            while status!='completed':
                time.sleep(120)
                status_response = requests.get(status_url, headers=headers)
                status=status_response.json().get('status')
                logging.info(f'Status:{status}')

            if status == 'completed':
                batches=status_response.json().get('batches')
                fileId=batches[0]['fileId']
                recordCount=batches[0]['recordCount']
                logging.info(f'File Id:{fileId}')
                logging.info(f'record count is:{recordCount}')
            
                if recordCount != 0:
                    result_url = base_url + f'/v1/files/{fileId}'
                    res_headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Content-Type': 'text/csv'
                    }
                    result_response = requests.get(result_url, headers=res_headers)
                    if result_response.status_code == 200:
                        csv_data = result_response.text
                        # Convert CSV data to pandas DataFrame
                        df = pd.read_csv(io.StringIO(csv_data))
                        df=df.drop_duplicates()

                        # Add Evergreen_ARR column and Termed_ARR
                        df['Evergreen_ARR'] = df.apply(lambda row: row['MRR'] if row['SubscriptionTermType'] == 'EVERGREEN' else 0, axis=1)

                        df['Termed_ARR'] = df.apply(lambda row: row['MRR'] * row['SubscriptionRenewalTerm'] if row['SubscriptionTermType'] == 'TERMED' else 0, axis=1)
                       
                        if 'RatePlanCreatedDate' in df:
                            df["RatePlanCreatedDate"] = pd.to_datetime(df["RatePlanCreatedDate"],errors='coerce')   
                        
                        if 'SubscriptionCreatedDate' in df:
                            df["SubscriptionCreatedDate"] = pd.to_datetime(df["SubscriptionCreatedDate"],errors='coerce')
                        
                        df.to_csv('dags/prod_tasks/ActiveProduct/RevenueForecast_june2.csv',sep=',',index=False)
                        # df.to_csv('dags/prod_tasks/ActiveProduct/Rateplancharge_data.csv',sep=',', index=False)
                    else:
                        logging.info(f"POST request failed. Status code: {result_response.status_code}")
                        logging.info(result_response.text)
                else:
                    logging.info('Zero records')        
        else: 
            logging.info(f"API request failed. Status code: {api_response.status_code}")
            logging.info(api_response.text)
    except Exception as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise
               

extract_revenueforecasting_from_zuora()