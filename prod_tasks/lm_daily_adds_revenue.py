#Python code for pulling 1years data from zuora and loading to PG "newzuoradata"
#date: 12/02/2024
#created by : Laxmikanth
 
from datetime import datetime, timedelta
import psycopg2
import csv
import pandas as pd
import requests
import logging
import io
import time
 

 
# def get_last_processed_value(**kwargs):
#     cursor = pg_hook.cursor()
#     try:
#         cursor.execute(f"SELECT value FROM reportsdb.tbl_zuora_metadata WHERE object = 'newzuoradata_z'")
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()
 
# extract data from zuora
def extract_from_zuora(**kwargs):
    base_url = 'https://rest.zuora.com'
    client_id ='af923dd5-8ef8-4f52-a0ff-3c43fdc58a0d'
    client_secret ='1+p5nR2ABN2SZVG9lbYRsFfCf1Qh=EsVgs5ItWl3A'
 
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
 
    try:
        # current_timestamp = datetime.now()
        # reportdate_tz = current_timestamp - timedelta(days=60)
        # reportdate = reportdate_tz.strftime('%Y-%m-%dT%H:%M:%SZ')  
        # logging.info('reportdate',reportdate)
        # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value").strftime('%Y-%m-%dT%H:%M:%SZ')  
        # print(last_processed_value)
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "zuoradata",
                        "query": f'''select RatePlan.Id as RatePlanId, RatePlan.Name as RatePlanName, RatePlan.AmendmentType as RatePlanAmendmentType, RatePlan.CreatedDate as RateplanCreatedDate, RatePlan.UpdatedDate as RatePlanUpdatedDate,
                            Product.Name as ProductName, Product.ProductFamily__c as ProductFamily, Account.Id as AccountId, Account.AccountNumber as AccountNumber, Account.CRMId as AccountCRMId, Account.Status as AccountStatus, Account.CreatedDate as AccountCreatedDate, Account.UpdatedDate as AccountUpdatedDate,
                            Subscription.Id as SubscriptionId, Subscription.Name as SubscriptionName, Subscription.OriginalId as SubscriptionOriginalId,Subscription.RenewalTerm as SubscriptionRenewalTerm,Subscription.Currency as SubscriptionCurrency,Subscription.PreviousSubscriptionId as PreviousSubscriptionId, 
                            Subscription.Status as SubscriptionStatus, Subscription.TermType as SubscriptionTermType, Subscription.Version as SubscriptionVersion, 
                            Subscription.termStartDate as SubscriptiontermStartDate, Subscription.termEndDate as SubscriptiontermEndDate, Subscription.CancelledDate as SubscriptionCancelledDate, Subscription.CreatedDate as SubscriptionCreatedDate, Subscription.UpdatedDate as SubscriptionUpdatedDate,
                            InvoiceItem.CreatedDate as InvoiceItemCreatedDate,sum(RatePlanCharge.mrr) as MRR, sum(RatePlanCharge.TCV) as TCV ,sum(InvoiceItem.Chargeamount) as TotalAddsRevenue from InvoiceItem
                            where RatePlan.CreatedDate >= '2022-01-01T00:00:00Z' and Subscription.Currency='USD' and Subscription.Version = 1 and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%'))
                            and Date(InvoiceItem.CreatedDate)=Date(RatePlan.CreatedDate)
                            group by RatePlan.Id, RatePlan.Name, RatePlan.AmendmentType, RatePlan.CreatedDate, RatePlan.UpdatedDate,
                            Product.Name, Product.ProductFamily__c, Account.Id, Account.AccountNumber, Account.CRMId, Account.Status,
                            Account.CreatedDate, Account.UpdatedDate,Subscription.Id, Subscription.Name, Subscription.OriginalId,Subscription.RenewalTerm,Subscription.Currency, Subscription.PreviousSubscriptionId,
                            Subscription.Status, Subscription.TermType, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate, Subscription.CancelledDate,
                            Subscription.CreatedDate, Subscription.UpdatedDate,InvoiceItem.CreatedDate Order By RatePlan.CreatedDate ASC''',
                        "type": "zoqlexport"
                        }
                        ]
            }        
        headers = {
            'Authorization': f'Bearer {access_token}',  
            'Content-Type': 'application/json'        
        }
       
        api_response = requests.post(query_url, headers=headers, json=query_payload)        
        if api_response.status_code == 200 :
            logging.info('Success')        
            query_job_id = api_response.json().get('id')            
            status_url = base_url + f'/v1/batch-query/jobs/{query_job_id}'
            status_response = requests.get(status_url, headers=headers)
            status=status_response.json().get('status')
            logging.info(f'Status before while:{status}')
 
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
                            #df['ReportDate'] = pd.to_datetime(df['RatePlan.CreatedDate']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%d')
                            # df['RateplanCreatedDatee'] = pd.to_datetime(df['RateplanCreatedDate'],utc=True)
                            # df['ReportDate']=df['RateplanCreatedDatee'].dt.date
                            # df['ReportDate'] = pd.to_datetime(df['RateplanCreatedDate'],utc=True)
                            # df['ReportDate']=df['ReportDate'].dt.date
                            df_grouped = df.groupby('RatePlanId', as_index=False).agg({
                            'RatePlanName': 'first',
                            'RatePlanAmendmentType': 'first',
                            'RateplanCreatedDate' : 'first',
                            'RatePlanUpdatedDate' : 'first',
                            'ProductName': 'first',
                            'ProductFamily': 'first',
                            'AccountId': 'first',
                            'AccountNumber': 'first',
                            'AccountCRMId': 'first',
                            'AccountStatus': 'first',
                            'AccountCreatedDate': 'first',
                            'AccountUpdatedDate': 'first',
                            'SubscriptionId': 'first',
                            'SubscriptionName': 'first',
                            'SubscriptionOriginalId': 'first',
                            'SubscriptionRenewalTerm': 'first',
                            'SubscriptionCurrency': 'first',
                            'PreviousSubscriptionId': 'first',
                            'SubscriptionStatus': 'first',
                            'SubscriptionTermType': 'first',
                            'SubscriptionVersion': 'first',
                            'SubscriptiontermStartDate': 'first',
                            'SubscriptiontermEndDate': 'first',
                            'SubscriptionCancelledDate': 'first',
                            'SubscriptionCreatedDate': 'first',
                            'SubscriptionUpdatedDate': 'first',
                            'InvoiceItemCreatedDate': 'first',
                            'MRR': 'sum',
                            'TCV': 'sum',
                            'TotalAddsRevenue': 'sum',     
                        })
                            df_grouped['RateplanCreatedDate'] = pd.to_datetime(df_grouped['RateplanCreatedDate'],utc=True)
                            df_grouped['ReportDate']=df_grouped['RateplanCreatedDate'].dt.date
                            df_grouped.to_csv('dags/dailyadd_with_revenue227.csv', index=False)
                             
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
 
extract_from_zuora()