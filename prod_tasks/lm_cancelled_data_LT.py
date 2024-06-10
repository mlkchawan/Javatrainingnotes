
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
                        "query": f'''select RatePlan.Id, RatePlan.Name,ProductRatePlan.ratePlanId__c, RatePlan.AmendmentType, RatePlan.CreatedDate as RatePlanCreatedDate, RatePlan.UpdatedDate, 
                            Product.Name, Product.ProductFamily__c, Account.Id, Account.AccountNumber, Account.CRMId, Account.Status, Account.CreatedDate, Account.UpdatedDate, 
                            Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type, Amendment.SubscriptionId, Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate, Amendment.SpecificUpdateDate, Amendment.CreatedDate, Amendment.UpdatedDate,
                            Subscription.Id, Subscription.Name, Subscription.OriginalId,Subscription.RenewalTerm,Subscription.Currency,Subscription.PreviousSubscriptionId, Subscription.Status, Subscription.TermType, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate, Subscription.CancelledDate, Subscription.CreatedDate, Subscription.UpdatedDate,
                            Subscription.pastDue__c,sum(RatePlanCharge.mrr), sum(RatePlanCharge.TCV) from RatePlanCharge
                            where (RatePlan.createdDate >= '2024-05-24T00:00:00Z' and RatePlan.createdDate < '2024-06-07T00:00:00Z' and Subscription.CancelledDate is not null  and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%')))
                            or (RatePlan.createdDate < '2024-05-24T00:00:00Z' and Subscription.CancelledDate >='2024-05-24' and Subscription.CancelledDate is not null and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%')))                                                     
                            group by RatePlan.Id, RatePlan.Name,ProductRatePlan.ratePlanId__c, RatePlan.AmendmentType, RatePlan.CreatedDate, RatePlan.UpdatedDate, 
                            Product.Name, Product.ProductFamily__c, Account.Id, Account.AccountNumber, Account.CRMId, Account.Status, 
                            Account.CreatedDate, Account.UpdatedDate, Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type, 
                            Amendment.SubscriptionId, Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate, Amendment.SpecificUpdateDate, 
                            Amendment.CreatedDate, Amendment.UpdatedDate, Subscription.Id, Subscription.Name, Subscription.OriginalId,Subscription.RenewalTerm,Subscription.Currency, Subscription.PreviousSubscriptionId, 
                            Subscription.Status, Subscription.TermType, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate, Subscription.CancelledDate, 
                            Subscription.CreatedDate, Subscription.UpdatedDate,Subscription.pastDue__c; ''',
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
                        df['RateplanDate'] = pd.to_datetime(df['RatePlanCreatedDate'],utc=True).dt.date
                        df["RateplanDate"] = pd.to_datetime(df["RateplanDate"])
                        filtered_dfs = []
                        report_dates = pd.date_range(start='2024-05-24', end='2024-06-07')
                        for date in report_dates:
                            report_date=date 
                            conditions = [
                            (df["RateplanDate"] == report_date.strftime('%Y-%m-%d')) &
                                # (df["Subscription: Cancelled Date"] <= report_date.strftime('%Y-%m-%d')) &
                                ((df["Amendment: Type"].isnull()) |
                                (~(((df["Amendment: Type"] == 'RemoveProduct') &
                                    (df["Amendment: Effective Date"] <= report_date.strftime('%Y-%m-%d'))) |
                                    ((df["Amendment: Type"] == 'NewProduct') &
                                    (df["Amendment: Effective Date"] > report_date.strftime('%Y-%m-%d'))) |
                                    (df["Subscription: Term Start Date"] > report_date.strftime('%Y-%m-%d')))))
                                     
                                # (df["RateplanDate"] < report_date.strftime('%Y-%m-%d')) &
                                # (df["Subscription: Cancelled Date"] == report_date.strftime('%Y-%m-%d')) &
                                
                                # ((df["Amendment: Type"].isnull()) |
                                # (~(((df["Amendment: Type"] == 'RemoveProduct') &
                                #     (df["Amendment: Effective Date"] <= report_date.strftime('%Y-%m-%d'))) |
                                #     ((df["Amendment: Type"] == 'NewProduct') &
                                #     (df["Amendment: Effective Date"] > report_date.strftime('%Y-%m-%d'))) |
                                #     (df["Subscription: Term Start Date"] > report_date.strftime('%Y-%m-%d')))))
                                ]
                            filtered_df = df[conditions[0]].copy()
                            filtered_df['ReportDate'] = report_date.strftime('%Y-%m-%d')
                            filtered_dfs.append(filtered_df.copy())
                            print(f"Report Date: {report_date}, Number of rows in filtered_df: {len(filtered_df)}")
                        
                        final_df = pd.concat(filtered_dfs,ignore_index=True)
                        final_df.to_csv('dags/prod_tasks/ActiveProduct/cancelleddata_May24.csv',sep=',',index=False)
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