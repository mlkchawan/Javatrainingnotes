from datetime import datetime, timedelta
import requests
import csv
import json
import pandas as pd
import io
import time
 
# Zuora API endpoint and credentials
base_url = 'https://rest.zuora.com'
client_id = 'f520c62a-0a73-4b31-be91-d9f460c280c7'
client_secret = 'zceUZlriX4NT1UCscwbiJXkHeHegVqmwr0ogWF6'
 
# Generate OAuth token
auth_url = f'{base_url}/oauth/token'
auth_payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}
 
auth_response = requests.post(auth_url, data=auth_payload)
auth_data = auth_response.json()
access_token = auth_data['access_token']
 
# Define queries for batch query
query_payload = {
    "queries": [
        {
            "name": "Cancelled_Subscriptions",
            "query": '''
                SELECT Subscription.Name AS SubscriptionName, Subscription.CancelledDate AS CancelledDate
                FROM Subscription
                WHERE Subscription.Status ='Cancelled'
                AND Subscription.CancelledDate >= '2018-01-01'
                AND Subscription.CancelledDate <= '2024-05-01'
            ''',
            "type": "zoqlexport"
        },
        {
            "name": "Active_Subscriptions",
            "query": '''
                select RatePlan.Id, RatePlan.Name AS RatePlanName, RatePlan.AmendmentType, RatePlan.CreatedDate, Product.Id,
                            Product.Name AS ProductName, Product.ProductFamily__c AS ProductFamily, ProductRatePlan.ratePlanId__c AS RatePlanId,Account.Id, Account.AccountNumber,  
                            Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type,
                            Amendment.SubscriptionId, Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate,
                            Amendment.CreatedDate, Subscription.Id,
                            Subscription.Name AS SubscriptionName, Subscription.OriginalId, Subscription.PreviousSubscriptionId, Subscription.Status,
                            Subscription.TermType,Subscription.RenewalTerm, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate, Subscription.CancelledDate,
                            Subscription.CreatedDate,RatePlanCharge.ChargedThroughDate,sum(RatePlanCharge.mrr) AS MRR, sum(RatePlanCharge.TCV) AS TCV from RatePlanCharge
                            where Subscription.TermStartDate <= '2024-04-01' and Subscription.CreatedDate <= '2024-04-01T00:00:00Z' and (Subscription.CancelledDate is null or Subscription.CancelledDate > '2024-04-01')
                            and RatePlanCharge.ChargedThroughDate >= '2024-04-01' and Subscription.Currency='USD'
                            and (Amendment.Type is null or (Amendment.Type = 'UpdateProduct' and Amendment.EffectiveDate is not null)
                            or (Amendment.Type = 'NewProduct' and Amendment.EffectiveDate <= '2024-04-01')
                            or (Amendment.Type = 'RemoveProduct' and Amendment.EffectiveDate > '2024-04-01'))
                            and((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%'))
                            group by RatePlan.Id, RatePlan.Name, RatePlan.AmendmentType, RatePlan.CreatedDate, Product.Id,
                            Product.Name, Product.ProductFamily__c, ProductRatePlan.ratePlanId__c,Account.Id, Account.AccountNumber,  
                            Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type,
                            Amendment.SubscriptionId, Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate,
                            Amendment.CreatedDate, Subscription.Id,
                            Subscription.Name, Subscription.OriginalId, Subscription.PreviousSubscriptionId, Subscription.Status,
                            Subscription.TermType,Subscription.RenewalTerm, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate, Subscription.CancelledDate,
                            Subscription.CreatedDate,RatePlanCharge.ChargedThroughDate
            ''',
            "type": "zoqlexport"
        }
    ]
}
 
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}
 
# Send batch query request to Zuora
response = requests.post(f'{base_url}/v1/batch-query/', json=query_payload, headers=headers)
response_data = response.json()
 
# Check if the request was successful
if 'id' in response_data:
    batch_job_id = response_data['id']
   
    # Check job status
    status_url = f'{base_url}/v1/batch-query/jobs/{batch_job_id}'
    while True:
        status_response = requests.get(status_url, headers=headers)
        status = status_response.json().get('status')
       
        if status == 'completed':
            break
        else:
            time.sleep(5)  # Wait for 5 seconds before checking again
   
    # Retrieve results for each query
    batches = status_response.json().get('batches')
    df_cancel = None  # Initialize DataFrame for Cancelled Subscriptions
    df_active = None  # Initialize DataFrame for Active Subscriptions
   
    for batch in batches:
        file_id = batch['fileId']
        result_url = f'{base_url}/v1/files/{file_id}'
       
        result_response = requests.get(result_url, headers=headers)
        if result_response.status_code == 200:
            csv_data = result_response.text
           
            # Load CSV data into DataFrame based on query name
            if batch['name'] == 'Cancelled_Subscriptions':
                df_cancel_1 = pd.read_csv(io.StringIO(csv_data))
                df_cancel.append(df_cancel_1)
            elif batch['name'] == 'Active_Subscriptions':
                df_active_1= pd.read_csv(io.StringIO(csv_data))
                df_active.append(df_active_1)
        else:
            print(f"Failed to retrieve data for batch query: {batch['name']}")
 
    df_cancel['CancelledDate'] = pd.to_datetime(df_cancel['CancelledDate'])
    target_date = pd.to_datetime('2024-04-01')
    df_cancel_f = df_cancel[df_cancel['CancelledDate'] < target_date]
    df_f = df_active.merge(df_cancel_f, on=['SubscriptionName'], how='left', indicator=True)
    df_A = df_f[df_f['_merge'] == 'left_only'].drop('_merge', axis=1)
    df_A.drop('CancelledDate', axis=1, inplace=True)
    grouped_df = df_A.groupby(['RatePlanName', 'ProductFamily', 'RatePlanId'], as_index=False).agg({
    'ProductName': ['first', 'size'],  # count occurrences of each product name and get the first product name
    'SubscriptionName': 'nunique'  # count unique subscription names
    })    
else:
    print("Failed to execute batch query:", response_data)
 
grouped_df.columns = ['RatePlanName', 'ProductFamily', 'ProductRatePlanId','ProductName', 'ProductCount', 'SubscriptionCount']
grouped_df['Date']='2024-04-01'
 
# Save the result to a CSV file
grouped_df.to_csv('./dags/prod_tasks/ActiveProduct/Active_sub_apr_1_2024.csv', index=False)