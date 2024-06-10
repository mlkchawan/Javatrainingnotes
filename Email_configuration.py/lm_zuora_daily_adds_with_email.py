#Email configuration to the airflow dag that will send the data to the mail id's whenever it runs 
# createdby : Laxmikanth M 
# created_date : 2024/05/23
# attempt : 5 

import io
import os
import time
import requests
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from openpyxl import Workbook

def extract_and_load_to_dataframe(**kwargs):
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
 
    try:
        current_timestamp = datetime.now()
        start = current_timestamp - timedelta(days=60)
        # end = current_timestamp - timedelta(days=1)
        start_date = start.strftime('%Y-%m-%d')
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
             "queries": [
                        {
                        "name": "zuoradailyadds",
                        "query": f'''select RatePlan.Id as RatePlanId, RatePlan.Name as RatePlanName, RatePlan.AmendmentType as RatePlanAmendmentType, RatePlan.CreatedDate as RateplanCreatedDate, RatePlan.UpdatedDate as RatePlanUpdatedDate,
                            Product.Name as ProductName, Product.ProductFamily__c as ProductFamily, Account.Id as AccountId, Account.AccountNumber as AccountNumber, Account.CRMId as AccountCRMId, Account.Status as AccountStatus, Account.CreatedDate as AccountCreatedDate, Account.UpdatedDate as AccountUpdatedDate,
                            Subscription.Id as SubscriptionId, Subscription.Name as SubscriptionName, Subscription.OriginalId as SubscriptionOriginalId,Subscription.RenewalTerm as SubscriptionRenewalTerm,Subscription.Currency as SubscriptionCurrency,Subscription.PreviousSubscriptionId as PreviousSubscriptionId, 
                            Subscription.Status as SubscriptionStatus, Subscription.TermType as SubscriptionTermType, Subscription.Version as SubscriptionVersion, 
                            Subscription.termStartDate as SubscriptiontermStartDate, Subscription.termEndDate as SubscriptiontermEndDate, Subscription.CancelledDate as SubscriptionCancelledDate, Subscription.CreatedDate as SubscriptionCreatedDate, Subscription.UpdatedDate as SubscriptionUpdatedDate,
                            InvoiceItem.CreatedDate as InvoiceItemCreatedDate,sum(RatePlanCharge.mrr) as MRR, sum(RatePlanCharge.TCV) as TCV ,sum(InvoiceItem.Chargeamount) as TotalAddsRevenue from InvoiceItem
                            where RatePlan.CreatedDate >= '{start_date}T00:00:00Z' and Subscription.Currency='USD' and Subscription.Version = 1 and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%'))
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
                        'TotalAddsRevenue': 'sum'})
                        
                        df_grouped['RateplanCreatedDatee'] = pd.to_datetime(df_grouped['RateplanCreatedDate'],utc=True)
                        # last_processed_value = df_grouped['RateplanCreatedDatee'].iloc[-1]
                        df_grouped['ReportDate']=df_grouped['RateplanCreatedDatee'].dt.date
                        df_grouped.drop(['RateplanCreatedDatee'], axis=1, inplace=True) 
                        
                        pivot_product = df_grouped.pivot_table(index=['ReportDate', 'ProductName'], aggfunc={'SubscriptionId': 'count'})
                        pivot_product = pivot_product.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()
                        

                        # Pivot using evergreen vs TERMED
                        pivot_term_type = df_grouped.pivot_table(index=['ReportDate'], columns=['SubscriptionTermType'], aggfunc='size').reset_index()
                        # pivot_term_type.columns.name = None  # Remove the columns' name
                        
                        pivot_term_type['Grand Total'] = pivot_term_type.sum(axis=1,numeric_only=True)
                                                
                        df_grouped['SubscriptionRenewalTerm'] = df_grouped['SubscriptionRenewalTerm'].fillna(1)
                        # Pivot using SubscriptionRenewalTerm
                        pivot_renewal_term =df_grouped.pivot_table(index=['ReportDate'], columns=['SubscriptionRenewalTerm'], aggfunc={'SubscriptionRenewalTerm': 'count'})
                        pivot_renewal_term.columns.name = None  # Remove the columns' name
                        # pivot_renewal_term=pivot_renewal_term.rename(columns={'SubscriptionRenewalTerm':' '})
                        
                        # numeric_cols = pivot_renewal_term.columns.difference(['ReportDate'])
                        pivot_renewal_term['Grand Total'] = pivot_renewal_term.sum(axis=1,numeric_only=True)

                        # grand_total_row = pivot_product.sum(axis=0).rename('Grand Total') 
                        # pivot_product = pivot_product.append(grand_total_row)

                        df_grouped['SubscriptionRenewalTerm'] = df_grouped['SubscriptionRenewalTerm'].replace(1," ")

                        pivot_product_count=df_grouped.pivot_table(index=['ReportDate','ProductName'],aggfunc={'TotalAddsRevenue':'sum'}).reset_index()

                        pivot_revenue=df_grouped.pivot_table(index=['ReportDate'],aggfunc={'SubscriptionId':"count",'TotalAddsRevenue':'sum'})
                        pivot_revenue = pivot_revenue.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()

                        pivot_sub_status=df_grouped.pivot_table(index=['ReportDate'], columns=['SubscriptionStatus'],aggfunc='size').reset_index()
                        
                        pivot_sub_status['Grand Total'] = pivot_sub_status.sum(axis=1,numeric_only=True)
                        
                        today = datetime.now().strftime('%Y-%m-%d')
                        
                        filename = f'/tmp/zuora_daily_adds_{today}.xlsx' 
                        # Write each pivot table to a different sheet in a single Excel file
                        with pd.ExcelWriter(filename) as writer:
                            df_grouped.to_excel(writer, sheet_name='Daily_Adds_Raw_Data',index=False)
                            pivot_product.to_excel(writer, sheet_name='Product_level_sub_Count', index=False)
                            pivot_term_type.to_excel(writer, sheet_name='Evergreen vs Termed', index=False)
                            pivot_renewal_term.to_excel(writer, sheet_name='Term Breakup')
                            pivot_product_count.to_excel(writer, sheet_name='product_level_revenue',index=False)
                            pivot_revenue.to_excel(writer, sheet_name='Count of Subscriptions &Revenue',index=False)
                            pivot_sub_status.to_excel(writer, sheet_name='Subscription Status',index=False) 
                        
                        # csv_file_path = '/tmp/zuora_daily_adds_2024.csv'
                        # df_grouped.to_csv(csv_file_path, index=False)

                        # Load it to xcom
                        # kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path) 
                        kwargs['ti'].xcom_push(key='csv_file_path2', value=filename)

                        kwargs['ti'].xcom_push(key='df_table', value=df)
                             
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


# Define the email task
def send_email(**kwargs):
    ti = kwargs['ti']
    # csv_file_path = ti.xcom_pull(task_ids='extract_and_load_to_dataframe', key='csv_file_path')
    filename = ti.xcom_pull(task_ids='extract_and_load_to_dataframe', key='csv_file_path2')
    
    email_task = EmailOperator(
        task_id='send_success_email',
        to=['laxmikanth.mudavath@infinite.com','mayank.bhardwaj@infinite.com','ranjith.garikapati@infinite.com','kishore.mudragada@infinite.com','arunkumar.kesa@infinite.com','ambika.reddy@infinite.com','sailakshmi.routhu@infinite.com'],
        # to=['laxmikanth.mudavath@infinite.com','arunkumar.kesa@infinite.com'],
        subject='Airflow Task Success: zuora_daily_adds_2024',
        html_content='The file that is attached contains the data extracted from zuora for daily adds for the last 30 days.',
        files=[filename],
        dag=dag,
    )
    email_task.execute(context=kwargs)

    # if os.path.exists(csv_file_path):
    #     os.remove(csv_file_path)
    #     logging.info(f'Removed file: {csv_file_path}')
    # else:
    #     logging.error(f'File not found:{csv_file_path}')

    if os.path.exists(filename):
        os.remove(filename)
        logging.info(f'Removed file: {filename}')
    else:
        logging.error(f'File not found:{filename}')

            

def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='df_table')    
    if df is None or df.empty:
        return 'skip_mail_task'
    else:
        return 'send_email'
    

# Default arguments for the DAG
default_args = {
     'owner': 'airflow',
    'start_date': datetime(2024, 5, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Define the DAG
dag = DAG(
    'Zuora_daily_adds_with_Email_configuration',
    default_args=default_args,
    description='Extract and send zuora daily adds data on successfull extraction from zuora',
    schedule_interval='30 11 * * *',
    tags = ['production','zuora','07','zuoradailyadds']
)

# Define the data extraction task
extract_task = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable=extract_and_load_to_dataframe,
    provide_context=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)

check_records_task = BranchPythonOperator(
    task_id='check_records',
    python_callable=check_records,
    provide_context=True,
    dag=dag,
)
 
 
skip_loading_task = DummyOperator(
    task_id='skip_mail_task',
    dag=dag,
)
 
#dependencies
extract_task >> check_records_task
check_records_task >> [send_email_task, skip_loading_task]


# # Set task dependencies
# extract_task 