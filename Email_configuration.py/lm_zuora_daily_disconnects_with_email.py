#Email configuration to the airflow dag that will send the data to the mail id's whenever it runs 
# createdby : Laxmikanth M 
# created_date : 2024/05/28
# attempt : 5

import io
import os
import time
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from openpyxl import Workbook
 
def extract_from_zuora(**kwargs):
    base_url = 'https://rest.zuora.com'
    client_id = Variable.get('generic_client_id_zuora_production')
    client_secret = Variable.get('generic_client_secret_zuora_production')

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
        start = current_timestamp - timedelta(days=30)
        end = current_timestamp - timedelta(days=1)
        start_date = start.strftime('%Y-%m-%d')
        end_date = end.strftime('%Y-%m-%d')
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
             "queries": [
                        {
                        "name": "zuora_daily_disconnects",
                        "query": f'''select Account.Id AS AccountId,
                            Subscription.Id AS SubscriptionId,
                            Account.AccountNumber AS AccountNumber,  
                            Account.Status AS AccountStatus,
                            Subscription.Status AS SubscriptionStatus, 
                            Product.Name AS ProductName, 
                            Product.ProductFamily__c AS ProductFamily,
                            RatePlan.Id AS RatePlanId,
                            RatePlan.Name AS RatePlanName,
                            ProductRatePlan.ratePlanId__c AS ProductRatePlanId, 
                            RatePlan.AmendmentType AS RatePlanAmendmentType, 
                            RatePlan.CreatedDate AS RatePlanCreatedDate, 
                            RatePlan.UpdatedDate AS RatePlanUpdatedDate,  
                            Account.CreatedDate AS AccountCreatedDate, 
                            Account.UpdatedDate AS AccountUpdatedDate, 
                            Amendment.Type AS AmendmentType,  
                            Amendment.EffectiveDate AS AmendmentEffectiveDate, 
                            Subscription.Name AS SubscriptionName, 
                            Subscription.OriginalId AS SubscriptionOriginalId,
                            Subscription.RenewalTerm AS SubscriptionRenewalTerm,
                            Subscription.Currency AS SubscriptionCurrency,
                            Subscription.PreviousSubscriptionId AS SubscriptionPreviousSubscriptionId, 
                            Subscription.TermType AS SubscriptionTermType, 
                            Subscription.Version AS SubscriptionVersion, 
                            Subscription.termStartDate AS SubscriptiontermStartDate, 
                            Subscription.termEndDate AS SubscriptiontermEndDate, 
                            Subscription.CancelledDate AS SubscriptionCancelledDate, 
                            Subscription.CreatedDate AS SubscriptionCreatedDate, 
                            Subscription.UpdatedDate AS SubscriptionUpdatedDate,
                            Subscription.pastDue__c AS SubscriptionpastDue,
                            sum(RatePlanCharge.mrr) AS MRR, 
                            sum(RatePlanCharge.TCV) AS TCV  
                                from RatePlanCharge
                            where (RatePlan.CreatedDate >= '{start_date}T00:00:00Z' and Subscription.CancelledDate <='{end_date}'  
                            and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') 
                            and (not Account.userId__c like 'yqa%')))
                            or (RatePlan.createdDate < '{start_date}T00:00:00Z' and Subscription.CancelledDate >='{start_date}' 
                            and Subscription.CancelledDate is not null and ((not Subscription.displayName__c like 'yqa%' 
                            and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%')))                                                    
                            group by 
                                RatePlan.Id, RatePlan.Name,ProductRatePlan.ratePlanId__c, RatePlan.AmendmentType, 
                                RatePlan.CreatedDate, RatePlan.UpdatedDate,
                            Product.Name, Product.ProductFamily__c, Account.Id, Account.AccountNumber, Account.CRMId, Account.Status,
                            Account.CreatedDate, Account.UpdatedDate, Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type,
                            Amendment.SubscriptionId, Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate, 
                            Amendment.SpecificUpdateDate,
                            Amendment.CreatedDate, Amendment.UpdatedDate, Subscription.Id, Subscription.Name, Subscription.OriginalId,
                            Subscription.RenewalTerm,Subscription.Currency, Subscription.PreviousSubscriptionId,
                            Subscription.Status, Subscription.TermType, Subscription.Version, Subscription.termStartDate, 
                            Subscription.termEndDate, Subscription.CancelledDate,
                            Subscription.CreatedDate, Subscription.UpdatedDate,Subscription.pastDue__c Order By RatePlan.CreatedDate ASC''',
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
                        df_raw = pd.read_csv(io.StringIO(csv_data))
                        df_raw['RateplanDate'] = pd.to_datetime(df_raw['RatePlanCreatedDate'],utc=True).dt.date
                        df_raw["RateplanDate"] = pd.to_datetime(df_raw["RateplanDate"])
                        filtered_dfs = []
                        report_dates = pd.date_range(start=start_date, end=end_date)
                        for date in report_dates:
                            report_date=date 
                            conditions = [
                            (df_raw["RateplanDate"] == report_date.strftime('%Y-%m-%d')) &
                                (df_raw["SubscriptionCancelledDate"] <= report_date.strftime('%Y-%m-%d')) &
                                ((df_raw["AmendmentType"].isnull()) |
                                (~(((df_raw["AmendmentType"] == 'RemoveProduct') &
                                    (df_raw["AmendmentEffectiveDate"] <= report_date.strftime('%Y-%m-%d'))) |
                                    ((df_raw["AmendmentType"] == 'NewProduct') &
                                    (df_raw["AmendmentEffectiveDate"] > report_date.strftime('%Y-%m-%d'))) |
                                    (df_raw["SubscriptiontermStartDate"] > report_date.strftime('%Y-%m-%d'))))),
                            (df_raw["RateplanDate"].dt.strftime('%Y-%m-%d') < report_date.strftime('%Y-%m-%d')) &
                                (df_raw["SubscriptionCancelledDate"] == report_date.strftime('%Y-%m-%d')) &
                                ((df_raw["AmendmentType"].isnull()) |
                                (~(((df_raw["AmendmentType"] == 'RemoveProduct') &
                                    (df_raw["AmendmentEffectiveDate"] <= report_date.strftime('%Y-%m-%d'))) |
                                    ((df_raw["AmendmentType"] == 'NewProduct') &
                                    (df_raw["AmendmentEffectiveDate"] > report_date.strftime('%Y-%m-%d'))) |
                                    (df_raw["SubscriptiontermStartDate"] > report_date.strftime('%Y-%m-%d')))))            
                            ]
                            filtered_df = df_raw[conditions[0] | conditions[1]].copy()
                            filtered_df['ReportDate'] = report_date.strftime('%Y-%m-%d')
                            filtered_dfs.append(filtered_df)
                            final_df = pd.concat(filtered_dfs,ignore_index=True)
                            
                            pivot_product = final_df.pivot_table(index=['ReportDate', 'ProductName'], aggfunc={'SubscriptionId': 'count'})
                            pivot_product = pivot_product.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()

                            # Pivot using evergreen vs TERMED
                            pivot_term_type = final_df.pivot_table(index=['ReportDate'], columns=['SubscriptionTermType'], aggfunc='size').reset_index()
                            # pivot_term_type.columns.name = None  # Remove the columns' name
                            pivot_term_type['Grand Total'] = pivot_term_type.sum(axis=1,numeric_only=True)

                            
                            final_df['SubscriptionRenewalTerm'] = final_df['SubscriptionRenewalTerm'].fillna(1)
                            
                            # Pivot using SubscriptionRenewalTerm
                            pivot_renewal_term =final_df.pivot_table(index=['ReportDate'], columns=['SubscriptionRenewalTerm'], aggfunc={'SubscriptionRenewalTerm': 'count'})
                            pivot_renewal_term.columns.name = None  # Remove the columns' name
                            pivot_renewal_term['Grand Total'] = pivot_renewal_term.sum(axis=1,numeric_only=True)

                            final_df['SubscriptionRenewalTerm'] = final_df['SubscriptionRenewalTerm'].replace(1," " )
                            final_df['SubscriptionRenewalTerm']=pd.to_numeric(final_df['SubscriptionRenewalTerm'], errors='coerce')
                            final_df['SubscriptionRenewalTerm']=final_df['SubscriptionRenewalTerm'].astype(float)
                            
                            pivot_sub_status=final_df.pivot_table(index=['ReportDate'], columns=['SubscriptionStatus'],aggfunc='size').reset_index()

                            today = datetime.now().strftime('%Y-%m-%d')
                            
                            filename = f'/tmp/zuora_dailyDisconnects_{today}.xlsx' 
                            # Write each pivot table to a different sheet in a single Excel file
                            with pd.ExcelWriter(filename) as writer:
                                final_df.to_excel(writer, sheet_name='Daily_Disconnects_Raw_Data',index=False)
                                pivot_product.to_excel(writer, sheet_name='Product_level_sub_Count', index=False)
                                pivot_term_type.to_excel(writer, sheet_name='Evergreen vs Termed', index=False)
                                pivot_renewal_term.to_excel(writer, sheet_name='Term Breakup')
                                pivot_sub_status.to_excel(writer, sheet_name='Subscription Status',index=False) 

                            kwargs['ti'].xcom_push(key='csv_file_path2', value=filename)
                            kwargs['ti'].xcom_push(key='df_table', value=final_df)
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
    csv_file_path2 = ti.xcom_pull(task_ids='extract_from_zuora', key='csv_file_path2')

    email_task = EmailOperator(
        task_id='send_success_email',
        to=['laxmikanth.mudavath@infinite.com','mayank.bhardwaj@infinite.com','ranjith.garikapati@infinite.com','kishore.mudragada@infinite.com','arunkumar.kesa@infinite.com','ambika.reddy@infinite.com','sailakshmi.routhu@infinite.com'],
        # to=['laxmikanth.mudavath@infinite.com','arunkumar.kesa@infinite.com'],
        subject='Airflow Task Success: zuora_daily_disconnects_2024',
        html_content='The file that is attached contains the data extracted from zuora for daily disconnects for the last 30 days.',
        files=[csv_file_path2],
        dag=dag,
    )
    email_task.execute(context=kwargs)

    if os.path.exists(csv_file_path2):
        os.remove(csv_file_path2)
        logging.info(f'Removed file:{csv_file_path2}')
    else:
        logging.error(f'File not found:{csv_file_path2}')    
 
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_from_zuora",key='df_table')    
    if df is None or df.empty:
        return 'skip_mail_task'
    else:
        return 'send_email'
    

# Default arguments for the DAG
default_args = {
     'owner': 'airflow',
    'start_date': datetime(2024, 5, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Define the DAG
dag = DAG(
    'Zuora_daily_disconnects_with_Email_configuration',
    default_args=default_args,
    description='Extract and send zuora daily adds data on successfull extraction from zuora',
    schedule_interval='30 11 * * *',
    tags = ['production','zuora','07','zuoradailydisconnects']
)

# Define the data extraction task
extract_task = PythonOperator(
    task_id='extract_from_zuora',
    python_callable=extract_from_zuora,
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


