#Python code for pulling 1years data from zuora and loading to PG "DailyAdds"
#date: 18/03/2024
#created by : Laxmikanth
 
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import requests
import logging
import io
import time
 
engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_conn()
pg_hook.autocommit = True


# def get_last_processed_value(**kwargs):
#     cursor = pg_hook.cursor()
#     try:
#         cursor.execute(f"SELECT value FROM reportsdb.tbl_zuora_metadata WHERE object = 'DailyAdds_z'")
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
                            Product.Name as ProductName, Product.ProductFamily__c as ProductFamily__c, Account.Id as AccountId, Account.AccountNumber as AccountNumber, Account.CRMId as AccountCRMId, Account.Status as AccountStatus, Account.CreatedDate as AccountCreatedDate, Account.UpdatedDate as AccountUpdatedDate,
                            Subscription.Id as SubscriptionId, Subscription.Name as SubscriptionName, Subscription.OriginalId as SubscriptionOriginalId,Subscription.RenewalTerm as SubscriptionRenewalTerm,Subscription.Currency as SubscriptionCurrency,Subscription.PreviousSubscriptionId as PreviousSubscriptionId, 
                            Subscription.Status as SubscriptionStatus, Subscription.TermType as SubscriptionTermType, Subscription.Version as SubscriptionVersion, 
                            Subscription.termStartDate as SubscriptiontermStartDate, Subscription.termEndDate as SubscriptiontermEndDate, Subscription.CancelledDate as SubscriptionCancelledDate, Subscription.CreatedDate as SubscriptionCreatedDate, Subscription.UpdatedDate as SubscriptionUpdatedDate,
                            InvoiceItem.CreatedDate as InvoiceItemCreatedDate,sum(RatePlanCharge.mrr) as MRR, sum(RatePlanCharge.TCV) as TCV ,sum(InvoiceItem.Chargeamount) as TotalAddsRevenue from InvoiceItem
                            where RatePlan.CreatedDate >= '2024-01-01T00:00:00Z' and Subscription.Currency='USD' and Subscription.Version = 1 and ((not Subscription.displayName__c like 'yqa%' and not Subscription.displayName__c like 'ysbsqa%') and (not Account.userId__c like 'yqa%'))
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
                            df['ReportDate1'] = pd.to_datetime(df['RateplanCreatedDate'],utc=True)
                            df['ReportDate']=df['ReportDate1'].dt.date
                          
                            last_processed_value = df['ReportDate1'].iloc[-1]
                            cursor = pg_hook.cursor()    
                            try:
                                if not df.empty:
                                    cursor.execute("INSERT INTO reportsdb.tbl_zuora_metadata (object, value) VALUES ('DailyAdds_z', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (last_processed_value, last_processed_value))
                                    pg_hook.commit()
                            finally:
                                cursor.close()
                            #Load it to xcom    
                            df.drop(['ReportDate1'], axis=1, inplace=True)    
                            kwargs['ti'].xcom_push(key='DailyAdds', value=df)
                             
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
 
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_from_zuora",key='DailyAdds')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
 
#loading data into Postgres
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_from_zuora",key='DailyAdds')
    if df is None or df.empty:
        return    
    try:
        #df.drop_duplicates(subset=["RatePlanid"], keep='first', inplace=True)  # Remove duplicates        
        df.to_sql(name='DailyAdds', con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')  
        cursor = pg_hook.cursor()
 
        #Add Primary Key Constraint:
        grant_query = ''' GRANT SELECT ON reportsdb."DailyAdds" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati; '''
        cursor.execute(grant_query)
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'DailyAdds';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb."DailyAdds" ADD PRIMARY KEY ("RatePlanId");'''
            cursor.execute(alter_query)
            pg_hook.commit()
    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        batch_size = 1000
        rows = []
        for _, row in df.iterrows():
            rows.append(tuple(row))
            if len(rows) == batch_size:
                upsert_query = f'''
                    INSERT INTO reportsdb."DailyAdds"({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("RatePlanId") DO UPDATE
                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                    '''
                cursor.executemany(upsert_query,rows)
                rows = []
            if rows:
                upsert_query = f'''
                    INSERT INTO reportsdb."DailyAdds"({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("RatePlanId") DO UPDATE 
                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                   
                '''
                cursor.executemany(upsert_query,rows)
        pg_hook.commit()
        cursor.close()
        logging.info("Data loaded.")
   
    finally:
        pg_hook.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='zuora_to_pg_DailyAdds_prod',
    default_args=default_args,
    description='Extract data from zuora and load into postgres',
    schedule_interval='@daily',
    tags = ['zuora','postgres','DailyAdds']
)
 
# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
#     )
 
#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_from_zuora',
    python_callable=extract_from_zuora,
    provide_context=True,
    dag=dag,
)
 
check_records_task = BranchPythonOperator(
    task_id='check_records',
    python_callable=check_records,
    provide_context=True,
    dag=dag,
)
 
create_and_load= PythonOperator(
    task_id='create_and_load_table',
    python_callable=create_and_load_table,
    provide_context=True,
    dag=dag,
)
 
skip_loading_task = DummyOperator(
    task_id='skip_loading_task',
    dag=dag,
)
 
#dependencies
extract_data >> check_records_task
check_records_task >> [create_and_load, skip_loading_task]