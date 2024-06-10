#Python code for data from zuora and loading to PG "newzuoradata"
#date: 12/02/2024
#created by : Laxmikanth

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import pandas as pd
import csv
import pandas as pd
import requests
import logging
import io
import time


engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_conn()
pg_hook.autocommit = True

# extract data from zuora 
def extract_from_zuora(**kwargs):

    base_url = 'https://rest.apisandbox.zuora.com'
    client_id =Variable.get('lm_client_id_zuora_sandbox')  
    client_secret =Variable.get('lm_client_secret_zuora_sandbox')


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
    try:
        query = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "RatePlanCharge",
                        "query": f'''select 
                         RatePlan.Id as RatePlanid, RatePlan.Name , RatePlan.AmendmentType, RatePlan.CreatedDate, RatePlan.UpdatedDate,
                                Product.Name, Product.ProductFamily__c, Account.Id, Account.AccountNumber,
                                Account.CRMId, Account.Status, Account.CreatedDate, Account.UpdatedDate,
                                Amendment.Id, Amendment.Code, Amendment.Name, Amendment.Type, Amendment.SubscriptionId,
                                Amendment.TermType, Amendment.Status, Amendment.TermStartDate, Amendment.EffectiveDate,
                                Amendment.SpecificUpdateDate, Amendment.CreatedDate, Amendment.UpdatedDate,
                                Subscription.Id, Subscription.Name, Subscription.OriginalId,Subscription.RenewalTerm,
                                Subscription.Currency,Subscription.PreviousSubscriptionId, Subscription.Status,
                                Subscription.TermType, Subscription.Version, Subscription.termStartDate, Subscription.termEndDate,
                                Subscription.CancelledDate, Subscription.CreatedDate, Subscription.UpdatedDate,
                                sum(RatePlanCharge.mrr), sum(RatePlanCharge.TCV),sum(InvoiceItem.ChargeAmount) as TotalAddsRevenue,
                                Subscription.termStartDate as ReportDate
                                from RatePlanCharge
            WHERE
                Subscription.termStartDate >= '2022-02-01' ''',
                        "type": "zoqlexport"
                        }
                        ]
            }         
    
        headers = {
            'Authorization': f'Bearer {access_token}',  
            'Content-Type': 'application/json'         
        }    
        api_response = requests.post(query, headers=headers, json=query_payload)
        data=api_response.json()
        print(data)
        if api_response.status_code == 200 :
                logging.info('Success')   
                print("success")     
                query_job_id = api_response.json().get('id')            
                status_url = base_url + f'/v1/batch-query/jobs/{query_job_id}'
                status_response = requests.get(status_url, headers=headers)
                status=status_response.json().get('status')
                logging.info(f'Status before while:{status}')
                print("here")

                while status!='completed':
                    time.sleep(30)
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
                        'Content-Type': 'application/json'
                    }
                    result_response = requests.get(result_url, headers=res_headers)
                    if result_response.status_code == 200:
                            csv_data = result_response.text
                            # Convert CSV data to pandas DataFrame
                            df = pd.read_csv(io.StringIO(csv_data))
                            df.fillna(' ')
                            kwargs['ti'].xcom_push(key='newzuoradata', value=df)
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

#loading data into Postgres
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_from_zuora",key='newzuoradata')    
    try:      
        df.to_sql(name='newzuoradata', con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')  
        cursor = pg_hook.cursor() 
        grant_query = ''' GRANT SELECT ON reportsdb."newzuoradata" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati; '''
        cursor.execute(grant_query)

        #Add Primary Key Constraint:
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'newzuoradata';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.newzuoradata ADD PRIMARY KEY ("RatePlanid");'''
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
                    INSERT INTO reportsdb.newzuoradata({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("RatePlanid") DO UPDATE SET
                    {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                '''
 
                cursor.executemany(upsert_query,rows)
                rows = []
            if rows:
                upsert_query = f'''
                    INSERT INTO reportsdb.newzuoradata({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("RatePlanid") DO UPDATE SET
                    {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                    
                '''
            cursor.executemany(upsert_query,rows)
        pg_hook.commit()
        cursor.close()
        logging.info("Data loaded.")
   
    finally:
        pg_hook.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='zuora_to_pg_data',
    default_args=default_args,
    description='Extract data from zuora and load into postgres',
    schedule_interval='@monthly',
    tags = ['zuora','postgres','newzuoradata']
)
 
# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
# )
 
#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_from_zuora',
    python_callable=extract_from_zuora,
    provide_context=True,
    dag=dag,
)
 
# check_records_task = BranchPythonOperator(
#     task_id='check_records',
#     python_callable=check_records,
#     provide_context=True,
#     dag=dag,
# )
 
create_and_load= PythonOperator(
    task_id='create_and_load_table',
    python_callable=create_and_load_table,
    provide_context=True,
    dag=dag,
)
 
# skip_loading_task = DummyOperator(
#     task_id='skip_loading_task',
#     dag=dag,
# )
 
#dependencies
# extract_data >> check_records_task
# check_records_task >> [create_and_load, skip_loading_task]
 
extract_data >> create_and_load