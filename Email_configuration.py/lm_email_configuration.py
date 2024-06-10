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
 
engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_conn()
pg_hook.autocommit = True

# Extract and load function
def extract_and_load_to_dataframe(**kwargs):
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
    try:
        # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value").strftime('%Y-%m-%dT%H:%M:%SZ')   
        # print(last_processed_value)
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "Rateplancharge",
                        "query": f'''select Payment.Id as PaymentId,Payment.Amount as PaymentAmount,
                                    Payment.Appliedamount as Appliedamount,
                                    Payment.Authtransactionid as Authtransactionid,
                                    Payment.Bankidentificationnumber as Bankidentificationnumber,
                                    Payment.Cancelledon as Cancelledon,
                                    Payment.Comment as Comment,
                                    Payment.Status as PaymentStatus,
                                    Payment.Settledon as Settledon,
                                    Payment.Submittedon as Submittedon
                                    from Payment where Payment.CreatedDate >= '2024-04-01T00:00:00Z' order by Payment.CreatedDate ASC limit 10 ;
                                    ''',
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
        query_job_id = api_response.json().get('id')
        print(api_response.json())       
        if api_response.status_code == 200 :
            logging.info('Success')        
            query_job_id = api_response.json().get('id') 
            print()           
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
                        csv_file_path = '/tmp/payment_data.csv'
                        df.to_csv(csv_file_path, index=False)
                        # df_table=df
                        # Load it to xcom
                        kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
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
    csv_file_path = ti.xcom_pull(task_ids='extract_and_load_to_dataframe', key='csv_file_path')
    
    email_task = EmailOperator(
        task_id='send_success_email',
        # to=['laxmikanth.mudavath@infinite.com','mayank.bhardwaj@infinite.com','ranjith.garikapati@infinite.com','kishore.mudragada@infinite.com','arunkumar.kesa@infinite.com','ambika.reddy@infinite.com','sailakshmi.routhu@infinite.com'],
        to=['laxmikanth.mudavath@infinite.com','arunkumar.kesa@infinite.com'],
        subject='Airflow Task Success: zuora_payment',
        html_content='The data extraction from zuora for payments has completed successfully and file attached.',
        files=[csv_file_path],
        dag=dag,
    )
    email_task.execute(context=kwargs)

    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        logging.info(f'Removed file: {csv_file_path}')
    else:
        logging.error(f'File not found:{csv_file_path}')
 
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='df_table')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
    
#loading data into Postgres
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='df_table')    
    if df is None or df.empty:
        return
    try:
             
        df.to_sql(name='zuora_payment',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor()  
        grant_query = ''' GRANT SELECT ON reportsdb."zuora_payment" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,mayankb,areddy,kmudragada; '''
        cursor.execute(grant_query)
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'zuora_payment';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.zuora_payment ADD PRIMARY KEY ("PaymentId");'''
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
                    INSERT INTO reportsdb.zuora_payment({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("PaymentId") DO UPDATE SET
                    {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])} 
                '''
                cursor.executemany(upsert_query,rows)
                rows = []
            if rows:
                upsert_query = f'''
                    INSERT INTO reportsdb.zuora_payment({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("PaymentId") DO UPDATE SET
                    {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                '''
                cursor.executemany(upsert_query,rows)
        pg_hook.commit()
        cursor.close()
        logging.info("Data loaded.")
   
    finally:
        pg_hook.close()

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
    'Email_configuration_demo',
    default_args=default_args,
    description='Extract and load revenue data, then send email on success',
    schedule_interval='30 11 * * *',
    tags = ['production','zuora','07','RevenueSumForCurAPByDate']
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
extract_task >> send_email_task >> check_records_task
check_records_task >> [create_and_load, skip_loading_task]


# # Set task dependencies
# extract_task 