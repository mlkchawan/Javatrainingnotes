#Python code for pulling contact data from zuora and loading to PG "taxationitem"
#date: 18/04/2024
#created by : Laxmikanth

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import psycopg2
import json
import io
import pandas as pd
import requests
import logging
import time

engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_conn()
pg_hook.autocommit = True

def extract_taxationitem_from_zuora(**kwargs):
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
    # zuora_object='contact'
    try:
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "taxationitem",
                        "query": f'''select Account.Id as Id,Account.Name as AccountName,
                        Account.TaxExemptStatus as ExemptionStatus,Account.Currency as Currency,
                        Taxationitem.TaxCode as TaxCode,Taxationitem.TaxAmount as TaxAmount,
                        Taxationitem.TaxRate as TaxRate,Invoice.InvoiceDate as InvoiceDate,
                        Invoice.InvoiceNumber as InvoiceNumber,product.SKU as Product_SKU,
                        product.Description as Product_Description,InvoiceItem.ChargeAmount as ChargeAmount,BillToContact.PostalCode as Billto_PostalCode,BillToContact.State as Billto_State,SoldToContact.PostalCode as Soldto_PostalCode,SoldToContact.state as Soldto_State,
                        productrateplancharge.Taxable as Taxable_amount from TaxationItem where Account.Currency = 'USD' and Invoice.InvoiceDate >= '2024-01-01';''',
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
                        df=df.fillna(" ")

                        df = df.assign(Original_Region='MD')
                        df = df.assign(Original_Code='20852')
                        df['Billto_PostalCode']=pd.to_numeric(df['Billto_PostalCode'], errors='coerce')
                        df['Soldto_PostalCode']=pd.to_numeric(df['Soldto_PostalCode'], errors='coerce')
                        if 'InvoiceDate' in df:
                            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
                                          
                    #Load it to xcom
                        kwargs['ti'].xcom_push(key='zuora_taxationitem', value=df)

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
    df = kwargs['ti'].xcom_pull(task_ids="extract_taxationitem_from_zuora",key='zuora_taxationitem')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
    
#loading data into Postgres
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_taxationitem_from_zuora",key='zuora_taxationitem')    
    if df is None or df.empty:
        return
    try:
             
        df.to_sql(name='zuora_taxationitem',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor()  
        grant_query = ''' GRANT SELECT ON reportsdb."zuora_taxationitem" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,mayankb,areddy,kmudragada; '''
        cursor.execute(grant_query)
        # pk_check_query = f'''
        #     SELECT COUNT(*)
        #     FROM information_schema.table_constraints
        #     WHERE constraint_type = 'PRIMARY KEY'
        #     AND table_schema = 'reportsdb'
        #     AND table_name = 'zuora_taxationitem';
        # '''
        # cursor.execute(pk_check_query)                    
        # primary_key_count = cursor.fetchone()[0]
        # if primary_key_count == 0:
        #     alter_query=f'''ALTER TABLE reportsdb.zuora_taxationitem ADD PRIMARY KEY ("AccountId");'''
        #     cursor.execute(alter_query)
        #     pg_hook.commit()
    except Exception as e:
        print(f"Caught an Exception: {e}")
        # cursor = pg_hook.cursor()
        # batch_size = 1000
        # rows = []
        # for _, row in df.iterrows():
        #     rows.append(tuple(row))
        #     if len(rows) == batch_size:
        #         upsert_query = f'''
        #             INSERT INTO reportsdb.zuora_taxationitem({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #             ON CONFLICT ("AccountId") DO UPDATE SET
        #             {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])} 
        #         '''
        #         cursor.executemany(upsert_query,rows)
        #         rows = []
        #     if rows:
        #         upsert_query = f'''
        #             INSERT INTO reportsdb.zuora_contact({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #             ON CONFLICT ("AccountId") DO UPDATE SET
        #             {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
        #         '''
        #         cursor.executemany(upsert_query,rows)
        # pg_hook.commit()
        cursor.close()
        logging.info("Data loaded.")
   
    finally:
        pg_hook.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='zuora_to_pg_taxationitem_qa',
    default_args=default_args,
    description='Extract data from zuora and load into postgres',
    schedule_interval='@daily',
    tags = ['zuora','sandbox','taxation']
)
 
# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
#     )
 
#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_taxationitem_from_zuora',
    python_callable=extract_taxationitem_from_zuora,
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
