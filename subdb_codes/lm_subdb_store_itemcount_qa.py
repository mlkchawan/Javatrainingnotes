#Python code for Extracting data from mysql and loading into Postgres "sub_store_itemcount"
#date: 01/04/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
import logging
import pandas as pd
import json


engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_conn()
pg_hook.autocommit = True

def get_last_processed_value():
    cursor = pg_hook.cursor()
    try:
        cursor.execute(f"SELECT value FROM reportsdb.sub_metadata WHERE object = 'sub_store_itemcount'")
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-prod')
    connection = mysql_hook.get_conn()
    mysql_hook.schema='subscriptions'

    # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value")
    # print(last_processed_value)  
   
    mysql_query =f"select * from extra_services WHERE date >= '2023-06-01' Order by created_at ASC ;"
    
    # mysql_query =f"SELECT * FROM subscription_logs WHERE date(create_date) >'{last_processed_value}' Order by create_date ASC"
    result = mysql_hook.get_pandas_df(mysql_query)
    
    # Extract 'theme_id','domain_name' field from 'meta' column and create a new column
    result['theme_id'] = result['meta'].apply(lambda x: json.loads(x).get('theme_id') if x else None)
    result['domain_name'] = result['meta'].apply(lambda x: json.loads(x).get('domain_name') if x else None)    

    #drop original meta column 
    result.drop('meta', axis=1, inplace=True)

    df =result.fillna(" ")
   
    # # Fetch the date from the last row and insert to sub_metadata table
    # try:
    #     if not df.empty:
    #         last_processed_value = result['create_date'].iloc[-1]
    #         cursor = pg_hook.cursor()
    #         try:
    #             cursor.execute(f"INSERT INTO reportsdb.sub_metadata (object, value) VALUES ('sub_billproviderorder', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (last_processed_value, last_processed_value))
    #             pg_hook.commit()
    #         finally:
    #             cursor.close()
    #     else:
    #         print("DataFrame is empty.")

    # except IndexError as e:
    #     logging.error(f"Error during data transfer: {str(e)}")
    #     raise    

    kwargs['ti'].xcom_push(key='sub_store_itemcount', value=df)
    
    #close the connection
    connection.close()
       
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='sub_store_itemcount')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
    
#loading data into Postgres 
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='sub_extra_services')    
    if df is None or df.empty:
        return
    try:
        df.to_sql(name='sub_store_itemcount',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor() 
        grant_query = ''' GRANT SELECT ON reportsdb."sub_store_itemcount" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,mayankb,areddy,kmudragada; '''
        cursor.execute(grant_query)

        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'sub_store_itemcount';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.sub_store_itemcount ADD PRIMARY KEY ("id");'''
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
                    INSERT INTO reportsdb.sub_store_itemcount({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("id") DO UPDATE
                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                '''

                cursor.executemany(upsert_query,rows)
                rows = []
            if rows:
                upsert_query = f'''
                    INSERT INTO reportsdb.sub_store_itemcount({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("id") DO UPDATE
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
    'start_date': datetime(2024, 4, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='subdb_to_pg_store_itemcount_qa',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@daily',
    tags = ['mysql','QA','storeitemcount'],
) 

get_last_processed_value = PythonOperator(
    task_id = 'get_last_processed_value',
    python_callable= get_last_processed_value,
    provide_context=True,
    dag=dag,
    )

#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable= extract_and_load_to_dataframe,
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
