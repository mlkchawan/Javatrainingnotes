#Python code for Extracting data from mysql and loading into Postgres "sub_charge_backs"
#date: 22/03/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
import logging
import pandas as pd


engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_conn()
pg_hook.autocommit = True

# def get_last_processed_value():
#     cursor = pg_hook.cursor()
#     try:
#         cursor.execute(f"SELECT value FROM reportsdb.sub_metadata WHERE object = 'sub_chargebacks'")
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-prod')

    connection = mysql_hook.get_conn()
    mysql_hook.schema='subscriptions'
    # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value")
    # print(last_processed_value)  
   
    #Execute the query and fetch data
    mysql_query =f"SELECT * FROM charge_backs where date(created_at) > '2022-01-01'"

    # mysql_query =f"SELECT * FROM subscription_services where modified_at > '{last_processed_value} Order by modified_at ASC'"
    result = mysql_hook.get_pandas_df(mysql_query)
    df =result.fillna("###")
    
    # Fetch the date from the last row and insert to sub_metadata table
    try:
        if not df.empty:
            last_processed_value = df['created_at'].iloc[-1]
            cursor = pg_hook.cursor()
            try:
                cursor.execute(f"INSERT INTO reportsdb.sub_metadata (object, value) VALUES ('sub_chargebacks', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (last_processed_value, last_processed_value))
                pg_hook.commit()
            finally:
                cursor.close()
        else:
            print("DataFrame is empty.")

    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise    

    kwargs['ti'].xcom_push(key='sub_chargebacks', value=df)
    
    #close the connection
    connection.close()
       
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='sub_chargebacks')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
    
#loading data into Postgres 
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='sub_chargebacks')    
    if df is None or df.empty:
        return
    try:
        df.to_sql(name='sub_charge_backs',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor() 

        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'sub_charge_backs';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.sub_charge_backs ADD PRIMARY KEY ("id");'''
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
                    INSERT INTO reportsdb.sub_charge_backs({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("id") DO UPDATE
                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                '''

                cursor.executemany(upsert_query,rows)
                rows = []
            if rows:
                upsert_query = f'''
                    INSERT INTO reportsdb.sub_charge_backs({", ".join(['"{}"'.format(col) for col in df.columns])})
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
    'start_date': datetime(2024, 1, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='subdb_to_pg_subscription_services_prod',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@monthly',
    tags = ['mysql','sandbox','surveyquestions'],
) 

# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
#     )

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

