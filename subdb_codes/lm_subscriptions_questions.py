#python code for extracting data from mysql and loading into Postgres "survey_question_choices"
#date: 29/01/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

engine = PostgresHook.get_hook('warehouse-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('warehouse-qa').get_conn()
pg_hook.autocommit = True

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-qa')
    connection = mysql_hook.get_conn()
   
    mysql_hook.schema='subscriptions'
    #Execute the query and fetch data
    mysql_query = 'SELECT * FROM survey_questions_choices limit 10'
    result = mysql_hook.get_pandas_df(mysql_query)

    # Load data into a Pandas DataFrame 
    df =result.fillna("###")
    kwargs['ti'].xcom_push(key='survey_questions_choices', value=df)

    #close the connection
    logging.info(f'{df}')
    connection.close()


#loading into postgres
        
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='survey_questions_choices')    
    try:
        
        df.to_sql(name='sub_survey_questions_choices',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
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
    dag_id='subdb_to_pg_survey_questions_choices_sandbox',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@monthly',
    tags = ['mysql','sandbox','survey_questions_choices'],
) 

#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable= extract_and_load_to_dataframe,
    provide_context=True,
    dag=dag,
)

create_and_load= PythonOperator(
    task_id='create_and_load_table',
    python_callable=create_and_load_table,
    provide_context=True,
    dag=dag,
)

#dependencies
extract_data >> create_and_load
