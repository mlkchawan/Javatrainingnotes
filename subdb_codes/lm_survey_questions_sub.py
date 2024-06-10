#python code for extracting data from mysql and loading into Postgres "survey_questions"
#date: 29/01/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_conn()
pg_hook.autocommit = True

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-prod')
    
    connection = mysql_hook.get_conn()
   
    mysql_hook.schema='subscriptions'
    #Execute the query and fetch data
    mysql_query = 'SELECT * FROM survey_questions limit 1000'
    result = mysql_hook.get_pandas_df(mysql_query)

    # Load data into a Pandas DataFrame 
    df =result.fillna("###")
    kwargs['ti'].xcom_push(key='survey_questions', value=df)

    #close the connection
    logging.info(f'{df}')
    connection.close()
 
#loading data into Postgres 
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='survey_questions')    
    try:
        df.to_sql(name='sub_survey_questions',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #alter the table to add primary key
        cursor = pg_hook.cursor() 
        grant_query = ''' GRANT SELECT ON reportsdb."sub_survey_questions" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati; ''' 
        cursor.execute(grant_query)
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'sub_survey_questions';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.sub_survey_questions ADD PRIMARY KEY ("id");'''
            cursor.execute(alter_query)
            pg_hook.commit()
    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor() 
        upsert_query = f'''
                    INSERT INTO reportsdb.sub_survey_questions({", ".join(['"{}"'.format(col) for col in df.columns])})
                    VALUES ({", ".join(['%s' for _ in df.columns])})
                    ON CONFLICT ("id") DO NOTHING
                '''
        cursor.execute(upsert_query)
        pg_hook.commit()
        logging.info("data loaded")   
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
    dag_id='subdb_to_pg_survey_questions_prod',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@monthly',
    tags = ['mysql','sandbox','surveyquestions'],
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