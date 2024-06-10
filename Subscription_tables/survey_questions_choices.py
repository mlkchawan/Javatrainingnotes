#python code for extracting data from mysql and loading into Postgres "survey_question_choices"
#date: 29/01/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import pandas as pd

engine = PostgresHook.get_hook('warehouse-qa-mb').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('warehouse-qa-mb').get_conn()
pg_hook.autocommit = True

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-qa')
    connection = mysql_hook.get_conn()
    
    #Execute the query and fetch data
    mysql_query = 'SELECT * FROM survey_question_choices '
    result = mysql_hook.get_records(mysql_query)

    # Load data into a Pandas DataFrame
    df = pd.DataFrame(result)
    kwargs['ti'].xcom_push(key='survey_question_choices', value=df)
    
    # #load to csv file
    # df.to_csv('dags/surveyquestions.csv',sep=',' ,index=False)
    connection.close()

#loading data into Postgres 
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='survey_question_choices')    
    try:
        df.to_sql(name='sub_survey_question_choices',con=engine, if_exists='replace', index=True, schema='reportsdb',chunksize=None, method='multi')
        cursor = pg_hook.cursor()
        print("Data inserted successfully")
           
    except psycopg2.Error as e:
        print("Error:", e)
        pg_hook.commit()
        cursor.close()  
    finally:
        pg_hook.close()    

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='mysql_to_postgres_surveyquestions_choices',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@daily'
) 

#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable=extract_and_load_to_dataframe,
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