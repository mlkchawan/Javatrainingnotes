#Python code for zuora_RevenueSumForCurAPByDate
#date: 20/05/2024
#created by : Laxmikanth

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
 

engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_conn()
pg_hook.autocommit = True 
        
def extract_and_load_to_dataframe(**kwargs):
    try:
        current_timestamp = datetime.now()
        start = current_timestamp - timedelta(days=2)
        end = current_timestamp - timedelta(days=1)
        start_date = start.strftime('%Y-%m-%d')
        end_date = end.strftime('%Y-%m-%d')
        date_range = pd.date_range(start=start_date, end=end_date)
        
        # all_data = []

        cursor = pg_hook.cursor()  
        for date_str in date_range:
            sql_query = f'''
            SELECT
                DATE('{date_str}') AS "RevenueDate",
                "ProductName",
                "Region",
                "Currency",
                SUM("RevenueAmount") AS "RevenueAmount"
            FROM reportsdb."RevenueSchedule"
            WHERE "AccountingPeriod" = (
                SELECT "Name"
                FROM reportsdb."AccountingPeriod"
                WHERE DATE('{date_str}') >= "StartDate"
                AND DATE('{date_str}') <= "EndDate"
            )
            AND "RevenueDate" <= DATE('{date_str}')
            GROUP BY "ProductName", "Region", "Currency";
            '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            if result:
                df = pd.DataFrame(result, columns=column_names)
                df.fillna(0, inplace=True)
                # all_data.append(df)
 
        # Concatenate all data into a single DataFrame
        # final_df = pd.concat(all_data, ignore_index=True)
                
        kwargs['ti'].xcom_push(key='zuora_RevenueSumForCurAPByDate', value=df)
        cursor.close()
    
    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise
   

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe", key='zuora_RevenueSumForCurAPByDate') 
    try:
        df.to_sql(name="zuora_RevenueSumForCurAPByDate", con=engine, if_exists='append', index=False, schema='reportsdb',chunksize=10, method='multi')  
        cursor = pg_hook.cursor()
        grant_query = ''' GRANT SELECT ON reportsdb."zuora_RevenueSumForCurAPByDate" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
        cursor.execute(grant_query)
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'zuora_RevenueSumForCurAPByDate';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb."zuora_RevenueSumForCurAPByDate" ADD PRIMARY KEY ("RevenueDate","ProductName","Region","Currency");'''
            cursor.execute(alter_query)
            pg_hook.commit()

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        for _, row in df.iterrows():
            upsert_query = '''
                INSERT INTO reportsdb.zuora_RevenueSumForCurAPByDate ("RevenueDate", "ProductName", "Region", "Currency", "RevenueAmount")
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT ("RevenueDate", "ProductName","Region","Currency")
                DO UPDATE SET
                    "RevenueAmount" = EXCLUDED."RevenueAmount";
            '''
            cursor.execute(upsert_query, (row['RevenueDate'], row['ProductName'], row['Region'], row['Currency'],row['RevenueAmount']))
 
        pg_hook.commit()
        cursor.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'zuora_RevenueSumForCurAPByDate_production',
    default_args=default_args,
    description='DAG for RevenueSumForCurAPByDate',
    schedule_interval='30 11 * * *',
    tags = ['production','zuora','07','RevenueSumForCurAPByDate']
)

fetch_data_task = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable=extract_and_load_to_dataframe,
    provide_context=True,
    dag=dag,
)
 
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

fetch_data_task >> load_data_task