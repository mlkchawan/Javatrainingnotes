#Python code for summarization of zuoradata for "Active","cancelled","newadds" count 
#date: 10/05/2024
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
        cursor = pg_hook.cursor()
        # final_df=pd.DataFrame()
        report_dates = pd.date_range(start=start_date, end=end_date) 
        for report_date in report_dates:                        
            sql_query = f'''select DATE('{report_date}') as date, a."ProductName", a."RatePlanName", count(*) as totalactive, sum("MRR") as mrr_active, sum("TCV") as tcv_active from reportsdb.zuoradata
                a inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
                where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}') 
                group by "SubscriptionName") b on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion" 
                where a."SubscriptionTermStartDate" <= DATE('{report_date}')  and (a."SubscriptionCancelledDate" is null or 
                a."SubscriptionCancelledDate" > DATE('{report_date}')) and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and 
                a."AmendmentEffectiveDate" is not null) or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}')) or 
                (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}'))) group by a."ProductName", a."RatePlanName";
                '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            df = pd.DataFrame(result, columns=column_names)
            # final_df=pd.concat([final_df,df],ignore_index=True)
            df.fillna(0)
            kwargs['ti'].xcom_push(key='zuora_dailyreportnew', value=df)
       
        cursor.close()
    
    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise
   

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe", key='zuora_dailyreportnew') 
    try:
        df.to_sql(name='zuora_dailyreportnew', con=engine, if_exists='append', index=False, schema='reportsdb',chunksize=10, method='multi')  
        cursor = pg_hook.cursor()
        grant_query = ''' GRANT SELECT ON reportsdb."zuora_dailyreportnew" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
        cursor.execute(grant_query)
        # pk_check_query = f'''
        #     SELECT COUNT(*)
        #     FROM information_schema.table_constraints
        #     WHERE constraint_type = 'PRIMARY KEY'
        #     AND table_schema = 'reportsdb'
        #     AND table_name = 'zuora_dailyreportnew';
        # '''
        # cursor.execute(pk_check_query)                    
        # primary_key_count = cursor.fetchone()[0]
        # if primary_key_count == 0:
        #     alter_query=f'''ALTER TABLE reportsdb.zuora_dailyreportnew ADD PRIMARY KEY ("date","ProductName","RatePlanName");'''
        #     cursor.execute(alter_query)
        #     pg_hook.commit()

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        for _, row in df.iterrows():
            upsert_query = f'''
                INSERT INTO reportsdb."zuora_dailyreportnew" ("date", "ProductName", "RatePlanName", "totalactive", "mrr_active", "tcv_active")
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ("date","ProductName","RatePlanName") DO UPDATE
                SET "totalactive" = EXCLUDED."totalactive",
                    "mrr_active" = EXCLUDED."mrr_active",
                    "tcv_active" = EXCLUDED."tcv_active";
            '''
            cursor.execute(upsert_query, (row['date'], row['ProductName'], row['RatePlanName'], row['totalactive'],row['mrr_active'],row['tcv_active']))
 
        pg_hook.commit()
        cursor.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'zuora_dailyreportnewactive_production',
    default_args=default_args,
    description='DAG for dailyreportnew',
    schedule_interval='30 11 * * *',
    tags = ['production','zuora','07','dailyreportnew']
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