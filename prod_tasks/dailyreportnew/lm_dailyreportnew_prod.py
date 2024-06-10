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
        # count_df = pd.DataFrame(columns=['date','ProductName','RatePlanName','totalactive', 'mrr_active', 'tcv_active','cancelled','cancelledchanged','mrr_cancelled','tcv_cancelled','new','changed','mrr_new','tcv_new'])

        report_dates = pd.date_range(start=start_date, end=end_date) 
        for report_date in report_dates:                        
            sql_query = f'''select DATE('{report_date}') as date,"ProductName", "RatePlanName", "cnt" as new, "changecnt" as changed,"mrr" as mrr_new, "tcv" as tcv_new from 
                            (select "ProductName", "RatePlanName", sum("cnt") as cnt, sum("changecnt") as changecnt, sum("mrr") as mrr, sum("tcv") as tcv  from 
                            (select "ProductName", "RatePlanName", count(*) as cnt, 0 as changecnt, sum("MRR") as mrr, sum("TCV") as tcv from reportsdb.zuoradata
                            where DATE("RatePlanCreatedDate") = DATE('{report_date}') and "SubscriptionTermStartDate" <= DATE('{report_date}') and "SubscriptionVersion" = 1 
                            group by "ProductName", "RatePlanName" union all
                            select "ProductName", "RatePlanName", count(*) as cnt,0 as changecnt, sum("MRR") as mrr, sum("TCV") as tcv from reportsdb.zuoradata
                            where DATE("RatePlanCreatedDate") < DATE('{report_date}') and "SubscriptionTermStartDate" = DATE('{report_date}') and "SubscriptionVersion" = 1 and "SubscriptionName" not in (select distinct "SubscriptionName" from reportsdb.zuoradata
                            where DATE("RatePlanCreatedDate") < DATE('{report_date}') and "SubscriptionCancelledDate" < DATE('{report_date}')) group by "ProductName", 
                            "RatePlanName" union all 
                            select "ProductName", "RatePlanName", count(*) as cnt, count(*) as changecnt, sum("MRR") as mrr, sum("TCV") as tcv from reportsdb.zuoradata 
                            where "AmendmentType" ='NewProduct' and DATE("RatePlanCreatedDate") = DATE('{report_date}') and "AmendmentEffectiveDate" <= DATE('{report_date}') and "AmendmentSubscriptionId" = "SubscriptionPreviousId" 
                            group by "ProductName", "RatePlanName" union all
                            select "ProductName", "RatePlanName", count(*) as cnt, count(*)  as changecnt, sum("MRR") as mrr, sum("TCV") as tcv from reportsdb.zuoradata 
                            where "AmendmentType" ='NewProduct' and DATE("RatePlanCreatedDate") < DATE('{report_date}') and "AmendmentEffectiveDate" = DATE('{report_date}') and "AmendmentSubscriptionId" = "SubscriptionPreviousId" and "SubscriptionName" not in 
                            (select distinct "SubscriptionName" from reportsdb.zuoradata 
                            where DATE("RatePlanCreatedDate") < DATE('{report_date}') and "SubscriptionCancelledDate" < DATE('{report_date}')) group by "ProductName", "RatePlanName") t1 
                            group by "ProductName", "RatePlanName") t2;
                '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
        
            df = pd.DataFrame(result, columns=column_names)
            df=df.fillna(0)
            # count_df=pd.concat([count_df,df],ignore_index=True)
        
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
        pk_check_query = f'''
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'PRIMARY KEY'
            AND table_schema = 'reportsdb'
            AND table_name = 'zuora_dailyreportnew';
        '''
        cursor.execute(pk_check_query)                    
        primary_key_count = cursor.fetchone()[0]
        if primary_key_count == 0:
            alter_query=f'''ALTER TABLE reportsdb.zuora_dailyreportnew ADD PRIMARY KEY ("date","ProductName","RatePlanName");'''
            cursor.execute(alter_query)
            pg_hook.commit()

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        for _, row in df.iterrows():
            upsert_query = '''
                INSERT INTO reportsdb.zuora_dailyreportnew ("date", "ProductName", "RatePlanName", "new", "changed", "mrr_new", "tcv_new")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("date", "ProductName", "RatePlanName")
                DO UPDATE SET
                    "new" = EXCLUDED."new",
                    "changed" = EXCLUDED."changed",
                    "mrr_new" = EXCLUDED."mrr_new",
                    "tcv_new" = EXCLUDED."tcv_new";
            '''
            cursor.execute(upsert_query, (row['date'], row['ProductName'], row['RatePlanName'], row['new'],row['changed'],row['mrr_new'],row['tcv_new']))
 
        pg_hook.commit()
        cursor.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'zuora_dailyreportnewCount_production',
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