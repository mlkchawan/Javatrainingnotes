from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
 

engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-qa').get_conn()
pg_hook.autocommit = True
        
def extract_and_load_to_dataframe(**kwargs):
    try:
        start_date = "2022-01-01"
        end_date = "2024-05-07"
        cursor = pg_hook.cursor()
        # count_df = pd.DataFrame(columns=['Date','Account_Count','Subscription_Count','Product_Count'])
        # final_df=pd.DataFrame()
        report_dates = pd.date_range(start=start_date, end=end_date) 
        for report_date in report_dates:                        
            sql_query = f'''select DATE('{report_date}') as date,"ProductName","RatePlanName","cnt" as cancelled,"changecnt" as cancelledchanged,"mrr" as "mrr_cancelled","tcv" as "tcv_cancelled" from (select "ProductName","RatePlanName",sum("cnt") 
            as cnt,sum("changecnt") as changecnt,sum(mrr) as mrr,sum(tcv) as tcv from (select "ProductName","RatePlanName",count(*) as cnt,count(*) 
            as changecnt,sum("MRR") as mrr,sum("TCV") as tcv from reportsdb.zuoradata where DATE("RatePlanCreatedDate") = DATE('{report_date}') and 
            "AmendmentEffectiveDate" <= DATE('{report_date}') and "AmendmentType" ='RemoveProduct' and "AmendmentSubscriptionId" = "SubscriptionPreviousId" 
            group by "ProductName","RatePlanName" union all select "ProductName","RatePlanName",count(*) as cnt,count(*) as changecnt,
            sum("MRR") as mrr,sum("TCV") as tcv from reportsdb.zuoradata where DATE("RatePlanCreatedDate") < DATE('{report_date}') and 
            "AmendmentEffectiveDate" = DATE('{report_date}') and "AmendmentType" ='RemoveProduct' and "AmendmentSubscriptionId" = "SubscriptionPreviousId" 
            and "SubscriptionName" not in (select distinct "SubscriptionName" from reportsdb.zuoradata where DATE("RatePlanCreatedDate") < DATE('{report_date}') 
            and "SubscriptionCancelledDate" < DATE('{report_date}')) group by "ProductName","RatePlanName" union all select "ProductName","RatePlanName",
            count(*) as cnt,0 as changecnt,sum("MRR") as mrr,SUM("TCV") as tcv from reportsdb.zuoradata where DATE("RatePlanCreatedDate") = DATE('{report_date}') and 
            "SubscriptionCancelledDate" <= DATE('{report_date}') and ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and 
            "AmendmentEffectiveDate" <= DATE('{report_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{report_date}')) or 
            ("SubscriptionTermStartDate" > DATE('{report_date}'))))) group by "ProductName","RatePlanName" union all select "ProductName","RatePlanName",
            count(*) as cnt,0 as changecnt,sum("MRR") as mrr,sum("TCV") as tcv from reportsdb.zuoradata where DATE("RatePlanCreatedDate") < DATE('{report_date}') 
            and "SubscriptionCancelledDate" = DATE('{report_date}') and ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and 
            "AmendmentEffectiveDate" <= DATE('{report_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{report_date}')) 
            or ("SubscriptionTermStartDate" > DATE('{report_date}'))))) group by "ProductName","RatePlanName") t1 group by "ProductName","RatePlanName") 
            t2;
                    '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
        
                    # Convert the result to a DataFrame
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

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        for _, row in df.iterrows():
            upsert_query = '''
                INSERT INTO reportsdb.zuora_dailyreportnew ("date", "ProductName", "RatePlanName", "cancelled", "cancelledchanged", "mrr_cancelled", "tcv_cancelled")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("date", "ProductName", "RatePlanName")
                DO UPDATE SET
                    "cancelled" = EXCLUDED."cancelled",
                    "cancelledchanged" = EXCLUDED."cancelledchanged",
                    "mrr_cancelled" = EXCLUDED."mrr_cancelled",
                    "tcv_cancelled" = EXCLUDED."tcv_cancelled";
            '''
            cursor.execute(upsert_query, (row['date'], row['ProductName'], row['RatePlanName'], row['cancelled'],row['cancelledchanged'],row['mrr_cancelled'],row['tcv_cancelled']))
 
        pg_hook.commit()
        cursor.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'zuora_dailyreportnewcancelled_sandbox',
    default_args=default_args,
    description='DAG for dailyreportnew',
    schedule_interval='0 7 * * *',
    tags = ['sandbox','zuora','07','dailyreportnew']
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