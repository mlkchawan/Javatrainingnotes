#Python code for "zuora_customer_demographics"
#date: 4/01/2024
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
        cursor = pg_hook.cursor()
        today = pd.Timestamp.now().date()
        start_date = today.replace(day=1)
        end_date = start_date + pd.offsets.MonthEnd(0)
        count_df = pd.DataFrame(columns=['Date', 'Country', 'State', 'Account_Count', 'Subscription_Count', 'Product_Count'])
        report_dates = pd.date_range(start=start_date, end=end_date, freq='MS')
 
        for report_date in report_dates:
            sql_query = f'''
            select DATE('{report_date}') as Date,a."AccountId",a."SubscriptionName",a."SubscriptionId",
            a."SubscriptionStatus",a."ProductFamily",a."ProductName", a."RatePlanName",c."Country",
            c."State",c."City" from reportsdb.zuoradata a
            inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
            where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}')
            group by "SubscriptionName") b
            on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion"
            inner join
            (select "Country","State","City","Accountid" from reportsdb.zuora_contact
            group by "Country","State","City","Accountid") c on a."AccountId"= c."Accountid"
            where a."SubscriptionTermStartDate" <= DATE('{report_date}') and 
            (a."SubscriptionCancelledDate" is null or a."SubscriptionCancelledDate" > DATE('{report_date}'))
            and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and a."AmendmentEffectiveDate" is not null)
            or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}'))
            or (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}')))'''

            cursor.execute(sql_query)
            records = cursor.fetchall()
            # column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(records, columns=['Date','AccountId','SubscriptionName','SubscriptionId','SubscriptionStatus','ProductFamily', 'ProductName','RatePlanName','Country','State','City']) 
            df['State'] = df['State'].apply(lambda x: '***' if pd.isna(x) else x)
            count_unique = df.groupby(['Date','Country','State']).agg(
                    {        
                    'AccountId': 'nunique',
                    'SubscriptionName': 'nunique',
                    'ProductName': 'size'
                    }).reset_index()
            count_unique.columns=['Date','Country','State','Account_Count','Subscription_Count','Product_Count']
            count_df=pd.concat([count_df,count_unique],ignore_index=True) 
       
        kwargs['ti'].xcom_push(key='zuora_customer_demographics', value=count_df)
        cursor.close()
    
    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe", key='zuora_customer_demographics') 
    try:
        df.to_sql(name='zuora_customer_demographics', con=engine, if_exists='append', index=False, schema='reportsdb',chunksize=10, method='multi')  
        cursor = pg_hook.cursor()
        grant_query = ''' GRANT SELECT ON reportsdb."zuora_customer_demographics" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,mayankb,areddy,kmudragada; '''
        cursor.execute(grant_query)

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()
        for _, row in df.iterrows():
            upsert_query = f'''
            INSERT INTO reportsdb."zuora_customer_demographics" ("Date", "Country", "State", "Account_Count", "Subscription_Count", "Product_Count")
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("Date", "Country", "State") DO NOTHING;
            '''
            cursor.execute(upsert_query, (row['Date'], row['Country'], row['State'], row['Account_Count'],
                                          row['Subscription_Count'], row['Product_Count']))
        
        pg_hook.commit()
        cursor.close()
    finally:
        pg_hook.close()
 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'customer_demographics_count',
    default_args=default_args,
    description='DAG for customer_demographics',
    schedule_interval='@monthly',
    tags = ['production','zuora','customer_demographics']
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




