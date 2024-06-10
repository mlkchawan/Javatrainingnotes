from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
 

engine = PostgresHook.get_hook('warehouse-prod-mb').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('warehouse-prod-mb').get_conn()
pg_hook.autocommit = True
        
def extract_and_load_to_dataframe(**kwargs):
    try:
        results = []
        cancelled_acc_ids=[]
        current_timestamp = datetime.now()
        start = current_timestamp - timedelta(days=2)
        end = current_timestamp - timedelta(days=1)
        start_date = start.strftime('%Y-%m-%d')
        end_date = end.strftime('%Y-%m-%d')
        cursor = pg_hook.cursor()
        count_df = pd.DataFrame(columns=['Date','Account_Count','Subscription_Count','Product_Count'])
        report_dates = pd.date_range(start=start_date, end=end_date)
        # month_dates = [(month, month + pd.offsets.MonthEnd()) for month in report_dates]
        # for start_date, end_date in month_dates:
        for report_date in report_dates:                        
            sql_query = f'''
                    select a."AccountId",a."SubscriptionName",a."ProductFamily",a."ProductName", a."RatePlanName" from reportsdb.zuoradata a
                    inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
                        where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}')
                            group by "SubscriptionName") b
                    on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion"
                    where a."SubscriptionTermStartDate" <= DATE('{report_date}')  and (a."SubscriptionCancelledDate" is null or a."SubscriptionCancelledDate" > DATE('{report_date}'))
                    and a."ProductName" != 'Business Maker' and a."ProductName" NOT LIKE '%India%'
                    and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and a."AmendmentEffectiveDate" is not null)
                    or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}'))
                    or (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}')))
                    '''
       
            cursor.execute(sql_query)                    
            result = cursor.fetchall()            
            results.extend(result)
            result_df = pd.DataFrame(results, columns=['AccountId','SubscriptionName','ProductFamily', 'ProductName','RatePlanName'])
            cancels = []
            end_date=report_date+pd.offsets.MonthEnd(0)
            print(report_date,end_date)
            can_dates = pd.date_range(start=report_date, end=end_date)
            
            for can_date in can_dates:                        
                sql_can_query = f'''
                        select DATE('{can_date}') as Date,"AccountId","SubscriptionName","ProductFamily","ProductName","RatePlanName" from reportsdb.zuoradata where DATE("RatePlanCreatedDate") = DATE('{can_date}') and "SubscriptionCancelledDate" <= DATE('{can_date}') and "ProductName" != 'Business Maker' and "ProductName" NOT LIKE '%India%' and  ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and "AmendmentEffectiveDate" <= DATE('{can_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{can_date}')) or ("SubscriptionTermStartDate" > DATE('{can_date}')))))
                        union all select DATE('{can_date}') as Date,"AccountId","SubscriptionName","ProductFamily","ProductName","RatePlanName" from reportsdb.zuoradata where DATE("RatePlanCreatedDate") < DATE('{can_date}') and "SubscriptionCancelledDate" = DATE('{can_date}') and "ProductName" != 'Business Maker' and "ProductName" NOT LIKE '%India%' and ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and "AmendmentEffectiveDate" <= DATE('{can_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{can_date}')) or ("SubscriptionTermStartDate" > DATE('{can_date}')))))                    
                        '''           
                cursor.execute(sql_can_query)                    
                cancel = cursor.fetchall()            
                cancels.extend(cancel)  
            cancel_df= pd.DataFrame(cancels, columns=['Date','AccountId','SubscriptionName','ProductFamily1', 'ProductName1','RatePlanName1'])
            merged_df = pd.merge(result_df, cancel_df, on=['AccountId', 'SubscriptionName'], how='left', indicator=True)
            cancelled_accounts = merged_df.groupby(['AccountId']).filter(lambda x: (x['_merge'] == 'both').all())
            cancelled_acc_ids.extend(cancelled_accounts[['AccountId', 'SubscriptionName','ProductFamily','ProductName','RatePlanName','Date']].values.tolist())
       
            cancels = []
            results = []
            final_cancel_df = pd.DataFrame(cancelled_acc_ids, columns=['AccountId', 'SubscriptionName','ProductFamily','ProductName','RatePlanName','Date'])
            count_unique = final_cancel_df.groupby('Date').agg(
                {        
                'AccountId': 'nunique',
                }).reset_index()
            count_unique.columns=['Date','  ']
            count_df=pd.concat([count_df,count_unique],ignore_index=True)
                # count_df[['Account_Count','Subscription_Count','Product_Count','Date']]=count_unique[['Account_Count','Subscription_Count','Product_Count','Date']]
            print(count_df)
               
        kwargs['ti'].xcom_push(key='turbifycounts', value=count_df)

    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise
   

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe", key='turbifycounts') 
    try:
        df.to_sql(name='turbifycounts', con=engine, if_exists='append', index=False, schema='reportsdb',chunksize=10, method='multi')  
        cursor = pg_hook.cursor()
 
        #Add Primary Key Constraint:
        grant_query = ''' GRANT SELECT ON reportsdb."turbifycounts" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati; '''
        cursor.execute(grant_query)

    except Exception as e:
        print(f"Caught an Exception: {e}")
        cursor = pg_hook.cursor()

        # Insert or update data into the "turbifycounts" table
        for _, row in df.iterrows():
            upsert_query = f'''
                INSERT INTO reportsdb."turbifycounts" ("Date", "Account_Count", "Subscription_Count", "Product_Count")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ("Date") DO UPDATE
                SET "Account_Count" = EXCLUDED."Account_Count",
                    "Subscription_Count" = EXCLUDED."Subscription_Count",
                    "Product_Count" = EXCLUDED."Product_Count";
            '''
            cursor.execute(upsert_query, (row['Date'], row['Account_Count'], row['Subscription_Count'], row['Product_Count']))
 
        pg_hook.commit()
        cursor.close()
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'postgres_data_turbifycounts_table',
    default_args=default_args,
    description='A DAG to transfer data between PostgreSQL databases',
    schedule_interval='@daily',
    tags = ['postgres','turbifycounts']
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
)

fetch_data_task >> load_data_taskx