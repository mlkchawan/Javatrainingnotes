from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

engine = PostgresHook.get_hook('warehouse-prod-mb').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('warehouse-prod-mb').get_conn()
pg_hook.autocommit = True 

def extract_activecount_from_postgres(**kwargs):
    cursor = pg_hook.cursor()
    results = []
    cancelled_acc_ids=[]
        # current_timestamp = datetime.now()
        # start = current_timestamp - timedelta(days=2)
        # end = current_timestamp - timedelta(days=1)
        # start_date = start.strftime('%Y-%m-%d')
        # end_date = end.strftime('%Y-%m-%d')
        # cursor = pg_hook.cursor()
    # count_df = pd.DataFrame(columns=['Date','Account_Count','Subscription_Count','Product_Count'])       
    report_dates = pd.date_range(start='2023-05-01', end='2024-03-13',freq='MS')
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
        for report_date in report_dates:
                # end_date = report_date + pd.offsets.MonthEnd(0)
                can_dates = pd.date_range(start='2023-06-01',end='2024-03-13',freq='D')
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
                final_cancel_df = pd.DataFrame(cancelled_acc_ids,
                                       columns=['AccountId', 'SubscriptionName', 'ProductFamily', 'ProductName',
                                                'RatePlanName', 'Date'])
           
        count_unique = final_cancel_df.groupby('Date').agg(
                {        
                    'AccountId': 'nunique',
                }).reset_index()
        count_unique.columns=['Date','Accounts_Cancelled'] 
    kwargs['ti'].xcom_push(key='Churn', value=count_unique)
    # Close the connection when done
    cursor.close() 
           
    # count_unique.to_csv('c:/dags/cancelled_acc_ids_count3.csv', index=False)
           

def load_churn_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_activecount_from_postgres",key='Churn')
    if df is None or df.empty:
        return      
    try:
        df.to_sql(name='turbifycounts', con=engine, if_exists='append', index=False, schema='reportsdb',chunksize=1000, method='multi')
    except Exception as e:
        print(f"Caught a load Exception: {e}") 
        cursor = pg_hook.cursor()
        upsert_query = f'''
                            INSERT INTO reportsdb.turbifycounts("Date","Accounts_Cancelled")
                            VALUES (%s, %s,)
                            ON CONFLICT ("Date") DO UPDATE
                            SET "Accounts_Cancelled"=EXCLUDED."Accounts_Cancelled";
                        '''
        for index, row in df.iterrows():
            values = (row['Date'], row['Accounts_Cancelled'])
            cursor.execute(upsert_query,values)
        pg_hook.commit()
        cursor.close()
    finally:
        pg_hook.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),    
}


dag = DAG(
    'zuora_turbifycounts_churn_Accounts_Cancelled',
    default_args=default_args,
    description='DAG for churncounts-Accounts',
    schedule_interval='0 7 * * *',
    tags = ['production','zuora','07','churncounts'], 
    catchup=False, 
)


fetch_task = PythonOperator(
    task_id='extract_activecount_from_postgres',
    python_callable=extract_activecount_from_postgres,
    provide_context=True,
    dag=dag,
)



load_task = PythonOperator(
    task_id='load_churn_to_postgres',
    python_callable=load_churn_to_postgres,
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(hours=2),
)



# Set task dependencies
fetch_task >> load_task