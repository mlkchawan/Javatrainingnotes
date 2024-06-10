#Python code for Extracting data from mysql and loading into Postgres "carts table" for type="change"
#date: 30/01/2024
#created by: Laxmikanth M

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
import logging
import pandas as pd
import json

engine = PostgresHook.get_hook('warehouse-prod-mb').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('warehouse-prod-mb').get_conn()
pg_hook.autocommit = True

# def get_last_processed_value():
#     cursor = pg_hook.cursor()
#     try:
#         cursor.execute(f"SELECT value FROM reportsdb.sub_metadata WHERE object = 'sub_subscr_services'")
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()

#Python function to extract data from MySQL and load into a DataFrame
def extract_and_load_to_dataframe(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='subscriptiondb-prod')
    connection = mysql_hook.get_conn()
    mysql_hook.schema = 'subscriptions'
    mysql_query = '''SELECT id, cart, created_at, modified_at
                     FROM carts
                     WHERE date(created_at) > '2022-01-01';'''
    result = mysql_hook.get_pandas_df(mysql_query)
    data = []
 
    for _, row in result.iterrows():
        json_data = json.loads(row['cart'])
        if json_data.get('type') == 'change':
            record = {
                'id': row['id'],
                'cart_type': json_data.get('type', ''),
                'cart_id': json_data.get('id', ''),
                'created_at': row['created_at'],
                'modified_at': row['modified_at']
            }
            
            if 'subscriptions' in json_data and isinstance(json_data['subscriptions'], list):
                for subscription in json_data['subscriptions']:
                    subscriptionNumber = subscription.get('subscriptionNumber', '')
                    previousSubscriptionNumber = subscription.get('previousSubscriptionNumber', '')

                    if 'meta' in subscription and isinstance(subscription['meta'], dict):
                        domain = subscription['meta'].get('domain', '')
                        meta_paymentToken = subscription['meta'].get('paymentToken', '')
                        meta_oacs = subscription['meta'].get('oacs', '')
                        meta_domain_extension = '.' + domain.split('.')[-1] if domain else ''
                        meta_meta_pvtreg = subscription['meta'].get('pvtreg', '')
                        meta_businessId = subscription['meta'].get('businessId', '')
                        
                    else:
                        domain = ''
                        meta_paymentToken = ''
                        meta_oacs = ''
                        meta_domain_extension = ''
                        meta_meta_pvtreg =''
                        meta_businessId =''
                       
 
                    add_rate_plans = subscription.get('addRatePlans', [])
                    for rate_plan in add_rate_plans:
                        rate_plan_id = rate_plan.get('ratePlanId', '')
                        product_id = rate_plan.get('productId', '')
                        rateplan_type = rate_plan.get('type', '')
                        family = rate_plan.get('family', '')
                       
 
                    remove_rate_plans = subscription.get('removeRatePlans', [])
                    for rate_plan in remove_rate_plans:
                        removeRatePlans_rate_plan_id = rate_plan.get('ratePlanId', '')
                        removeRatePlans_product_id = rate_plan.get('productId', '')
                        removeRatePlans_rateplan_type = rate_plan.get('type', '')
                        removeRatePlans_family = rate_plan.get('family', '')
                       
 
                    existing_rate_plans = subscription.get('existingRatePlans', [])
                    for rate_plan in existing_rate_plans:
                        existingRatePlans_rate_plan_id = rate_plan.get('ratePlanId', '')
                        existingRatePlans_product_id = rate_plan.get('productId', '')
                        existingRatePlans_rateplan_type = rate_plan.get('type', '')
                        existingRatePlans_family = rate_plan.get('family', '')
                        existingRatePlans_ratePlanStatus = rate_plan.get('ratePlanStatus', '')
 
                    data.append({
                        'id': row['id'],
                        'created_at': row['created_at'],
                        'modified_at': row['modified_at'],
                        'cart_type': json_data.get('type', ''),
                        'cart_id': json_data.get('id', ''),
                       
                        'subscriptionNumber': subscriptionNumber,
                        'previousSubscriptionNumber': previousSubscriptionNumber,
                        'paymentToken': meta_paymentToken,
                        'oacs': meta_oacs,
                        'domain': domain,
                        'domain_extension': meta_domain_extension,
                        'pvtreg': meta_meta_pvtreg,
                        'businessId': meta_businessId,
                        
                        'addRatePlans_rate_plan_id': rate_plan_id,
                        'addRatePlans_product_id': product_id,
                        'addRatePlans_rateplan_type': rateplan_type,
                        'addRatePlans_family': family,
                       
                        'removeRatePlans_rate_plan_id': removeRatePlans_rate_plan_id,
                        'removeRatePlans_product_id': removeRatePlans_product_id,
                        'removeRatePlans_rateplan_type': removeRatePlans_rateplan_type,
                        'removeRatePlans_family': removeRatePlans_family,
                      
                        'existingRatePlans_rate_plan_id': existingRatePlans_rate_plan_id,
                        'existingRatePlans_product_id': existingRatePlans_product_id,
                        'existingRatePlans_rateplan_type': existingRatePlans_rateplan_type,
                        'existingRatePlans_family': existingRatePlans_family,
                        'existingRatePlans_ratePlanStatus': existingRatePlans_ratePlanStatus
                    })
    data.append(record)
 
    # Create DataFrame
        
    df = pd.DataFrame(data)
    # df = df.drop(columns=['meta'])    
    
    kwargs['ti'].xcom_push(key='subscription_carts', value=df)
    
    # Close the connection
    connection.close()

       
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='subscription_carts')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'create_and_load_table'
    
#loading data into Postgres 
def create_and_load_table(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='subscription_carts')    
    # if df is None or df.empty:
    #     return
    try:
        df.to_sql(name='sub_carts_change',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor()
        grant_query = ''' GRANT SELECT ON reportsdb."sub_carts_change" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
        cursor.execute(grant_query) 

        # pk_check_query = f'''
        #     SELECT COUNT(*)
        #     FROM information_schema.table_constraints
        #     WHERE constraint_type = 'PRIMARY KEY'
        #     AND table_schema = 'reportsdb'
        #     AND table_name = 'sub_carts_table';
        # '''
        # cursor.execute(pk_check_query)                    
        # primary_key_count = cursor.fetchone()[0]
        # if primary_key_count == 0:
        #     alter_query=f'''ALTER TABLE reportsdb.sub_carts_table ADD PRIMARY KEY ("id");'''
        #     cursor.execute(alter_query)
        #     pg_hook.commit()
    except Exception as e:
        print(f"Caught an Exception: {e}")
        # cursor = pg_hook.cursor()
        # batch_size = 1000
        # rows = []
        # for _, row in df.iterrows():
        #     rows.append(tuple(row))
        #     if len(rows) == batch_size:
        #         upsert_query = f'''
        #             INSERT INTO reportsdb.sub_carts_table({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})                    
        #         '''

        #         cursor.executemany(upsert_query,rows)
        #         rows = []
        #     if rows:
        #         upsert_query = f'''
        #             INSERT INTO reportsdb.sub_carts_table({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #         '''
        #     cursor.executemany(upsert_query,rows)

        
        # logging.info("Data loaded.")
   
    finally:
        #pg_hook.commit()
        # cursor.close()
        pg_hook.close() 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='subdb_to_pg_carts_changes_prod',
    default_args=default_args,
    description='Extract data from MySQL and load into postgres',
    schedule_interval='@daily',
    tags = ['mysql','prod','carts'],
) 

# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
#     )

#Define an operator that executes the Python function
extract_data = PythonOperator(
    task_id='extract_and_load_to_dataframe',
    python_callable= extract_and_load_to_dataframe,
    provide_context=True,
    dag=dag,
)


check_records_task = BranchPythonOperator(
    task_id='check_records',
    python_callable=check_records,
    provide_context=True,
    dag=dag,
)

create_and_load= PythonOperator(
    task_id='create_and_load_table',
    python_callable=create_and_load_table,
    provide_context=True,
    dag=dag,
)

skip_loading_task = DummyOperator(
    task_id='skip_loading_task',
    dag=dag,
)

#dependencies
extract_data >> check_records_task
check_records_task >> [create_and_load, skip_loading_task]
	
	