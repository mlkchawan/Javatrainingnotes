#Python code Sub_domain_Summary table 
#date: 23/02/2024
#created by : Laxmikanth

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging
from airflow.models import Variable 

engine = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_sqlalchemy_engine()
pg_hook = PostgresHook.get_hook('pg-warehouse-airflowetlusr-prod').get_conn()
pg_hook.autocommit = True

# def get_last_processed_value(**kwargs):
#     cursor = pg_hook.cursor()
#     try:
#         cursor.execute(f"SELECT value FROM salesforce_airflow.sf_metadata WHERE object = 'CSAT_Summarytable_z'")
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()        
        
def extract_and_load_to_dataframe(**kwargs):
    try:
        # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value").strftime('%Y-%m-%d')
        # print(last_processed_value)
        cursor = pg_hook.cursor()
        postgres_query = f'''
        select a.id,a.status,date(a.created_at) as "created_at",date(a.modified_at)as "modified_at",a.user_id,a.business_id,a.external_id as "subscription_name",b.name,b.subscription_rateplan_id,b.subscription_id,
            c.rateplan_id,a.display_name from reportsdb.sub_subscriptions a join reportsdb.sub_subscription_rateplans c on a.id=c.subscription_id join 
            reportsdb.sub_subscription_services b on b.subscription_rateplan_id=c.id ''';
        cursor.execute(postgres_query)
        records = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        df = pd.DataFrame(records, columns=column_names)
        df['domain_extension'] = df['display_name'].apply(lambda x: x.split('.')[-1] if '.' in x else x)
        # try:
        #     if not df.empty:
        #         last_processed_value = df['CaseCreated'].iloc[-1]
        #         try:
        #             cursor.execute(f"INSERT INTO salesforce_airflow.sf_metadata (object, value) VALUES ('CSAT_Summarytable_z', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (last_processed_value, last_processed_value))
                
        #         finally:
        #             cursor.close()
        #     else:
        #         print("DataFrame is empty.")
 
        # except IndexError as e:
        #     logging.error(f"Error during data transfer: {str(e)}")
        #     raise                    
        # Push the DataFrame to XCom
        kwargs['ti'].xcom_push(key='Cart_Summary_table', value=df)
 
        cursor.close()
    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise 
   

#checking records in dataframe
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='Cart_Summary_table')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'load_data_to_postgres'

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='Cart_Summary_table')  
 
    try:
        #df.drop_duplicates(subset=["RatePlanid"], keep='first', inplace=True)  # Remove duplicates        
        df.to_sql(name='Sub_Domain_Summary', con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')  
        cursor = pg_hook.cursor()
 
        #Add Primary Key Constraint:
        grant_query = ''' GRANT SELECT ON reportsdb."Sub_Domain_Summary" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
        cursor.execute(grant_query)
        # pk_check_query = f'''
        #     SELECT COUNT(*)
        #     FROM information_schema.table_constraints
        #     WHERE constraint_type = 'PRIMARY KEY'
        #     AND table_schema = 'salesforce_airflow'
        #     AND table_name = 'CSAT_Summarytable_Salesforce';
        # '''
        # cursor.execute(pk_check_query)                    
        # primary_key_count = cursor.fetchone()[0]
        # if primary_key_count == 0:
        #     alter_query=f'''ALTER TABLE salesforce_airflow.CSAT_Summarytable_Salesforce ADD PRIMARY KEY ("SurveyID");'''
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
        #             INSERT INTO reportsdb.CSAT_Summarytable_Salesforce({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #             ON CONFLICT ("SurveyID") DO UPDATE SET
        #             {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
        #         '''
        #         cursor.executemany(upsert_query,rows)
        #         rows = []
        #     if rows:
        #         upsert_query = f'''
        #             INSERT INTO reportsdb.CSAT_Summarytable_Salesforce({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #             ON CONFLICT ("SurveyID") DO UPDATE SET
        #             {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
                   
        #         '''
        #         cursor.executemany(upsert_query,rows)
        # pg_hook.commit()
        # cursor.close()
   
    finally:
        pg_hook.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Sub_Domain_Summary_table_prod',
    default_args=default_args,
    description='A DAG to transfer data between PostgreSQL databases',
    schedule_interval='@daily',
    tags = ['postgres','CSAT']
)

# get_last_processed_value = PythonOperator(
#     task_id = 'get_last_processed_value',
#     python_callable= get_last_processed_value,
#     provide_context=True,
#     dag=dag,
#     ) 

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

check_records_task = BranchPythonOperator(
    task_id='check_records',
    python_callable=check_records,
    provide_context=True,
    dag=dag,
)


skip_loading_task = DummyOperator(
    task_id='skip_loading_task',
    dag=dag,
)
 
#dependencies 
fetch_data_task >> check_records_task
check_records_task >> [load_data_task, skip_loading_task]