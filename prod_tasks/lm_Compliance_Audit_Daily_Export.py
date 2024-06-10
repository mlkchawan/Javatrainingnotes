#Python code Compliance_Audit_Daily_Export 
#date: 14/03/2024
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
#         cursor.execute(f"SELECT value FROM salesforce_airflow.sf_metadata WHERE object = 'Compliance_Audit_Daily'")
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()        
        
def extract_and_load_to_dataframe(**kwargs):
    try:
        # last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value").strftime('%Y-%m-%d')
        # print(last_processed_value)
        cursor = pg_hook.cursor()
        postgres_query = f'''select u."Name" as "Agent",u."Avaya_ID__c" as "avaya_id",u."ManagerId" as manager,u."Alias" as "VZ_ID",u."LOB__c",
                            u."Division",op."Opportunity_Number__c",op."CloseDate",op."Total_Order_Value__c",op."Lead__c",op."Sales_Type__c" as "Sales_type",
                            c."CaseNumber",ca."Name",ca."Pass_Fail__c",ca."Opportunity__c",ca."Case__c",ca."Notes__c",ld."Lead_Number__c",date(ca."CreatedDate") as "CreatedDate",ca."Audit_Type__c",
                            ca."Agent__c" ,ca."Failure_Reasons__c",ca."LastModifiedDate",ca."LastModifiedById",co."Name" as "Auditor_name"
                            from salesforce_airflow."Compliance_Audit__c" ca 
                            left join salesforce_airflow."Opportunity" op on ca."Opportunity__c"=op."Id"
                            left join salesforce_airflow."User" u on ca."Agent__c"=u."Id"
                            left join salesforce_airflow."User" co on ca."CreatedById"=co."Id"
                            left join salesforce_airflow."Lead" ld on op."Lead__c"=ld."Id" 
                            left join salesforce_airflow."Case" c on ca."Case__c"=c."Id" 
                            where date(ca."CreatedDate") > '2023-01-01' '''  
        cursor.execute(postgres_query)
        records = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        df = pd.DataFrame(records, columns=column_names)
        try:
            if not df.empty:
                last_processed_value = df['CreatedDate'].iloc[-1]
                try:
                    cursor.execute(f"INSERT INTO salesforce_airflow.sf_metadata (object, value) VALUES ('Compliance_Audit_Daily', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (last_processed_value, last_processed_value))
                
                finally:
                    cursor.close()
            else:
                print("DataFrame is empty.")
 
        except IndexError as e:
            logging.error(f"Error during data transfer: {str(e)}")
            raise                    
        # Push the DataFrame to XCom
        kwargs['ti'].xcom_push(key='Compliance_Audit_Daily_Export', value=df)
 
        cursor.close()
    except IndexError as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise 
   

#checking records in dataframe
def check_records(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='Compliance_Audit_Daily_Export')    
    if df is None or df.empty:
        return 'skip_loading_task'
    else:
        return 'load_data_to_postgres'

#loading data to postgres    
def load_data_to_postgres(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids="extract_and_load_to_dataframe",key='Compliance_Audit_Daily_Export')  
 
    try:
        #df.drop_duplicates(subset=["RatePlanid"], keep='first', inplace=True)  # Remove duplicates        
        df.to_sql(name='Compliance_Audit_Daily_Export', con=engine, if_exists='replace', index=False, schema='salesforce_airflow',chunksize=1000, method='multi')  
        cursor = pg_hook.cursor()
 
        #Add Primary Key Constraint:
        grant_query = ''' GRANT SELECT ON salesforce_airflow."Compliance_Audit_Daily_Export" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
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
        #             INSERT INTO salesforce_airflow.Compliance Audit Daily Export({", ".join(['"{}"'.format(col) for col in df.columns])})
        #             VALUES ({", ".join(['%s' for _ in df.columns])})
        #             ON CONFLICT ("SurveyID") DO UPDATE SET
        #             {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in df.columns])}
        #         '''
        #         cursor.executemany(upsert_query,rows)
        #         rows = []
        #     if rows:
        #         upsert_query = f'''
        #             INSERT INTO salesforce_airflow.Compliance Audit Daily Export({", ".join(['"{}"'.format(col) for col in df.columns])})
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
    'start_date': datetime(2024, 3, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'Compliance_Audit_Daily_Export_prod',
    default_args=default_args,
    description='A DAG to transfer data between PostgreSQL databases',
    schedule_interval='@daily',
    tags = ['postgres','Compliance_Audit']
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