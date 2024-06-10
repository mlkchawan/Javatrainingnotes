from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from simple_salesforce import Salesforce
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd

class sf_etl_SurveyGizmo_Question_Response__c(BaseOperator):
    @apply_defaults
    def __init__(self, salesforce_conn_id, postgres_conn_id, salesforce_object, *args, **kwargs):
        super(sf_etl_SurveyGizmo_Question_Response__c, self).__init__(*args, **kwargs)
        self.salesforce_conn_id = salesforce_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.salesforce_object = salesforce_object        

    def execute(self, context):
        # Connect to Salesforce        
        sf_hook = SalesforceHook(self.salesforce_conn_id)
        salesforce_fields=sf_hook.get_available_fields(self.salesforce_object)
        escaped_sf_fields = [f'"{field}"' if ' ' in field else field for field in salesforce_fields]
        print(salesforce_fields)
        
        # Connect to PostgreSQL
        engine = PostgresHook.get_hook(self.postgres_conn_id).get_sqlalchemy_engine()
        pg_hook = PostgresHook.get_hook(self.postgres_conn_id).get_conn()
        pg_hook.autocommit = True

        try:
            last_processed_value = self.get_last_processed_value(pg_hook).strftime('%Y-%m-%dT%H:%M:%SZ')   
            print(last_processed_value)
            query = f"SELECT {', '.join(escaped_sf_fields)} FROM {self.salesforce_object} WHERE LastModifiedDate > {last_processed_value} ORDER BY LastModifiedDate ASC LIMIT 500000"
            print(query)
            result = sf_hook.make_query(
            query=query,            
            )
            
            if result['records'] :
                data = pd.DataFrame.from_dict(pd.json_normalize(result['records']), orient='columns')
                #DEALING SF DEFAULT DATETIME FORMAT
                if 'LastModifiedDate' in data:
                    data["LastModifiedDate"] = pd.to_datetime(data["LastModifiedDate"],errors='coerce')
                if 'CreatedDate' in data:
                    data["CreatedDate"] = pd.to_datetime(data["CreatedDate"],errors='coerce') 
                if 'attributes' in data:
                    del data['attributes'] 

                #Code block to handle new columns
                data_columns=set(data.columns)
                if engine.has_table(self.salesforce_object,schema='salesforce_airflow'):
                    self.log.info("Table Exists")                
                    existing_columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'salesforce_airflow' AND table_name = '{self.salesforce_object}';"
                    existing_columns = pd.read_sql(existing_columns_query, engine)['column_name'].tolist() 
                    new_columns = list(data_columns - set(existing_columns))
                    if new_columns:
                        self.log.info(f'New Columns are :{new_columns}')
                        cursor = pg_hook.cursor()
                        for new_column in new_columns:                            
                            data_type = data[new_column].dtype
                            # Converting pandas data type to equivalent PostgreSQL data type
                            destination_data_type = 'VARCHAR' if data_type == 'object' else 'INTEGER' if data_type in ['int64', 'int32', 'int16', 'int8'] else 'TEXT'
                            alter_query = f'ALTER TABLE "salesforce_airflow"."{self.salesforce_object}" ADD COLUMN "{new_column}" {destination_data_type};'
                            cursor.execute(alter_query)
                            pg_hook.commit()    

                data.drop_duplicates(subset='Id', keep='last', inplace=True)
                data.reset_index(drop=True, inplace=True)
                # fetch columns for data DF
                columns=data.columns.tolist()
                chunksize = 1000
                num_chunks = len(data) // chunksize + (len(data) % chunksize > 0)
                for i in range(num_chunks):
                    start_idx = i * chunksize
                    end_idx = (i + 1) * chunksize
                    chunk = data.iloc[start_idx:end_idx]
                    try:
                        #Handle Null literals
                        for col in columns:
                            chunk.loc[:,col]=chunk[col].replace('\x00','',regex=True )

                        chunk.to_sql(self.salesforce_object, con=engine, if_exists='append', index=False, schema='salesforce_airflow', method='multi')

                        #Add Primary Key Constraint:
                        cursor = pg_hook.cursor()
                        pk_check_query = f'''
                            SELECT COUNT(*)
                            FROM information_schema.table_constraints
                            WHERE constraint_type = 'PRIMARY KEY'
                            AND table_schema = 'salesforce_airflow'
                            AND table_name = '{self.salesforce_object}';
                        '''
                        cursor.execute(pk_check_query)                    
                        primary_key_count = cursor.fetchone()[0]
                        if primary_key_count == 0:
                            pk_query=f'ALTER TABLE "salesforce_airflow"."{self.salesforce_object}" ADD PRIMARY KEY ("Id");'
                            cursor.execute(pk_query)
                            pg_hook.commit()
                    except Exception as e:
                        print(f"An error occurred for chunk {i + 1}:{e}")
                        
                        cursor = pg_hook.cursor()
                        batch_size = 100
                        rows = []
                        for _, row in chunk.iterrows():
                            rows.append(tuple(row))
                            if len(rows) == batch_size:
                                upsert_query = f'''
                                    INSERT INTO "salesforce_airflow"."{self.salesforce_object}"({", ".join(['"{}"'.format(col) for col in chunk.columns])})
                                    VALUES ({", ".join(["%s" for _ in chunk.columns])})
                                    ON CONFLICT ("Id") DO UPDATE
                                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in chunk.columns])};
                                '''
                                encoded_rows = [
                                    tuple(value.encode('utf-8', errors='replace').decode('utf-8', errors='replace') if isinstance(value, str) else value for value in row)
                                    for row in rows
                                ]
                        # Bind parameters and execute the upsert query for each row in the DataFrame
                                cursor.executemany(upsert_query, encoded_rows)
                                rows = []
                            if rows:
                                upsert_query = f'''
                                    INSERT INTO "salesforce_airflow"."{self.salesforce_object}"({", ".join(['"{}"'.format(col) for col in chunk.columns])})
                                    VALUES ({", ".join(['%s' for _ in chunk.columns])})
                                    ON CONFLICT ("Id") DO UPDATE
                                    SET {", ".join(['"{}" = EXCLUDED."{}"'.format(col, col) for col in chunk.columns])}
                                '''
                                encoded_rows = [
                                    tuple(value.encode('utf-8', errors='replace').decode('utf-8', errors='replace') if isinstance(value, str) else value for value in row)
                                    for row in rows
                                ]

                                cursor.executemany(upsert_query, encoded_rows)
                        self.log.info(f"Data transfer complete for chunk:{i+1}")
                        pg_hook.commit()
                    
                    finally:
                        # Close the cursor and connection
                        cursor.close()
                        self.update_last_processed_value(pg_hook, chunk["LastModifiedDate"].iloc[-1])
                
                self.log.info("Data transfer complete.")

            else:
                self.log.info("No new records to transfer.")
            
        except Exception as e:
            self.log.error(f"Error during data transfer: {str(e)}")
            raise

        finally:
            # Close connections
            pg_hook.close()

    def get_last_processed_value(self, pg_hook):
        cursor = pg_hook.cursor()

        try:
            cursor.execute(f"SELECT value FROM salesforce_airflow.sf_metadata WHERE object = '{self.salesforce_object}'")
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()
    
    def update_last_processed_value(self, pg_hook, value):
        cursor = pg_hook.cursor()

        try:
            cursor.execute(f"INSERT INTO salesforce_airflow.sf_metadata (object, value) VALUES ('{self.salesforce_object}', %s) ON CONFLICT (object) DO UPDATE SET value = %s", (value, value))
            pg_hook.commit()
        finally:
            cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 2),
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
    'email': ['mayank.bhardwaj@infinite.com','ranjith.garikapati@infinite.com','kishore.mudragada@infinite.com','arunkumar.kesa@infinite.com','laxmikanth.mudavath@infinite.com'],
    'email_on_failure': True,
}


dag = DAG(
    'sf_to_pg_SurveyGizmo_Question_Response__c_production',
    default_args=default_args,
    description='DAG to incrementally load Salesforce SurveyGizmo_Question_Response__c data to PostgreSQL',
    tags = ['production','salesforce','15','SurveyGizmo_Question_Response__c'],
    schedule_interval='0 15 * * *',
    catchup=False, 
)

# Define the sf_etl_SurveyGizmo_Question_Response__c task
salesforce_to_postgres_task = sf_etl_SurveyGizmo_Question_Response__c(
    task_id='sf_to_pg_SurveyGizmo_Question_Response__c_prod',
    salesforce_conn_id='salesforce-prod',
    postgres_conn_id='warehouse-prod-mb',
    salesforce_object='SurveyGizmo_Question_Response__c',
    execution_timeout=timedelta(hours=4),    
    dag=dag,
)

# Set up task dependencies
salesforce_to_postgres_task