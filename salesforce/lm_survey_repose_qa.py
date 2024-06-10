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

class sf_etl_SurveyGizmo_Survey_Response__c(BaseOperator):
    @apply_defaults
    def __init__(self, salesforce_conn_id, postgres_conn_id, salesforce_object, *args, **kwargs):
        super(sf_etl_SurveyGizmo_Survey_Response__c, self).__init__(*args, **kwargs)
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