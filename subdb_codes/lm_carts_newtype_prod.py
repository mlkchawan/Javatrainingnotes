#Python code for Extracting data from mysql and loading into Postgres "carts table" for type="newtype"
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
    mysql_query = '''SELECT id, cart, created_at, modified_at FROM carts WHERE date(created_at) > '2022-01-01';'''
    result = mysql_hook.get_pandas_df(mysql_query)
    data = []
    for _, row in result.iterrows():
        json_data = json.loads(row['cart'])
        if 'type' in json_data and json_data['type'] in ['change', 'reactivate', 'update']:
            continue  # Skip records with unwanted cart type
 
        record = {
            'id': row['id'],
            'created_at': row['created_at'],
            'modified_at': row['modified_at'],
        }
 
        # Check if cart type exists and is not one of the unwanted types
        if 'type' in json_data and json_data['type'] not in ['change', 'reactivate', 'update']:
            record['cart_type'] = json_data.get('type', '')

        if 'meta' in json_data and isinstance(json_data['meta'], dict):
                meta_flow =   json_data['meta'].get('flow', '')
        else:
            meta_flow = ''    

        if 'user' in json_data and isinstance(json_data['user'], dict):
                        user_id = json_data['user'].get('userId', '')
        else:
            user_id = ''

        if 'subscriptions' in json_data and isinstance(json_data['subscriptions'], list):
            for subscription in json_data['subscriptions']:
                subscriptionNumber = subscription.get('subscriptionNumber', '')
                previousSubscriptionNumber = subscription.get('previousSubscriptionNumber', '')
                subscription_id = subscription.get('id', '')
                baseProductId = subscription.get('baseProductId', '')
                if 'meta' in subscription and isinstance(subscription['meta'], dict):
                    domain = subscription['meta'].get('domain', '')
                    meta_paymentToken = subscription['meta'].get('paymentToken', '')
                    meta_oacs = subscription['meta'].get('oacs', '')
                    meta_domain_extension = '.' + domain.split('.')[-1] if domain else ''
                    meta_meta_pvtreg = subscription['meta'].get('pvtreg', '')
                    meta_businessId = subscription['meta'].get('businessId', '')
                    meta_action = subscription['meta'].get('action', '')
                else:
                    domain = ''
                    meta_paymentToken = ''
                    meta_oacs = ''
                    meta_domain_extension = ''
                    meta_meta_pvtreg = ''
                    meta_businessId = ''
                    meta_action = ''
                
                add_rate_plans = subscription.get('addRatePlans', []) if isinstance(subscription.get('addRatePlans', []), list) else []
                remove_rate_plans = subscription.get('removeRatePlans', []) if isinstance(subscription.get('removeRatePlans', []), list) else []
                existing_rate_plans = subscription.get('existingRatePlans', []) if isinstance(subscription.get('existingRatePlans', []), list) else []
                # add_rate_plans = subscription.get('addRatePlans', [])
                if add_rate_plans :
                    for rate_plan in add_rate_plans:
                        rate_plan_id = rate_plan.get('ratePlanId', '')
                        product_id = rate_plan.get('productId', '')
                        rateplan_type = rate_plan.get('type', '')
                        family = rate_plan.get('family', '')
                        charges = rate_plan.get('charges', {})
                        if charges:
                            onetime_charges = charges.get('onetime', [])
                            recurring_charges = charges.get('recurring', [])
                    
                            if onetime_charges:
                                for charge in onetime_charges:
                                    onetime_id = charge.get('id', '')
                                    charge_name = charge.get('name', '')
                                    charge_Id = charge.get('chargeId', '')
                                    billingId = charge.get('billingId', '')
                                    chargeModel = charge.get('chargeModel', '')
                                    charge_type = charge.get('type', '')
                                    pricedetails = charge.get('priceDetails', {})
                                    currency = pricedetails.get('currency', '') if isinstance(pricedetails, dict) else ''
                                    actual_price = pricedetails.get('actualPrice', '') if isinstance(pricedetails, dict) else ''
                                    final_price = pricedetails.get('price', '') if isinstance(pricedetails, dict) else ''
                                    discountAmount = pricedetails.get('discountAmount', '') if isinstance(pricedetails, dict) else ''
                                    discountPercentage = pricedetails.get('discountPercentage', '') if isinstance(pricedetails, dict) else ''
                                    discountPeriod = pricedetails.get('discountPeriod', '') if isinstance(pricedetails, dict) else ''
                                    model = pricedetails.get('model', '') if isinstance(pricedetails, dict) else ''
                                    billingperiod = charge.get('billingPeriod', '')
                                    data.append({
                                        'id': row['id'],
                                        'created_at': row['created_at'],
                                        'modified_at': row['modified_at'],
                                        'cart_type': json_data.get('type', ''),
                                        'cart_id': json_data.get('id', ''),
                                        'meta_flow': meta_flow,
                                        'user_id': user_id,
                                        'subscription_id': subscription_id,
                                        'baseProductId': baseProductId,
                                        'subscriptionNumber': subscriptionNumber,
                                        'previousSubscriptionNumber': previousSubscriptionNumber,
                                        'paymentToken': meta_paymentToken,
                                        'oacs': meta_oacs,
                                        'domain': domain,
                                        'domain_extension': meta_domain_extension,
                                        'pvtreg': meta_meta_pvtreg,
                                        'businessId': meta_businessId,
                                        'action': meta_action,
                                        'addRatePlans_rate_plan_id': rate_plan_id,
                                        'addRatePlans_product_id': product_id,
                                        'addRatePlans_rateplan_type': rateplan_type,
                                        'addRatePlans_family': family,
                                        'onetime_id': onetime_id,
                                        'onetime_charge_name': charge_name,
                                        'onetime_chargeId': charge_Id,
                                        'onetime_billingId': billingId,
                                        'onetime_chargeModel': chargeModel,
                                        'onetime_charge_type': charge_type,
                                        'onetime_currency': currency,
                                        'onetime_actual_price': actual_price,
                                        'onetime_price': final_price,
                                        'onetime_price_model': model,
                                        'onetime_discountAmount': discountAmount,
                                        'onetime_discountPercentage': discountPercentage,
                                        'onetime_discountPeriod': discountPeriod,
                                        'onetime_billingperiod': billingperiod
                                    })
                        
                            if recurring_charges:
                                for charge in recurring_charges:
                                    recurring_id = charge.get('id', '')
                                    recurring_name = charge.get('name', '')
                                    billingId = charge.get('billingId', '')
                                    chargeModel = charge.get('chargeModel', '')
                                    charge_Id = charge.get('chargeId', '')
                                    charge_type = charge.get('type', '')
                                    pricedetails = charge.get('priceDetails', {})
                                    currency = pricedetails.get('currency', '') if isinstance(pricedetails, dict) else ''
                                    actual_price = pricedetails.get('actualPrice', '') if isinstance(pricedetails, dict) else ''
                                    final_price = pricedetails.get('price', '') if isinstance(pricedetails, dict) else ''
                                    discountAmount = pricedetails.get('discountAmount', '') if isinstance(pricedetails, dict) else ''
                                    discountPercentage = pricedetails.get('discountPercentage', '') if isinstance(pricedetails, dict) else ''
                                    discountPeriod = pricedetails.get('discountPeriod', '') if isinstance(pricedetails, dict) else ''
                                    model = pricedetails.get('model', '') if isinstance(pricedetails, dict) else ''
                                    billingperiod = charge.get('billingPeriod', '')
                                    data.append({
                                        'id': row['id'],
                                        'created_at': row['created_at'],
                                        'modified_at': row['modified_at'],
                                        'cart_type': json_data.get('type', ''),
                                        'cart_id': json_data.get('id', ''),
                                        'meta_flow': meta_flow,
                                        'user_id': user_id,
                                        'subscription_id': subscription_id,
                                        'baseProductId': baseProductId,
                                        'subscriptionNumber': subscriptionNumber,
                                        'previousSubscriptionNumber': previousSubscriptionNumber,
                                        'paymentToken': meta_paymentToken,
                                        'oacs': meta_oacs,
                                        'domain': domain,
                                        'domain_extension': meta_domain_extension,
                                        'pvtreg': meta_meta_pvtreg,
                                        'businessId': meta_businessId,
                                        'action': meta_action,
                                        'addRatePlans_rate_plan_id': rate_plan_id,
                                        'addRatePlans_product_id': product_id,
                                        'addRatePlans_rateplan_type': rateplan_type,
                                        'addRatePlans_family': family,
                                        'recurring_id': recurring_id,
                                        'recurring_name': recurring_name,
                                        'recurring_billingId': billingId,
                                        'recurring_chargeModel': chargeModel,
                                        'recurring_charge_id': charge_Id,
                                        'recurring_charge_type': charge_type,
                                        'recurring_currency': currency,
                                        'recurring_actual_price': actual_price,
                                        'recurring_price': final_price,
                                        'recurring_price_model': model,
                                        'recurring_discountAmount': discountAmount,
                                        'recurring_discountPercentage': discountPercentage,
                                        'recurring_discountPeriod': discountPeriod,
                                        'recurring_billingperiod': billingperiod
                
                                    })
                        else:
                             data.append({
                            'id': row['id'],
                            'created_at': row['created_at'],
                            'modified_at': row['modified_at'],
                            'cart_type': json_data.get('type', ''),
                            'cart_id': json_data.get('id', ''),
                            'meta_flow': meta_flow,
                            'user_id': user_id,
                            'subscription_id': subscription_id,
                            'baseProductId': baseProductId,
                            'subscriptionNumber': subscriptionNumber,
                            'previousSubscriptionNumber': previousSubscriptionNumber,
                            'paymentToken': meta_paymentToken,
                            'oacs': meta_oacs,
                            'domain': domain,
                            'domain_extension': meta_domain_extension,
                            'pvtreg': meta_meta_pvtreg,
                            'businessId': meta_businessId,
                            'action': meta_action,
                            'addRatePlans_rate_plan_id': rate_plan_id,
                            'addRatePlans_product_id': product_id,
                            'addRatePlans_rateplan_type': rateplan_type,
                            'addRatePlans_family': family})
                    
                if remove_rate_plans :
                    for rate_plan in remove_rate_plans:
                        removeRatePlans_rate_plan_id = rate_plan.get('ratePlanId', '')
                        removeRatePlans_product_id = rate_plan.get('productId', '')
                        removeRatePlans_rateplan_type = rate_plan.get('type', '')
                        removeRatePlans_family = rate_plan.get('family', '')
                        data.append({
                        'id': row['id'],
                        'created_at': row['created_at'],
                        'modified_at': row['modified_at'],
                        'cart_type': json_data.get('type', ''),
                        'cart_id': json_data.get('id', ''),
                        'meta_flow': meta_flow,
                        'user_id': user_id,
                        'subscription_id': subscription_id,
                        'baseProductId': baseProductId,
                        'subscriptionNumber': subscriptionNumber,
                        'previousSubscriptionNumber': previousSubscriptionNumber,
                        'paymentToken': meta_paymentToken,
                        'oacs': meta_oacs,
                        'domain': domain,
                        'domain_extension': meta_domain_extension,
                        'pvtreg': meta_meta_pvtreg,
                        'businessId': meta_businessId,
                        'action': meta_action,                    
                        'removeRatePlans_rate_plan_id': removeRatePlans_rate_plan_id,
                        'removeRatePlans_product_id': removeRatePlans_product_id,
                        'removeRatePlans_rateplan_type': removeRatePlans_rateplan_type,
                        'removeRatePlans_family': removeRatePlans_family})
                    
                
                if existing_rate_plans :
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
                        'meta_flow': meta_flow,
                        'user_id': user_id,
                        'subscription_id': subscription_id,
                        'baseProductId': baseProductId,
                        'subscriptionNumber': subscriptionNumber,
                        'previousSubscriptionNumber': previousSubscriptionNumber,
                        'paymentToken': meta_paymentToken,
                        'oacs': meta_oacs,
                        'domain': domain,
                        'domain_extension': meta_domain_extension,
                        'pvtreg': meta_meta_pvtreg,
                        'businessId': meta_businessId,
                        'action': meta_action,
            
                        'existingRatePlans_rate_plan_id': existingRatePlans_rate_plan_id,
                        'existingRatePlans_product_id': existingRatePlans_product_id,
                        'existingRatePlans_rateplan_type': existingRatePlans_rateplan_type,
                        'existingRatePlans_family': existingRatePlans_family,
                        'existingRatePlans_ratePlanStatus': existingRatePlans_ratePlanStatus
                    })
                        
                # else:
                #     data.append({
                #         'id': row['id'],
                #         'created_at': row['created_at'],
                #         'modified_at': row['modified_at'],
                #         'cart_type': json_data.get('type', ''),
                #         'cart_id': json_data.get('id', ''),
                #         'subscription_id': subscription_id,
                #         'baseProductId': baseProductId,
                #         'subscriptionNumber': subscriptionNumber,
                #         'previousSubscriptionNumber': previousSubscriptionNumber,
                #         'paymentToken': meta_paymentToken,
                #         'oacs': meta_oacs,
                #         'domain': domain,
                #         'domain_extension': meta_domain_extension,
                #         'pvtreg': meta_meta_pvtreg,
                #         'businessId': meta_businessId,
                #         'action': meta_action
                #     })
                                 
    data.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    numeric_cols = ['onetime_actual_price', 'onetime_price', 'onetime_discountAmount', 'onetime_discountPercentage',
                    'onetime_discountPeriod', 'onetime_billingperiod', 'recurring_actual_price', 'recurring_price',
                    'recurring_discountAmount', 'recurring_discountPercentage', 'recurring_discountPeriod',
                    'recurring_billingperiod']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

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
        df.to_sql(name='sub_carts_new',con=engine, if_exists='replace', index=False, schema='reportsdb',chunksize=1000, method='multi')
        #Add Primary Key Constraint:
        cursor = pg_hook.cursor()
        grant_query = ''' GRANT SELECT ON reportsdb."sub_carts_new" TO sailakshmir,laxmikanthm,arunkumark,rgarikapati,areddy; '''
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
    'start_date': datetime(2024, 3, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
 
#Create a DAG
dag = DAG(
    dag_id='subdb_to_pg_carts_newtype_prod',
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
	
	