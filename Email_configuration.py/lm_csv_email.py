from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import io
import pandas as pd
import requests
import logging
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
 
# Function to extract data and save to CSV
def extract_and_save_to_csv(**kwargs):
    base_url = 'https://rest.zuora.com'
    client_id ='f520c62a-0a73-4b31-be91-d9f460c280c7'
    client_secret ='zceUZlriX4NT1UCscwbiJXkHeHegVqmwr0ogWF6'
    
    #Generate OAuth token
    auth_url = f'{base_url}/oauth/token'
    auth_payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret 
    }

    auth_response = requests.post(auth_url, data=auth_payload)
    auth_data = auth_response.json()
    access_token = auth_data['access_token']
    logging.info(access_token)
    try:
        last_processed_value = kwargs['ti'].xcom_pull(task_ids="get_last_processed_value").strftime('%Y-%m-%dT%H:%M:%SZ')   
        print(last_processed_value)
        query_url = f'{base_url}/v1/batch-query/'
        query_payload = {
            "queries": [
                        {
                        "name": "Rateplancharge",
                        "query": f'''select Payment.Id as PaymentId,Payment.Amount as PaymentAmount,
                                    Payment.Appliedamount as Appliedamount,
                                    Payment.Authtransactionid as Authtransactionid,
                                    Payment.Bankidentificationnumber as Bankidentificationnumber,
                                    Payment.Cancelledon as Cancelledon,
                                    Payment.Comment as Comment,
                                    Payment.Status as PaymentStatus,
                                    Payment.Settledon as Settledon,
                                    Payment.Submittedon as Submittedon
                                    from Payment where Payment.CreatedDate >= '2024-04-01T00:00:00Z' order by Payment.CreatedDate ASC ;
                                    ''',
                        "type": "zoqlexport"
                        }
                        ]
            }        
        logging.info(query_payload)

        headers = {
            'Authorization': f'Bearer {access_token}',  
            'Content-Type': 'application/json'         
        }
        
        api_response = requests.post(query_url, headers=headers, json=query_payload) 
        query_job_id = api_response.json().get('id')
        print(api_response.json())       
        if api_response.status_code == 200 :
            logging.info('Success')        
            query_job_id = api_response.json().get('id') 
            print()           
            status_url = base_url + f'/v1/batch-query/jobs/{query_job_id}'
            status_response = requests.get(status_url, headers=headers)
            status=status_response.json().get('status')
            logging.info(f'Status before while:{status}')

            while status!='completed':
                time.sleep(120)
                status_response = requests.get(status_url, headers=headers)
                status=status_response.json().get('status')
                logging.info(f'Status:{status}')

            if status == 'completed':
                batches=status_response.json().get('batches')
                fileId=batches[0]['fileId']
                recordCount=batches[0]['recordCount']
                logging.info(f'File Id:{fileId}')
                logging.info(f'record count is:{recordCount}')
            
                if recordCount != 0:
                    result_url = base_url + f'/v1/files/{fileId}'
                    res_headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Content-Type': 'text/csv'
                    }
                    result_response = requests.get(result_url, headers=res_headers)
                    if result_response.status_code == 200:
                        csv_data = result_response.text
                        # Convert CSV data to pandas DataFrame
                        df = pd.read_csv(io.StringIO(csv_data))
                        df.to_csv('/tmp/payment_data.csv', index=False)
                        
                        #Load it to xcom
                        kwargs['ti'].xcom_push(key='zuora_payment', value=df)

                    else:
                        logging.info(f"POST request failed. Status code: {result_response.status_code}")
                        logging.info(result_response.text)
                else:
                    logging.info('Zero records')        
        else: 
            logging.info(f"API request failed. Status code: {api_response.status_code}")
            logging.info(api_response.text)

    except Exception as e:
        logging.error(f"Error during data transfer: {str(e)}")
        raise

 
# Function to send email with CSV attachment
def send_email_with_csv(**kwargs):
    # Email settings
    smtp_server = 'smtp.your-email-provider.com'
    smtp_port = 587
    smtp_username = 'your-email@example.com'
    smtp_password = 'your-email-password'
    from_address = 'your-email@example.com'
    to_address = 'recipient@example.com'
    subject = 'Daily Revenue Report'
    body = 'Please find the attached daily revenue report.'
 
    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
 
    # Attach the CSV file
    filename = '/tmp/revenue_data.csv'
    with open(filename, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={filename}')
        msg.attach(part)
 
    # Send the email
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(from_address, to_address, msg.as_string())
    server.quit()
 
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Define the DAG
dag = DAG('send_csv_email', 
        default_args=default_args, 
        schedule_interval='@daily'
        )
 
# Define the tasks
t1 = PythonOperator(
    task_id='extract_and_save_to_csv',
    python_callable=extract_and_save_to_csv,
    provide_context=True,
    dag=dag
)
 
t2 = PythonOperator(
    task_id='send_email_with_csv',
    python_callable=send_email_with_csv,
    provide_context=True,
    dag=dag
)
 
# Set task dependencies
t1 >> t2