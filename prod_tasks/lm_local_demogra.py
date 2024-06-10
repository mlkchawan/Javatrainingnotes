import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from sshtunnel import SSHTunnelForwarder

ssh_host = '52.2.99.154'
ssh_port = 2222
ssh_username = 'laxmikanthm'
ssh_password = 'laxmimanthm620107'
 
# PostgreSQL connection
pg_host = 'warehouse-fivetran-enc-2-flk.cpwffxl53him.us-east-1.rds.amazonaws.com'  # SSH tunnel will forward connection to localhost
pg_port = 5432  
pg_username = 'laxmikanthm'
pg_password = 'laxmimanthm620107'
pg_database = 'datawarehouse'
 
# Create SSH tunnel
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_username,
    ssh_password=ssh_password,
    remote_bind_address=(pg_host, pg_port),
) as tunnel:
    # Connect to PostgreSQL through the SSH tunnel
    conn = psycopg2.connect(
        user=pg_username,
        password=pg_password,
        host='127.0.0.1', 
        port=tunnel.local_bind_port,
        database=pg_database
    )
    with conn.cursor() as cursor:
        current_timestamp = datetime.now()
        start = current_timestamp - timedelta(days=2)
        end = current_timestamp - timedelta(days=1)
        start_date = start.strftime('%Y-%m-%d')
        end_date = end.strftime('%Y-%m-%d')
        count_df = pd.DataFrame(columns=['Date','Account_Count','Subscription_Count','Product_Count'])
        report_dates = pd.date_range(start=start_date, end=end_date) 
        for report_date in report_dates:
            sql_query = f'''select DATE('{report_date}') as Date, a."AccountId",a."SubscriptionName",a."SubscriptionId",
            a."SubscriptionStatus",a."ProductFamily",a."ProductName", a."RatePlanName",a."SubscriptionTermType",a."SubscriptionTermStartDate",
            a."SubscriptionTermEndDate",a."MRR",a."TCV",c."Country",c."State",c."City" from reportsdb.zuoradata a
            inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
            where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}')
            group by "SubscriptionName") b on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion"
            inner join (select "Country","State","City","Accountid" from reportsdb.zuora_contact
            group by "Country","State","City","Accountid") c on a."AccountId"= c."Accountid"
            where a."SubscriptionTermStartDate" <= DATE('{report_date}') and 
            (a."SubscriptionCancelledDate" is null or a."SubscriptionCancelledDate" > DATE('{report_date}') 
            and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and a."AmendmentEffectiveDate" is not null)
            or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}')
            or (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}'))) limit 200000'''
        cursor.execute(sql_query) 
        records = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=column_names)             
        df.to_csv('dags/demographydata77.csv', index=False)           
    conn.close()