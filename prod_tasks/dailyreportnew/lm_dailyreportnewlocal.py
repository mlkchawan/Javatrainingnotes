import psycopg2
import pandas as pd
import datetime
from sshtunnel import SSHTunnelForwarder
 
# SSH connection settings
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
        host='127.0.0.1', # Use 127.0.0.1 when using SSH tunnel
        port=tunnel.local_bind_port,
        database=pg_database
    )
   
        # Specify the start and end date for the report period
    start_date = datetime.date(2023, 1, 1)
    end_date = datetime.date(2023, 1, 3)
   
    # # Generate the first day of each month
    # first_days = pd.date_range(start=start_date, end=end_date, freq='MS')
   
    # # Generate the last day of each month
    # last_days = pd.date_range(start=start_date, end=end_date, freq='M') + pd.offsets.MonthEnd(0)
   
    # Combine the first and last days of each month
    report_dates = pd.date_range(start=start_date, end=end_date,freq='D')
   
    final_df=pd.DataFrame()
    # count_df = pd.DataFrame(columns=['date','ProductName','RatePlanName','cancelled','cancelledchanged','mrr_cancelled ','tcv_cancelled', 'totalactive', 'mrr_active', 'tcv_active','new','changed','mrr_new','tcv_new'])

    with conn.cursor() as cursor:
        # for report_date in report_dates:
        # current_timestamp = datetime.now()
        # start = current_timestamp - timedelta(days=3)
        # end = current_timestamp - timedelta(days=2)
        # start_date = start.strftime('%Y-%m-%d')
        # end_date = end.strftime('%Y-%m-%d')
        start_date = "2024-05-05"
        end_date = "2024-05-07"
        
        # cursor = pg_hook.cursor()

        report_dates = pd.date_range(start=start_date, end=end_date,freq='D') 
        for report_date in report_dates:                        
            sql_query = f'''select DATE('{report_date}') as date, a."ProductName", a."RatePlanName", count(*) as totalactive, sum("MRR") as mrr_active, sum("TCV") as tcv_active from reportsdb.zuoradata
                a inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
                where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}') 
                group by "SubscriptionName") b on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion" 
                where a."SubscriptionTermStartDate" <= DATE('{report_date}')  and (a."SubscriptionCancelledDate" is null or 
                a."SubscriptionCancelledDate" > DATE('{report_date}')) and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and 
                a."AmendmentEffectiveDate" is not null) or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}')) or 
                (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}'))) group by a."ProductName", a."RatePlanName";
                '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
        
                    # Convert the result to a DataFrame
            column_names = [desc[0] for desc in cursor.description]
            
            df = pd.DataFrame(result, columns=column_names)
            
            final_df=pd.concat([final_df,df],ignore_index=True)
            final_df.fillna(0)
            final_df.to_csv('dags/dailyreportnew_count4.csv', index=False)
   
    # Close cursor before closing connection
        cursor.close()
    conn.close()