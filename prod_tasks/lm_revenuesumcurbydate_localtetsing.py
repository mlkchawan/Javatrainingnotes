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
    # start_date = datetime.date(2023, 1, 1)
    # end_date = datetime.date(2023, 1, 3)
   
    # # # Generate the first day of each month
    # # first_days = pd.date_range(start=start_date, end=end_date, freq='MS')
   
    # # # Generate the last day of each month
    # # last_days = pd.date_range(start=start_date, end=end_date, freq='M') + pd.offsets.MonthEnd(0)
   
    # # Combine the first and last days of each month
    # report_dates = pd.date_range(start=start_date, end=end_date,freq='D')
   
    # final_df=pd.DataFrame()
    # # count_df = pd.DataFrame(columns=['date','ProductName','RatePlanName','cancelled','cancelledchanged','mrr_cancelled ','tcv_cancelled', 'totalactive', 'mrr_active', 'tcv_active','new','changed','mrr_new','tcv_new'])

    with conn.cursor() as cursor:
        start_date = datetime.date(2024, 5, 17)
        end_date = datetime.date(2024, 5, 18)
        date_range = pd.date_range(start=start_date, end=end_date)
        
        all_data = []
 
        # connection = pg_hook.get_conn()
        # connection.autocommit = True
        # cursor = connection.cursor()
 
        for date_str in date_range:
            # date_str = date.strftime('%Y-%m-%d')
            sql_query = f'''
            SELECT
                '{date_str}' AS "RevenueDate",
                "ProductName",
                "Region",
                "Currency",
                SUM("RevenueAmount") AS "RevenueAmount"
            FROM reportsdb."RevenueSchedule"
            WHERE "AccountingPeriod" = (
                SELECT "Name"
                FROM reportsdb."AccountingPeriod"
                WHERE '{date_str}' >= "StartDate"
                AND '{date_str}' <= "EndDate"
            )
            AND "RevenueDate" <= '{date_str}'
            GROUP BY "ProductName", "Region", "Currency";
            '''
            cursor.execute(sql_query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            if result:
                df = pd.DataFrame(result, columns=column_names)
                df.fillna(0, inplace=True)
                all_data.append(df)
 
        # Concatenate all data into a single DataFrame
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv('dags/prod_tasks/RevenuesCurbyDate.csv', index=False)
   
    # Close cursor before closing connection
        cursor.close()
    conn.close()