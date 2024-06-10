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
        results = []
        cancelled_acc_ids=[]
        current_timestamp = datetime.now()
        # start = current_timestamp - timedelta(days=2)
        # end = current_timestamp - timedelta(days=1)
        # start_date = start.strftime('%Y-%m-%d')
        # end_date = end.strftime('%Y-%m-%d')
        # cursor = pg_hook.cursor()
        count_df = pd.DataFrame(columns=['Date','Account_Count','Subscription_Count','Product_Count'])
        report_dates = pd.date_range(start='2024-02-01', end='2024-02-29', freq='MS')
        # month_dates = [(month, month + pd.offsets.MonthEnd()) for month in report_dates]
        # for start_date, end_date in month_dates:
        for report_date in report_dates:                        
            sql_query = f'''
                    select a."AccountId",a."SubscriptionName",a."ProductFamily",a."ProductName", a."RatePlanName" from reportsdb.zuoradata a
                    inner join (select "SubscriptionName", max("SubscriptionVersion") as "SubscriptionVersion" from reportsdb.zuoradata
                        where "SubscriptionTermStartDate" <= DATE('{report_date}') and DATE("SubscriptionCreatedDate") <= DATE('{report_date}')
                            group by "SubscriptionName") b
                    on a."SubscriptionName" = b."SubscriptionName" and a."SubscriptionVersion" = b."SubscriptionVersion"
                    where a."SubscriptionTermStartDate" <= DATE('{report_date}')  and (a."SubscriptionCancelledDate" is null or a."SubscriptionCancelledDate" > DATE('{report_date}'))
                    and a."ProductName" != 'Business Maker' and a."ProductName" NOT LIKE '%India%'
                    and (a."AmendmentType" is null or (a."AmendmentType" = 'UpdateProduct' and a."AmendmentEffectiveDate" is not null)
                    or (a."AmendmentType" = 'NewProduct' and a."AmendmentEffectiveDate" <= DATE('{report_date}'))
                    or (a."AmendmentType" = 'RemoveProduct' and a."AmendmentEffectiveDate" > DATE('{report_date}')))
                    '''
       
            cursor.execute(sql_query)                    
            result = cursor.fetchall()            
            results.extend(result)
            result_df = pd.DataFrame(results, columns=['AccountId','SubscriptionName','ProductFamily', 'ProductName','RatePlanName'])
            # count_unique = result_df.groupby('Date').agg(
            #     {        
            #     'AccountId': 'nunique',
            #     'SubscriptionName': 'nunique',
            #     'ProductName': 'size'
            #     }).reset_index()
            # count_unique.columns=['Date','Account_Count','Subscription_Count','Product_Count']
            # count_df=pd.concat([count_df,count_unique],ignore_index=True)
            # print(count_df)
            cancels = []
            end_date=report_date+pd.offsets.MonthEnd(0)
            print(report_date,end_date)
            # can_dates = pd.date_range(start=report_date, end=end_date)
            for report_date in report_dates:
                end_date = report_date + pd.offsets.MonthEnd(0)
                can_dates = pd.date_range(start=report_date, end=end_date, freq='D')
                for can_date in can_dates:                  
                    sql_can_query = f'''
                            select DATE('{can_date}') as Date,"AccountId","SubscriptionName","ProductFamily","ProductName","RatePlanName" from reportsdb.zuoradata where DATE("RatePlanCreatedDate") = DATE('{can_date}') and "SubscriptionCancelledDate" <= DATE('{can_date}') and "ProductName" != 'Business Maker' and "ProductName" NOT LIKE '%India%' and  ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and "AmendmentEffectiveDate" <= DATE('{can_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{can_date}')) or ("SubscriptionTermStartDate" > DATE('{can_date}')))))
                            union all select DATE('{can_date}') as Date,"AccountId","SubscriptionName","ProductFamily","ProductName","RatePlanName" from reportsdb.zuoradata where DATE("RatePlanCreatedDate") < DATE('{can_date}') and "SubscriptionCancelledDate" = DATE('{can_date}') and "ProductName" != 'Business Maker' and "ProductName" NOT LIKE '%India%' and ("AmendmentType" is null or (not (("AmendmentType" = 'RemoveProduct' and "AmendmentEffectiveDate" <= DATE('{can_date}')) or ("AmendmentType" = 'NewProduct' and "AmendmentEffectiveDate" > DATE('{can_date}')) or ("SubscriptionTermStartDate" > DATE('{can_date}')))))                    
                            '''          
                    cursor.execute(sql_can_query)                    
                    cancel = cursor.fetchall()            
                    cancels.extend(cancel)  
                cancel_df= pd.DataFrame(cancels, columns=['Date','AccountId','SubscriptionName','ProductFamily1', 'ProductName1','RatePlanName1'])
                # cancel_df['Date']= can_date
                merged_df = pd.merge(result_df, cancel_df, on=['AccountId', 'SubscriptionName'], how='left', indicator=True)
                cancelled_accounts = merged_df.groupby(['AccountId']).filter(lambda x: (x['_merge'] == 'both').all())
                cancelled_acc_ids.extend(cancelled_accounts[['AccountId', 'SubscriptionName','ProductFamily','ProductName','RatePlanName','Date']].values.tolist())
        
                cancels = []
                results = []
            final_cancel_df = pd.DataFrame(cancelled_acc_ids, columns=['AccountId', 'SubscriptionName','ProductFamily','ProductName','RatePlanName','Date'])
            count_unique = final_cancel_df.groupby('Date').agg(
                {        
                    'AccountId': 'nunique',
                }).reset_index()
            count_unique.columns=['Date','cancelled_acc_ids_count']
            count_unique.to_csv('dags/cancelled_acc_ids_count01.csv', index=False)
            # count_df=pd.concat([count_df,count_unique],ignore_index=True)
                            # count_df[['Account_Count','Subscription_Count','Product_Count','Date']]=count_unique[['Account_Count','Subscription_Count','Product_Count','Date']]
            # print(count_unique)
 
    # Close the connection when done
    conn.close()