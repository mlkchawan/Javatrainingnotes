import pandas  as pd

df=pd.read_csv('C:/TestCsvs/zuora_daily_adds_2024.csv')
# # df=pd.read_csv('C:/TestCsvs/zuora_daily_disconnects_2024.csv')
pivot=pd.DataFrame()

fill_value=1

# pivot= df.pivot_table(index=['ReportDate', 'ProductName'], aggfunc={'SubscriptionId': 'count'})
# pivot_= pivot_product.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()
 
# # # pivot=df.pivot_table(index=['ReportDate','ProductName','ProductFamily','SubscriptionTermType'],values=['MRR','TotalAddsRevenue'],aggfunc={'count','sum'})

# # ## count product wise _count
# # pivot=df.pivot_table(index=['ReportDate','ProductName','ProductFamily'],aggfunc={'ProductName':"count",'TotalAddsRevenue':'sum'})

# # ## pivot using evergreen Vs TERMED
# # pivot=df.pivot_table(index=['ReportDate'],columns=['SubscriptionTermType'],aggfunc={'SubscriptionTermType':'count'})

# # # ## pivot using SubscriptionRenewalTerm
df['SubscriptionRenewalTerm'] = df['SubscriptionRenewalTerm'].fillna(1)
pivot=df.pivot_table(index=['ReportDate'],columns=['SubscriptionRenewalTerm'],aggfunc={'SubscriptionRenewalTerm':'count'})

# Calculate row-wise totals
pivot['Grand Total'] = pivot.sum(axis=1,numeric_only=True)

# grand_total_row = pivot.sum(axis=0).rename('Grand Total').to_frame().T 
# pivot1 = pd.concat([pivot, grand_total_row], ignore_index=False) 

# grand_total_row = pivot.sum(axis=0).rename(('Grand Total',)) 
# pivot1 = pivot.append(grand_total_row)
# pivot = pivot.sum(numeric_only=True, axis=0)
# pivot = pd.DataFrame({'Column':pivot.index, 'Total':pivot.values})

# Calculate column-wise totals
# column_totals = pivot.sum(numeric_only=True)
# column_totals['ReportDate'] = 'Total'
 
# Convert the totals Series to a DataFrame
# totals_df = pd.DataFrame([column_totals])
 
# # Concatenate the totals row to the pivot table
# pivot = pd.concat([pivot, totals_df])

# Ensure 'ReportDate' is the first column
# pivot = pivot[['ReportDate'] + [col for col in pivot.columns if col != 'ReportDate']]

df['SubscriptionRenewalTerm'] = df['SubscriptionRenewalTerm'].replace(1," ")
# # ## pivot using Subscription status
# # # pivot=df.pivot_table(index=['ReportDate'],columns=['SubscriptionStatus'],aggfunc={'SubscriptionStatus':'count'})

# # ## pivot using subscriptio id count & total adds revenue
# # # pivot=df.pivot_table(index=['ReportDate'],aggfunc={'SubscriptionId':"count",'TotalAddsRevenue':'sum'})

# ## product wise revenue
# pivot=df.pivot_table(index=['ReportDate','ProductName'],aggfunc={'TotalAddsRevenue':'sum'})

df.to_csv('C:/TestCsvs/zuora_daily.csv',index=True)
pivot.to_csv('C:/TestCsvs/zuora_daily_adds_20248.csv',index=True)
################################################################

# #count product wise 
# # pivot=df.pivot_table(index=['ReportDate','ProductName','ProductFamily'],aggfunc={'ProductName':"count"})

# # ## pivot using evergreen Vs TERMED
# pivot2=df.pivot_table(index=['ReportDate'],columns=['SubscriptionTermType'],aggfunc={'SubscriptionTermType':'count'})

# # # ## pivot using SubscriptionRenewalTerm
# pivot3=df.pivot_table(index=['ReportDate'],columns=['SubscriptionRenewalTerm'],aggfunc={'SubscriptionRenewalTerm':'count'})

# pivot.append(pivot2)

# pivot.to_csv('C:/TestCsvs/zuora_daily_disconnects_20244.csv',index=True)

# -----------------------
# import pandas as pd
 
# # Read the CSV files
# df = pd.read_csv('C:/TestCsvs/zuora_daily_adds_2024.csv')
# # df= pd.read_csv('C:/TestCsvs/zuora_daily_disconnects_2024.csv')
 
# # Merge the two dataframes if needed
# # df = pd.concat([df_adds, df_disconnects])
 
# # Count product wise
# pivot_product = df.pivot_table(index=['ReportDate', 'ProductName'], aggfunc={'SubscriptionId': 'count'})
# pivot_product = pivot_product.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()
 
# # Pivot using evergreen vs TERMED
# pivot_term_type = df.pivot_table(index=['ReportDate'], columns=['SubscriptionTermType'], aggfunc='size').reset_index()
# # pivot_term_type.columns.name = None  # Remove the columns' name
 
# # Pivot using SubscriptionRenewalTerm
# pivot_renewal_term = df.pivot_table(index=['ReportDate'], columns=['SubscriptionRenewalTerm'], aggfunc={'SubscriptionRenewalTerm': 'count'})
# pivot_renewal_term.columns.name = None  # Remove the columns' name
# pivot_renewal_term=pivot_renewal_term.rename(columns={'SubscriptionRenewalTerm':' '})

# pivot_product_count=df.pivot_table(index=['ReportDate','ProductName'],aggfunc={'TotalAddsRevenue':'sum'}).reset_index()

# pivot_revenue=df.pivot_table(index=['ReportDate'],aggfunc={'SubscriptionId':"count",'TotalAddsRevenue':'sum'})
# pivot_revenue = pivot_revenue.rename(columns={'SubscriptionId': 'SubscriptionCount'}).reset_index()

# pivot_sub_status=df.pivot_table(index=['ReportDate'], columns=['SubscriptionStatus'],aggfunc='size').reset_index()



# # filename = 'C:/TestCsvs/zuora_daily_combined_2024.xlsx' 
# # Write each pivot table to a different sheet in a single Excel file
# with pd.ExcelWriter('C:/TestCsvs/zuora_daily_combined_2024.xlsx') as writer:
#     pivot_product.to_excel(writer, sheet_name='Product_wise_Count', index=False)
#     pivot_term_type.to_excel(writer, sheet_name='Term_Type_Count', index=False)
#     pivot_renewal_term.to_excel(writer, sheet_name='Renewal_Term_Count')
#     pivot_product_count.to_excel(writer, sheet_name='product_count_revenue',index=False)
#     pivot_revenue.to_excel(writer, sheet_name='Subscription_count_revenue',index=False)
#     pivot_sub_status.to_excel(writer, sheet_name='subscription_status_count',index=False)
