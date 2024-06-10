#Python code to extract data from Zuora Objects
#created by : Mayank Bhardwaj
#Attempt : 8
#Date : 10/07/2023
#code is too generic....needs to be automated for objects
#fields have to be explicitly mentioned in queryString.......
 
import requests
import csv
import json
import pandas as pd
import io
import time
 
# Zuora API endpoint and credentials
base_url = 'https://rest.zuora.com'
client_id = 'f520c62a-0a73-4b31-be91-d9f460c280c7'
client_secret = 'zceUZlriX4NT1UCscwbiJXkHeHegVqmwr0ogWF6'
 
#Generate OAuth token
auth_url = f'{base_url}/oauth/token'
auth_payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}
 
auth_response = requests.post(auth_url, data=auth_payload)
auth_data = auth_response.json()
print(auth_data)
 
access_token = auth_data['access_token']
 
#API request to fetch data
query = f'{base_url}/v1/batch-query/'
query_payload = {
            "queries": [
                        {
                        "name": "jan_31_2023",
                        "query": f'''
                            select                             
                            Invoice.Amount as InvoiceAmount,Invoice.DueDate as InvoiceDueDate,Invoice.Id as InvoiceId,Invoice.InvoiceNumber as InvoiceNumber,
                            Invoice.PostedDate as PostedDate,Invoice.RefundAmount as InvoiceRefundAmount,Invoice.Status as InvoiceStatus,Invoice.TargetDate as InvoiceTargetDate,Invoice.TaxAmount as TaxAmount,Invoice.PaymentTerm as PaymentTerm,
                            Invoice.NextActionDate__c as NextActionDate,Invoice.treatmentLevel__c as treatmentLevel,Invoice.InCollection__c as InvoiceInCollection,Invoice.RetryStatus__c as InvoiceRetryStatus,
                                    Payment.Id as PaymentId,Payment.Amount as PaymentAmount,
                                    Payment.Appliedamount as Appliedamount,
                                    Payment.Authtransactionid as Authtransactionid,
                                    Payment.Bankidentificationnumber as Bankidentificationnumber,
                                    Payment.Cancelledon as Cancelledon,
                                    Payment.Comment as Comment,
                                    Payment.Status as PaymentStatus,
                                    Payment.Settledon as Settledon,
                                    Payment.Submittedon as Submittedon,
                                    Payment.Currency as Currency,
                                    Paymentmethod.id as Paymentmethodid,Payment.Isstandalone as Isstandalone,
                                    Payment.Gateway as Gateway,
                                    Payment.Gatewayresponse as Gatewayresponse,
                                    Payment.Gatewayreconciliationstatus as Gatewayreconciliationstatus,
                                    Payment.Gatewayresponsecode as Gatewayresponsecode,
                                    Payment.Gatewayreconciliationreason as Gatewayreconciliationreason,
                                    Payment.Gatewaystate as Gatewaystate,
                                   
                                    Payment.Createddate as PaymentCreateddate,
                                   
                                    Payment.Updateddate as PaymentUpdatedDate,
                                    Payment.Paymentnumber as Paymentnumber,
                                    Payment.Paymentrunid__c as Paymentrunid,
                                    Payment.Source as Source,
                                    Payment.Sourcename as Sourcename,
                                    Payment.Referenceid as Referenceid,
                                    Payment.Refundamount as PaymentRefundamount,Payment.Type as PaymentType,
                                    PaymentMethodSnapshot.Id as Paymentmethodsnapshotid,
                                    Payment.Prepayment as Prepayment,
                                    Account.Id AS AccountId, 
                                    Account.AccountNumber AS AccountNumber,
                                    Account.Name AS AccountName,
                                    Account.APM__c AS APM,
                                    Account.Batch AS Batch,
                                    Account.LastInvoiceDate AS LastInvoiceDate,
                                    Account.RetryStatus__c AS Account_RetryStatus,
                                    Account.TotalInvoiceBalance AS TotalInvoiceBalance,
                                    Account.CreatedDate AS Account_CreatedDate,
                                    Account.UpdatedDate AS Account_UpdatedDate,
                                    SoldToContact.Id AS SoldToId,
                                    SoldToContact.AccountId AS SoldTo_AccountId,
                                    SoldToContact.Address1 AS Address,
                                    SoldToContact.City AS City,
                                    SoldToContact.State AS State,
                                    SoldToContact.Country AS Country,
                                    SoldToContact.TaxRegion AS TaxRegion,
                                    SoldToContact.PostalCode AS PostalCode,
                                    SoldToContact.MobilePhone AS MobilePhone,
                                    SoldToContact.WorkPhone AS WorkPhone,
                                    SoldToContact.PersonalEmail AS PersonalEmail,
                                    SoldToContact.WorkEmail AS WorkEmail,
                                    DefaultPaymentMethod.Id AS DefaultPaymentMethodId,
                                    DefaultPaymentMethod.LastTransactionStatus AS LastTransactionStatus,
                                    DefaultPaymentMethod.LastFailedSaleTransactionDate AS LastFailedSaleTransactionDate,      
                            PaymentMethod.AchBankName as AchBankName,PaymentMethod.BankCode as BankCode,
                            PaymentMethod.BankName as BankName,
                            PaymentMethod.TotalNumberOfErrorPayments as TotalNumberOfErrorPayments,
                            PaymentMethod.TotalNumberOfProcessedPayments as TotalNumberOfProcessedPayments,
                            PaymentMethod.Type as PaymentMethodType,PaymentMethod.CreditCardType as PaymentMethodCreditCardType,
                            PaymentMethod.NumConsecutiveFailures as NumConsecutiveFailures
                            from InvoicePayment
                            
                            where Invoice.PostedDate>='2024-01-01T00:00:00Z' and Invoice.Currency='USD' limit 100
                            ''',
                        "type": "zoqlexport"
                        }
                        ]
            }
 
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}
 
response = requests.post(query, json=query_payload, headers=headers)
query_job_id = response.json().get('id')
print(response.json())
time.sleep(6)
status_url = base_url + f'/v1/batch-query/jobs/{query_job_id}'
status_response = requests.get(status_url, headers=headers)
print(status_response.json())
# time.sleep(60)
status=status_response.json().get('status')
# Check for HTTP status code 200 (OK)
while status != 'completed':
    time.sleep(60)
    status_response = requests.get(status_url, headers=headers)
    print(status_response.json().get('status'))
    status=status_response.json().get('status')
if status_response.json().get('status') == 'completed':
    batches=status_response.json().get('batches')
    fileId=batches[0]['fileId']
    # Download the result
    result_url = base_url + f'/v1/files/{fileId}'
    headers1 = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'text/csv'
    }
    result_response = requests.get(result_url, headers=headers1)
if result_response.status_code == 200:
    csv_data = result_response.text
    df = pd.read_csv(io.StringIO(csv_data))
    df.fillna(0, inplace=True)
    if 'Payment_UpdatedDate' in df:
        df['Payment_UpdatedDate'] = pd.to_datetime(df['Payment_UpdatedDate'])
    if 'Payment_Createddate' in df:
        df['Payment_Createddate'] = pd.to_datetime(df['Payment_Createddate'],errors='coerce')
    if 'Account_UpdatedDate' in df:
        df['Account_UpdatedDate'] = pd.to_datetime(df['Account_UpdatedDate'])
    if 'Account_CreatedDate' in df:
        df['Account_CreatedDate'] = pd.to_datetime(df['Account_CreatedDate'],errors='coerce')
    if 'PostedDate' in df:
        df['PostedDate'] = pd.to_datetime(df['PostedDate'],errors='coerce')

    df.to_csv('dags/prod_tasks/ActiveProduct/payment_newQuery.csv',sep=',', index=False)
 
else:
    print(f"POST request failed. Status code: {response.status_code}")
    print(result_response.text)