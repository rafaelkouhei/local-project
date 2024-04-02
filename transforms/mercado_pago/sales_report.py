import pandas as pd
from datetime import datetime
df = pd.read_csv('/Users/rafaelsumiya/Downloads/collection_mp_sales_report.csv', sep=';')
pd.set_option('display.max_columns', None)

df = df.rename(columns=lambda x: x.lower()[x.index('(')+1:x.index(')')])

df = df[['date_created', 'date_approved', 'date_released', 'item_id', 'external_reference', 'status', 'status_detail', 'operation_type', 'transaction_amount', 'mercadopago_fee', 'net_received_amount', 'installments', 'payment_type', 'amount_refunded', 'financing_fee', 'rejection_causes']]
df = df.astype({'date_created': str, 'date_approved': str, 'date_released': str, 'item_id': str, 'external_reference': str, 'status': str, 'status_detail': str, 'operation_type': str, 'transaction_amount': float, 'mercadopago_fee': float, 'net_received_amount': float, 'installments': int, 'payment_type': str, 'amount_refunded': float, 'financing_fee': float, 'rejection_causes': str})
df[['date_created', 'date_approved', 'date_released']] = df[['date_created', 'date_approved', 'date_released']].map(lambda x: datetime.strptime(x, '%d/%m/%Y %H:%M:%S') if x != 'nan' else x)
df = df.replace({'nan': None})

# print(df[df['status'] != 'canceled'].groupby(by='status').sum('amount_refunded'))

df.to_parquet('/Users/rafaelsumiya/Downloads/sales_collection.parquet')