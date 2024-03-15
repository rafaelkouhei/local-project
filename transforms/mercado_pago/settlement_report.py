import pandas as pd
from datetime import datetime

df = pd.read_excel('/Users/rafaelsumiya/Downloads/settlement-report.xlsx')
pd.set_option('display.max_columns', None)
df = df.rename(columns=lambda x: x.lower())
df = df.astype({'external_reference': str, 'source_id': str, 'payment_method_type': str, 'payment_method': str, 'transaction_type': str, 'transaction_amount': float, 'origin_date': str, 'fee_amount': float, 'approval_date': str, 'real_amount': float, 'coupon_amount': float, 'financing_fee_amount': float, 'installments': int, 'money_release_date': str})
df[['origin_date', 'approval_date', 'money_release_date']] = df[['origin_date', 'approval_date', 'money_release_date']].map(lambda x: datetime.strptime(x[:10], '%Y-%m-%d') if x != 'nan' else None)

df.to_parquet('/Users/rafaelsumiya/Downloads/settlement_report.parquet')