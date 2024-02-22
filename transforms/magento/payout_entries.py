import pandas as pd
from datetime import datetime

df = pd.read_excel('/Users/rafaelsumiya/Downloads/payout_shipments_entries.xlsx')
pd.set_option('display.max_columns', None)

# Applying transformations and cleansing
df = df.drop(['Vendor', 'Shipment ID', 'Entry Status', 'Transaction', 'Processed At'], axis=1)
df.columns = df.columns.map(lambda x: x.lower().replace(' ', '_'))
df = df.astype({'entry_id': str, 'vendor_id': str, 'marketplace': str, 'marketplace_id': str, 'shipment_increment': str, 'entry_type': str, 'entry_total': float, 'entry_message': str, 'created_at': str})
df['created_at'] = df['created_at'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if x != 'nan' else x.replace('nan', ''))

# Loading into Parquet
df.to_parquet('/Users/rafaelsumiya/Downloads/payout_entries.parquet')