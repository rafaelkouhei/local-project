import pandas as pd
from datetime import datetime
import re

# Preparing columns
pd.set_option('display.max_columns', None)
df = pd.read_csv('/Users/rafaelsumiya/Downloads/tracking_codes.csv')

# Applying transformations and cleansing
df = df.rename(columns=lambda x: x.lower().replace(' ', '_').replace('.', '_'))
df = df.astype({'marketplace': str, 'marketplace_id': str, 'order_id': str, 'shipment_id': str, 'vendor': str, 'customer': str, 'shipping_city': str, 'region': str, 'order_status': str, 'shipment_status': str, 'deliv_time': int, 'track_number': str})

df[['shipment_date', 'track_date', 'first_event', 'deliv__date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv__date']].astype(str)
df[['shipment_date', 'track_date', 'first_event', 'deliv__date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv__date']].map(lambda x: x.replace('nan', ''))
df[['shipment_date', 'track_date', 'first_event', 'deliv__date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv__date']].map(lambda x: datetime.strptime(x, '%b %d, %Y %H:%M:%S %p') if x != '' else None)
df = df.drop(columns=['customer', 'event_days', 'magento2_integrated', 'magento2_int__at'])
df['vendor'] = df['vendor'].map(lambda x: x.replace('nan', '') if x == 'nan' else x[:x.index(' ')])
df['marketplace_id'] = df['marketplace_id'].map(lambda x: x.replace('nan', ''))
df.to_parquet('/Users/rafaelsumiya/Downloads/tracking_codes.parquet')