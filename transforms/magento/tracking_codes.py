import pandas as pd
from datetime import datetime
import re

pd.set_option('display.max_columns', None)
df = pd.read_csv('/Users/rafaelsumiya/Downloads/tracking_codes.csv')
df = df.rename(columns=lambda x: x.lower().replace(' ', '_').replace('.', ''))
df[['shipment_date', 'track_date', 'first_event', 'deliv_date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv_date']].astype(str)
# df[['shipment_date', 'track_date', 'first_event', 'deliv_date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv_date']].map(lambda x: x.replace('nan', ''))

df[['shipment_date', 'track_date', 'first_event', 'deliv_date']] = df[['shipment_date', 'track_date', 'first_event', 'deliv_date']].map(lambda x: datetime.strptime(x, '%b %d, %Y %H:%M:%S %p') if x != 'nan' else x.replace('nan', ''))

# print(df[['shipment_date', 'track_date', 'first_event', 'deliv_date']])
t = df.query('event_days == "nan"')
print(t)
# print(df['event_days'] == 'nan')
# df = df.astype({'marketplace': str, 'marketplace_id': str, 'order_id': str, 'shipment_id': str, 'vendor': str, 'customer': str, 'shipping_city': str, 'region': str, 'order_status': str, 'shipment_status': str, 'event_days': int, 'delivtime': int, 'track_number': str})
# print(set(df['event_days']))