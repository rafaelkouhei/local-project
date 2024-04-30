import pandas as pd
import glob
pd.set_option('display.max_columns', None)
df = pd.read_excel('/Users/rafaelsumiya/Downloads/payout_shipments.xlsx')

df = df.rename(columns=lambda x: x.lower().replace(' ', '_'))
df = df[['vendor', 'shipment', 'pay_status']]
df = df.astype({'vendor': str, 'shipment': str})
df.to_parquet('/Users/rafaelsumiya/Downloads/payout_shipments.parquet')