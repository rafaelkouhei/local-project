import pandas as pd
import re
from datetime import datetime

# Preparing columns
pd.set_option('display.max_columns', None)
df = pd.read_csv('/Users/rafaelsumiya/Downloads/dropship_reportItem.csv')
df = df.rename(columns=lambda x: x.lower().replace(' ', '_'))
swap_name = {'order_#': 'order_id', 'po_#': 'shipment_id', 'po_date': 'shipment_date', 'po_status': 'shipment_status', 'vendor_id': 'seller', 'vendor': 'seller_name', 'po_item_sku': 'item_sku', 'po_item_name': 'item_name', 'po_item_price': 'item_price', 'po_item_discount': 'item_discount', 'po_item_cost': 'item_cost', 'po_item_qty': 'item_qty', 'po_item_tax': 'item_tax', 'po_item_row_total': 'item_row_total'}
df = df.rename(columns=swap_name)

# Applying transformations and cleansing
# Parsing date columns
df[['order_date', 'shipment_date']] = df[['order_date', 'shipment_date']].map(lambda x: datetime.strptime(x, '%b %d, %Y %H:%M:%S %p'))
# Group of columns from string to float
df[['item_price', 'item_discount', 'item_cost', 'item_tax', 'item_row_total']] = df[['item_price', 'item_discount', 'item_cost', 'item_tax', 'item_row_total']].map(lambda x: re.sub('[^0-9|.]', '', str(x)))
df[['item_price', 'item_discount', 'item_cost', 'item_tax', 'item_row_total']] = df[['item_price', 'item_discount', 'item_cost', 'item_tax', 'item_row_total']].map(lambda x: x.replace('', '0.00') if x == '' else x)
# Seller column
df['seller'] = df['seller'].fillna(0)
df['seller'] = df['seller'].astype(int)
df['seller'] = df['seller'].astype(str)

# Loading into Parquet
df = df.astype({'order_id': str, 'order_status': str, 'shipment_id': str, 'shipment_status': str, 'seller': str, 'item_sku': str, 'item_name': str, 'item_price': float, 'item_discount': float, 'item_cost': float, 'marketplace': str, 'canal_id': str, 'item_qty': int, 'item_tax': float, 'item_row_total': float})
df = df.replace({'nan': None})
df.to_parquet('/Users/rafaelsumiya/Downloads/drop_item.parquet')