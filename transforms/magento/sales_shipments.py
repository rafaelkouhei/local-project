import pandas as pd
df = pd.read_excel('/Users/rafaelsumiya/Downloads/sales_shipments.xlsx')
df = df.astype({'ID': str, 'Shipment Increment': str, 'Order Increment': str, 'Customer Name': str, 'Customer E-mail': str, 'Total Items': int, 'Cost': float, 'Liquid': float, 'Total Sale': float, 'Total Order': float, 'Discount': int, 'Vendor Id': str, 'Vendor': str, 'Status': str, 'Canal': str, 'Canal ID': str, 'Created At': str, 'Comission %': int, 'Octo Gain': float, 'Vendor Gain': float, 'Item ID': str, 'Item SKU': str, 'Item SKU Seller': str, 'Item Qty': int, 'Item Cost': float, 'Item Price': float, 'Item PO Price': float, 'Item Dollar': float, 'Item Brand': str, 'customs_value': float})
df['Created At'] = pd.to_datetime(df['Created At'])
df = df.rename(columns=lambda x: x.lower().replace(' ', '_'))
df.to_parquet('/Users/rafaelsumiya/Downloads/sales_shipments.parquet')