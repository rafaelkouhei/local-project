import pandas as pd
from datetime import datetime
df = pd.read_csv('/Users/rafaelsumiya/Downloads/CRM Afiliados - Acompanhamento financeiro.csv')
pd.set_option('display.max_columns', None)
df = df[['Desconto', 'PO #', 'PO Date', 'PO Item SKU']]
swap_name = {'PO #': 'shipment_id', 'Desconto': 'desconto', 'PO Date': 'shipment_date', 'PO Item SKU': 'sku'}
df = df.rename(columns=swap_name)
df['shipment_id'] = df['shipment_id'].astype('string')
df['desconto'] = df['desconto'].map(lambda x: x.replace('%', ''))
df['desconto'] = df['desconto'].astype('float')
df['desconto'] = df['desconto'].map(lambda x: x / 100)
df['shipment_date'] = df['shipment_date'].map(lambda x: datetime.strptime(x, '%d/%m/%Y'))
df.to_parquet('/Users/rafaelsumiya/Downloads/crm_afiliados.parquet')