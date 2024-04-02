import pandas as pd
from datetime import datetime
df = pd.read_csv('/Users/rafaelsumiya/Downloads/OctoSquad _ CRM Afiliados - Acompanhamento financeiro.csv')
pd.set_option('display.max_columns', None)
df = df[['Tipo de ação', 'Desconto', 'PO #', 'PO Date', 'PO Item SKU']]
swap_name = {'Tipo de ação': 'tipo_acao', 'PO #': 'shipment_id', 'Desconto': 'desconto', 'PO Date': 'shipment_date', 'PO Item SKU': 'sku'}
df = df.rename(columns=swap_name)
df['shipment_id'] = df['shipment_id'].astype(str).map(lambda x: x.replace('.0', ''))
df['desconto'] = df['desconto'].astype(str).map(lambda x: x.replace('%', ''))
df['desconto'] = df['desconto'].astype('float')
df['desconto'] = df['desconto'].map(lambda x: x / 100)
df['shipment_date'] = df['shipment_date'].astype(str).map(lambda x: datetime.strptime(x, '%m/%d/%Y') if x != 'nan' else None)
df = df.drop(df[df['shipment_id'] == 'nan'].index)
df = df.replace({'nan': None})
df.to_parquet('/Users/rafaelsumiya/Downloads/crm_afiliados.parquet')