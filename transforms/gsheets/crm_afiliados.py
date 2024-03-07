import pandas as pd
from datetime import datetime
df = pd.read_csv('/Users/rafaelsumiya/Downloads/CRM Afiliados - Acompanhamento financeiro.csv')
pd.set_option('display.max_columns', None)
df = df[['Desconto', 'Order #', 'Order Date']]
swap_name = {'Order #': 'order_id', 'Desconto': 'desconto', 'Order Date': 'order_date'}
df = df.rename(columns=swap_name)
df['order_id'] = df['order_id'].astype('string')
df['desconto'] = df['desconto'].map(lambda x: x.replace('%', ''))
df['desconto'] = df['desconto'].astype('float')
df['desconto'] = df['desconto'].map(lambda x: x / 100)
df['order_date'] = df['order_date'].map(lambda x: datetime.strptime(x, '%d/%m/%Y'))
df.to_parquet('/Users/rafaelsumiya/Downloads/crm_afiliados.parquet')