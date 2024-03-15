import pandas as pd
from unidecode import unidecode
from datetime import datetime

df = pd.read_excel('/Users/rafaelsumiya/Downloads/multi.xlsx')
pd.set_option('display.max_columns', None)
df = df.rename(columns=lambda x: unidecode(x.lower().replace(' ', '_').replace('(', '').replace(')', '')))
df['numero_do_pedido'] = df['numero_do_pedido'].astype(str).map(lambda x: x[:9] if len(x) > 9 and x.startswith('0') else x)
df['data_de_criacao'] = df['data_de_criacao'].map(lambda x: datetime.strptime(x[:10], '%d/%m/%Y'))

df.to_parquet('/Users/rafaelsumiya/Downloads/customer_msg.parquet')