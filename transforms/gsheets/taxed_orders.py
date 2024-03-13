import pandas as pd
from datetime import datetime
import re
pd.set_option('display.max_columns', None)
df = pd.read_csv('/Users/rafaelsumiya/Downloads/Pedidos Tributados - Pedidos tributados NOVO.csv')
df = df.loc[:, 'TrackingNumber':'Responsável Pgto']

df = df.rename(columns=lambda x: x.replace('\n', '_').replace(' ', '_').lower())
swap_names = {'trackingnumber': 'tracking_number', 'data_que_foi_tributado': 'tax_date', 'data_da_compra': 'purchase_date', 'loja': 'marketplace', 'n_pedido_marketplace': 'canal_id', 'qtd': 'qty', 'produtos': 'prod_title', 'valor_total_da_compra_no_marketplace': 'purchase_value', 'imposto_60%': 'tax_sixty_percent', 'valor_do_imposto_cobrado': 'billed_value', 'valor_declarado_no_dis_em_dolar': 'declared_value', 'cliente__aceitou__arcar?': 'customer_paying', 'responsável_pgto': 'payment_to'}
df = df.rename(columns=swap_names)
df['qty'] = df['qty'].fillna(1)

df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']] = df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']].astype(str)
df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']] = df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']].map(lambda x: re.sub(r'[^\d]', '', x))
df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']] = df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']].map(lambda x: x or None)
df = df.astype({'tracking_number': str, 'tax_date': str, 'purchase_date': str, 'seller': str, 'marketplace': str, 'canal_id': str, 'shipment': str, 'qty': int, 'sku': str, 'prod_title': str, 'purchase_value': float, 'tax_sixty_percent': float, 'billed_value': float, 'declared_value': float, 'customer_paying': bool, 'payment_to': str})
df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']] = df[['purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value']].map(lambda x: x / 100)
df[['tax_date', 'purchase_date']] = df[['tax_date', 'purchase_date']].map(lambda x: datetime.strptime(x, '%d/%m/%Y') if x != 'nan' else None)
df['shipment'] = df['shipment'].map(lambda x: x.replace('.0', ''))
df.to_parquet('/Users/rafaelsumiya/Downloads/taxed_orders.parquet')