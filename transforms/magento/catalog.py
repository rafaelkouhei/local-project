import pandas as pd
df = pd.read_csv('/Users/rafaelsumiya/Downloads/export_customers.csv', on_bad_lines='skip')
pd.set_option('display.max_columns', None)
# print(df.describe())
df = df.drop(columns=df.columns.difference(['sku', 'sku_seller', 'udropship_vendor', 'status', 'visibility', 'name', 'description', 'descricao_octoshop', 'url_key', 'model', 'export_magento2', 'brand', 'tributo_octo', 'special_price', 'cost', 'ean', 'opin', 'color', 'is_in_stock', 'version', 'customs_english_name', 'customs_value', 'hs_code', 'image', 'meta_title', 'meta_keyword', 'meta_description', 'asin', 'qty', 'customs_english_name']))
# print(df.columns.tolist())

df = df[(df['status'] == 'Enabled') | (df['status'] == 'Disabled')]
df = df.astype({'sku': str, 'sku_seller': str, 'udropship_vendor': str, 'status': str, 'visibility': str, 'name': str, 'description': str, 'descricao_octoshop': str, 'url_key': str, 'model': str, 'export_magento2': str, 'brand': str, 'tributo_octo': str, 'special_price': float, 'cost': float, 'ean': str, 'opin': str, 'color': str, 'is_in_stock': int, 'version': str, 'customs_english_name': str, 'customs_value': float, 'hs_code': str, 'image': str, 'meta_title': str, 'meta_keyword': str, 'meta_description': str, 'asin': str, 'qty': int})
df[['sku', 'sku_seller', 'udropship_vendor', 'status', 'visibility', 'name', 'description', 'descricao_octoshop', 'url_key', 'model', 'export_magento2', 'brand', 'tributo_octo', 'ean', 'opin', 'color', 'version', 'customs_english_name', 'hs_code', 'image', 'meta_title', 'meta_keyword', 'meta_description', 'asin']] = df[['sku', 'sku_seller', 'udropship_vendor', 'status', 'visibility', 'name', 'description', 'descricao_octoshop', 'url_key', 'model', 'export_magento2', 'brand', 'tributo_octo', 'ean', 'opin', 'color', 'version', 'customs_english_name', 'hs_code', 'image', 'meta_title', 'meta_keyword', 'meta_description', 'asin']].map(lambda x: x.replace('nan', ''))
df = df.replace({'nan': None})
# df['ean'] = df['ean'].replace('', '')
df['ean'] = df['ean'].map(lambda x: x.replace('.0', ''))

df.to_parquet('/Users/rafaelsumiya/Downloads/export_customers.parquet')