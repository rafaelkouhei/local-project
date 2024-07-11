import pandas as pd
df = pd.read_csv('/Users/rafaelsumiya/Downloads/base_2.csv')
pd.set_option('display.max_columns', None)

df = df[['sku', 'configurable_variations']]
# print(df.head(50))

df.to_parquet('/Users/rafaelsumiya/Downloads/base_2.parquet')