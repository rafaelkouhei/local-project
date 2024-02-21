import pandas as pd
df = pd.read_csv('/Users/rafaelsumiya/Downloads/export_customers.csv')
pd.set_option('display.max_columns', None)
print(df.head())
# df.to_parquet('/Users/rafaelsumiya/Downloads/export_customers.parquet')