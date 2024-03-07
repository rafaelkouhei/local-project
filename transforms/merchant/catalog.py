import pandas as pd
df = pd.read_csv('/Users/rafaelsumiya/Downloads/prod_category_raw.csv', skiprows=2)
pd.set_option('display.max_columns', None)
swap_names = {'Código': 'sku', 'Categoria (nível 1)': 'category_1', 'Categoria (nível 2)': 'category_2', 'Categoria (nível 3)': 'category_3', 'Categoria (nível 4)': 'category_4', 'Categoria (nível 5)': 'category_5'}
df = df.rename(columns=swap_names)
df.to_parquet('/Users/rafaelsumiya/Downloads/prod_category.parquet')