import pandas as pd
df = pd.read_excel('/Users/rafaelsumiya/Downloads/greenhouse_effect.xlsx', sheet_name='GEE Estados')
pd.set_option('display.max_columns', None)
# print(df['Emissão / Remoção / Bunker'].unique())
# print((df['Emissão / Remoção / Bunker'] == 'Remoção') | (df['Emissão / Remoção / Bunker'] == 'Remoção NCI'))
print(df[df['Emissão / Remoção / Bunker'].isin(['Remoção', 'Remoção NCI'])])