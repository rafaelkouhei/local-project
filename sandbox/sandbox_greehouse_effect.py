import pandas as pd
df = pd.read_excel('/Users/rafaelsumiya/Downloads/greenhouse_effect.xlsx', sheet_name='GEE Estados', nrows=1000)
pd.set_option('display.max_columns', None)
# print(df['Emissão / Remoção / Bunker'].unique())
# print((df['Emissão / Remoção / Bunker'] == 'Remoção') | (df['Emissão / Remoção / Bunker'] == 'Remoção NCI'))
# print(df[df['Emissão / Remoção / Bunker'].isin(['Remoção', 'Remoção NCI'])])
# print(df.loc[df['Emissão / Remoção / Bunker'].isin(['Remoção', 'Remoção NCI']), 1970:2021].max())
# print(df.loc[df['Emissão / Remoção / Bunker'] == 'Bunker', 'Estado'].unique())
df = df[df['Emissão / Remoção / Bunker'] == 'Emissão']
df = df.drop(columns='Emissão / Remoção / Bunker')

# melting (wide to long / normalized)
df_info = list(df.loc[:, 'Nível 1 - Setor':'Produto'].columns)
df_gas = list(df.loc[:, 1970:2021].columns)
df = df.melt(id_vars=df_info, value_vars=df_gas, var_name='Ano', value_name='Emissão')

grouped_gas = df.groupby('Gás')[['Emissão']].sum().sort_values('Emissão', ascending=False)
# print(grouped_gas.groups)
# print(grouped_gas.get_group('CO2 (t)'))
# print(grouped_gas)
# print(grouped_gas.iloc[0:9].sum() / grouped_gas.sum())

df_gas_by_section = df.groupby(['Gás', 'Nível 1 - Setor'])[['Emissão']].sum().sort_values('Emissão', ascending=False)
print(df_gas_by_section)