import pandas as pd
df = pd.read_excel('/Users/rafaelsumiya/Downloads/greenhouse_effect.xlsx', sheet_name='GEE Estados')
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
# print(df_gas_by_section.xs(('CO2 (t)', 'Mudança de Uso da Terra e Floresta'), level=[0, 1]))
# print(df_gas_by_section.xs('CO2 (t)', level=0).idxmax())
df_gas_by_section =df_gas_by_section.swaplevel(0, 1).groupby(level=0).idxmax()

# gas_by_section_max = df_gas_by_section.groupby(level=0).max().values
# summarized = df_gas_by_section.groupby(level=0).idxmax()
# summarized.insert(1, 'Qtd. Emissão', gas_by_section_max)

# Merging DataFrames
df_gas_by_state = df[df['Ano']==2021].groupby('Estado')[['Emissão']].sum().reset_index()

df_pop = pd.read_excel('/Users/rafaelsumiya/Downloads/POP2022_Municipios.xls', skiprows=1, skipfooter=34)
df_estado = df_pop.assign(pop = df_pop['POPULAÇÃO'].replace('\(\d{1,2}\)', '', regex=True), pop_2 = lambda x: x.loc[:, 'pop'].replace('\.', '', regex=True))
df_estado = df_estado.astype({'pop_2': int})
df_uf_pop = df_estado.groupby('UF')[['pop_2']].sum().reset_index()

gas_by_pop = pd.merge(df_gas_by_state, df_uf_pop, left_on='Estado', right_on='UF')
gas_by_pop = gas_by_pop.assign(emiss_per_capita = gas_by_pop['Emissão'] / gas_by_pop['pop_2']).sort_values('emiss_per_capita', ascending=False)

print(gas_by_pop)