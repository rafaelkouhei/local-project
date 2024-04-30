import pandas as pd
df_pop = pd.read_excel('/Users/rafaelsumiya/Downloads/POP2022_Municipios.xls', skiprows=1, skipfooter=34)
pd.set_option('display.max_columns', None)
df_estado = df_pop.groupby('UF').sum(numeric_only=True)

df_estado_2 = df_pop.assign(pop = df_pop['POPULAÇÃO'].replace('\(\d{1,2}\)', '', regex=True), pop_2 = lambda x: x.loc[:, 'pop'].replace('\.', '', regex=True))
# print(df[df['POPULAÇÃO'].str.contains('\(', na=False)])
df_estado_2 = df_estado_2.astype({'pop_2': int})

df_uf_pop = df_estado_2.groupby('UF')[['pop_2']].sum().reset_index()

# print(df_estado_2[df_estado_2['POPULAÇÃO'].str.contains('\(', na=False)])
print(df_uf_pop)