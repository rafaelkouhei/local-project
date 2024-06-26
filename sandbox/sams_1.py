import pandas as pd
df = pd.read_csv("/Users/rafaelsumiya/Downloads/Case Analytics Engineer Sam's Club/CASE_PRATICO_SAMS_CLUB.csv")

### Verifica se a base de dados precisa de algum tratamento
pd.set_option('display.max_columns', None) # remove o limite de exibição das colunas
print(df.info())
print(df.head(10))
print(df.isnull().sum()) # verifica se tem coluna com campos vazios

### Converte as colunas para os tipos corretos
# df['periodo'] = pd.to_datetime(df['periodo'], frormat='%Y-%m-%d')
# df[['id_clube', 'socio', 'ticket', 'item_id']] = df[['id_clube', 'socio', 'ticket', 'item_id']].astype(str)

### Agrupa as dimensões e soma as colunas númericas
# df_grouped = df.groupby(['periodo', 'id_clube', 'socio', 'canal', 'ticket', 'departamento', 'item_id', 'item_descricao']).sum(numeric_only=True)
# print(df_grouped.head(10))
# print(df_grouped.shape)
# df_grouped.to_parquet("/Users/rafaelsumiya/Downloads/Case Analytics Engineer Sam's Club/sample.parquet")

# df_grouped = df.groupby(['periodo', 'id_clube', 'socio', 'canal', 'ticket', 'departamento', 'item_id', 'item_descricao']).sum(numeric_only=True)
# print(df_grouped.head(10))
# print(df_grouped.shape)