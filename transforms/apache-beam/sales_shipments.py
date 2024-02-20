import pandas as pd
df = pd.read_excel('/Users/rafaelsumiya/Downloads/sales_shipments.xlsx', nrows=10) # parâmetro nrows para determinar quantas linhas ler; parâmetro usecols para escolher quais colunas ler, seja pelo nome ou índice da coluna ['x', 'y'] ou [0, 1]

sheet_id = '1xG_3nr0di8bGihmLq-aiaOEKQQt-UG814-S1txCHbs0'
url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet'
gs = pd.read_csv(url)
print(gs.head())

### Informações do Data Frame
print(df.shape) # número de linhas e colunas
print(df.columns) # lista todas as colunas
print(df.info()) # lista todas as colunas e o tipo de cada uma

print(df.groupby('Item Brand').mean(numeric_only=True))
print(df.groupby('Item Brand')[['Item PO Price']].sum(numeric_only=True))
df_brand_sales = df.groupby('Item Brand')[['Item PO Price']].sum(numeric_only=True)
df_brand_sales.plot(kind='barh', figsize=(14, 10), color ='purple')
print(df.groupby(['Item SKU', 'Item Brand'])['Item PO Price'].sum(numeric_only=True))
print(df.Status.unique())
status_canceled = ['canceled ']
not_canceled = df.query('@status_canceled not in Status')
sorted_data = not_canceled['Item Brand'].value_counts(normalize=True).to_frame().sort_values('Item Brand')
sorted_data.plot(kind='bar', figsize=(14, 10), color ='green', edgecolor='black', xlabel = 'Tipos', ylabel = 'Percentual')
print(sorted_data)
print(df.query('`Item Brand` == "Sennheiser"').head(10))

### Tratamento de dados nulos
print(df.isnull().sum()) # verifica se tem campos nulos e soma
df = df.fillna(0) # preenche os campos nulos
df.dropna() # remove a linha inteira caso tenha algum campo nulo
df.interpolate() # interpola os dados nulos

### Removendo linhas e colunas
linhas_remover = df.query('Status == "canceled "').index
df.drop(linhas_remover, axis=0, inplace=True) # axis 0 para linhas, 1 para colunas; inplace=True para modificar o Data Frame diretamente, sem precisar atribuir
df.drop('Item SKU', axis=1, inplace=True) # remover uma coluna
print(df.query('Status == "canceled "')[['Shipment Increment', 'Status', 'Item SKU']])

### Filtrando resultado
filtro1 = df['Status'] == 'canceled '
filtro2 = df['Item Brand'] == 'Meta'
filtro3 = (filtro1) & (filtro2)
filtro4 = (df['Status'] == 'canceled ') & (df['Item Brand'] == 'Meta')
print(df[['Shipment Increment', 'Status', 'Item SKU', 'Item Brand']][filtro1 & filtro2].head())
print(df[['Shipment Increment', 'Status', 'Item SKU', 'Item Brand']][filtro3].head())
print(df[['Shipment Increment', 'Status', 'Item SKU', 'Item Brand']][(df['Status'] == 'canceled ') & (df['Item Brand'] == 'Meta')].head())
print(df[['Shipment Increment', 'Status', 'Item SKU', 'Item Brand']][(df['Item Brand'] == 'Razer') | (df['Item Brand'] == 'Logitech')].head(50))
teste_save = df[['Shipment Increment', 'Status', 'Item SKU', 'Item Brand']][(df['Item Brand'] == 'Razer') | (df['Item Brand'] == 'Logitech')].head(50)
teste_save.to_csv('/Users/rafaelsumiya/Downloads/teste.csv', index=False)

## Criando colunas novas
df['Valor Total'] = df['Item PO Price'] * df['Item Qty']
print(df[['Shipment Increment', 'Item PO Price', 'Item Qty', 'Valor Total']])

### Criação de colunas novas através de função
df['bool'] = df['Vendor Id'].apply(lambda x: True if x == 60 or x == 491 else False)
print(df.info())