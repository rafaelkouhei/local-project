import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, inspect, text
engine = create_engine('sqlite:///:memory:')

df_html = pd.read_html('https://en.wikipedia.org/wiki/AFI%27s_100_Years...100_Movies')[1]
pd.set_option('display.max_columns', None)
# print(df_html.info())
# df_html.to_html('/Users/rafaelsumiya/Downloads/movies.html')
# df_html.to_csv('/Users/rafaelsumiya/Downloads/movies.csv', index=False)
# df_xml = pd.read_xml('/Users/rafaelsumiya/Downloads/imdb_top_1000.xml')
# print(df_xml)

url = 'https://raw.githubusercontent.com/alura-cursos/Pandas/main/clientes_banco.csv'
df_sql = pd.read_csv(url)

# print(df_sql)
df_sql.to_sql('clientes', engine, index=False)
inspector = inspect(engine)
# print(inspector.get_table_names())
employed_sql = pd.read_sql('SELECT * FROM clientes WHERE Categoria_de_renda = "Empregado"', engine)
# print(employed_sql)
# print(type(employed_sql))
# print(pd.read_sql_table('clientes', engine))
with engine.connect() as conn:
    conn.execute(text('UPDATE clientes SET Grau_escolaridade="Ensino superior" WHERE ID_Cliente=5008808'))
print(employed_sql['ID_Cliente'])