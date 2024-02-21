import apache_beam as beam
from apache_beam.io import ReadFromText
import pyarrow
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import re
import unicodedata

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

#Schemas
table_schema = [
    ('tracking_number', pyarrow.string()),
    ('tax_date', pyarrow.date32()),
    ('purchase_date', pyarrow.date32()),
    ('seller', pyarrow.string()),
    ('marketplace', pyarrow.string()),
    ('canal_id', pyarrow.string()),
    ('shipment', pyarrow.string()),
    ('qty', pyarrow.int16()),
    ('sku', pyarrow.string()),
    ('prod_title', pyarrow.string()),
    ('purchase_value', pyarrow.float32()),
    ('tax_sixty_percent', pyarrow.float32()),
    ('billed_value', pyarrow.float32()),
    ('declared_value', pyarrow.float32()),
    ('customer_paying', pyarrow.bool_()),
    ('payment_to', pyarrow.string())]

table_dict = ['tracking_number', 'tax_date', 'purchase_date', 'seller', 'marketplace', 'canal_id', 'shipment', 'qty', 'sku', 'prod_title', 'purchase_value', 'tax_sixty_percent', 'billed_value', 'declared_value', 'customer_paying', 'payment_to']

#Functions
def convert_float(x):
    try:
        return float(re.sub(r'[^0-9|,]', '', x).replace(',', '.'))
    except:
        return None

def convert_int(x):
    if x == '':
        return 1
    else:
        return int(re.sub(r'[^0-9]', '', x))

def seller_id(x):
    try:
        return x[0:x.index('-')]
    except:
        return None

def parse_datetime(x):
    if x != '' and x != '#N/A':
        x = datetime.strptime(x, '%d/%m/%Y')
    else:
        x = None
    return x

def convert_bool(x):
    if x == 'Sim':
        return True
    else:
        return False

def transform_pedidos(x):
    tracking_number, tax_date, purchase_date, seller, marketplace, canal_id, shipment, qty, sku, prod_title, purchase_value, tax_sixty_percent, billed_value, declared_value, customer_paying, payment_to = x
    return tracking_number, parse_datetime(tax_date), parse_datetime(purchase_date), seller, marketplace, canal_id, shipment, convert_int(qty), sku, prod_title, convert_float(purchase_value), convert_float(tax_sixty_percent), convert_float(billed_value), convert_float(declared_value), convert_bool(customer_paying), payment_to

pedidos_parquet = (
    pipeline
    | 'Read from Text' >> ReadFromText('/Users/rafaelsumiya/Downloads/Pedidos Tributados - export.tsv', skip_header_lines=1)
    | 'Text to List' >> beam.Map(lambda x: x.split('\t'))
    | 'Filter empty rows' >> beam.Filter(lambda x: x[0] != '')
    | 'Transform columns' >> beam.Map(lambda x: transform_pedidos(x))
    | 'List to Dict' >> beam.Map(lambda y, x: dict(zip(x, y)), table_dict)
    | 'Write to Parquet' >> beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/pedidos_tributados', file_name_suffix='.parquet', schema=pyarrow.schema(table_schema))
    # | 'Catalog Parquet - Print' >> beam.Map(print)
)

pipeline.run()