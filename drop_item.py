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
    ('order_id', pyarrow.string()),
    ('order_date', pyarrow.date32()),
    ('order_status', pyarrow.string()),
    ('shipment_id', pyarrow.string()),
    ('shipment_date', pyarrow.date32()),
    ('shipment_status', pyarrow.string()),
    ('seller', pyarrow.string()),
    ('item_sku', pyarrow.string()),
    ('item_price', pyarrow.float32()),
    ('item_cost', pyarrow.float32()),
    ('marketplace', pyarrow.string()),
    ('canal_id', pyarrow.string()),
    ('item_qty', pyarrow.int16()),
    ('item_row_total', pyarrow.float32())]

table_dict = ['order_id', 'order_date', 'order_status', 'shipment_id', 'shipment_date', 'shipment_status', 'seller', 'item_sku', 'item_price', 'item_cost', 'marketplace', 'canal_id', 'item_qty', 'item_row_total']

#Functions
def convert_float(x):
    try:
        y = float(re.sub(r'[^0-9|.]', '', x))
    except:
        y = None
    return y

def convert_int(x):
    try:
        y = int(re.sub(r'[^0-9]', '', x))
    except:
        y = None
    return y

def parse_datetime(x):
    if x != '':
        x = datetime.strptime(x, '%b %d, %Y %H:%M:%S %p')
    return x

def transform_drop_item(x):
    order_id, order_date, order_status, shipment_id, shipment_date, shipment_status, seller, seller_name, item_sku, item_name, item_price, item_discount, item_cost, marketplace, canal_id, item_qty, item_tax, item_row_total = x
    return str(convert_int(order_id)), parse_datetime(order_date), order_status, str(shipment_id), parse_datetime(shipment_date), shipment_status, str(seller), item_sku, convert_float(item_price), convert_float(item_cost), marketplace, canal_id, convert_int(item_qty), convert_float(item_row_total)

#PCollections
drop_item = (
    pipeline
    | 'Read from text' >> ReadFromText('/Users/rafaelsumiya/Downloads/dropship_reportItem.csv', skip_header_lines=1)
    | 'Text to List' >> beam.Map(lambda x: x.split('","'))
    | 'Remove double quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
    | 'Length == 18' >> beam.Filter(lambda x: len(x) == 18)
    | 'Tranform columns' >> beam.Map(transform_drop_item)
    | 'Transform to Dictionary' >> beam.Map(lambda y, x: dict(zip(x, y)), table_dict)
    | 'Create Parquet file' >> beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/dropship_reportItem', file_name_suffix='.parquet', schema=pyarrow.schema(table_schema))
    # | 'Dropship Item - Print' >> beam.Map(print)
)

pipeline.run()