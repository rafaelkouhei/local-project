import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToParquet
import pyarrow
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import re
import unicodedata

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

#Schemas
catalog_schema = [
    ('sku', pyarrow.string()),
    ('type', pyarrow.string()),
    ('name', pyarrow.string()),
    ('sku_seller', pyarrow.string()),
    ('asin', pyarrow.string()),
    ('markup_amazon', pyarrow.float64()),
    ('special_price', pyarrow.float64()),
    ('cost', pyarrow.float64()),
    ('special_price_amazon', pyarrow.float64()),
    ('status', pyarrow.string()),
    ('visibility', pyarrow.string()),
    ('brand', pyarrow.string()),
    ('qty', pyarrow.int64()),
    ('opin', pyarrow.string()),
    ('export_magento2', pyarrow.string()),
    ('ean', pyarrow.string()),
    ('image', pyarrow.string()),
    ('amazon_price_sync', pyarrow.string()),
    ('is_in_stock', pyarrow.string()),
    ('seller', pyarrow.string())]

catalog_dict = ['sku', 'type', 'name', 'sku_seller', 'asin', 'markup_amazon', 'special_price', 'cost', 'special_price_amazon', 'status', 'visibility', 'brand', 'qty', 'opin', 'export_magento2', 'ean', 'image', 'amazon_price_sync', 'is_in_stock', 'seller']

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

def seller_id(x):
    try:
        return x[0:x.index('-')]
    except:
        return None

def transform_catalog(x):
    sku, type, name, sku_seller, asin, markup_amazon, special_price, cost, special_price_amazon, status, visibility, brand, qty, opin, export_magento2, ean, image, amazon_price_sync, is_in_stock = x
    return sku, type, name, sku_seller, asin, convert_float(markup_amazon), convert_float(special_price), convert_float(cost), convert_float(special_price_amazon), status, visibility, brand, convert_int(qty), opin, export_magento2, ean, image, amazon_price_sync, is_in_stock, seller_id(sku)

catalog_parquet = (
    pipeline
    | 'Catalog Parquet - Read from Text' >> ReadFromText('/Users/rafaelsumiya/Downloads/export_customers.csv', skip_header_lines=1)
    | 'Catalog Parquet - Text to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
    | beam.Filter(lambda x: len(x) == 19)
    | beam.Map(lambda x: [i.replace('"', '') for i in x])
    | beam.Filter(lambda x: x[1] != '')
    | beam.Map(transform_catalog)
    | beam.Map(lambda y, x: dict(zip(x, y)), catalog_dict)
    | beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/catalog', file_name_suffix='.parquet', schema=pyarrow.schema(catalog_schema))
    # | 'Catalog Parquet - Print' >> beam.Map(print)
)

pipeline.run()