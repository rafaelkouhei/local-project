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

#Columns
catalog_col = 'sku;type;name;sku_seller;asin;markup_amazon;special_price;cost;special_price_amazon;status;visibility;brand;qty;opin;export_magento2;ean;image;amazon_price_sync;is_in_stock'
drop_item_col = 'order_id;order_date;order_status;shipment_id;shipment_date;shipment_status;seller;item_sku;item_price;item_cost;marketplace;canal_id;item_qty;item_row_total'
tracking_codes_col = 'marketplace;canal_id;order_id;shipment_id;seller;region;order_status;shipment_status;shipment_date;track_date;first_event;event_days;deliv_date;deliv_time;track_number'

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
    ('is_in_stock', pyarrow.string())]

catalog_dict = ['sku', 'type', 'name', 'sku_seller', 'asin', 'markup_amazon', 'special_price', 'cost', 'special_price_amazon', 'status', 'visibility', 'brand', 'qty', 'opin', 'export_magento2', 'ean', 'image', 'amazon_price_sync', 'is_in_stock']

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
    if x is not None and x != '':
        return x[0:x.index(' ')]
    else:
        return None

def parse_datetime(x):
    if x != '':
        x = datetime.strptime(x, '%b %d, %Y %H:%M:%S %p')
    return x

def region_format(x):
    region_dict = {'^SP|^SAO|PAULO|^S.+O$': 'State of Sao Paulo'}
    x = unicodedata.normalize('NFKD', x.upper()).encode('ascii', 'ignore').decode('ascii')
    y = region_dict[x]
    return y

def transform_catalog(x):
    sku, type, name, sku_seller, asin, markup_amazon, special_price, cost, special_price_amazon, status, visibility, brand, qty, opin, export_magento2, ean, image, amazon_price_sync, is_in_stock = x
    return sku, type, name, sku_seller, asin, convert_float(markup_amazon), convert_float(special_price), convert_float(cost), convert_float(special_price_amazon), status, visibility, brand, convert_int(qty), opin, export_magento2, ean, image, amazon_price_sync, is_in_stock

def transform_drop_item(x):
    order_id, order_date, order_status, shipment_id, shipment_date, shipment_status, seller, seller_name, item_sku, item_name, item_price, item_discount, item_cost, marketplace, canal_id, item_qty, item_tax, item_row_total = x
    return convert_float(order_id), parse_datetime(order_date), order_status, shipment_id, parse_datetime(shipment_date), shipment_status, seller, item_sku, convert_float(item_price), convert_float(item_cost), marketplace, canal_id, item_qty, convert_float(item_row_total)

def transform_tracking_codes(x):
    marketplace, canal_id, order_id, shipment_id, seller, customer, shipping_city, region, order_status, shipment_status, shipment_date, track_date, first_event, event_days, deliv_date, deliv_time, mag2_int, mag2_int_at, track_number = x
    return marketplace, canal_id, convert_float(order_id), shipment_id, seller_id(seller), region_format(region), order_status, shipment_status, parse_datetime(shipment_date), parse_datetime(track_date), parse_datetime(first_event), event_days, parse_datetime(deliv_date), deliv_time, track_number

#PCollections
catalog_csv = (
    # pipeline
    # | 'Catalog CSV - Read from text' >> ReadFromText('export_customers.csv', skip_header_lines=1)
    # | 'Catalog CSV - String to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
    # | 'Catalog CSV - Filter List length == 19' >> beam.Filter(lambda x: len(x) == 19)
    # | 'Catalog CSV - Remove double quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
    # | 'Catalog CSV - Filter empty values' >> beam.Filter(lambda x: x[1] != '')
    # | 'Catalog CSV - Convert all columns to string' >> beam.Map(lambda x: [str(i) for i in x])
    # | 'Catalog CSV - List to Text' >> beam.Map(lambda x: ';'.join(x))
    # | 'Catalog CSV - Create CSV file' >> WriteToText('catalog', file_name_suffix='.csv', header=catalog_col)
    # # | 'Catalog CSV - Print' >> beam.Map(print)
)
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
drop_item = (
    # pipeline
    # | 'Dropship Item - Read from text' >> ReadFromText('dropship_reportItem.csv', skip_header_lines=1)
    # | 'Dropship Item - String to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
    # | 'Dropship Item - Remove double quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
    # | 'Dropship Item - Array length == 18' >> beam.Filter(lambda x: len(x) == 18)
    # | 'Dropship Items - Tranform columns' >> beam.Map(transform_drop_item)
    # | 'Dropship Items - Convert all columns to string' >> beam.Map(lambda x: [str(i) for i in x])
    # | 'Dropship Items - List to Text' >> beam.Map(lambda x: ';'.join(x))
    # | 'Dropship Items - Create CSV file' >> WriteToText('drop_item', file_name_suffix='.csv', header=drop_item_col)
    # # | 'Dropship Item - Print' >> beam.Map(print)
)
tracking_codes = (
#     pipeline
#     | 'Tracking Codes - Read from text' >> ReadFromText('tracking_codes.csv', skip_header_lines=1)
#     | 'Tracking Codes - String to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
#     | 'Tracking Codes - Remove Double Quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
#     | 'Tracking Codes - len == 19' >> beam.Filter(lambda x: len(x) == 19)
#     | 'Tracking Codes - Transform columns' >> beam.Map(transform_tracking_codes)
#     | 'Tracking Codes - Convert all columns to string' >> beam.Map(lambda x: [str(i) for i in x])
#     | 'Tracking Codes - list to Text' >> beam.Map(lambda x: ';'.join(x))
#     # | 'Tracking Codes - Create CSV file' >> WriteToText('tracking_codes', file_name_suffix='.csv', header=tracking_codes_col)
#     | 'Tracking Codes - Print' >> beam.Map(print)
)

pipeline.run()