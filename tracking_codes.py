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
tracking_codes_schema = [
    ('canal', pyarrow.string()),
    ('canal_id', pyarrow.string()),
    ('order_id', pyarrow.string()),
    ('shipment_id', pyarrow.string()),
    ('seller', pyarrow.string()),
    ('region', pyarrow.string()),
    ('order_status', pyarrow.string()),
    ('shipment_status', pyarrow.string()),
    ('shipment_date', pyarrow.date32()),
    ('track_date', pyarrow.date32()),
    ('first_event', pyarrow.date32()),
    ('event_days', pyarrow.int16()),
    ('deliv_date', pyarrow.date32()),
    ('deliv_time', pyarrow.int16()),
    ('track_number', pyarrow.string())]

tracking_codes_dict = ['canal', 'canal_id', 'order_id', 'shipment_id', 'seller', 'region', 'order_status', 'shipment_status', 'shipment_date', 'track_date', 'first_event', 'event_days', 'deliv_date', 'deliv_time', 'track_number']

#Functions
def convert_float(x):
    try:
        y = float(re.sub(r'[^0-9|.]', '', x))
    except:
        y = None
    return y

def convert_int(x):
    if x != None and x != '':
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

def transform_tracking_codes(x):
    marketplace, canal_id, order_id, shipment_id, seller, customer, shipping_city, region, order_status, shipment_status, shipment_date, track_date, first_event, event_days, deliv_date, deliv_time, mag2_int, mag2_int_at, track_number = x
    return marketplace, canal_id, str(convert_int(order_id)), shipment_id, seller_id(seller), region, order_status, shipment_status, parse_datetime(shipment_date), parse_datetime(track_date), parse_datetime(first_event), convert_int(event_days), parse_datetime(deliv_date), convert_int(deliv_time), track_number

#PCollections
tracking_codes = (
    pipeline
    | 'Read from text' >> ReadFromText('/Users/rafaelsumiya/Downloads/tracking_codes.csv', skip_header_lines=1)
    | 'String to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
    | 'Remove Double Quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
    | 'len == 19' >> beam.Filter(lambda x: len(x) == 19)
    | 'Transform columns' >> beam.Map(transform_tracking_codes)
    | 'Transform to Dictionary' >> beam.Map(lambda y, x: dict(zip(x, y)), tracking_codes_dict)
    | 'Write to Parquet' >> beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/tracking_codes', file_name_suffix='.parquet', schema=pyarrow.schema(tracking_codes_schema))
    # | 'Tracking Codes - Print' >> beam.Map(print)
)

pipeline.run()