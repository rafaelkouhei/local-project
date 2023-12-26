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
table_schema = [
    ('date', pyarrow.date32()),
    ('source_id', pyarrow.string()),
    ('external_reference', pyarrow.string()),
    ('record_type', pyarrow.string()),
    ('description', pyarrow.string()),
    ('net_credit_amount', pyarrow.float32()),
    ('net_debit_amount', pyarrow.float32()),
    ('gross_amount', pyarrow.float32()),
    ('seller_amount', pyarrow.float32()),
    ('mp_fee_amount', pyarrow.float32()),
    ('financing_fee_amount', pyarrow.float32()),
    ('shipping_fee_amount', pyarrow.float32()),
    ('taxes_amount', pyarrow.float32()),
    ('coupon_amount', pyarrow.float32()),
    ('installments', pyarrow.int32()),
    ('payment_method', pyarrow.string()),
    ('tax_detail', pyarrow.string()),
    ('tax_amount_telco', pyarrow.float32()),
    ('transaction_approval_date', pyarrow.date32()),
    ('pos_id', pyarrow.string()),
    ('pos_name', pyarrow.string()),
    ('external_pos_id', pyarrow.string()),
    ('store_id', pyarrow.string()),
    ('store_name', pyarrow.string()),
    ('external_store_id', pyarrow.string()),
    ('currency', pyarrow.string()),
    ('taxes_disaggregated', pyarrow.string()),
    ('shipping_id', pyarrow.string()),
    ('shipment_mode', pyarrow.string()),
    ('order_id', pyarrow.string()),
    ('pack_id', pyarrow.string()),
    ('metadata', pyarrow.string()),
    ('effective_coupon_amount', pyarrow.float32()),
    ('poi_id', pyarrow.string()),
    ('card_initial_number', pyarrow.string()),
    ('operation_tags', pyarrow.string()),
    ('item_id', pyarrow.string()),
    ('poi_bank_name', pyarrow.string()),
    ('poi_wallet_name', pyarrow.string())]

table_dict = ['date', 'source_id', 'external_reference', 'record_type', 'description', 'net_credit_amount', 'net_debit_amount', 'gross_amount', 'seller_amount', 'mp_fee_amount', 'financing_fee_amount', 'shipping_fee_amount', 'taxes_amount', 'coupon_amount', 'installments', 'payment_method', 'tax_detail', 'tax_amount_telco', 'transaction_approval_date', 'pos_id', 'pos_name', 'external_pos_id', 'store_id', 'store_name', 'external_store_id', 'currency', 'taxes_disaggregated', 'shipping_id', 'shipment_mode', 'order_id', 'pack_id', 'metadata', 'effective_coupon_amount', 'poi_id', 'card_initial_number', 'operation_tags', 'item_id', 'poi_bank_name', 'poi_wallet_name']

#Functions
def convert_float(x):
    if x == '':
        return None
    else:
        return float(re.sub(r'[^0-9|.]', '', x))

def convert_int(x):
    if x == '':
        return None
    else:
        return int(re.sub(r'[^0-9]', '', x))

def parse_datetime(x):
    if x != '':
        return datetime.strptime(x[0:19], '%Y-%m-%dT%H:%M:%S')
    else:
        return None

def transform_columns(x):
    date, source_id, external_reference, record_type, description, net_credit_amount, net_debit_amount, gross_amount, seller_amount, mp_fee_amount, financing_fee_amount, shipping_fee_amount, taxes_amount, coupon_amount, installments, payment_method, tax_detail, tax_amount_telco, transaction_approval_date, pos_id, pos_name, external_pos_id, store_id, store_name, external_store_id, currency, taxes_disaggregated, shipping_id, shipment_mode, order_id, pack_id, metadata, effective_coupon_amount, poi_id, card_initial_number, operation_tags, item_id, poi_bank_name, poi_wallet_name = x
    return parse_datetime(date), source_id, external_reference, record_type, description, convert_float(net_credit_amount), convert_float(net_debit_amount), convert_float(gross_amount), convert_float(seller_amount), convert_float(mp_fee_amount), convert_float(financing_fee_amount), convert_float(shipping_fee_amount), convert_float(taxes_amount), convert_float(coupon_amount), convert_int(installments), payment_method, tax_detail, convert_float(tax_amount_telco), parse_datetime(transaction_approval_date), pos_id, pos_name, external_pos_id, store_id, store_name, external_store_id, currency, taxes_disaggregated, shipping_id, shipment_mode, order_id, pack_id, metadata, convert_float(effective_coupon_amount), poi_id, card_initial_number, operation_tags, item_id, poi_bank_name, poi_wallet_name

#PCollections
drop_item = (
    pipeline
    | 'Read from text' >> ReadFromText('/Users/rafaelsumiya/Downloads/octo_payout.csv', skip_header_lines=1)
    | 'Text to List' >> beam.Map(lambda x: x.split(';'))
    | beam.Filter(lambda x: x[0] != '')
    | 'Tranform columns' >> beam.Map(transform_columns)
    | 'Transform to Dictionary' >> beam.Map(lambda y, x: dict(zip(x, y)), table_dict)
    | 'Create Parquet file' >> beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/octo_payout', file_name_suffix='.parquet', schema=pyarrow.schema(table_schema))
    | 'Dropship Item - Print' >> beam.Map(print)
)

pipeline.run()