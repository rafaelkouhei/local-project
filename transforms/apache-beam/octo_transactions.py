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
    ('date_created', pyarrow.date32()),
    ('date_approved', pyarrow.date32()),
    ('date_released', pyarrow.date32()),
    ('counterpart_name', pyarrow.string()),
    ('counterpart_nickname', pyarrow.string()),
    ('counterpart_email', pyarrow.string()),
    ('counterpart_phone_number', pyarrow.string()),
    ('buyer_document', pyarrow.string()),
    ('item_id', pyarrow.string()),
    ('reason', pyarrow.string()),
    ('external_reference', pyarrow.string()),
    ('seller_custom_field', pyarrow.string()),
    ('operation_id', pyarrow.string()),
    ('status', pyarrow.string()),
    ('status_detail', pyarrow.string()),
    ('operation_type', pyarrow.string()),
    ('transaction_amount', pyarrow.float32()),
    ('mercadopago_fee', pyarrow.float32()),
    ('marketplace_fee', pyarrow.float32()),
    ('shipping_cost', pyarrow.float32()),
    ('coupon_fee', pyarrow.float32()),
    ('net_received_amount', pyarrow.float32()),
    ('installments', pyarrow.int32()),
    ('payment_type', pyarrow.string()),
    ('amount_refunded', pyarrow.float32()),
    ('refund_operator', pyarrow.string()),
    ('claim_id', pyarrow.string()),
    ('chargeback_id', pyarrow.string()),
    ('marketplace', pyarrow.string()),
    ('order_id', pyarrow.string()),
    ('merchant_order_id', pyarrow.string()),
    ('campaign_id', pyarrow.string()),
    ('campaign_name', pyarrow.string()),
    ('activity_url', pyarrow.string()),
    ('id', pyarrow.string()),
    ('shipment_status', pyarrow.string()),
    ('buyer_address', pyarrow.string()),
    ('tracking_number', pyarrow.string()),
    ('operator_name', pyarrow.string()),
    ('store_id', pyarrow.string()),
    ('pos_id', pyarrow.string()),
    ('external_id', pyarrow.string()),
    ('financing_fee', pyarrow.float32())]

table_dict = ['date_created', 'date_approved', 'date_released', 'counterpart_name', 'counterpart_nickname', 'counterpart_email', 'counterpart_phone_number', 'buyer_document', 'item_id', 'reason', 'external_reference', 'seller_custom_field', 'operation_id', 'status', 'status_detail', 'operation_type', 'transaction_amount', 'mercadopago_fee', 'marketplace_fee', 'shipping_cost', 'coupon_fee', 'net_received_amount', 'installments', 'payment_type', 'amount_refunded', 'refund_operator', 'claim_id', 'chargeback_id', 'marketplace', 'order_id', 'merchant_order_id', 'campaign_id', 'campaign_name', 'activity_url', 'id', 'shipment_status', 'buyer_address', 'tracking_number', 'operator_name', 'store_id', 'pos_id', 'external_id', 'financing_fee']

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
        return datetime.strptime(x, '%d/%m/%Y %H:%M:%S')
    else:
        return None

def transform_columns(x):
    date_created, date_approved, date_released, counterpart_name, counterpart_nickname, counterpart_email, counterpart_phone_number, buyer_document, item_id, reason, external_reference, seller_custom_field, operation_id, status, status_detail, operation_type, transaction_amount, mercadopago_fee, marketplace_fee, shipping_cost, coupon_fee, net_received_amount, installments, payment_type, amount_refunded, refund_operator, claim_id, chargeback_id, marketplace, order_id, merchant_order_id, campaign_id, campaign_name, activity_url, id, shipment_status, buyer_address, tracking_number, operator_name, store_id, pos_id, external_id, financing_fee = x
    return parse_datetime(date_created), parse_datetime(date_approved), parse_datetime(date_released), counterpart_name, counterpart_nickname, counterpart_email, counterpart_phone_number, buyer_document, item_id, reason, external_reference, seller_custom_field, operation_id, status, status_detail, operation_type, convert_float(transaction_amount), convert_float(mercadopago_fee), convert_float(marketplace_fee), convert_float(shipping_cost), convert_float(coupon_fee), convert_float(net_received_amount), convert_int(installments), payment_type, convert_float(amount_refunded), refund_operator, claim_id, chargeback_id, marketplace, order_id, merchant_order_id, campaign_id, campaign_name, activity_url, id, shipment_status, buyer_address, tracking_number, operator_name, store_id, pos_id, external_id, convert_float(financing_fee)

#PCollections
drop_item = (
    pipeline
    | 'Read from text' >> ReadFromText('/Users/rafaelsumiya/Downloads/octo_transactions.csv', skip_header_lines=1)
    | 'Text to List' >> beam.Map(lambda x: x.split(';'))
    | 'Tranform columns' >> beam.Map(transform_columns)
    | 'Transform to Dictionary' >> beam.Map(lambda y, x: dict(zip(x, y)), table_dict)
    | 'Create Parquet file' >> beam.io.WriteToParquet('/Users/rafaelsumiya/Downloads/octo_transactions', file_name_suffix='.parquet', schema=pyarrow.schema(table_schema))
    # | 'Dropship Item - Print' >> beam.Map(print)
)

pipeline.run()