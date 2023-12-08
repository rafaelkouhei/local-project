import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

#Columns
catalog_col = 'sku;type;name;sku_seller;asin;markup_amazon;special_price;cost;special_price_amazon;status;visibility;brand;qty;opin;export_magento2;ean;image;amazon_price_sync;is_in_stock'

def seller_id(x):
    if x is not None and x != '':
        return x[0:x.index(' ')]
    else:
        return None

#PCollections
catalog_csv = (
    pipeline
    | 'Catalog CSV - Read from text' >> ReadFromText('export_customers.csv', skip_header_lines=1)
    | 'Catalog CSV - String to List' >> beam.Map(lambda x: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", x))
    | 'Catalog CSV - Filter List length == 19' >> beam.Filter(lambda x: len(x) == 19)
    | 'Catalog CSV - Remove double quote' >> beam.Map(lambda x: [i.replace('"', '') for i in x])
    | 'Catalog CSV - Filter empty values' >> beam.Filter(lambda x: x[1] != '')
    | 'Catalog CSV - Convert all columns to string' >> beam.Map(lambda x: [str(i) for i in x])
    | 'Catalog CSV - List to Text' >> beam.Map(lambda x: ';'.join(x))
    # | 'Catalog CSV - Create CSV file' >> WriteToText('catalog', file_name_suffix='.csv', header=catalog_col)
    | 'Catalog CSV - Print' >> beam.Map(print)
)

pipeline.run()