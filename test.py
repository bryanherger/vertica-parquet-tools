# abstracted from https://arrow.apache.org/docs/python/parquet.html
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
df = pd.DataFrame({'one': [-1, np.nan, 2.5],
	'two': ['foo', 'bar', 'baz'],
	'three': [True, False, True]},
	index=list('abc'))
print 'df::' , df
table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')
table2 = pq.read_table('example.parquet')
print 'table2::', table2.to_pandas()
parquet_file = pq.ParquetFile('example.parquet')
print 'metadata::', parquet_file.metadata
print 'schema::', parquet_file.schema
# TPCDS example
print 'store'
parquet_file = pq.ParquetFile('store/000000_0')
print 'metadata::', parquet_file.metadata
print 'schema::', parquet_file.schema
# inventory/inv_date_sk=2450815/000000_0
print 'inventory'
parquet_file = pq.ParquetFile('inventory/inv_date_sk=2450815/000000_0')
print 'metadata::', parquet_file.metadata
print 'schema::', parquet_file.schema
