# abstracted from https://arrow.apache.org/docs/python/parquet.html
import os, sys
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
# type map
vtypes = {}
vtypes['INT32'] = 'int'
vtypes['INT64'] = 'int'
vtypes['BYTE_ARRAY'] = 'varchar'
vtypes['DOUBLE'] = 'float'
# get the file or directory
arg1 = sys.argv[1]
if os.path.isfile(arg1):
	print 'file::', arg1
	parquet_file = pq.ParquetFile(arg1)
	print 'metadata::', parquet_file.metadata
	print 'schema::', parquet_file.schema
	comma = ''
	print 'CREATE EXTERNAL TABLE schema_name.table_name ('
	for s in parquet_file.schema:
		# print comma, s.name, s.physical_type, vtypes[s.physical_type]
		print comma, s.name, vtypes[s.physical_type]
		comma = ','
	print ') AS COPY FROM \'',arg1,'\' PARQUET'
	exit()
print 'directory::', arg1
parquet_file = pq.ParquetDataset(arg1)
#print dir(parquet_file), vars(parquet_file)
#print len(parquet_file.pieces)
#print parquet_file.pieces[0].partition_keys
hive_cols = ''
comma = ''
if len(parquet_file.pieces[0].partition_keys) > 0:
	for pk in parquet_file.pieces[0].partition_keys:
		print pk[0], '||', pk[1]
		hive_cols = hive_cols + comma + pk[0]
		comma = ','
print 'metadata::', parquet_file.metadata
print 'schema::', parquet_file.schema
comma = ''
print 'CREATE EXTERNAL TABLE '+arg1.replace('/','')+' ('
for s in parquet_file.schema:
	# print comma, s.name, s.physical_type, vtypes[s.physical_type]
	print comma, s.name, vtypes[s.physical_type]
	comma = ','
if len(hive_cols) > 0:
	print ',', hive_cols
	hive_cols = '(hive_partition_cols=\''+hive_cols+'\')'
print ') AS COPY FROM \'',arg1,'*\' PARQUET'+hive_cols
