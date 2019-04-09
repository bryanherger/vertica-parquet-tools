# vertica-parquet-tools
A collection of Python and possibly other tools to help analyze and load Parquet files into Vertica.

Some notes about the scripts, and some code fragments:
import Python dataframes to SQL

Sources:
https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html
https://github.com/LocusEnergy/sqlalchemy-vertica-python

Comments: SQLAlchemy attempts to use TEXT data type, not present in Vertica, so need to cast as VARCHAR as shown using dtype dict.
Also, need to set chunksize=1 otherwise will attempt to send multiple values per INSERT which is not supported.

Installed Python 3.6.5 on CentOS 7.3 for these tests.

Command line to install dependencies:
`pip install numpy pandas psycopg2-binary pyarrow sqlalchemy sqlalchemy-vertica-python`

Quick start: generate a sample CREATE EXTERNAL TABLE AS COPY statement with:
`python create_external_table_parquet.py example.parquet`
This script will attempt to convert directory structures into partition columns.  Open an issue here if you have any trouble.

Python script to insert a DataFrame into a new table using SQLAlchemy:
```python
import sqlalchemy as sa
from sqlalchemy.types import VARCHAR
import pandas as pd
engine = sa.create_engine('vertica+vertica_python://myuser:mypassword@localhost:5433/mydb')
df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
df
df.to_sql('dfusers', con=engine, if_exists='replace', index=False, chunksize=1, dtype={"name": VARCHAR()})
engine.execute("SELECT * FROM dfusers").fetchall()
df1 = pd.DataFrame({'name' : ['User 4', 'User 5']})
df1.to_sql('dfusers', con=engine, if_exists='append', index=False, chunksize=1, dtype={"name": VARCHAR()})
engine.execute("SELECT * FROM dfusers").fetchall()
quit()
```
Pandas dataframes to Parquet files: specify column order and types and write CREATE EXTERNAL TABLE statements
(PyArrow)
```python
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
```
