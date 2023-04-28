# Orders Testing

Testing the impact of Small Files across various Filesystems (HDFS and Ozone) and the various file formats (text, orc, parquet).

We'll also test Ozone bucket configurations for default and [fso (Prefix based File System Optimized)](https://ozone.apache.org/docs/1.3.0/feature/prefixfso.html) buckets.

## Running the Tests

*Parameters*
- DB - Prefix of the database
- FILE_TYPE - File Format for Table (TEXTFILE, ORC, or PARQUET)
- DB_EXT_LOC - Location to store the external tables.  IE: hdfs/ofs/s3
- ORDER_SRC_LOC - Location of the JSON source data created for text

### From Beeline*

_ORC on HDFS_

`hive -f orders.sql --hivevar DB=orders --hivevar FILE_TYPE=ORC --hivevar DB_EXT_LOC=/warehouse/tablespace/external/hive/orders_orc.db --hivevar ORDER_SRC_LOC=/user/dstreev/datasets/orders`

_ORC on OFS (non-fso)_

`hive -f orders.sql --hivevar DB=orders --hivevar FILE_TYPE=ORC --hivevar DB_EXT_LOC=ofs://warehouse/external/hive/orders_orc.db --hivevar ORDER_SRC_LOC=/user/dstreev/datasets/orders`

_Query Results_

`grep 'QUERY  |\|10 rows selected\|^           2.*warehouse' bl_*.out`

| Set\|| START\|| END\|rows affected\|rows selected\|Run DAG\|TOTAL_LAUNCHED_TASKS\|BYTES_READ\|BYTES_WRITTEN\|_OPS\|_OP_\|CREATED_FILES\|SUCCEEDED_TASKS\|CPU_MILLISECONDS\|VIRTUAL_MEMORY_BYTES\|| SET\|| Set\|| hive.

### Get the lines need from output
'| \(Set\|START\|END\|SET\|hive.\)\|\(FILE_B\|HDFS_\|OFS_\|CREATED_FILES\|_TASKS\|MEMORY_BYTES\|CPU_\|[0-9]\{3\} rows affected\|:      Map\|:  Reducer\|Run DAG\)'

`grep -n '| \(Set\|START\|END\|SET\|hive.\)\|\(FILE_B\|HDFS_\|OFS_\|CREATED_FILES\|_TASKS\|MEMORY_BYTES\|CPU_\|[0-9]\{3\} rows affected\|:      Map\|:  Reducer\|Run DAG\)' *.out > ~/temp/small-base.txt`


## Extracting the run type from the file name:

`bl_SL(.*)_TT(.*)_S(true|false)_(.*)_[0-9]{4}-\d\d-\d\d_\d\d-\d\d-\d\d\.out\:\d*\:`
