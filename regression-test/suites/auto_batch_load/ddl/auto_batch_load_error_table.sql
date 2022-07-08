CREATE TABLE IF NOT EXISTS auto_batch_load_error_table (
    col1 bigint,
    col2 varchar(10),
    col3 varchar(10),
    col4 varchar(10),
    col5 varchar(10),
    col6 varchar(10),
    col7 varchar(10),
    col8 varchar(10)
)
DUPLICATE KEY(col1, col2)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES (
  "replication_num" = "1",
  "auto_batch_load"="true"
)
