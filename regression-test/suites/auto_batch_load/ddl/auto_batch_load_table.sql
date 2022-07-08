CREATE TABLE IF NOT EXISTS auto_batch_load_table (
    col1 bigint,
    col2 varchar(20),
    col3 varchar(20),
    col4 varchar(20),
    col5 varchar(20),
    col6 varchar(20),
    col7 varchar(20),
    col8 varchar(20)
)
DUPLICATE KEY(col1, col2)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES (
  "replication_num" = "1",
  "auto_batch_load"="true"
)
