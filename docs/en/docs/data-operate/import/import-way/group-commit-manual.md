---
{
    "title": "Group Commit",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Group Commit

Group commit load does not introduce a new import method, but an extension of `INSERT INTO tbl VALUS(...)`、`Stream Load`、`Http Stream`.

In Doris, all methods of data loading are independent jobs which initiate a new transaction and generate a new data version. In the scenario of high-frequency writes, both transactions and compactions are under great pressure. Group commit load reduces the number of transactions and compactions by combining multiple small load tasks into one load job, and thus improve write performance.

It should be noted that the group commit is returned after the data is writed to WAL, at this time, the data is not visible for users, the default time interval is 10 seconds.

## Basic operations

If the table schema is:
```sql
CREATE TABLE `dt` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NULL,
    `score` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
```

### INSERT INTO VALUES

```sql
# Config session variable to enable the group commit, the default value is false
mysql> set enable_insert_group_commit = true;

# The retured label is start with 'group_commit', which is the label of the real load job
mysql> insert into dt values(1, 'Bob', 90), (2, 'Alice', 99);
Query OK, 2 rows affected (0.05 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# The returned label and txn_id are the same as the above, which means they are handled in on load job  
mysql> insert into dt(id, name) values(3, 'John');
Query OK, 1 row affected (0.01 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# The data is not visible
mysql> select * from dt;
Empty set (0.01 sec)

# After about 10 seconds, the data is visible
mysql> select * from dt;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Bob   |    90 |
|    2 | Alice |    99 |
|    3 | John  |  NULL |
+------+-------+-------+
3 rows in set (0.02 sec)
```

### Stream Load

If the content of `data.csv` is:
```sql
4,Amy,60
5,Ross,98
```

```sql
# Add 'group_commit:true' configuration in the http header

curl --location-trusted -u {user}:{passwd} -T data.csv -H "group_commit:true"  -H "column_separator:,"  http://{fe_host}:{http_port}/api/db/dt/_stream_load
{
    "TxnId": 7009,
    "Label": "group_commit_c84d2099208436ab_96e33fda01eddba8",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 35,
    "StreamLoadPutTimeMs": 5,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 26
}

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

### Http Stream

```sql
# Add 'group_commit:true' configuration in the http header

curl --location-trusted -u {user}:{passwd} -T data.csv  -H "group_commit:true" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://{fe_host}:{http_port}/api/_http_stream
{
    "TxnId": 7011,
    "Label": "group_commit_3b45c5750d5f15e5_703428e462e1ebb0",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 65,
    "StreamLoadPutTimeMs": 41,
    "ReadDataTimeMs": 47,
    "WriteDataTimeMs": 23
}

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

### Use `PreparedStatement`

To reduce the CPU cost of SQL parsing and query planning, we provide the `PreparedStatement` in the FE. When using `PreparedStatement`, the SQL and its plan will be cached in the session level memory cache and will be reused later on, which reduces the CPU cost of FE. The following is an example of using PreparedStatement in JDBC:

1. Setup JDBC url and enable server side prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true
```

2. Using `PreparedStatement`

```java
PreparedStatement statement = conn.prepareStatement("INSERT INTO dt VALUES (?, ?, ?)");
statement.setInt(1, 5);
statement.setString(2, "Tiger");
statement.setInt(3, 100);
int rows = statement.executeUpdate();
```

## Relevant system configuration

### Session variable

+ enable_insert_group_commit

  If this configuration is true, FE will judge whether the `INSERT INTO VALUES` can be group commit, the conditions are as follows:
  + Not a transaction insert, as `Begin`; `INSERT INTO VALUES`; `COMMIT`
  + Not specifying partition, as `INSERT INTO dt PARTITION()`
  + Not specifying label, as `INSERT INTO dt WITH LABEL {label} VALUES`
  + VALUES does not contain any expression, as `INSERT INTO dt VALUES (1 + 100)`

  The default value is false, use `SET enable_insert_group_commit = true;` command to enable it.

### BE configuration

+ group_commit_interval_ms

  The time interval of the internal group commit load job will stop and start a new internal job, the default value is 10000 milliseconds.

+ group_commit_replay_wal_dir
+ group_commit_sync_wal_batch
+ group_commit_replay_wal_retry_num
+ group_commit_replay_wal_retry_interval_seconds