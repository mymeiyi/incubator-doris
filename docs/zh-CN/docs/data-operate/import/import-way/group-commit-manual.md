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

攒批写入没有引入一种新的导入方式，而是对`INSERT INTO tbl VALUS(...)`、`Stream Load`、`Http Stream`的扩展。

在 Doris 中，所有的数据写入都是一个独立的导入作业，发起一个新的事务，产生一个新的数据版本。在高频写入的场景下，对transaction和compaction都产生了较大的压力。攒批写通过把多个小的写入合成一个写入作业，减少了transaction和compaction的次数，缓解了系统内部的压力，提高了写入的性能。

需要注意的是，攒批写入后的数据不能立刻读出，默认为10秒后可以读出。

## 基本操作

假如表的结构为：
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
# 配置session变量开启攒批,默认为false
mysql> set enable_insert_group_commit = true;

# 这里返回的label是group_commit开头的，是真正消费数据的导入关联的label，可以区分出是否攒批了
mysql> insert into dt values(1, 'Bob', 90), (2, 'Alice', 99);
Query OK, 2 rows affected (0.05 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# 可以看出这个label, txn_id和上一个相同，说明是攒到了同一个导入任务中
mysql> insert into dt(id, name) values(3, 'John');
Query OK, 1 row affected (0.01 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# 不能立刻查询到
mysql> select * from dt;
Empty set (0.01 sec)

# 10秒后可以查询到
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

假如`data.csv`的内容为：
```sql
4,Amy,60
5,Ross,98
```

```sql
# 导入时在header中增加"group_commit:true"配置

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

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

### Http Stream

```sql
# 导入时在header中增加"group_commit:true"配置

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

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

### 使用`PreparedStatement`

为了减少 SQL 解析和生成规划的开销， 我们在 FE 端支持了 MySQL 协议的`PreparedStatement`特性。当使用`PreparedStatement`时，SQL 和其导入规划将被缓存到 Session 级别的内存缓存中，后续的导入直接使用缓存对象，降低了 FE 的 CPU 压力。下面是在 JDBC 中使用 PreparedStatement 的例子：

1. 设置 JDBC url 并在 Server 端开启 prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true
```

2. 使用 `PreparedStatement`

```java
// use `?` for placement holders
PreparedStatement readStatement = conn.prepareStatement("INSERT INTO dt VALUES (?, ?, ?)");
...
readStatement.setInt(1234);
ResultSet resultSet = readStatement.executeQuery();
...
readStatement.setInt(1235);
resultSet = readStatement.executeQuery();
...
```

## 相关系统配置

### Session变量

+ enable_insert_group_commit

  当该参数设置为 true 时，会判断用户发起的`INSERT INTO VALUES`语句是否符合攒批的条件，如果符合，该语句的执行会进入到攒批写入中。主要的判断逻辑包括：
  + 不是事务写入，即`Begin`; `INSERT INTO VALUES`; `COMMIT`方式
  + 不指定partition，即`INSERT INTO dt PARTITION()`等指定partition的语句
  + 不指定label，即`INSERT INTO dt WITH LABEL {label} VALUES`
  + VALUES中不能包含表达式，即`INSERT INTO dt VALUES (1 + 100)`

  默认为 false。可通过 `SET enable_insert_group_commit = true;` 来设置。

### BE 配置

+ group_commit_interval_ms

  攒批写入开启多久后结束，默认为10000，即10秒。

+ group_commit_replay_wal_dir
+ group_commit_sync_wal_batch
+ group_commit_replay_wal_retry_num
+ group_commit_replay_wal_retry_interval_seconds