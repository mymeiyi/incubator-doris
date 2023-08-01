// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("insert_group_commit_into_unique") {
    def table = "insert_group_commit_into_unique"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 10) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] == expectedRowCount) {
                break
            }
            retry++
        }
    }

    def getAlterTableState = {
        def retry = 0
        while (true) {
            sleep(2000)
            def state = sql "show alter table column where tablename = '${table}' order by CreateTime desc "
            logger.info("alter table state: ${state}")
            if (state.size()> 0 && state[0][9] == "FINISHED") {
                return true
            }
            retry++
            if (retry >= 10) {
                return false
            }
        }
        return false
    }

    try {
        // create table
        sql """ drop table if exists ${table}; """
        sql """
        CREATE TABLE `${table}` (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL default "0"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `name`)
        PARTITION BY RANGE(id)
        (
            PARTITION p1 VALUES LESS THAN ("100")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        sql """ set enable_insert_group_commit = true; """

        // 1. insert into
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        def result = sql """ insert into ${table} values (1, 'a', 10),(5, 'q', 50),(500, 'z', 500);  """
        logger.info("result: " + result)
        assertTrue(result.size() == 1, "insert into should success")
        assertTrue(result[0][0] == 2, "insert into 2 rows")
        sql """ insert into ${table}(name, id, score) values ('q', 5, 50);  """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        getRowCount(6)
        qt_sql """ select * from ${table} order by id asc; """

        // 2. insert into and delete
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id, name) values(4, 'd1');  """
        sql """ insert into ${table}(id, name) values(4, 'd1');  """
        sql """ delete from ${table} where id = 1; """
        sql """ delete from ${table} where id = 4; """
        sql """ insert into ${table} values (1, 'a', 11),(5, 'q', 51);  """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        getRowCount(5)
        // check (id, name, score): (1, 'a', 11)
        def success = false
        def retry = 0
        while (retry < 10) {
            sleep(2000)
            result = sql """ select * from ${table} where id = 1 and name = 'a'; """
            logger.info("result: " + result)
            if (result.size() > 0 && result[0][2] == 11) {
                success = true
                break
            }
            retry++
        }
        assertTrue(success)
        qt_sql """ select * from ${table} order by id asc; """

        // 3. insert into and light schema change: add column
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        sql """ insert into ${table} values (1, 'a', 12),(5, 'q', 52);  """
        sql """ alter table ${table} ADD column age int after name; """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        assertTrue(getAlterTableState(), "add column should success")
        getRowCount(6)
        qt_sql """ select * from ${table} order by id asc; """

        // 4. insert into and truncate table
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        sql """ insert into ${table} values (1, 'a', 5, 10),(5, 'q', 6, 50);  """
        sql """ truncate table ${table}; """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        getRowCount(2)
        qt_sql """ select id, name, score from ${table} order by id asc; """

        // 5. insert into and schema change: modify column order
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        sql """ insert into ${table} values (1, 'a', 5, 13),(5, 'q', 6, 53);  """
        sql """ alter table ${table} order by (id, name, score, age); """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        assertTrue(getAlterTableState(), "modify column order should success")
        getRowCount(6)
        qt_sql """ select * from ${table} order by id asc; """

        // 6. insert into and light schema change: drop column
        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        sql """ insert into ${table} values (1, 'a', 14, 6),(5, 'q', 54, 7);  """
        sql """ alter table ${table} DROP column age; """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        assertTrue(getAlterTableState(), "drop column should success")
        getRowCount(6)
        qt_sql """ select id, name, score from ${table} order by id asc; """

        // 7. insert into and into into select
        def table2 = table + "_2"
        sql """ drop table if exists ${table2}; """
        sql """
        CREATE TABLE `${table2}` (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL default "0"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        """
        sql """ insert into ${table2} values (1, 'a', 15),(5, 'q', 55);  """

        sql """ insert into ${table}(name, id) values('c', 3);  """
        sql """ insert into ${table}(id) values(4);  """
        sql """ insert into ${table} values (1, 'a', 16),(5, 'q', 56),(3, 'c', 36);  """
        sql """ insert into ${table} select * from ${table2}; """
        sql """ insert into ${table}(id, name) values(2, 'b');  """
        sql """ insert into ${table}(id) select 6; """

        getRowCount(6)
        qt_sql """ select * from ${table} order by id asc; """

        // 8. insert into with delete sign
        sql """ insert into ${table}(id, name, score, __DORIS_DELETE_SIGN__) values (1, 'a', 15, 1),(5, 'q', 55, 0); """
        getRowCount(5)
        qt_sql """ select * from ${table} order by id asc; """

        // 9. insert into with begin, insert, commit
        sql """ insert into ${table} values (7, 'a', 76);  """
        sql """ begin; """
        sql """ insert into ${table} values (7, 'a', 77);  """
        sql """ commit; """

        getRowCount(6)
        qt_sql """ select * from ${table} order by id asc; """
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
