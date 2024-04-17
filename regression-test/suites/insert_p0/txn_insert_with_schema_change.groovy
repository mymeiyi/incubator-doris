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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("txn_insert_with_schema_change") {
    def table = "txn_insert_with_schema_change"
    sql " SET enable_nereids_planner = true; "
    sql " SET enable_fallback_to_original_planner = false; "

    def dbName = "regression_test_insert_p0"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")
    List<String> errors = new ArrayList<>()
    CountDownLatch insertLatch = new CountDownLatch(1)
    CountDownLatch schemaChangeLatch = new CountDownLatch(1)

    for (int j = 0; j < 3; j++) {
        def tableName = table + "_" + j
        sql """ DROP TABLE IF EXISTS $tableName force """
        sql """
            create table $tableName (
                `ID` int(11) NOT NULL,
                `NAME` varchar(100) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            duplicate KEY(`id`) 
            distributed by hash(id) buckets 1
            properties("replication_num" = "1"); 
        """
    }
    sql """ insert into ${table}_0 values(0, '0', 0) """
    sql """ insert into ${table}_1 values(1, '1', 1), (2, '2', 2) """
    sql """ insert into ${table}_2 values(3, '3', 3), (4, '4', 4), (5, '5', 5), (6, '6', 6) """

    def getAlterTableState = { job_state ->
        def retry = 0
        sql "use ${dbName};"
        while (true) {
            sleep(2000)
            def state = sql " show alter table column where tablename = '${table}_0' order by CreateTime desc limit 1"
            logger.info("alter table state: ${state}")
            if (state.size() > 0 && state[0][9] == job_state) {
                return
            }
            retry++
            if (retry >= 10) {
                break
            }
        }
        Assert.fail("alter table job state is not ${job_state} after retry 10 times")
    }

    def txnInsert = {
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement statement = conn.createStatement()) {
            statement.execute("SET enable_nereids_planner = true")
            statement.execute("SET enable_fallback_to_original_planner = false")
            statement.execute("begin")
            statement.execute("insert into ${table}_0 select * from ${table}_1;")

            schemaChangeLatch.countDown()
            insertLatch.await(2, TimeUnit.MINUTES)

            statement.execute("insert into ${table}_0(id, name, score) select * from ${table}_2;")
            statement.execute("commit")
        } catch (Exception e) {
            logger.error("txn insert failed", e)
            errors.add("txn insert failed " + e.getMessage())
        }
    }

    def schemaChange = { sql, job_state ->
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement statement = conn.createStatement()) {
            schemaChangeLatch.await(2, TimeUnit.MINUTES)
            statement.execute(sql)
            getAlterTableState(job_state)
            insertLatch.countDown()
        } catch (Exception e) {
            logger.error("schema change failed", e)
            errors.add("schema change failed " + e.getMessage())
        }
    }

    // 1. do light weight schema change: add column
    /*Thread insert_thread = new Thread(() -> txnInsert())
    Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table}_0 ADD column age int after name;", "FINISHED"))
    insert_thread.start()
    schema_change_thread.start()
    insert_thread.join()
    schema_change_thread.join()

    logger.info("errors: " + errors)
    assertEquals(0, errors.size())
    order_qt_select1 """select id, name, score from ${table}_0 """
    getAlterTableState("FINISHED")
    order_qt_select2 """select id, name, score from ${table} """*/

    // 2. do hard weight schema change: change order
    Thread insert_thread = new Thread(() -> txnInsert())
    Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table}_0 order by (id, score, name);", "WAITING_TXN"))
    insert_thread.start()
    schema_change_thread.start()
    insert_thread.join()
    schema_change_thread.join()

    logger.info("errors: " + errors)
    assertEquals(0, errors.size())
    order_qt_select1 """select id, name, score from ${table}_0 """
    getAlterTableState("FINISHED")
    order_qt_select2 """select id, name, score from ${table} """
}
