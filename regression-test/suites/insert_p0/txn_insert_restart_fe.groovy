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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite("txn_insert_restart_fe") {
    def get_observer_fe_url = {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        if (fes.size() > 1) {
            for (def fe : fes) {
                if (fe.IsMaster == "false") {
                    return "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                }
            }
        }
        return null
    }

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.enableDebugPoints()
    options.feConfigs.add('publish_wait_time_second=-1')
    docker(options) {
        // ---------- test restart fe ----------
        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]

        sql 'CREATE TABLE tbl_1 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'INSERT INTO tbl_1 VALUES (1, 11)'
        order_qt_select_1 'SELECT * FROM tbl_1'

        sql 'CREATE TABLE tbl_2 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'INSERT INTO tbl_2 VALUES (2, 11)'
        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_2 'SELECT * FROM tbl_2'

        // stop publish, insert succ, txn is commit but not visible
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_3 'SELECT * FROM tbl_2'

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_4 'SELECT * FROM tbl_2'

        // select from observer
        def observer_fe_url = get_observer_fe_url()
        if (observer_fe_url != null) {
            logger.info("observer url: $observer_fe_url")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = observer_fe_url) {
                order_qt_select_observer """ select * from ${dbName}.tbl_2 """
            }
        }

        result = sql_return_maparray 'SHOW PROC "/transactions"'
        logger.info("show txn result: ${result}")
        def runningTxn = result.find { it.DbName.indexOf(dbName) >= 0 }.RunningTransactionNum as int
        assertEquals(2, runningTxn)

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // should publish visible
        order_qt_select_5 'SELECT * FROM tbl_2'

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_6 'SELECT * FROM tbl_2'

        if (observer_fe_url != null) {
            logger.info("observer url: $observer_fe_url")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = observer_fe_url) {
                order_qt_select_observer_2 """ select * from ${dbName}.tbl_2 """
            }
        }

        // ---------- test schema change and restart fe ----------
        // stop publish, insert succ, txn is commit but not visible
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])
        sql 'CREATE TABLE tbl_3 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'CREATE TABLE tbl_4 (k1 INT, k2 INT, v INT SUM) AGGREGATE KEY (k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 10'
        sql 'CREATE TABLE tbl_5 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        for (int i : [2, 3, 4, 5]) {
            sql 'begin'
            if (i != 4) {
                sql """ INSERT INTO tbl_${i} SELECT * FROM tbl_1 """
                sql """ INSERT INTO tbl_${i} SELECT * FROM tbl_1 """
            } else {
                sql """ INSERT INTO tbl_${i} SELECT k1, k2, 0 FROM tbl_1 """
                sql """ INSERT INTO tbl_${i} SELECT k1, k2, 1 FROM tbl_1 """
            }
            try {
                sql 'commit'
            } catch (Exception e) {
                logger.info("commit error: " + e.getMessage())
                assertTrue(e.getMessage().contains("transaction commit successfully, BUT data will be visible later"))
            }
            order_qt_select_10 """ SELECT * FROM tbl_${i} """
        }

        result = sql_return_maparray 'SHOW PROC "/transactions"'
        runningTxn = result.find { it.DbName.indexOf(dbName) >= 0 }.RunningTransactionNum as int
        assertEquals(4, runningTxn)

        sql "ALTER TABLE tbl_2 ADD COLUMN k3 INT DEFAULT '-1'"
        sql 'CREATE MATERIALIZED VIEW tbl_3_mv AS SELECT k1, k1 + k2 FROM tbl_3'
        sql 'ALTER TABLE tbl_4 ADD ROLLUP tbl_3_r1(k1, v)'
        sql 'ALTER TABLE tbl_5 ORDER BY (k2, k1)'

        sleep(5000)
        def jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_2'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName = 'tbl_3'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE ROLLUP WHERE TableName = 'tbl_4'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_5'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // should publish visible
        order_qt_select_12 'SELECT * FROM tbl_2'
        order_qt_select_13 'SELECT * FROM tbl_3'
        order_qt_select_14 'SELECT * FROM tbl_4'
        order_qt_select_15 'SELECT * FROM tbl_5'

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_2'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName = 'tbl_3'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE ROLLUP WHERE TableName = 'tbl_4'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_5'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)
    }
}
