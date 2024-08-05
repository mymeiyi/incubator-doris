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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("replay_wal_restart_fe") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs.add('sys_log_verbose_modules=org.apache.doris')
    options.beConfigs.add('sys_log_verbose_modules=*')
    options.beConfigs.add('enable_java_support=false')
    docker(options) {
        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]

        // group commit load error and stop replay
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.load_error")
        GetDebugPoint().enableDebugPointForAllBEs("WalTable.replay_wals.stop")

        // 1 wal need to replay
        sql 'CREATE TABLE tbl_2 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ( "replication_num" = "1", "group_commit_interval_ms"="1000")'
        sql 'SET GROUP_COMMIT = ASYNC_MODE'
        sql 'INSERT INTO tbl_2 VALUES (1, 2)'

        // do schema change
        sql 'ALTER TABLE tbl_2 ORDER BY (k2, k1)'
        for (int i = 0; i < 30; i++) {
            def jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_2' order by CreateTime desc;"
            assertTrue(jobs.size() >= 1)
            logger.info("alter job: ${jobs[0]}, state=${jobs[0].State}, equal=" + (jobs[0].State == 'RUNNING'))
            if (jobs[0].State == 'RUNNING') {
                break
            }
            sleep(1000)
        }

        // restart fe
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // check schema change status
        for (int i = 0; i < 30; i++) {
            jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_2' order by CreateTime desc;"
            assertTrue(jobs.size() >= 1)
            logger.info("alter job: ${jobs[0]}")
            if (jobs[0].State == 'FINISHED') {
                break
            }
            sleep(1000)
        }

        // start replay wal and check row count
        GetDebugPoint().disableDebugPointForAllBEs("WalTable.replay_wals.stop")
        for (int i = 0; i < 30; i++) {
            result = sql "select count(*) from tbl_2"
            logger.info("rowCount: ${result}")
            if (result[0][0] >= 1) {
                break
            }
            sleep(1000)
        }
        order_qt_select_1 'SELECT * FROM tbl_2'

        // check schema change status
        for (int i = 0; i < 30; i++) {
            jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_2' order by CreateTime desc;"
            assertTrue(jobs.size() >= 1)
            logger.info("alter job: ${jobs[0]}")
            if (jobs[0].State == 'FINISHED') {
                break
            }
            sleep(1000)
        }
    }
}
