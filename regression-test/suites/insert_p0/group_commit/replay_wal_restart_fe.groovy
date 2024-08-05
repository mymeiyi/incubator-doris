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
    // options.feConfigs.add('publish_wait_time_second=-1')
    // options.feConfigs.add('commit_timeout_second=10')
    options.feConfigs.add('sys_log_verbose_modules=org.apache.doris')
    options.beConfigs.add('sys_log_verbose_modules=*')
    options.beConfigs.add('enable_java_support=false')
    docker(options) {
        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.load_error")
        GetDebugPoint().enableDebugPointForAllFEs("FrontendServiceImpl.loadTxnRollback.error")

        sql 'CREATE TABLE tbl_2 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ( "replication_num" = "1")'
        sql 'SET GROUP_COMMIT = ASYNC_MODE'
        sql 'INSERT INTO tbl_2 VALUES (1, 2)'

        // make group commit load error and replay error(there is 1 wal)

        // do schema change
        sql 'ALTER TABLE tbl_2 ORDER BY (k2, k1)'
        // schema change wait for wal


        // stop be
        cluster.stopBackends()

        // restart fe
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // check schema change status

        // start be
        cluster.startBackends()
        sleep(10000)

        // wal replay should success
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
            {
                def row_count = sql "select count(*) from ${dbName}.tbl_2"
                logger.info("rowCount: ${row_count}")
                return row_count[0][0] == 1
            }
        )

        order_qt_select_1 'SELECT * FROM tbl_2'
    }
}
