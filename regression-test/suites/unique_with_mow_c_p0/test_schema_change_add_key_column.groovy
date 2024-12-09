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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_schema_change_add_key_column", "nonConcurrent") {
    def db = "regression_test_unique_with_mow_c_p0"
    def tableName = "test_schema_change_add_key_column"

    def getAlterTableState = {
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(11) NULL, 
            `k2` int(11) NULL, 
            `v3` int(11) NULL,
            `v4` int(11) NULL
        ) unique KEY(`k1`, `k2`) 
        cluster by(`v3`, `v4`) 
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """


    onFinish {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
    }

    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        file 'test_schema_change_add_key_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            // assertEquals(2500, json.NumberTotalRows)
            // assertEquals(0, json.NumberFilteredRows)
        }
    }

    // check segment num is 2
    // do schema change
    /*sql """
        ALTER TABLE ${tableName} ADD COLUMN `k3` int(11) key
    """*/

    // getAlterTableState()
    // select data
}
