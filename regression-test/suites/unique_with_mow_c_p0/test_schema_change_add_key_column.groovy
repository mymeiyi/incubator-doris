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
            time 100
        }
        return true
    }

    def getTabletStatus = {
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        logger.info("tablets: ${tablets}")
        assertEquals(1, tablets.size())
        String compactionUrl = ""
        for (Map<String, String> tablet : tablets) {
            compactionUrl = tablet["CompactionStatus"]
        }
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        assertEquals(2, tabletJson.rowsets.size())
        def rowset1 = tabletJson.rowsets.get(1)
        logger.info("rowset1: ${rowset1}")
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

    // batch_size is 4164 in csv_reader.cpp
    // _batch_size is 8192 in vtablet_writer.cpp
    def backendId_to_params = get_be_param("doris_scanner_row_bytes")
    onFinish {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
        set_original_be_param("doris_scanner_row_bytes", backendId_to_params)
    }
    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
    set_be_param.call("doris_scanner_row_bytes", "1")

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
            assertEquals(8192, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    // check generate 3 segments
    getTabletStatus()

    // do schema change
    sql """ ALTER TABLE ${tableName} ADD COLUMN `k3` int(11) key """
    getAlterTableState()
    // check generate 1 segments
    getTabletStatus()

    // select data
    def result = sql """ select `k1`, `k2`, count(*) a from ${tableName} group by `k1`, `k2` having a > 1; """
    logger.info("result: ${result}")
    assertEquals(0, result.size())
    qt_sql """ select * from ${tableName} where `k1` = 12345; """
}
