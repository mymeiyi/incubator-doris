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

suite("txn_insert_publish_timeout", "nonConcurrent") {
    def table = "txn_insert_publish_timeout"

    sql " SET enable_nereids_planner = true; "
    sql " SET enable_fallback_to_original_planner = false; "

    for (int j = 0; j < 3; j++) {
        def tableName = table + "_" + j
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            create table $tableName (
                k1 int, 
                k2 double,
                k3 varchar(100),
                k4 array<int>,
                k5 array<boolean>
            ) distributed by hash(k1) buckets 1
            properties("replication_num" = "1"); 
        """
    }
    sql """insert into ${table}_1 values(1, 2.2, "abc", [], []), (2, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """
    sql """insert into ${table}_2 values(3, 2.2, "abc", [], []), (4, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """

    // insert into select
    def backendId_to_params = get_be_param("pending_data_expire_time_sec")
    try {
        // test be report tablet and expire txns and fe handle it
        set_be_param.call("pending_data_expire_time_sec", "1")
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')

        sql """ begin; """
        sql """ insert into ${table}_0 select * from ${table}_1; """
        sql """ insert into ${table}_0 select * from ${table}_2; """
        sql """ insert into ${table}_0 select * from ${table}_1; """
        sql """ insert into ${table}_0 select * from ${table}_2; """
        sql """ commit; """

        def result = sql "SELECT COUNT(*) FROM ${table}_0"
        rowCount = result[0][0]
        assertEquals(0, rowCount)

        sleep(10000)
    } finally {
        set_original_be_param("pending_data_expire_time_sec", backendId_to_params)
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')

        def rowCount = 0
        for (int i = 0; i < 30; i++) {
            def result = sql "SELECT COUNT(*) FROM ${table}_0"
            rowCount = result[0][0]
            if (rowCount == 12) {
                return
            }
            sleep(2000)
        }
        assertEquals(12, rowCount)
    }

}
