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

suite("test_txn_insert") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_txn_case")
        return
    }
    def txnTableName = "test_txn_insert"
    for (int i = 0; i < 3; i++) {
        sql "DROP TABLE IF EXISTS ${txnTableName}_${i} force"
        sql """
           CREATE TABLE if NOT EXISTS ${txnTableName}_${i} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           DUPLICATE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
               "replication_allocation" = "tag.location.default: 1"
           )
        """
        sql """ALTER TABLE ${txnTableName}_${i} set ("binlog.enable" = "true")"""
        assertTrue(syncer.getSourceMeta("${txnTableName}_${i}"))

        target_sql "DROP TABLE IF EXISTS ${txnTableName}_${i} force"
        target_sql """
                  CREATE TABLE if NOT EXISTS ${txnTableName}_${i} 
                  (
                      `test` INT,
                      `id` INT
                  )
                  ENGINE=OLAP
                  DUPLICATE KEY(`test`, `id`)
                  DISTRIBUTED BY HASH(id) BUCKETS 1 
                  PROPERTIES ( 
                      "replication_allocation" = "tag.location.default: 1"
                  )
              """
        assertTrue(syncer.getTargetMeta("${txnTableName}_${i}"))
    }

    def sync = { String tableName ->
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.ingestBinlog())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        target_sql " sync "
    }

    def check_row_count = { String tableName, int count ->
        def res = target_sql """SELECT count() FROM ${tableName}"""
        logger.info("target row count: ${res}")
        assertEquals(count, res[0][0])
    }

    logger.info("=== Test 1: insert values ===")
    sql """ INSERT INTO ${txnTableName}_0 VALUES (1, 0) """
    sync("${txnTableName}_0")
    def res = target_sql """SELECT * FROM ${txnTableName}_0 WHERE test=1 """
    assertEquals(res.size(), 1)

    logger.info("=== Test 2: txn insert values ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_0 VALUES (2, 0), (3, 0) """
    sql """ INSERT INTO ${txnTableName}_0 VALUES (4, 0) """
    sql """ commit """
    sync("${txnTableName}_0")
    check_row_count("${txnTableName}_0", 4)

    logger.info("=== Test 3: txn insert select into 1 table ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 4)

    logger.info("=== Test 4: txn insert select into 1 table twice ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 12)

    logger.info("=== Test 5: txn insert select into 2 tables ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_2 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    // should in one sync
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 20)
    check_row_count("${txnTableName}_2", 4)

    // partitions

    // delete and insert

    // insert and delete

    // mow tables(15, 16, 17, 18)


    // End Test
    syncer.closeBackendClients()
}