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

import com.mysql.cj.jdbc.StatementImpl
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("sub_txn_duplicate") {
    // case 1
    def table_txn = "sub_txn_dup"
    def table_normal = "sub_txn_dup_n"
    for (def i in 1..3) {
        for (def prefix: [table_normal, table_txn]) {
            sql """ drop table if exists ${prefix}_${i} """
            sql """
            CREATE TABLE ${prefix}_${i} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            """
            if (i == 1) {
                sql """ insert into ${prefix}_${i} values(1, "a", 1), (2, "b", 2); """
            }
        }
    }
    sql """ set enable_query_in_transaction_load = true """
    for (def prefix: [table_normal, table_txn]) {
        if (prefix == table_txn) {
            sql """ begin; """
        }

        sql """ insert into ${prefix}_3 select * from ${prefix}_2; """
        order_qt_select_1 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_3 select * from ${prefix}_1; """
        order_qt_select_2 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_3; """
        order_qt_select_3 """ select * from ${prefix}_2; """

        sql """ insert into ${prefix}_1 select * from ${prefix}_2; """
        order_qt_select_4 """ select * from ${prefix}_1; """

        if (prefix == table_txn) {
            sql """ commit; """
        }
        order_qt_select_51 """ select * from ${prefix}_1; """
        order_qt_select_52 """ select * from ${prefix}_2; """
        order_qt_select_53 """ select * from ${prefix}_3; """
    }
}
