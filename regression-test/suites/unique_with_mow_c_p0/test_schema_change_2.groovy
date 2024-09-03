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

import org.codehaus.groovy.runtime.IOGroovyMethods

// TODO: find a way to check the file content
suite("test_schema_change_2") {
    def db = "regression_test_unique_with_mow_c_p0"
    def tableName = "test_schema_change_2"

    def getAlterTableState = {
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    test {
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `c1` int(11) NULL, 
                `c2` int(11) NULL, 
                `c3` int(11) NULL
            ) unique KEY(`c1`) 
            cluster by(`c3`, `c2`) 
            DISTRIBUTED BY HASH(`c1`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "light_schema_change" = "false"
            );
        """
        exception "Unique merge-on-write table with cluster keys must enable light schema change"
    }
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int(11) NULL, 
            `c2` int(11) NULL, 
            `c3` int(11) NULL
        ) unique KEY(`c1`)
        PARTITION BY RANGE(`c1`)
        ( 
            PARTITION `p_1000` VALUES [0, 10000) 
        )
        cluster by(`c3`, `c2`)
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (11, 28, 38), (10, 29, 39) """
    qt_select_original """select * from ${tableName}"""

    /****** add value column ******/
    // after cluster key
    sql """ alter table ${tableName} ADD column c4 int(11) after c3; """
    assertTrue(getAlterTableState(), "add column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, c4) VALUES (13, 27, 36, 40), (12, 26, 37, 40) """
    qt_select_add_c4 """select * from ${tableName}"""

    // before cluster key
    sql """ alter table ${tableName} ADD column c5 int(11) after c1; """
    assertTrue(getAlterTableState(), "add column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, c4, c5) VALUES (15, 20, 34, 40, 50), (14, 20, 35, 40, 50) """
    qt_select_add_c5 """select * from ${tableName}"""

    // in the middle of cluster key
    sql """ alter table ${tableName} ADD column c6 int(11) after c2; """
    assertTrue(getAlterTableState(), "add column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, c4, c5, c6) VALUES (17, 20, 32, 40, 50, 60), (16, 20, 33, 40, 50, 60) """
    qt_select_add_c6 """select * from ${tableName}"""

    /****** add key column ******/
    sql """ alter table ${tableName} ADD column k2 int(11) key after c1; """
    assertTrue(getAlterTableState(), "add column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (19, 20, 30, 200), (18, 20, 31, 200) """
    qt_select_add_k2 """select * from ${tableName}"""

    /****** TODO add cluster key column ******/

    /****** drop value column ******/
    sql """ alter table ${tableName} drop column c4; """
    assertTrue(getAlterTableState(), "drop column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (119, 20, 30, 200), (118, 20, 31, 200) """
    qt_select_drop_c4 """select * from ${tableName}"""

    sql """ alter table ${tableName} drop column c5; """
    assertTrue(getAlterTableState(), "drop column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (117, 20, 32, 200), (116, 20, 33, 200) """
    qt_select_drop_c5 """select * from ${tableName}"""

    sql """ alter table ${tableName} drop column c6; """
    assertTrue(getAlterTableState(), "drop column should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (115, 25, 34, 200), (114, 24, 35, 200) """
    qt_select_drop_c6 """select * from ${tableName}"""

    /****** drop key column ******/
    test {
        sql """ alter table ${tableName} drop column k2; """
        exception "Can not drop key column in Unique data model table"
    }

    /****** TODO does not support drop cluster key column because the data must reorder ******/
    test {
        sql """ alter table ${tableName} drop column c3; """
        exception "Can not drop cluster key column in Unique data model table"
    }

    /****** reorder ******/
    sql """ alter table ${tableName} order by(c1, k2, c3, c2); """
    assertTrue(getAlterTableState(), "reorder should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (113, 23, 36, 200), (112, 22, 37, 200) """
    qt_select_reorder """select * from ${tableName}"""

    /****** modify key column data type ******/
    sql """ alter table ${tableName} modify column k2 BIGINT key; """
    assertTrue(getAlterTableState(), "modify should success")
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (111, 21, 38, 200), (110, 20, 39, 200) """
    qt_select_modify_k2 """select * from ${tableName}"""

    /****** TODO does not support modify cluster key column data type ******/
    test {
        sql """ alter table ${tableName} modify column c2 BIGINT; """
        exception "Can not modify cluster key column"
    }

    /****** create mv ******/
    def mv_name = "k2_c3"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
    createMV """ create materialized view ${mv_name} as select c1, c3 from ${tableName}; """
    sql """ INSERT INTO ${tableName}(c1, c2, c3, k2) VALUES (211, 21, 38, 200), (210, 20, 39, 200) """
    qt_select_create_mv_base """select * from ${tableName}"""
    qt_select_create_mv_mv """select c1, c3 from ${tableName}"""

    /****** add partition ******/

    /****** truncate table ******/

    /****** backup restore ******/

    /****** specify index, not base index ******/

    /****** one sql contain multi column changes ******/
}
