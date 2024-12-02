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

suite('complex_insert_ck') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql 'drop table if exists t1_ck'
    sql 'drop table if exists t2_ck'
    sql 'drop table if exists t3_ck'

    sql '''
        create table t1_ck (
            id int,
            id1 int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id, id1)
        cluster by(id1, c1, c4, id)
        distributed by hash(id, id1) buckets 13
        properties(
            'replication_num'='1'
        );
    '''

    sql '''
        create table t2_ck (
            id int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id)
        cluster by(c1, id)
        distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    '''

    sql '''
        create table t3_ck (
            id int
        ) distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    '''

    sql '''
        INSERT INTO t1_ck VALUES
            (1, 10, 1, '1', 1.0, '2000-01-01'),
            (2, 20, 2, '2', 2.0, '2000-01-02'),
            (3, 30, 3, '3', 3.0, '2000-01-03');
    '''

    sql '''
        INSERT INTO t2_ck VALUES
            (1, 10, '10', 10.0, '2000-01-10'),
            (2, 20, '20', 20.0, '2000-01-20'),
            (3, 30, '30', 30.0, '2000-01-30'),
            (4, 4, '4', 4.0, '2000-01-04'),
            (5, 5, '5', 5.0, '2000-01-05');
    '''

    sql '''
        INSERT INTO t3_ck VALUES
            (1),
            (4),
            (5);
    '''

    sql 'insert into t1_ck(id, c1, c2, c3) select id, c1 * 2, c2, c3 from t1_ck'
    sql 'sync'
    qt_sql_1 'select * from t1_ck, t2_ck, t3_ck order by t1_ck.id, t1_ck.id1, t2_ck.id, t2_ck.c1, t3_ck.id'

    sql 'insert into t2_ck(id, c1, c2, c3) select id, c1, c2 * 2, c3 from t2_ck'
    sql 'sync'
    qt_sql_2 'select * from t1_ck, t2_ck, t3_ck order by t1_ck.id, t1_ck.id1, t2_ck.id, t2_ck.c1, t3_ck.id'

    sql 'insert into t2_ck(c1, c3) select c1 + 1, c3 + 1 from (select id, c1, c3 from t1_ck order by id, c1 limit 10) t1_ck, t3_ck'
    sql 'sync'
    qt_sql_3 'select * from t1_ck, t2_ck, t3_ck order by t1_ck.id, t1_ck.id1, t2_ck.id, t2_ck.c1, t3_ck.id'
}