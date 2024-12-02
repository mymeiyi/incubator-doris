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

suite("test_select") {
    sql """ DROP TABLE IF EXISTS test_select """

    sql """ 
        create table test_select (
            pk int,
            col_char_255__undef_signed char(255)  null  ,
            col_char_255__undef_signed_not_null char(255)  not null  ,
            col_char_100__undef_signed char(100)  null  ,
            col_char_100__undef_signed_not_null char(100)  not null  ,
            col_varchar_255__undef_signed varchar(255)  null  ,
            col_varchar_255__undef_signed_not_null varchar(255)  not null  ,
            col_varchar_1000__undef_signed varchar(1000)  null  ,
            col_varchar_1000__undef_signed_not_null varchar(1000)  not null  ,
            col_varchar_1001__undef_signed varchar(1001)  null  ,
            col_varchar_1001__undef_signed_not_null varchar(1001)  not null  ,
            col_string_undef_signed string  null  ,
            col_string_undef_signed_not_null string  not null
        ) engine=olap
        UNIQUE KEY(pk)
        cluster by(col_varchar_255__undef_signed_not_null, col_varchar_1000__undef_signed_not_null)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test_select(pk,col_char_255__undef_signed,col_char_255__undef_signed_not_null,col_char_100__undef_signed,col_char_100__undef_signed_not_null,col_varchar_255__undef_signed,col_varchar_255__undef_signed_not_null,col_varchar_1000__undef_signed,col_varchar_1000__undef_signed_not_null,col_varchar_1001__undef_signed,col_varchar_1001__undef_signed_not_null,col_string_undef_signed,col_string_undef_signed_not_null) values (0,'300.343','9999-12-31 23:59:59','that','b','1','a','would','1','20240803','2024-07-01','20240803','0'),(1,'r','x','or','1',null,'0','1','9999-12-31 23:59:59','r','the','1','s'),(2,'n','20240803','20240803','9999-12-31 23:59:59','2024-07-01','she','t','i','1','here','0','u'),(3,'but','then','s','2024-08-03 13:08:30','0','can''t','that','2024-08-03 13:08:30','1','1','x','2024-08-03 13:08:30');
    """

    order_qt_sql """ 
        select col_varchar_255__undef_signed_not_null  from test_select where col_char_255__undef_signed is not null  ORDER BY col_varchar_255__undef_signed_not_null LIMIT 2 ;
    """
}
