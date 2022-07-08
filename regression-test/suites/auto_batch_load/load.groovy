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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
import java.util.Random;

suite("sql_action", "auto_batch_load") {
    def auto_batch_load_table = "auto_batch_load_table"
    def auto_batch_load_error_table = "auto_batch_load_error_table"
    def tables = [/* auto_batch_load_table, */ auto_batch_load_error_table]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    def ROW_COUNT = 10000
    def BATCH_ROW_NUM = 500
    def COLUMN_VALUE_SIZE = 15

    for (String table in tables) {
        def i = 0
        while (i < ROW_COUNT) {
            def j = i + BATCH_ROW_NUM < ROW_COUNT ? i + BATCH_ROW_NUM : ROW_COUNT;
            def result = sql "INSERT INTO $table values" + generateRandomRows(i, j, 8, COLUMN_VALUE_SIZE)
            assertTrue(result.size() == 1)
            assertTrue(result[0].size() == 1)
            assertTrue(result[0][0] == BATCH_ROW_NUM, "Insert should update $BATCH_ROW_NUM rows")
            i = j
        }
    }

    // The sleep time must be greater than the auto commit interval(see be config 'auto_batch_load_interval_seconds')
    Thread.sleep(5000)

    // check result for auto_batch_load_table
    /* {
        def result = sql """select count(*) from $auto_batch_load_table"""
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == ROW_COUNT, "Select should count $ROW_COUNT rows for $auto_batch_load_table table")
    } */

    // check result for auto_batch_load_error_table
    {
        def result = sql """select count(*) from $auto_batch_load_error_table"""
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == 0, "Select should count 0 rows for $auto_batch_load_error_table table")
    }
}

private String generateRandomRows(long indexStart, long indexEnd, int columnNum, int columnValueSize) {
    if (columnNum <= 0) {
        return ""
    }
    String rows = ""
    for (long i = indexStart; i < indexEnd; i++) {
        if (i != indexStart) {
            rows += ","
        }
        rows += generateRandomRow(i, columnNum, columnValueSize)
    }
    return rows
}

private String generateRandomRow(long index, int columnNum, int columnValueSize) {
    String row = "(" + index
    for (int i = 1; i < columnNum; i++) {
        row += ", '" + generateRandomString(columnValueSize) + "'"
    }
    row += ")"
    return row
}

private String generateRandomString(int length) {
    if (length <= 0) {
        return ""
    }
    final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    Random random = new Random()
    StringBuilder builder = new StringBuilder(length)
    for (int i = 0; i < length; i++) {
        int index = random.nextInt(CHARACTERS.length())
        builder.append(CHARACTERS.charAt(index))
    }
    return builder.toString()
}