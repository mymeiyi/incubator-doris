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

import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.CyclicBarrier

suite("update") {
    // get doris-db from s3
    def dirPath = context.file.parent
    def fileName = "doris-dbgen"
    def fileUrl = "http://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/doris-dbgen"
    def filePath = Paths.get(dirPath, fileName)
    if (!Files.exists(filePath)) {
        new URL(fileUrl).withInputStream { inputStream ->
            Files.copy(inputStream, filePath)
        }
        def file = new File(dirPath + "/" + fileName)
        file.setExecutable(true)
    }

    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    String feHttpAddress = context.config.feHttpAddress
    def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

    String realDb = context.config.getDbNameByFile(context.file)
    String user = context.config.jdbcUser
    String password = context.config.jdbcPassword

    def tmp_dir = "/mnt/disk2/meiyi/tmp/update"
    def log_file = new File("${tmp_dir}/update_log.log")
    log_file.createNewFile()
    def fileWriter = new FileWriter(log_file, false)
    fileWriter.close()
    def lock1 = new ReentrantLock()

    def cmd = ""

    int key_num = 10
    int value_num = 50
    int column_num = key_num + value_num
    int rows = 200000
    def max_thread_num = 1

    def agg_table = "test_mow_partial_update_agg"
    def mow_table = "test_mow_partial_update_mow"

    def keys_schema = "test_mow_partial_update_concurrent2_keys"
    def values_schema = "test_mow_partial_update_concurrent2_values"
    def keys_config = "${tmp_dir}/keys_config.yaml"
    def values_config = "${tmp_dir}/values_config.yaml"

    def keys = "${tmp_dir}/keys"
    def keys_with_0 = "${tmp_dir}/keys_with_0"
    def values = "${tmp_dir}/values"
    def data = "${tmp_dir}/data_full"
    def data_tmp = "${tmp_dir}/data_tmp"

    int row_blocks = 100000
    int col_blocks = 10

    def gen_cols = { col, l, r ->
        def cols = ""
        for (int i = l; i < r; i++) {
            cols += "${col}${i}"
            if (i != r - 1) {
                cols += ","
            }
        }
        return cols
    }

    def gen_agg_cols = { col, l, r, agg_type ->
        def cols = ""
        for (int i = l; i < r; i++) {
            cols += "${agg_type}${col}${i})"
            if (i != r - 1) {
                cols += ","
            }
        }
        return cols
    }

    def gen_config_file = { file, name, col, num, l, r ->
        def s = "tables:\n"
        s += "  ${name}:\n"
        for (int i = 0; i < num; i++) {
            s += "    ${col}${i}:\n"
            s += "      range: {min: ${l}, max: ${r}}\n"
        }

        try {
            File f = new File(file)
            FileWriter w = new FileWriter(f)
            w.write(s)
            w.close()
        } catch (IOException e) {
            println "出现异常: ${e.message}"
        }
    }

    def lmin = -2000000
    def lmax = 2000000
    gen_config_file(keys_config, keys_schema, "k", key_num, lmin, lmax)
    gen_config_file(values_config, values_schema, "v", value_num, lmin, lmax)

    def key_cols = gen_cols("k", 0, key_num)
    def value_cols = gen_cols("v", 0, value_num)

    def create_keys_schema = { name ->
        def stmt = "DROP TABLE IF EXISTS ${name} FORCE"
        sql stmt
        stmt = "CREATE TABLE `${name}`("
        for (int i = 0; i < key_num; i++) {
            stmt += """ k${i} BIGINT NULL DEFAULT "0" """
            if (i < key_num - 1) {
                stmt += ","
            } else {
                stmt += ")"
            }
        }
        stmt += """ENGINE=OLAP DUPLICATE KEY(k0, k1)
        DISTRIBUTED BY HASH(k0,k1) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = 'true'
        );"""
        sql stmt
    }

    def create_values_schema = { name ->
        def stmt = "DROP TABLE IF EXISTS ${name} FORCE"
        sql stmt
        stmt = "CREATE TABLE `${name}`("
        for (int i = 0; i < value_num; i++) {
            stmt += """ v${i} BIGINT NULL DEFAULT "0" """
            if (i < value_num - 1) {
                stmt += ","
            } else {
                stmt += ")"
            }
        }
        stmt += """ENGINE=OLAP DUPLICATE KEY(v0,v1)
        DISTRIBUTED BY HASH(v0,v1) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = 'true'
        );"""
        sql stmt
    }

    def create_validation_table = { name ->
        def stmt = "DROP TABLE IF EXISTS ${name} FORCE"
        sql stmt
        stmt = "CREATE TABLE `${name}`("
        for (int i = 0; i < key_num; i++) {
            stmt += "k${i} BIGINT NOT NULL,"
        }
        for (int i = 0; i < value_num; i++) {
            stmt += """ v${i} BIGINT REPLACE_IF_NOT_NULL NULL DEFAULT "0" """
            if (i < value_num - 1) {
                stmt += ","
            } else {
                stmt += ")"
            }
        }
        stmt += """ENGINE=OLAP AGGREGATE KEY(${key_cols})
        DISTRIBUTED BY HASH(k0, k1) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = 'true'
        );"""
        sql stmt
    }

    def create_partial_update_table = { name ->
        def stmt = "DROP TABLE IF EXISTS ${name} FORCE"
        sql stmt
        stmt = "CREATE TABLE `${name}`("
        for (int i = 0; i < key_num; i++) {
            stmt += "k${i} BIGINT NOT NULL,"
        }
        for (int i = 0; i < value_num; i++) {
            stmt += """ v${i} BIGINT NULL DEFAULT "0" """
            if (i < value_num - 1) {
                stmt += ","
            } else {
                stmt += ")"
            }
        }
        stmt += """ENGINE=OLAP UNIQUE KEY(${key_cols})
        DISTRIBUTED BY HASH(k0,k1) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = 'true'
        );"""
        sql stmt
    }

    create_keys_schema(keys_schema)
    create_values_schema(values_schema)
    create_validation_table(agg_table)
    create_partial_update_table(mow_table)

    def gen_keys = {  ->
        cmd = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb} --table ${keys_schema} --rows ${rows} --bulk-size ${rows} --config ${keys_config} --http-port ${http_port} --max-threads 1 --null-frequency 0 --print """
        if (password) {
            cmd += " --password ${password}"
        }
        cmd += " > ${keys}"
        logger.info("command: " + cmd)
        def proc = ["bash", "-c", cmd].execute()
        proc.waitFor()

        cmd = "sed -i '/%/d' ${keys}"
        logger.info("command: " + cmd)
        proc = ["bash", "-c", cmd].execute()
        proc.waitFor()
    }

    gen_keys()

    def gen_values = { ->
        cmd = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb} --table ${values_schema} --rows ${rows} --bulk-size ${rows} --config ${values_config} --http-port ${http_port} --max-threads 1 --null-frequency 0 --print """
        if (password) {
            cmd += " --password ${password}"
        }
        cmd += " > ${values}"
        logger.info("command: " + cmd)
        def proc = ["bash", "-c", cmd].execute()
        proc.waitFor()

        cmd = "sed -i '/%/d' ${values}"
        logger.info("command: " + cmd)
        proc = ["bash", "-c", cmd].execute()
        proc.waitFor()
    }

    gen_values();

    def init_tables = { ->
        awk_cmd = """ awk -F "|" '{for (i=1; i<=${key_num}; i++) {printf("%s|", \$i);} for(i=1;i<=${value_num};i++){printf("0%s",(i==${value_num}?"\\n":"|"));}}' ${keys} > ${keys_with_0} """
        logger.info("awk command: " + awk_cmd)
        proc = ["bash", "-c", awk_cmd].execute()
        proc.waitFor()

        streamLoad {
            table agg_table
            set 'column_separator', '|'

            file keys_with_0
            time 10000 // 10s

            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
            }
        }

        streamLoad {
            table mow_table
            set 'column_separator', '|'

            file keys_with_0
            time 10000 // 10s

            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
            }
        }
    }

    init_tables()

    def gen_data = { ->
        gen_values()
        cmd = """paste -d "|" ${keys} ${values} > ${data} """
        logger.info("command: " + cmd)
        def proc = ["bash", "-c", cmd].execute()
        proc.waitFor()

        cmd = "awk 'NR<=${rows}' ${data} > ${data_tmp}"
        proc = ["bash", "-c", cmd].execute()
        proc.waitFor()

        cmd = "mv ${data_tmp} ${data}"
        proc = ["bash", "-c", cmd].execute()
        proc.waitFor()

        def rid = 1
        for (int i = 0; i < rows/row_blocks; i++, rid += row_blocks) {
            def name = "${tmp_dir}/data_${i}"
            cmd = """ awk 'NR >= ${rid} && NR < ${rid+row_blocks}' ${data} > ${name}"""
            proc = ["bash", "-c", cmd].execute()
            proc.waitFor()
        }
    }
    gen_data()

    def get_table_digest = {tableName, agg_type ->
        boolean ok = true
        def agg_cols = "${gen_agg_cols("k", 0, key_num, agg_type)},${gen_agg_cols("v", 0, value_num, agg_type)}"
        def stmt = "select ${agg_cols} from ${tableName}"
        def res
        do {
            try {
                res = sql stmt
                ok = true
            } catch (Throwable t) {
                ok = false
            }
        } while(!ok)
        logger.info("${res}")
        return res
    }

    def check_consistency_roughly = { A, B ->
        def agg_types = ["max(", "min(", "sum("]
        for (int i = 0; i < agg_types.size; i++) {
            def digestA = get_table_digest(A, agg_types[i])
            def digestB = get_table_digest(B, agg_types[i])
            if ("${digestA}" != "${digestB}") {
                logger.info("check consistency for ${A} and ${B} failed")
                logger.info("digest for ${A} with ${agg_types[i]}: ${digestA}")
                logger.info("digest for ${B} with ${agg_types[i]}: ${digestB}")
                return false
            }
        }
        return true
    }

    def gen_full_delta = { row_block, col_block ->
        assertTrue(rows % row_block == 0)
        assertTrue(value_num % col_block == 0)
        def ret = []
        for (int i = 0, rid = 0; i < rows/row_block; i++, rid += row_block) {
            for (int j = 0, cid = 0; j < value_num/col_block; j++, cid += col_block) {
                logger.info("${i}, ${cid}, ${cid+col_block}")
                ret.add([i, cid, cid+col_block])
            }
        }
        return ret
    }

    def add_columns = {
        try {
            sql """alter table ${mow_table} add column av1 int null default "99"; """
            sql "alter table ${mow_table} add column av2 int null;"
            sql "select sum(av1),sum(av2) from ${mow_table};"
        } catch (Throwable t) {
        }
        logger.info("add columns")
    }

    def delete_columns = {
        try {
            sql "alter table ${mow_table} drop column av1;"
            sql "alter table ${mow_table} drop column av2;"
        } catch (Throwable t) {
        }
        logger.info("drop columns")
    }

    def apply_delta = { rid, l, r, table_name ->
        def cur_cols = key_cols
        if (l != 0) {
            cur_cols += ",${gen_cols("z",0,l)}"
        }
        cur_cols += ",${gen_cols("v",l,r)}"
        if (r != value_num) {
            cur_cols += ",${gen_cols("z",r,value_num)}"
        }
        logger.info("rowid: " + rid + ", value:" + l + "," + r)
        def cur_file = "${tmp_dir}/data_${rid}"
        def ok = false
        def cnt = 0
        while (!ok) {
            try {
                streamLoad {
                    table table_name
                    set 'column_separator', '|'
                    set 'columns', cur_cols
                    set 'partial_columns', 'true'

                    file cur_file
                    time 10000 // 10s

                    check { result, exception, startTime, endTime ->
                        def json = parseJson(result)
                        if ("Success".equals(json.Status) && row_blocks == json.NumberTotalRows && row_blocks == json.NumberLoadedRows) {
                            ok = true
                            logger.info("rowid:${rid}, cols:${l}-${r-1}, set ok:${ok}")
                        }
                    }
                }
                ok = true
            } catch (Throwable t) {
                ok = false
            }
            if (cnt > 0) {
                logger.info("rowid:${rid}, cols:${l}-${r-1}, retry: ${cnt}, ok:${ok}")
            }
            cnt++
            sleep(500)
        }

        def cur_log = "[rid:${rid}][cols:${l}-${r-1}]:"
        def cmd1 = ""
        cmd1 = """ awk -F "|" 'NR==1 {for(i=${key_num+l+1};i<=${key_num+r};i++){printf("%s", \$i); if(i<${r+key_num}){printf("|");}}}' ${cur_file}"""
        def proc = ["bash", "-c", cmd1].execute()
        def sout = new StringBuilder(), serr = new StringBuilder()
        proc.consumeProcessOutput(sout, serr)
        proc.waitFor()
        cur_log += "${sout}"

        logger.info("cur_log: ${cur_log}")
        lock1.lock()
        log_file << cur_log
        lock1.unlock()
    }

    def stop = new AtomicBoolean()
    stop.set(false)
    ReentrantLock lock = new ReentrantLock()
    Condition condition = lock.newCondition()
    int row_num = rows / row_blocks
    int col_num = value_num / col_blocks
    int th_num = row_num * col_num
    logger.info("th_num: ${th_num}")
    def cyclicBarrier = new CyclicBarrier(th_num + 1)
    def ths = []
    def delta = gen_full_delta(row_blocks, col_blocks)
    logger.info("delta.size(): ${delta.size()}")
    for (int i = 0; i < delta.size(); i++) {
        def cur_d = delta[i]
        ths.add(Thread.startDaemon{
            def id = i
            def rid = cur_d[0]
            def l = cur_d[1]
            def r = cur_d[2]
            while (!stop.get()) {
                logger.info("sub1 ${id} arrived")
                cyclicBarrier.await()
                apply_delta(rid, l, r, mow_table)
                logger.info("sub2 ${id} arrived")
                cyclicBarrier.await()
            }
        })
    }

    def run_epoch = { ->
        gen_data()

        // apply to deltas to validation table
        streamLoad {
            table agg_table
            set 'column_separator', '|'

            file data
            time 10000 // 10s

            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
            }
        }

        cyclicBarrier.await() // trigger update

        cyclicBarrier.await() // wait for all the threads to complete the update

        // do validation
        assertTrue(check_consistency_roughly(mow_table, agg_table))
    }

    def epochs = -1
    def idx = 1
    int flag = 1

    def checkDupKey = {  ->
        def ok = false
        while (!ok) {
            try {
                def res = sql """ select ${key_cols}, count(*) a from ${mow_table} group by ${key_cols} having a > 1 """
                logger.info("dup key result:" + res)
                if (res[0]) {
                    logger.error("duplicate key")
                    assertTrue(false)
                }
                ok = true
            } catch (Throwable e) {
                ok = false
            }
            sleep(500)
        }
        sleep(5000)
    }
    Thread.startDaemon {
        checkDupKey()
    }

    while (epochs < 0 || epochs-- > 0) {
        lock1.lock()
        log_file << "==== echo ${idx} begin ====\n"
        lock1.unlock()
        if (flag == 1) {
            add_columns()
            flag = 0
        } else {
            delete_columns()
            flag = 1
        }
        run_epoch()
        lock1.lock()
        log_file << "==== echo ${idx} end ====\n"
        lock1.unlock()
        ++idx
    }
    stop.set(true)
}