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

testGroups = "p0"

jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"

// exclude groups and exclude suites is more prior than include groups and include suites.
// keep them in lexico order(add/remove cases between the sentinals and sort):
// * sort lines in vim: select lines and then type :sort
// * sort lines in vscode: https://ulfschneider.io/2023-09-01-sort-in-vscode/
excludeSuites = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "mv_contain_external_table," + // run on external pipeline
    "set_replica_status," + // not a case for cloud mode, no need to run
    "test_be_inject_publish_txn_fail," + // not a case for cloud mode, no need to run
    "test_dump_image," +
    "test_index_failure_injection," +
    "test_information_schema_external," +
    "test_profile," +
    "test_publish_timeout," +
    "test_refresh_mtmv," + // not supported yet
    "test_report_version_missing," +
    "test_set_partition_version," +
    "test_spark_load," +
    "test_index_lowercase_fault_injection," +
    "test_index_compaction_failure_injection," +
    "test_partial_update_2pc_schema_change," + // mow 2pc
    "test_query_sys_rowsets," + // rowsets sys table
    "test_unique_table_debug_data," + // disable auto compaction
    "test_insert," + // txn insert
    "test_full_compaction_run_status," +
    "test_topn_fault_injection," +
    "test_table_properties," +
    "test_default_hll," +
    "test_default_pi," +
    "test_full_compaction," +
    "test_full_compaction_by_table_id," +
    "test_create_table," +
    "txn_insert," +
    "test_update_mow," +
    "test_new_update," +
    "test_update_unique," +
    "test_partial_update_generated_column," +
    "nereids_partial_update_native_insert_stmt," +
    "partial_update," +
    "nereids_update_on_current_timestamp," +
    "update_on_current_timestamp," +
    "test_default_bitmap_empty," +
    "nereids_delete_mow_partial_update," +
    "delete_mow_partial_update," +
    "partial_update_seq_col," +
    "nereids_partial_update_native_insert_stmt_complex," +
    "regression_test_variant_delete_and_update," +
    "test_unique_table_auto_inc_partial_update_correct_stream_load," +
    "test_unique_table_auto_inc," +
    "test_unique_table_auto_inc_partial_update_correct_insert," +
    "test_update_schema_change," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

excludeDirectories = "000_the_start_sentinel_do_not_touch," + // keep this line as the first line
    "external_table_p0," + // run on external pipeline
    "cloud/multi_cluster," + // run in specific regression pipeline
    "cloud_p0/cache," +
    "workload_manager_p1," +
    "nereids_rules_p0/subquery," +
    "backup_restore," + // not a case for cloud mode, no need to run
    "cold_heat_separation," +
    "storage_medium_p0," +
    "ccr_syncer_p0," +
    "ccr_mow_syncer_p0," +
    "hdfs_vault_p2," +
    "inject_hdfs_vault_p0," +
    "unique_with_mow_p0/flexible," +
    "unique_with_mow_p0/partial_update," +
    "unique_with_mow_c_p0/partial_update," +
    "zzz_the_end_sentinel_do_not_touch" // keep this line as the last line

max_failure_num = 50

// test_routine_load
enableKafkaTest=true

// vim: tw=10086 et ts=4 sw=4:

// trino-connector catalog test config
enableTrinoConnectorTest = false

s3Source = "aliyun"
