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

#include "runtime/auto_batch_load_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <memory>
#include <sstream>

#include "client_cache.h"
#include "common/object_pool.h"
#include "olap/storage_engine.h"
#include "olap/wal_reader.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "util/file_utils.h"
#include "util/path_util.h"
#include "util/url_coding.h"

namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

class RuntimeProfile;

AutoBatchLoadTable::AutoBatchLoadTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id), _wal_id(UnixMillis()) {
    std::string path = _exec_env->storage_engine()->auto_batch_load_dir();
    path = path_util::join_path_segments(path, std::to_string(_db_id));
    path = path_util::join_path_segments(path, std::to_string(_table_id));
    // create auto batch load table directory if it does not exist
    if (!FileUtils::check_exist(path)) {
        Status st = FileUtils::create_dir(path);
        CHECK(st.ok()) << st.to_string();
    }
    _table_load_dir = path;
}

AutoBatchLoadTable::~AutoBatchLoadTable() {}

Status AutoBatchLoadTable::auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                                           int64_t& txn_id) {
    std::lock_guard<std::mutex> lock(_lock);
    // 1. create wal and begin txn if needed
    if (!_begin) {
        // create wal writer
        _wal_writer = std::make_shared<WalWriter>(_wal_path(_next_wal_id()));
        RETURN_IF_ERROR(_wal_writer->init());
        // begin a txn if needed
        bool label_already_exists = false;
        RETURN_IF_ERROR(_begin_auto_batch_load(_wal_id, _label, _fragment_instance_id, _txn_id,
                                               label_already_exists));
        _begin = true;
    }
    // 2. get pip
    auto pipe = _exec_env->fragment_mgr()->get_pipe(_fragment_instance_id);
    if (pipe == nullptr) {
        return Status::InternalError("pip is null");
    }
    // 3. append to wal
    RETURN_IF_ERROR(_wal_writer->append_rows(request->data()));
    // 4. write row to pip
    for (int i = 0; i < request->data_size(); ++i) {
        PDataRow* row = new PDataRow();
        row->CopyFrom(request->data(i));
        RETURN_IF_ERROR(pipe->append_and_flush(reinterpret_cast<char*>(&row), sizeof(row),
                                               sizeof(row) + row->ByteSizeLong()));
    }
    label = _label;
    txn_id = _txn_id;
    return Status::OK();
}

bool AutoBatchLoadTable::need_commit() {
    std::lock_guard<std::mutex> lock(_lock);
    return _wal_writer != nullptr ? _need_commit() : false;
}

Status AutoBatchLoadTable::commit() {
    std::lock_guard<std::mutex> lock(_lock);
    if (_wal_writer == nullptr || !_need_commit()) {
        return Status::Cancelled("auto batch load does not need commit");
    }
    auto pipe = _exec_env->fragment_mgr()->get_pipe(_fragment_instance_id);
    if (pipe == nullptr) {
        return Status::InternalError("pip is null");
    }
    RETURN_IF_ERROR(_commit_auto_batch_load(pipe));
    _begin = false;
    return Status::OK();
}

Status AutoBatchLoadTable::recovery_wal(const int64_t& wal_id, const std::string& wal_path) {
    Status st;
    // 0. if wal length is 0, delete wal and return
    auto wal_reader = WalReader(wal_path);
    wal_reader.init();
    if (wal_reader.file_length() == 0) {
        LOG(INFO) << "Wal: " << wal_path << " length is 0";
        st = FileUtils::remove(wal_path);
        return Status::OK();
    }
    // 1. begin txn
    std::string label;
    TUniqueId fragment_instance_id;
    int64_t txn_id;
    // check if this label is used and abort the previous txn
    bool label_already_exists = false;
    st = _begin_auto_batch_load(wal_id, label, fragment_instance_id, txn_id, label_already_exists);
    if (!st.ok()) {
        if (label_already_exists) {
            // abort the previous txn and begin the new txn
            std::string reason = "recovery wal";
            RETURN_IF_ERROR(_abort_txn(label, reason));
            RETURN_IF_ERROR(_begin_auto_batch_load(wal_id, label, fragment_instance_id, txn_id, label_already_exists));
        } else {
            LOG(ERROR) << "begin auto batch load error when recovery wal: " << st.to_string();
            return st;
        }
    }
    auto pipe = _exec_env->fragment_mgr()->get_pipe(fragment_instance_id);
    if (pipe == nullptr) {
        return Status::InternalError("pip is null");
    }
    // 2. read rows from wal_reader
    while (true) {
        doris::PDataRow prow;
        st = wal_reader.read_row(prow);
        if (st.ok()) {
            PDataRow* row = new PDataRow();
            row->CopyFrom(prow);
            // 3. write row to stream load pip
            RETURN_IF_ERROR(pipe->append_and_flush(reinterpret_cast<char*>(&row), sizeof(row),
                                                   sizeof(row) + row->ByteSizeLong()));
        } else if (st.is_end_of_file()) {
            break;
        } else {
            LOG(WARNING) << "read wal file: " << wal_path << ", error, " << st.to_string();
            return st;
        }
    }
    // TODO handle error
    // 4. finish pip and commit txn
    RETURN_IF_ERROR(pipe->finish());
    // 5. wait for tnx is commit or visible
    st = _wait_txn_success(label, txn_id);
    // 6. delete wal if success
    if (st.ok()) {
        st = FileUtils::remove(wal_path);
    }
    return st;
}

int64_t AutoBatchLoadTable::_next_wal_id() {
    ++_wal_id;
    return _wal_id;
}

std::string AutoBatchLoadTable::_wal_path(int64_t wal_id) {
    return path_util::join_path_segments(_table_load_dir, std::to_string(wal_id));
}

Status AutoBatchLoadTable::_begin_auto_batch_load(const int64_t& wal_id, std::string& label,
                                                  TUniqueId& fragment_instance_id, int64_t& txn_id,
                                                  bool& label_already_exists) {
    Status status;
    TBeginAutoBatchLoadRequest request;
    label = "auto_batch_load_" + std::to_string(_table_id) + "_" + std::to_string(wal_id);
    request.__set_db_id(_db_id);
    request.__set_table_id(_table_id);
    request.__set_label(label);
    TBeginAutoBatchLoadResult result;
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    try {
        client->beginAutoBatchLoad(result, request);
    } catch (TTransportException& e) {
        // reopen the client
        Status master_status = client.reopen(config::thrift_rpc_timeout_ms);
        if (!master_status.ok()) {
            LOG(WARNING) << "Reopen to get frontend client failed, with address:"
                         << _exec_env->master_info()->network_address.hostname << ":"
                         << _exec_env->master_info()->network_address.port;
            return Status::InternalError(
                    "Reopen to get frontend client failed, with address:" +
                    _exec_env->master_info()->network_address.hostname + ":" +
                    std::to_string(_exec_env->master_info()->network_address.port));
        }
        LOG(WARNING) << "begin auto batch load failed, retry!";
        client->beginAutoBatchLoad(result, request);
    }
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed begin auto batch load"
                     << ", status code=" << result.status.status_code
                     << ", error=" << result.status.error_msgs;
        if (result.status.status_code == TStatusCode::LABEL_ALREADY_EXISTS) {
            label_already_exists = true;
            return Status::InternalError(
                    "failed auto batch load begin because label already exists");
        }
    }
    fragment_instance_id = result.fragment_instance_id;
    txn_id = result.txn_id;
    return status;
}

Status AutoBatchLoadTable::_abort_txn(std::string& label, std::string& reason) {
    Status status = Status::OK();
    TAbortAutoBatchLoadRequest request;
    request.__set_db_id(_db_id);
    request.__set_label(label);
    request.__set_reason(reason);
    TAbortAutoBatchLoadResult result;
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    try {
        client->abortAutoBatchLoad(result, request);
    } catch (TTransportException& e) {
        // reopen the client
        Status master_status = client.reopen(config::thrift_rpc_timeout_ms);
        if (!master_status.ok()) {
            return Status::InternalError(
                    "Reopen to get frontend client failed, with address:" +
                    _exec_env->master_info()->network_address.hostname + ":" +
                    std::to_string(_exec_env->master_info()->network_address.port));
        }
        client->abortAutoBatchLoad(result, request);
    }
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed abort txn with label=" << label
                     << ", status code=" << result.status.status_code
                     << ", error=" << result.status.error_msgs;
        return Status::InternalError("failed abort txn");
    }
    return status;
}

bool AutoBatchLoadTable::_need_commit() {
    return _wal_writer->row_count() >= config::auto_batch_load_row_count ||
           _wal_writer->file_length() >= AUTO_LOAD_BATCH_SIZE_BYTES ||
           _wal_writer->elapsed_time() / NANOS_PER_SEC >=
                   config::check_auto_compaction_interval_seconds;
}

Status AutoBatchLoadTable::_commit_auto_batch_load(std::shared_ptr<StreamLoadPipe> pipe) {
    // TODO error handle
    // 1. finish pip and commit
    Status st = pipe->finish();
    // 2. wait for tnx is commit or visible
    st = _wait_txn_success(_label, _txn_id);
    // 3. close wal
    _wal_writer->finalize();
    // 4. delete wal if success
    st = FileUtils::remove(_wal_writer->file_name());
    _wal_writer.reset();
    return st;
}

Status AutoBatchLoadTable::_wait_txn_success(std::string& label, int64_t txn_id) {
    Status status = Status::OK();
    TWaitingTxnStatusRequest request;
    request.__set_db_id(_db_id);
    request.__set_label(label);
    request.__set_txn_id(txn_id);
    TWaitingTxnStatusResult result;
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    try {
        client->waitingTxnStatus(result, request);
    } catch (TTransportException& e) {
        // reopen the client
        Status master_status = client.reopen(config::thrift_rpc_timeout_ms);
        if (!master_status.ok()) {
            return Status::InternalError(
                    "Reopen to get frontend client failed, with address:" +
                    _exec_env->master_info()->network_address.hostname + ":" +
                    std::to_string(_exec_env->master_info()->network_address.port));
        }
        client->waitingTxnStatus(result, request);
    }
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed get txn status"
                  << ", status code=" << result.status.status_code
                  << ", error=" << result.status.error_msgs;
        return Status::InternalError("failed get txn status");
    }
    auto txn_status = result.txn_status;
    if (txn_status == TTransactionStatus::COMMITTED || txn_status == TTransactionStatus::VISIBLE) {
        return Status::OK();
    } else if (txn_status == TTransactionStatus::PREPARE ||
               txn_status == TTransactionStatus::PRECOMMITTED) {
        // TODO sleep and retry
        return Status::InternalError("txn state is: " + to_string(txn_status) +
                                     ", for label: " + label);
    } else if (txn_status == TTransactionStatus::ABORTED ||
               txn_status == TTransactionStatus::UNKNOWN) {
        LOG(WARNING) << "Commit txn error, label: " << label << ", txn_state: " << txn_status
                     << ", status: " << status.to_string() << ", msg: " << result.status.error_msgs;
        return Status::InternalError("txn state is: " + to_string(txn_status) +
                                     ", for label: " + label);
    } else {
        return Status::InternalError("Unknown txn state: " + to_string(txn_status) +
                                     ", for label: " + label);
    }
}

} // namespace doris
