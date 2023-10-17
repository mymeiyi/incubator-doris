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

#include "runtime/new_group_commit_mgr.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/HeartbeatService.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include "client_cache.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/sink/group_commit_block_sink.h"

namespace doris {

class TPlan;

Status NewLoadBlockQueue::add_block(const UniqueId& load_id,
                                    std::shared_ptr<vectorized::Block> block) {
    // DCHECK(block->get_schema_version() == schema_version);
    std::unique_lock l(*_mutex);
    RETURN_IF_ERROR(_status);
    // LOG(INFO) << "sout: add block=\n" << block->dump_data(0);
    if (block->rows() > 0) {
        _block_queue.push_back(block);
        _load_ids.emplace(load_id);
    }
    _cv->notify_one();
    return Status::OK();
}

Status NewLoadBlockQueue::get_block(vectorized::Block* block, bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(*_mutex);
    if (!need_commit) {
        auto left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                         std::chrono::steady_clock::now() - _start_time)
                                         .count();
        if (left_seconds <= 0) {
            need_commit = true;
        }
    }
    while (_status.ok() && _block_queue.empty() &&
           (!need_commit || (need_commit && !_load_ids.empty()))) {
        // TODO make 10s as a config
        auto left_seconds = 10;
        if (!need_commit) {
            left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now() - _start_time)
                                        .count();
            if (left_seconds <= 0) {
                need_commit = true;
                break;
            }
        }
#if !defined(USE_BTHREAD_SCANNER)
        _cv->wait_for(l, std::chrono::seconds(left_seconds));
#else
        _cv->wait_for(l, left_seconds * 1000000);
#endif
    }
    if (!_block_queue.empty()) {
        auto& future_block = _block_queue.front();
        block->swap(*future_block.get());
        *find_block = true;
        _block_queue.pop_front();
    }
    if (_block_queue.empty()) {
        if (need_commit && _load_ids.empty()) {
            *eos = true;
        } else {
            *eos = false;
        }
    }
    return Status::OK();
}

void NewLoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(*_mutex);
    if (_load_ids.find(load_id) != _load_ids.end()) {
        _load_ids.erase(load_id);
        _cv->notify_one();
    }
}

void NewLoadBlockQueue::cancel(const Status& st) {
    DCHECK(!st.ok());
    std::unique_lock l(*_mutex);
    _status = st;
    _block_queue.clear();
}

Status NewGroupCommitTable::get_first_block_load_queue(
        int64_t table_id, int64_t base_schema_version,
        std::shared_ptr<NewLoadBlockQueue>& load_block_queue) {
    DCHECK(table_id == _table_id);
    {
        std::unique_lock l(_mutex);
        while (load_block_queue == nullptr) {
            for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
                // TODO if block schema version is less than fragment schema version, return error
                if (!it->second->need_commit && it->second->schema_version == base_schema_version) {
                    if (base_schema_version == it->second->schema_version) {
                        load_block_queue = it->second;
                        break;
                    } else if (base_schema_version < it->second->schema_version) {
                        return Status::DataQualityError("schema version not match");
                    }
                }
            }
            if (load_block_queue != nullptr) {
                break;
            }
            if (!_need_plan_fragment) {
                _need_plan_fragment = true;
                RETURN_IF_ERROR(_thread_pool->submit_func([&] { _create_group_commit_load(); }));
            }
#if !defined(USE_BTHREAD_SCANNER)
            _cv.wait_for(l, std::chrono::seconds(4));
#else
            _cv.wait_for(l, 4 * 1000000);
#endif
        }
    }
    /*if (load_block_queue == nullptr) {
        Status st = Status::OK();
        for (int i = 0; i < 3; ++i) {
            std::unique_lock l(_request_fragment_mutex);
            // check if there is a re-usefully fragment
            {
                std::unique_lock l1(_lock);
                for (auto it = _load_block_queues.begin(); it != _load_block_queues.end(); ++it) {
                    // TODO if block schema version is less than fragment schema version, return error
                    if (!it->second->need_commit) {
                        if (base_schema_version == it->second->schema_version) {
                            load_block_queue = it->second;
                            break;
                        } else if (base_schema_version < it->second->schema_version) {
                            return Status::DataQualityError("schema version not match");
                        }
                    }
                }
            }
            if (load_block_queue == nullptr) {
                st = _create_group_commit_load(table_id, load_block_queue);
                if (LIKELY(st.ok())) {
                    break;
                }
            }
        }
        RETURN_IF_ERROR(st);
        if (load_block_queue->schema_version != base_schema_version) {
            // TODO check this is the first block
            return Status::DataQualityError("schema version not match");
        }
    }*/
    return Status::OK();
}

Status NewGroupCommitTable::_create_group_commit_load() {
    {
        // TODO check in a lock
        /*if (!_need_plan_fragment) {
            return Status::OK();
        }*/
    }
    LOG(INFO) << "sout: start create group commit load, table_id=" << _table_id;
    TStreamLoadPutRequest request;
    std::stringstream ss;
    ss << "insert into " << _table_id << " select * from group_commit(\"table_id\"=\"" << _table_id
       << "\")";
    request.__set_load_sql(ss.str());
    UniqueId load_id = UniqueId::gen_uid();
    TUniqueId tload_id;
    tload_id.__set_hi(load_id.hi);
    tload_id.__set_lo(load_id.lo);
    request.__set_loadId(tload_id);
    std::string label = "group_commit_" + load_id.to_string();
    request.__set_label(label);
    request.__set_token("group_commit"); // this is a fake, fe not check it now
    request.__set_max_filter_ratio(1.0);
    request.__set_strictMode(false);
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }
    TStreamLoadPutResult result;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&result, &request](FrontendServiceConnection& client) {
                client->streamLoadPut(result, request);
            },
            10000L));
    Status st = Status::create(result.status);
    if (!st.ok()) {
        LOG(WARNING) << "create group commit load error, st=" << st.to_string();
    }
    RETURN_IF_ERROR(st);
    auto schema_version = result.base_schema_version;
    auto is_pipeline = result.__isset.pipeline_params;
    auto& params = result.params;
    auto& pipeline_params = result.pipeline_params;
    int64_t txn_id;
    TUniqueId instance_id;
    if (!is_pipeline) {
        DCHECK(params.fragment.output_sink.olap_table_sink.db_id == _db_id);
        txn_id = params.txn_conf.txn_id;
        instance_id = params.params.fragment_instance_id;
    } else {
        DCHECK(pipeline_params.fragment.output_sink.olap_table_sink.db_id == _db_id);
        txn_id = pipeline_params.txn_conf.txn_id;
        DCHECK(pipeline_params.local_params.size() == 1);
        instance_id = pipeline_params.local_params[0].fragment_instance_id;
    }
    VLOG_DEBUG << "create plan fragment, db_id=" << _db_id << ", table=" << _table_id
               << ", schema version=" << schema_version << ", label=" << label
               << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id)
               << ", is_pipeline=" << is_pipeline;
    {
        auto load_block_queue =
                std::make_shared<NewLoadBlockQueue>(instance_id, label, txn_id, schema_version);
        std::unique_lock l(_mutex);
        _load_block_queues.emplace(instance_id, load_block_queue);
        _need_plan_fragment = false;
        _cv.notify_all();
    }
    st = _exec_plan_fragment(_db_id, _table_id, label, txn_id, is_pipeline, params,
                             pipeline_params);
    if (!st.ok()) {
        _finish_group_commit_load(_db_id, _table_id, label, txn_id, instance_id, st, true, nullptr);
    }
    return st;
}

Status NewGroupCommitTable::_finish_group_commit_load(int64_t db_id, int64_t table_id,
                                                   const std::string& label, int64_t txn_id,
                                                   const TUniqueId& instance_id, Status& status,
                                                   bool prepare_failed, RuntimeState* state) {
    {
        std::lock_guard<doris::Mutex> l(_mutex);
        if (prepare_failed || !status.ok()) {
            auto it = _load_block_queues.find(instance_id);
            if (it != _load_block_queues.end()) {
                it->second->cancel(status);
            }
        }
        _load_block_queues.erase(instance_id);
        // TODO notify
    }
    Status st;
    Status result_status;
    if (status.ok()) {
        // commit txn
        TLoadTxnCommitRequest request;
        request.__set_auth_code(0); // this is a fake, fe not check it now
        request.__set_db_id(db_id);
        request.__set_table_id(table_id);
        request.__set_txnId(txn_id);
        if (state) {
            request.__set_commitInfos(state->tablet_commit_infos());
        }
        TLoadTxnCommitResult result;
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnCommit(result, request);
                },
                10000L);
        result_status = Status::create(result.status);
    } else {
        // abort txn
        TLoadTxnRollbackRequest request;
        request.__set_auth_code(0); // this is a fake, fe not check it now
        request.__set_db_id(db_id);
        request.__set_txnId(txn_id);
        request.__set_reason(status.to_string());
        TLoadTxnRollbackResult result;
        TNetworkAddress master_addr = _exec_env->master_info()->network_address;
        st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnRollback(result, request);
                },
                10000L);
        result_status = Status::create(result.status);
    }
    if (!st.ok()) {
        LOG(WARNING) << "request finish error, db_id=" << db_id << ", table_id=" << table_id
                     << ", label=" << label << ", txn_id=" << txn_id
                     << ", instance_id=" << print_id(instance_id)
                     << ", executor status=" << status.to_string()
                     << ", request commit status=" << st.to_string();
        return st;
    }
    // TODO handle execute and commit error
    std::stringstream ss;
    ss << "finish group commit, db_id=" << db_id << ", table_id=" << table_id << ", label=" << label
       << ", txn_id=" << txn_id << ", instance_id=" << print_id(instance_id);
    if (prepare_failed) {
        ss << ", prepare status=" << status.to_string();
    } else {
        ss << ", execute status=" << status.to_string();
    }
    ss << ", commit status=" << result_status.to_string();
    if (state && !(state->get_error_log_file_path().empty())) {
        ss << ", error_url=" << state->get_error_log_file_path();
    }
    LOG(INFO) << ss.str();
    return st;
}

Status NewGroupCommitTable::_exec_plan_fragment(int64_t db_id, int64_t table_id,
                                             const std::string& label, int64_t txn_id,
                                             bool is_pipeline,
                                             const TExecPlanFragmentParams& params,
                                             const TPipelineFragmentParams& pipeline_params) {
    auto finish_cb = [db_id, table_id, label, txn_id, this](RuntimeState* state, Status* status) {
        _finish_group_commit_load(db_id, table_id, label, txn_id, state->fragment_instance_id(),
                                  *status, false, state);
    };
    if (is_pipeline) {
        return _exec_env->fragment_mgr()->exec_plan_fragment(pipeline_params, finish_cb);
    } else {
        return _exec_env->fragment_mgr()->exec_plan_fragment(params, finish_cb);
    }
}

Status NewGroupCommitTable::get_load_block_queue(const TUniqueId& instance_id,
                                              std::shared_ptr<NewLoadBlockQueue>& load_block_queue) {
    std::unique_lock l(_mutex);
    auto it = _load_block_queues.find(instance_id);
    if (it == _load_block_queues.end()) {
        return Status::InternalError("group commit load instance " + print_id(instance_id) +
                                     " not found");
    }
    load_block_queue = it->second;
    return Status::OK();
}

NewGroupCommitMgr::NewGroupCommitMgr(ExecEnv* exec_env) : _exec_env(exec_env) {
    // TODO stop
    auto st = ThreadPoolBuilder("GroupCommitThreadPool")
                      .set_min_threads(config::group_commit_insert_threads)
                      .set_max_threads(config::group_commit_insert_threads)
                      .build(&_thread_pool);
    DCHECK(st.ok());
}

Status NewGroupCommitMgr::get_first_block_load_queue(
        int64_t db_id, int64_t table_id, int64_t base_schema_version,
        std::shared_ptr<NewLoadBlockQueue>& load_block_queue) {
    std::shared_ptr<NewGroupCommitTable> group_commit_table;
    {
        std::lock_guard wlock(_lock);
        if (_table_map.find(table_id) == _table_map.end()) {
            _table_map.emplace(table_id, std::make_shared<NewGroupCommitTable>(
                                                 _exec_env, _thread_pool.get(), db_id, table_id));
        }
        group_commit_table = _table_map[table_id];
    }
    return group_commit_table->get_first_block_load_queue(table_id, base_schema_version, load_block_queue);
}

Status NewGroupCommitMgr::get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                            std::shared_ptr<NewLoadBlockQueue>& load_block_queue) {
    std::shared_ptr<NewGroupCommitTable> group_commit_table;
    {
        std::lock_guard<doris::Mutex> l(_lock);
        auto it = _table_map.find(table_id);
        if (it == _table_map.end()) {
            return Status::NotFound("table_id: " + std::to_string(table_id) +
                                    ", instance_id: " + print_id(instance_id) + " dose not exist");
        }
        group_commit_table = it->second;
    }
    return group_commit_table->get_load_block_queue(instance_id, load_block_queue);
}
} // namespace doris