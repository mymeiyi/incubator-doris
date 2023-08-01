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

#include "runtime/group_commit_mgr.h"

#include <gen_cpp/PaloInternalService_types.h>
#include "gen_cpp/BackendService.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/Types_types.h"
// #include "gen_cpp/internal_service.pb.h"

#include "client_cache.h"
#include "common/object_pool.h"
#include "exec/data_sink.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/scan/new_file_scan_node.h"

namespace doris {

class TPlan;
class FragmentExecState;

Status LoadInstanceInfo::add_block(std::shared_ptr<vectorized::FutureBlock> block) {
    // LOG(INFO) << "sout: LoadInstanceInfo::_add_block, instance=" << load_instance_id.to_string();
    DCHECK(block->schema_version == schema_version);
    DCHECK(block->schema_hash == schema_hash);
    std::unique_lock l(*_mutex);
    RETURN_IF_ERROR(_status);
    if (block->rows() > 0) {
        _block_queue.push_back(block);
    }
    if (block->eos) {
        _block_unique_ids.erase(block->unique_id);
    } else if (block->first) {
        _block_unique_ids.emplace(block->unique_id);
    }
    _cv->notify_one();
    return Status::OK();
}

Status LoadInstanceInfo::get_block(vectorized::Block* block, bool* find_block, bool* eos) {
    *find_block = false;
    *eos = false;
    std::unique_lock l(*_mutex);
    /*LOG(INFO) << "sout: when get block, block queue size=" << _block_queue.size()
              << ", block_unique_ids size=" << _block_unique_ids.size()
              << ", need_commit=" << need_commit << ", st=" << _status.to_string()
              << ", instance=" << load_instance_id.to_string();*/
    if (!need_commit) {
        auto left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                         std::chrono::steady_clock::now() - _start_time)
                                         .count();
        /*LOG(INFO) << "sout: left time=" << left_seconds
                  << ", instance=" << load_instance_id.to_string();*/
        if (left_seconds <= 0) {
            need_commit = true;
        }
    }
    while (_status.ok() && _block_queue.empty() &&
           (!need_commit || (need_commit && !_block_unique_ids.empty()))) {
        // make 10s as a config
        auto left_seconds = 10;
        if (!need_commit) {
            left_seconds = 10 - std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now() - _start_time)
                                        .count();
            /*LOG(INFO) << "sout: left time=" << left_seconds
                      << ", instance=" << load_instance_id.to_string();*/
            if (left_seconds <= 0) {
                need_commit = true;
                break;
            }
        }
        /*LOG(INFO) << "sout: start wait, block queue size=" << _block_queue.size()
                  << ", block_unique_ids size=" << _block_unique_ids.size()
                  << ", need_commit=" << need_commit << ", st=" << _status.to_string()
                  << ", instance=" << load_instance_id.to_string();*/
#if !defined(USE_BTHREAD_SCANNER)
        _cv->wait_for(l, std::chrono::seconds(left_seconds));
#else
        _cv->wait_for(l, left_seconds * 1000000);
#endif
    }
    if (!_block_queue.empty()) {
        LOG(INFO) << "sout: block type=" << typeid(*block).name();
        auto& future_block = _block_queue.front();
        auto* fblock = static_cast<vectorized::FutureBlock*>(block);
        fblock->swap_future_block(future_block);
        *find_block = true;
        _block_queue.pop_front();
    }
    if (_block_queue.empty()) {
        if (need_commit && _block_unique_ids.empty()) {
            *eos = true;
        } else {
            *eos = false;
        }
    }
    return Status::OK();
}

void LoadInstanceInfo::remove_block_id(const UniqueId& unique_id) {
    std::unique_lock l(*_mutex);
    if (_block_unique_ids.find(unique_id) != _block_unique_ids.end()) {
        _block_unique_ids.erase(unique_id);
        _cv->notify_one();
        // LOG(INFO) << "sout: remove block id=" << unique_id << ", instance_id=" << load_instance_id;
    }
}

void LoadInstanceInfo::cancel(const Status& st) {
    DCHECK(!st.ok());
    std::unique_lock l(*_mutex);
    _status = st;
    while (!_block_queue.empty()) {
        {
            auto& future_block = _block_queue.front();
            auto block_status = std::make_tuple<bool, Status, int64_t, int64_t>(
                    true, Status(st), future_block->rows(), 0);
            std::unique_lock<doris::Mutex> l0(*(future_block->lock));
            block_status.swap(*(future_block->block_status));
            future_block->cv->notify_all();
        }
        _block_queue.pop_front();
    }
}

Status GroupCommitTable::get_block_load_instance_info(
        int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadInstanceInfo>& load_instance_info) {
    DCHECK(table_id == _table_id);
    DCHECK(block->first == true);
    {
        std::unique_lock l(_lock);
        // LOG(INFO) << "sout: GroupCommitTable::_add_block, no related instance";
        for (auto it = load_instance_infos.begin(); it != load_instance_infos.end(); ++it) {
            // TODO if block schema version is less than fragment schema version, return error
            if (!it->second->need_commit && it->second->schema_version == block->schema_version) {
                if (block->schema_version == it->second->schema_version) {
                    load_instance_info = it->second;
                    break;
                } else if (block->schema_version < it->second->schema_version) {
                    return Status::DataQualityError("schema version not match");
                }
            }
        }
    }
    if (load_instance_info == nullptr) {
        // LOG(INFO) << "sout: load instance info is null";
        Status st = Status::OK();
        for (int i = 0; i < 3; ++i) {
            std::unique_lock l(_request_fragment_mutex);
            // check if there is a re-usefully fragment
            {
                std::unique_lock l1(_lock);
                // LOG(INFO) << "sout: check if a reuse instance, size=" << load_instance_infos.size();
                for (auto it = load_instance_infos.begin(); it != load_instance_infos.end(); ++it) {
                    // TODO if block schema version is less than fragment schema version, return error
                    if (!it->second->need_commit) {
                        if (block->schema_version == it->second->schema_version) {
                            load_instance_info = it->second;
                            /*LOG(INFO) << "sout: find a fragment to reuse, i=" << i
                                      << ", schema_version=" << load_instance_info->schema_version
                                      << ", schema_hash=" << load_instance_info->schema_hash
                                      << ", instance_id=" << load_instance_info->load_instance_id
                                      << ", block_id=" << print_id(block->unique_id);*/
                            break;
                        } else if (block->schema_version < it->second->schema_version) {
                            return Status::DataQualityError("schema version not match");
                        }
                    }
                }
            }
            if (load_instance_info == nullptr) {
                // LOG(INFO) << "sout: start create a load fragment, i=" << i;
                st = _create_group_commit_load(table_id, load_instance_info);
                /*LOG(INFO) << "sout: finish create a load fragment, i=" << i
                          << ", st=" << st.to_string();*/
                if (LIKELY(st.ok())) {
                    break;
                }
            }
        }
        RETURN_IF_ERROR(st);
        if (load_instance_info->schema_version != block->schema_version) {
            // TODO check this is the first block
            // LOG(INFO) << "sout: version does not match, return DataQualityError";
            return Status::DataQualityError("schema version not match");
        }
    }
    /*LOG(INFO) << "sout: load instance info=" << load_instance_info->load_instance_id.to_string()
              << ", block unique id=" << print_id(block->unique_id);*/
    return Status::OK();
}

Status GroupCommitTable::_create_group_commit_load(
        int64_t table_id, std::shared_ptr<LoadInstanceInfo>& load_instance_info) {
    TRequestGroupCommitFragmentRequest request;
    request.__set_db_id(_db_id);
    // request.__set_be_id(_exec_env->master_info()->backend_id);
    request.__set_table_id(table_id);
    TRequestGroupCommitFragmentResult result;
    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    // LOG(INFO) << "sout: start requestGroupCommitFragment";
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&result, &request](FrontendServiceConnection& client) {
                client->requestGroupCommitFragment(result, request);
            },
            10000L));
    Status st = Status::create(result.status);
    /*LOG(INFO) << "sout: finish requestGroupCommitFragment, st=" << st.to_string()
              << ", id=" << print_id(result.params.params.fragment_instance_id);*/
    if (!st.ok()) {
        LOG(WARNING) << "create group commit load error, st=" << st.to_string();
    }
    RETURN_IF_ERROR(st);
    auto& params = result.params;
    auto wal_id = result.wal_id;
    auto db_id = result.params.fragment.output_sink.olap_table_sink.db_id;
    auto instance_id = params.params.fragment_instance_id;
    auto schema_version = result.base_schema_version;
    auto schema_hash = result.base_schema_hash;
    VLOG_DEBUG << "create plan fragment, wal_id=" << wal_id << ", table=" << table_id
               << ", instance_id=" << print_id(instance_id) << ", schema version=" << schema_version
               << ", schema_hash=" << schema_hash;
    // LOG(INFO) << "sout: start _exe_plan_fragment";
    {
        load_instance_info =
                std::make_shared<LoadInstanceInfo>(instance_id, schema_version, schema_hash);
        std::unique_lock l(_lock);
        load_instance_infos.emplace(instance_id, load_instance_info);
        // LOG(INFO) << "sout: add a new load instance info=" << print_id(instance_id);
    }
    st = _exe_plan_fragment(db_id, table_id, wal_id, params);
    if (!st.ok()) {
        LOG(WARNING) << "execute plan fragment error, st=" << st << ", wal_id=" << wal_id
                     << ", instance_id=" << print_id(instance_id);
        std::unique_lock l(_lock);
        load_instance_infos.erase(instance_id);
        // should notify all blocks
        load_instance_info->cancel(st);
    }
    // LOG(INFO) << "sout: finish _exe_plan_fragment, st=" << st.to_string();
    return st;
}

Status GroupCommitTable::_exe_plan_fragment(int64_t db_id, int64_t table_id, int64_t wal_id,
                                            const TExecPlanFragmentParams& params) {
    auto st = _exec_env->fragment_mgr()->exec_plan_fragment(
            params, [db_id, table_id, params, this](RuntimeState* state, Status* status) {
                auto& instance_id = state->fragment_instance_id();
                {
                    std::lock_guard<doris::Mutex> l(_lock);
                    load_instance_infos.erase(instance_id);
                }
                // status = executor->status();
                /*if (!status.ok()) {
                    LOG(WARNING) << "group commit error, executor status=" << status.to_string()
                                 << ", add wal to recover, wal=" << wal_id;
                    _exec_env->wal_mgr()->add_recover_wal(
                            std::to_string(db_id), std::to_string(table_id),
                            std::vector<std::string> {std::to_string(wal_id)});
                }*/
                TFinishGroupCommitRequest request;
                request.__set_db_id(db_id);
                // request.__set_be_id(_exec_env->master_info()->backend_id);
                request.__set_table_id(table_id);
                request.__set_txn_id(params.txn_conf.txn_id);
                // request.__set_instance_id(instance_id);
                request.__set_status(status->to_thrift());
                request.__set_commit_infos(state->tablet_commit_infos());
                // request.status.__set_status_code(status.code());
                TFinishGroupCommitResult result;
                // plan this load
                TNetworkAddress master_addr = _exec_env->master_info()->network_address;
                Status st;
                st = ThriftRpcHelper::rpc<FrontendServiceClient>(
                        master_addr.hostname, master_addr.port,
                        [&result, &request](FrontendServiceConnection& client) {
                            client->finishGroupCommit(result, request);
                        },
                        10000L);
                if (!st.ok()) {
                    LOG(WARNING) << "request commit error, table_id=" << table_id
                                 << ", executor status=" << status->to_string()
                                 << ", request commit status=" << st.to_string()
                                 << ", instance_id=" << print_id(instance_id);
                    /*_exec_env->wal_mgr()->add_recover_wal(
                            std::to_string(db_id), std::to_string(table_id),
                            std::vector<std::string> {std::to_string(wal_id)});*/
                    return;
                }
                st = Status::create(result.status);
                /*TLoadTxnCommitRequest request;
                request.db_id = db_id;
                request.tbl = ctx->table;
                request.txnId = ctx->txn_id;
                request.sync = true;
                request.__set_commitInfos(executor->runtime_state()->tablet_commit_infos());
                // request.__isset.commitInfos = true;
                request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);*/
                if (!st.ok()) {
                    LOG(WARNING) << "group commit error, commit status=" << st.to_string();
                    /*_exec_env->wal_mgr()->add_recover_wal(
                            std::to_string(db_id), std::to_string(table_id),
                            std::vector<std::string> {std::to_string(wal_id)});*/
                } else if (status->ok()) {
                    /*std::string wal_path = _exec_env->wal_mgr()->wal_dir() + "/" +
                                           std::to_string(db_id) + "/" + std::to_string(table_id) +
                                           "/" + std::to_string(wal_id);
                    _exec_env->wal_mgr()->delete_wal(wal_path);*/
                }
                VLOG_DEBUG << "finish commit, table_id=" << table_id
                           << ", status=" << status->to_string() << ", st=" << st.to_string()
                           << ", instance_id=" << print_id(instance_id);
            });
    return st;
}

Status GroupCommitTable::get_load_instance_info(
        const TUniqueId& instance_id, std::shared_ptr<LoadInstanceInfo>& load_instance_info) {
    std::unique_lock l(_lock);
    auto it = load_instance_infos.find(instance_id);
    if (it == load_instance_infos.end()) {
        return Status::InternalError("instance " + print_id(instance_id) + " not found");
    }
    load_instance_info = it->second;
    return Status::OK();
}

GroupCommitMgr::GroupCommitMgr(ExecEnv* exec_env) : _exec_env(exec_env) {
    ThreadPoolBuilder("InsertIntoGroupCommitThreadPool")
            /*.set_min_threads(config::group_commit_min_thread)
            .set_max_threads(config::group_commit_max_thread)*/
            .set_min_threads(10)
            .set_max_threads(10)
            .build(&_insert_into_thread_pool);
}

GroupCommitMgr::~GroupCommitMgr() {}

Status GroupCommitMgr::group_commit_insert(int64_t table_id, const TPlan& plan,
                                           const TDescriptorTable& tdesc_tbl,
                                           const TScanRangeParams& scan_range_params,
                                           const PGroupCommitInsertRequest* request,
                                           int64_t* loaded_rows, int64_t* total_rows) {
    auto& nodes = plan.nodes;
    DCHECK(nodes.size() > 0);
    auto& plan_node = nodes.at(0);

    TUniqueId pipe_id;
    pipe_id.__set_hi(request->pipe_id().hi());
    pipe_id.__set_lo(request->pipe_id().lo());

    std::vector<std::shared_ptr<doris::vectorized::FutureBlock>> future_blocks;
    {
        std::shared_ptr<LoadInstanceInfo> load_instance_info;
        // new a stream load pipe, put it into stream load mgr, append row in thread pool
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                -1 /* total_length */, true /* use_proto */);
        std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
        ctx->pipe = pipe;
        RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(pipe_id, ctx));
        std::unique_ptr<int, std::function<void(int*)>> remove_pipe_func((int*)0x01, [&](int*) {
            if (load_instance_info != nullptr) {
                load_instance_info->remove_block_id(pipe_id);
            }
            _exec_env->new_load_stream_mgr()->remove(pipe_id);
        });
        _insert_into_thread_pool->submit_func(
                std::bind<void>(&GroupCommitMgr::_append_row, this, pipe, request));

        std::unique_ptr<RuntimeState> runtime_state = RuntimeState::create_unique();
        runtime_state->set_query_mem_tracker(std::make_shared<MemTrackerLimiter>(
                MemTrackerLimiter::Type::LOAD, fmt::format("Load#Id={}", print_id(pipe_id)), -1));
        DescriptorTbl* desc_tbl = nullptr;
        RETURN_IF_ERROR(DescriptorTbl::create(runtime_state->obj_pool(), tdesc_tbl, &desc_tbl));
        runtime_state->set_desc_tbl(desc_tbl);
        auto file_scan_node = vectorized::NewFileScanNode(runtime_state->obj_pool(), plan_node, *desc_tbl);
        std::unique_ptr<int, std::function<void(int*)>> close_scan_node_func(
                (int*)0x01, [&](int*) {
                    file_scan_node.close(runtime_state.get());
                });
        // TFileFormatType::FORMAT_PROTO, TFileType::FILE_STREAM, set _range.load_id
        RETURN_IF_ERROR(file_scan_node.init(plan_node, runtime_state.get()));
        RETURN_IF_ERROR(file_scan_node.prepare(runtime_state.get()));
        std::vector<TScanRangeParams> params_vector;
        params_vector.emplace_back(scan_range_params);
        file_scan_node.set_scan_ranges(params_vector);
        RETURN_IF_ERROR(file_scan_node.open(runtime_state.get()));

        std::unique_ptr<doris::vectorized::Block> _block =
                doris::vectorized::Block::create_unique();
        bool eof = false;
        bool first = true;
        while (!eof) {
            // LOG(INFO) << "sout: start get block, thread_id=" << std::this_thread::get_id();
            // TODO what to do if read one block error
            RETURN_IF_ERROR(file_scan_node.get_next(runtime_state.get(), _block.get(), &eof));
            /*LOG(INFO) << "sout: finish get block, thread_id=" << std::this_thread::get_id()
                      << ", rows=" << _block->rows() << ", eof=" << eof;*/
            /*if (UNLIKELY(_block->rows() == 0)) {
                continue;
            }*/
            std::shared_ptr<doris::vectorized::FutureBlock> future_block =
                    std::make_shared<doris::vectorized::FutureBlock>();
            future_block->swap(*(_block.get()));
            // TODO remove schema hash
            future_block->set_info(request->base_schema_version(), 0,
                                   pipe_id, first, eof);
            // TODO what to do if add one block error
            if (load_instance_info == nullptr) {
                RETURN_IF_ERROR(_get_block_load_instance_info(request->db_id(), table_id,
                                                              future_block, load_instance_info));
            }
            // RETURN_IF_ERROR(_add_block(table_id, future_block));
            RETURN_IF_ERROR(load_instance_info->add_block(future_block));
            if (future_block->rows() > 0) {
                future_blocks.emplace_back(future_block);
            }
            first = false;
        }
    }

    for (const auto& future_block : future_blocks) {
        std::unique_lock<doris::Mutex> l(*(future_block->lock));
        if (!std::get<0>(*(future_block->block_status))) {
            future_block->cv->wait(l);
        }
        auto st = std::get<1>(*(future_block->block_status));
        *total_rows += std::get<2>(*(future_block->block_status));
        *loaded_rows += std::get<3>(*(future_block->block_status));
    }
    // TODO
    return Status::OK();
}

Status GroupCommitMgr::_append_row(std::shared_ptr<io::StreamLoadPipe> pipe,
                                   const PGroupCommitInsertRequest* request) {
    /*LOG(INFO) << "sout: start add row num=" << request->data_size()
              << ", thread_id=" << std::this_thread::get_id();*/
    for (int i = 0; i < request->data_size(); ++i) {
        std::unique_ptr<PDataRow> row(new PDataRow());
        row->CopyFrom(request->data(i));
        // TODO append may error when pipe is cancelled
        RETURN_IF_ERROR(pipe->append(std::move(row)));
    }
    pipe->finish();
    // LOG(INFO) << "sout: add row num=" << request->data_size();
    return Status::OK();
}

Status GroupCommitMgr::_get_block_load_instance_info(
        int64_t db_id, int64_t table_id, std::shared_ptr<vectorized::FutureBlock> block,
        std::shared_ptr<LoadInstanceInfo>& load_instance_info) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard wlock(_lock);
        if (_table_map.find(table_id) == _table_map.end()) {
            _table_map.emplace(table_id,
                               std::make_shared<GroupCommitTable>(_exec_env, db_id, table_id));
        }
        group_commit_table = _table_map[table_id];
    }
    return group_commit_table->get_block_load_instance_info(table_id, block, load_instance_info);
}

Status GroupCommitMgr::get_load_instance_info(
        int64_t table_id, const TUniqueId& instance_id,
        std::shared_ptr<LoadInstanceInfo>& load_instance_info) {
    std::shared_ptr<GroupCommitTable> group_commit_table;
    {
        std::lock_guard<doris::Mutex> l(_lock);
        auto it = _table_map.find(table_id);
        if (it == _table_map.end()) {
            return Status::NotFound("table_id: " + std::to_string(table_id) +
                                    ", instance_id: " + print_id(instance_id) + " dose not exist");
        }
        group_commit_table = it->second;
    }
    return group_commit_table->get_load_instance_info(instance_id, load_instance_info);
}
} // namespace doris