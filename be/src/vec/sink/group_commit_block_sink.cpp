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

#include "vec/sink/group_commit_block_sink.h"

#include "runtime/group_commit_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/core/future_block.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"
#include "vec/sink/vtablet_sink.h"

namespace doris {

namespace stream_load {

GroupCommitBlockSink::GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                           const std::vector<TExpr>& texprs, Status* status)
        : VOlapTableSink(pool, row_desc, texprs, status) {}

Status GroupCommitBlockSink::send(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    LOG(INFO) << "sout: call GroupCommitBlockSink::send";
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status status = Status::OK();
    auto rows = input_block->rows();
    if (UNLIKELY(rows == 0)) {
        return status;
    }
    std::shared_ptr<vectorized::Block> block;
    bool has_filtered_rows = false;
    RETURN_IF_ERROR(VOlapTableSink::validate_and_convert_block(state, input_block, eos, block,
                                                               has_filtered_rows));
    LOG(INFO) << "sout: after convert block=" << block->dump_data(0);
    // add block into block queue
    return add_block(state, block, eos);
    // return Status::OK();
}

Status GroupCommitBlockSink::close(RuntimeState* state, Status close_status) {
    VOlapTableSink::close(state, close_status);
    std::shared_ptr<vectorized::Block> block = std::make_shared<vectorized::Block>();
    LOG(INFO) << "sout: add a eos block";
    return add_block(state, block, true);
}

Status GroupCommitBlockSink::add_block(RuntimeState* state,
                                       std::shared_ptr<vectorized::Block> block, bool eos) {
    auto cloneBlock = block->clone_without_columns();
    auto res_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
    for (int i = 0; i < block->rows(); ++i) {
        res_block.add_row(block.get(), i);
    }
    std::shared_ptr<doris::vectorized::FutureBlock> future_block =
            std::make_shared<doris::vectorized::FutureBlock>();

    future_block->swap(*(block.get()));
    int64_t schema_version = 0;
    int64_t db_id = 12230;
    int64_t table_id = 32043; // 12241;
    TUniqueId load_id;
    load_id.__set_hi(load_id.hi);
    load_id.__set_lo(load_id.lo);
    future_block->set_info(schema_version, load_id, _first_block, eos);
    _first_block = false;
    // TODO what to do if add one block error
    if (_load_block_queue == nullptr) {
        RETURN_IF_ERROR(state->exec_env()->group_commit_mgr()->get_first_block_load_queue(
                db_id, table_id, future_block, _load_block_queue));
        /*ctx->label = _load_block_queue->label;
        ctx->txn_id = _load_block_queue->txn_id;*/
        state->set_import_label(_load_block_queue->label);
    }
    LOG(INFO) << "sout: add block to queue=\n" << future_block->dump_data(0);
    _future_blocks.emplace_back(future_block);
    return _load_block_queue->add_block(future_block);
}

} // namespace stream_load
} // namespace doris
