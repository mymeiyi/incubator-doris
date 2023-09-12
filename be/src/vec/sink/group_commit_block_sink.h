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

#pragma once
// #include "vtablet_sink.h"
#include "exec/data_sink.h"
// #include "vec/core/future_block.h"
#include "vec/exprs/vexpr_fwd.h"
// #include "vec/sink/vtablet_sink.h"

namespace doris {

// class LoadBlockQueue;
// class FutureBlock;
class OlapTableSchemaParam;
//class MemTracker;

namespace stream_load {

class OlapTableBlockConvertor;

class GroupCommitBlockSink : public DataSink {
public:
    GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                         const std::vector<TExpr>& texprs, Status* status);

    ~GroupCommitBlockSink() override;

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override;

    Status close(RuntimeState* state, Status close_status) override;
private:
    Status validate_and_convert_block(RuntimeState* state, vectorized::Block* input_block, bool eos,
                                      std::shared_ptr<vectorized::Block>& block,
                                      bool& has_filtered_rows);

    // ObjectPool* _pool;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<OlapTableBlockConvertor> _block_convertor;
    RuntimeState* _state = nullptr;
    // std::shared_ptr<MemTracker> _mem_tracker;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::shared_ptr<OlapTableSchemaParam> _schema;
};

} // namespace stream_load
} // namespace doris
