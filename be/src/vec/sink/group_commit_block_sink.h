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
#include "vtablet_sink.h"
#include "vec/core/future_block.h"

namespace doris {

class LoadBlockQueue;
// class FutureBlock;

namespace stream_load {

class GroupCommitBlockSink : public VOlapTableSink {
public:
    GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                              const std::vector<TExpr>& texprs, Status* status);

    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override;

    Status close(RuntimeState* state, Status close_status) override;
private:
    Status add_block(RuntimeState* state,
                     std::shared_ptr<vectorized::Block> block, bool eos);
    std::shared_ptr<doris::LoadBlockQueue> _load_block_queue;
    bool _first_block = true;
    std::vector<std::shared_ptr<vectorized::FutureBlock>> _future_blocks;
};

} // namespace stream_load
} // namespace doris
