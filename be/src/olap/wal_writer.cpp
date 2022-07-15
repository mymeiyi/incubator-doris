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

#include "olap/wal_writer.h"

#include "olap/storage_engine.h"
#include "olap/utils.h"

namespace doris {

WalWriter::WalWriter(const std::string& file_name) : _file_name(file_name), _row_count(0) {
    timer.start();
}

WalWriter::~WalWriter() {}

Status WalWriter::init() {
    Status res = Status::OK();
    if (!(res = _file_handler.open_with_mode(_file_name, O_CREAT | O_EXCL | O_WRONLY,
                                             S_IRUSR | S_IWUSR))) {
        LOG(WARNING) << "fail to open wal file. [file_name=" << _file_name << "]";
        return res;
    }
    return Status::OK();
}

Status WalWriter::finalize() {
    Status st = _file_handler.close();
    if (!st.ok()) {
        LOG(WARNING) << "fail to close wal file. [err=" << Errno::str() << "]";
    }
    return st;
}

Status WalWriter::append_rows(const PDataRowArray& rows) {
    int64_t row_count = 0;
    size_t total_size = 0;
    for (const auto& row : rows) {
        total_size += ROW_LENGTH_SIZE + row.ByteSizeLong();
        ++row_count;
    }
    // allocate memory from heap, since stack size is limited
    // uint8_t row_binary[total_size];
    std::shared_ptr<void> binary(malloc(total_size), free);
    uint8_t* row_binary = static_cast<uint8_t*>(binary.get());
    memset(row_binary, 0, total_size);
    size_t offset = 0;
    for (const auto& row : rows) {
        unsigned long row_length = row.GetCachedSize();
        memcpy(row_binary + offset, &row_length, ROW_LENGTH_SIZE);
        offset += ROW_LENGTH_SIZE;
        memcpy(row_binary + offset, row.SerializeAsString().data(), row_length);
        offset += row_length;
    }
    // write rows
    RETURN_IF_ERROR(_file_handler.write(row_binary, total_size));
    // calculate row count
    _row_count += row_count;
    return Status::OK();
}

} // namespace doris
