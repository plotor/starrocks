// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/type.h>

namespace starrocks::lake::format {

    class StarRocksFormatWriter {
    public:
        static arrow::Result<StarRocksFormatWriter *> create(int64_t tablet_id, int64_t txn_id,
                                                             const struct ArrowSchema *output_schema,
                                                             std::string tablet_root_path,
                                                             const std::unordered_map<std::string, std::string> options);

        static arrow::Result<StarRocksFormatWriter *> create(int64_t tablet_id, int64_t txn_id,
                                                             const std::shared_ptr<arrow::Schema> output_schema,
                                                             std::string tablet_root_path,
                                                             const std::unordered_map<std::string, std::string> options);

        virtual ~StarRocksFormatWriter() = default;

        virtual arrow::Status open() = 0;

        virtual void close() = 0;

        virtual arrow::Status write(const struct ArrowArray *c_arrow_array) = 0;

        virtual arrow::Status flush() = 0;

        virtual arrow::Status finish() = 0;

    protected:
        StarRocksFormatWriter() = default;

        StarRocksFormatWriter(StarRocksFormatWriter &&) = delete;

        StarRocksFormatWriter &operator=(StarRocksFormatWriter &&) = delete;
    };

} // namespace starrocks::lake::format