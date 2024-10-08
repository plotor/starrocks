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

class StarRocksFormatReader {
public:
    static arrow::Result<StarRocksFormatReader*> create(int64_t tablet_id, int64_t version,
                                                        struct ArrowSchema* required_schema,
                                                        struct ArrowSchema* output_schema, std::string tablet_root_path,
                                                        std::unordered_map<std::string, std::string> options);

    static arrow::Result<StarRocksFormatReader*> create(int64_t tablet_id, int64_t version,
                                                        std::shared_ptr<arrow::Schema> required_schema,
                                                        std::shared_ptr<arrow::Schema> output_schema,
                                                        std::string tablet_root_path,
                                                        std::unordered_map<std::string, std::string> options);

    virtual ~StarRocksFormatReader() = default;

    virtual arrow::Status open() = 0;

    virtual void close() = 0;

    virtual arrow::Status get_next(struct ArrowArray* c_arrow_array) = 0;

public:
    StarRocksFormatReader(StarRocksFormatReader&&) = delete;

    StarRocksFormatReader& operator=(StarRocksFormatReader&&) = delete;

protected:
    StarRocksFormatReader() = default;
};

} // namespace starrocks::lake::format
