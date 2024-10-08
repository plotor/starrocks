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

// project dependencies
#include "starrocks_arrow_converter.h"

// starrocks dependencies
#include "storage/chunk_helper.h"

namespace starrocks::lake::format {

    arrow::Status BaseArrowConverter::checkSchema() {
        if (!_sr_schema) {
            return arrow::Status::Invalid("StarRocks fields should not be null");
        }
        if (!_arrow_schema) {
            return arrow::Status::Invalid("Arrow fields should not be null");
        }
        if (_arrow_schema->num_fields() != _sr_schema->num_fields()) {
            return arrow::Status::Invalid("number fields not match with arrow:", _arrow_schema->num_fields(),
                                          ", sr:", _sr_schema->num_fields());
        }
        return arrow::Status::OK();
    }

    arrow::Status BaseArrowConverter::init() {
        ARROW_RETURN_NOT_OK(checkSchema());
        size_t num_fields = _arrow_schema->num_fields();
        for (int i = 0; i < num_fields; i++) {
            if (_arrow_schema->field(i)->name() != _sr_schema->field(i)->name()) {
                return arrow::Status::Invalid("field name not match: arrow field name(",
                                              _arrow_schema->field(i)->name(),
                                              ") vs sr field name(", _sr_schema->field(i)->name(), ")");
            }
            ARROW_ASSIGN_OR_RAISE(auto converter,
                                  ColumnConverter::create(_arrow_schema->field(i)->type(), _sr_schema->field(i),
                                                          _pool));
            _converters.push_back(converter);
        }
        return arrow::Status::OK();
    }

    class ChunkToRecordBatchConverterImpl : public ChunkToRecordBatchConverter {
    public:
        ChunkToRecordBatchConverterImpl(const std::shared_ptr<starrocks::Schema> sr_schema,
                                        const std::shared_ptr<arrow::Schema> arrow_schema, arrow::MemoryPool *pool)
                : ChunkToRecordBatchConverter(sr_schema, arrow_schema, pool) {}

        arrow::Result<std::shared_ptr<arrow::RecordBatch>> convert(const Chunk *chunk) override {
            if (chunk == nullptr) {
                return nullptr;
            }
            if (chunk->num_columns() != _arrow_schema->num_fields()) {
                return arrow::Status::Invalid("number fields not match");
            }

            size_t num_fields = _arrow_schema->num_fields();
            std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
            for (size_t idx = 0; idx < num_fields; ++idx) {
                ARROW_ASSIGN_OR_RAISE(arrays[idx], _converters[idx]->toArrowArray(chunk->columns()[idx]));
            }
            return arrow::RecordBatch::Make(_arrow_schema, chunk->num_rows(), std::move(arrays));
        }
    };

    class RecordBatchToChunkConverterImpl : public RecordBatchToChunkConverter {
    public:
        RecordBatchToChunkConverterImpl(const std::shared_ptr<starrocks::Schema> sr_schema,
                                        const std::shared_ptr<arrow::Schema> arrow_schema, arrow::MemoryPool *pool)
                : RecordBatchToChunkConverter(sr_schema, arrow_schema, pool) {}

        arrow::Result<std::unique_ptr<starrocks::Chunk>> convert(
                const std::shared_ptr<arrow::RecordBatch> rbatch) override {
            if (!rbatch) {
                return nullptr;
            }
            if (rbatch->num_columns() != _sr_schema->num_fields()) {
                return arrow::Status::Invalid("number fields not match");
            }

            size_t num_fields = rbatch->num_columns();
            auto chunk = starrocks::ChunkHelper::new_chunk(*_sr_schema, rbatch->num_rows());
            for (size_t idx = 0; idx < num_fields; ++idx) {
                ARROW_RETURN_NOT_OK(_converters[idx]->toSrColumn(rbatch->column(idx), chunk->columns()[idx]));
            }
            return chunk;
        }
    };

    arrow::Result<std::shared_ptr<ChunkToRecordBatchConverter>> ChunkToRecordBatchConverter::create(
            const std::shared_ptr<starrocks::Schema> sr_schema, const std::shared_ptr<arrow::Schema> arrow_schema,
            arrow::MemoryPool *pool) {
        return std::make_shared<ChunkToRecordBatchConverterImpl>(sr_schema, arrow_schema, pool);
    }

    arrow::Result<std::shared_ptr<RecordBatchToChunkConverter>> RecordBatchToChunkConverter::create(
            const std::shared_ptr<starrocks::Schema> sr_schema, const std::shared_ptr<arrow::Schema> arrow_schema,
            arrow::MemoryPool *pool) {
        return std::make_shared<RecordBatchToChunkConverterImpl>(sr_schema, arrow_schema, pool);
    }

} // namespace starrocks::lake::format