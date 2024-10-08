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
#define FMT_HEADER_ONLY

#include <glog/logging.h>

//arrow dependencies
#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/result.h"
#include "arrow/type.h"

// project dependencies
#include "convert/starrocks_arrow_converter.h"
#include "format_utils.h"
#include "starrocks_format_writer.h"

// starrocks dependencies
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "starrocks_format/starrocks_lib.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/protobuf_file.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake::format {

    class StarRocksFormatWriterImpl : public StarRocksFormatWriter {
    public:
        StarRocksFormatWriterImpl(int64_t tablet_id, int64_t txn_id, const std::shared_ptr<arrow::Schema> output_schema,
                                  std::string tablet_root_path,
                                  const std::unordered_map<std::string, std::string> options)
                : _tablet_id(tablet_id),
                  _txn_id(txn_id),
                  _output_schema(std::move(output_schema)),
                  _tablet_root_path(std::move(tablet_root_path)),
                  _options(std::move(options)) {
            _provider = std::make_shared<FixedLocationProvider>(_tablet_root_path);

            _writer_type = WriterType::kHorizontal;
            auto it = _options.find("starrocks.format.writer_type");
            if (it != _options.end()) {
                std::string writer_type = it->second;
                _writer_type = WriterType(std::stoi(writer_type));
            }
            _max_rows_per_segment =
                    getIntOrDefault(_options, "starrocks.format.rows_per_segment",
                                    std::numeric_limits<uint32_t>::max());
        }

        arrow::Status open() override {
            LOG(INFO) << " Open tablet writer " << _tablet_id << " txnId: " << _txn_id
                      << " location: " << _tablet_root_path;
            if (!_tablet_writer) {
                // support the below file system options, same as hadoop aws fs options
                // fs.s3a.path.style.access
                // fs.s3a.access.key
                // fs.s3a.secret.key
                // fs.s3a.endpoint
                // fs.s3a.endpoint.region
                // fs.s3a.connection.ssl.enabled
                // fs.s3a.retry.limit
                // fs.s3a.retry.interval
                auto fs_options = filter_map_by_key_prefix(_options, "fs.");
                FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(auto fs,
                                                    FileSystem::Create(_tablet_root_path, FSOptions(fs_options)));
                // get tablet schema;
                FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(auto metadata, get_tablet_metadata(fs));
                _tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
                _tablet = std::make_unique<Tablet>(_lake_tablet_manager, _tablet_id, _provider, _tablet_schema);
                // create tablet writer
                FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(_tablet_writer,
                                                    _tablet->new_writer(_writer_type, _txn_id, _max_rows_per_segment));
                _tablet_writer->set_fs(fs);
                _tablet_writer->set_location_provider(_provider);

                auto sr_schema = std::make_shared<starrocks::Schema>(ChunkHelper::convert_schema(_tablet_schema));
                ARROW_ASSIGN_OR_RAISE(_arrow_converter, RecordBatchToChunkConverter::create(sr_schema, _output_schema,
                                                                                            arrow::default_memory_pool()));
                ARROW_RETURN_NOT_OK(_arrow_converter->init());
            }
            return to_arrow_status(_tablet_writer->open());
        }

        void close() override { _tablet_writer->close(); }

        arrow::Status write(const struct ArrowArray *c_arrow_array) override {
            ARROW_ASSIGN_OR_RAISE(auto rbatch,
                                  arrow::ImportRecordBatch(const_cast<struct ArrowArray *>(c_arrow_array),
                                                           _output_schema));
            ARROW_ASSIGN_OR_RAISE(auto chunk, _arrow_converter->convert(rbatch));
            if (chunk && chunk->num_rows() > 0) {
                return to_arrow_status(_tablet_writer->write(*chunk));
            }
            return arrow::Status::OK();
        }

        arrow::Status flush() override { return to_arrow_status(_tablet_writer->flush()); }

        arrow::Status finish() override {
            _tablet_writer->finish();
            return to_arrow_status(finish_txn_log());
        }

    private:
        Status finish_txn_log() {
            auto txn_log = std::make_shared<TxnLog>();
            txn_log->set_tablet_id(_tablet_id);
            txn_log->set_txn_id(_txn_id);
            auto op_write = txn_log->mutable_op_write();
            for (auto &f: _tablet_writer->files()) {
                if (is_segment(f.path)) {
                    op_write->mutable_rowset()->add_segments(std::move(f.path));
                    op_write->mutable_rowset()->add_segment_size(f.size.value());
                } else if (is_del(f.path)) {
                    op_write->add_dels(std::move(f.path));
                } else {
                    return Status::InternalError(fmt::format("unknown file {}", f.path));
                }
            }
            op_write->mutable_rowset()->set_num_rows(_tablet_writer->num_rows());
            op_write->mutable_rowset()->set_data_size(_tablet_writer->data_size());
            op_write->mutable_rowset()->set_overlapped(false);
            return put_txn_log(std::move(txn_log));
        }

        Status put_txn_log(const TxnLogPtr &log) {
            if (UNLIKELY(!log->has_tablet_id())) {
                return Status::InvalidArgument("txn log does not have tablet id");
            }
            if (UNLIKELY(!log->has_txn_id())) {
                return Status::InvalidArgument("txn log does not have txn id");
            }

            auto txn_log_path = _provider->txn_log_location(log->tablet_id(), log->txn_id());
            auto fs_options = filter_map_by_key_prefix(_options, "fs.");
            ASSIGN_OR_RETURN(auto fs, FileSystem::Create(txn_log_path, FSOptions(fs_options)));
            ProtobufFile file(txn_log_path, fs);
            return file.save(*log);

            return Status::OK();
        }

        StatusOr<TabletMetadataPtr> get_tablet_metadata(std::shared_ptr<FileSystem> fs) {
            std::vector<std::string> objects{};
            // TODO: construct prefix in LocationProvider
            std::string prefix = fmt::format("{:016X}_", _tablet_id);
            auto root = _provider->metadata_root_location(_tablet_id);

            auto scan_cb = [&](std::string_view name) {
                if (HasPrefixString(name, prefix)) {
                    objects.emplace_back(join_path(root, name));
                }
                return true;
            };
            RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));

            if (objects.size() == 0) {
                return Status::NotFound(fmt::format("tablet {} metadata not found", _tablet_id));
            }
            std::sort(objects.begin(), objects.end());
            auto metadata_location = objects.back();
            return _lake_tablet_manager->get_tablet_metadata(fs, metadata_location, true);
        }

    private:
        // input members
        int64_t _tablet_id;
        int64_t _txn_id;
        std::shared_ptr<arrow::Schema> _output_schema;
        std::string _tablet_root_path;
        std::unordered_map<std::string, std::string> _options;
        // other members
        std::shared_ptr<TabletSchema> _tablet_schema;
        std::unique_ptr<Tablet> _tablet;
        WriterType _writer_type;
        uint32_t _max_rows_per_segment;
        std::shared_ptr<FixedLocationProvider> _provider;
        std::unique_ptr<TabletWriter> _tablet_writer;
        std::shared_ptr<RecordBatchToChunkConverter> _arrow_converter;
    };

    arrow::Result<StarRocksFormatWriter *> StarRocksFormatWriter::create(
            int64_t tablet_id, int64_t txn_id, const struct ArrowSchema *output_arrow_schema,
            const std::string tablet_root_path, const std::unordered_map<std::string, std::string> options) {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> output_schema,
                              arrow::ImportSchema(const_cast<struct ArrowSchema *>(output_arrow_schema)));
        return create(tablet_id, txn_id, std::move(output_schema), std::move(tablet_root_path), std::move(options));
    }

    arrow::Result<StarRocksFormatWriter *> StarRocksFormatWriter::create(
            int64_t tablet_id, int64_t txn_id, const std::shared_ptr<arrow::Schema> output_schema,
            const std::string tablet_root_path, const std::unordered_map<std::string, std::string> options) {
        StarRocksFormatWriterImpl *format_writer = new StarRocksFormatWriterImpl(
                tablet_id, txn_id, std::move(output_schema), std::move(tablet_root_path), std::move(options));
        return format_writer;
    }

} // namespace starrocks::lake::format
