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

#include <aws/core/Aws.h>
#include <glog/logging.h>

#include <filesystem>
#include <fstream>

#include "common/config.h"
#include "fs/fs_s3.h"
#include "runtime/time_types.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_define.h"
#include "util/timezone_utils.h"

namespace starrocks::lake {

static bool _starrocks_format_inited = false;
Aws::SDKOptions aws_sdk_options;

void starrocks_format_initialize(void) {
    setenv("STARROCKS_HOME", "./", 0);
    setenv("UDF_RUNTIME_DIR", "./", 0);

    if (!_starrocks_format_inited) {
        // load config file
        std::string conf_file = std::filesystem::current_path();
        conf_file += "/starrocks.conf";
        const char* config_file_path = conf_file.c_str();
        std::ifstream ifs(config_file_path);
        if (!ifs.good()) {
            config_file_path = nullptr;
        }
        if (!starrocks::config::init(config_file_path)) {
            LOG(WARNING) << "load config file " << config_file_path << " failed!";
            return;
        }

        Aws::InitAPI(aws_sdk_options);

        date::init_date_cache();

        TimezoneUtils::init_time_zones();

        LOG(INFO) << "init starrocks format module successfully";
        _starrocks_format_inited = true;
    }
}

void starrocks_format_shutdown(void) {
    if (_starrocks_format_inited) {
        Aws::ShutdownAPI(aws_sdk_options);
        LOG(INFO) << "shutdown starrocks format module successfully";
    }
}

} // namespace starrocks::lake
