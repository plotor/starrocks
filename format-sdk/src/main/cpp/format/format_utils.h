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

#include <arrow/result.h>
#include <arrow/status.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"

static inline std::unordered_map<std::string, std::string> filter_map_by_key_prefix(
        const std::unordered_map<std::string, std::string> &input, const std::string &prefix) {
    std::unordered_map<std::string, std::string> result;
    for (auto it = input.begin(); it != input.end(); it++) {
        if (it->first.compare(0, prefix.size(), prefix) == 0) {
            result[it->first] = it->second;
        }
    }
    return result;
}

// The value is in big-endian byte-order.
// If the first bit is 1, it is a negative number.
template<typename T>
static inline T big_endian_bytes_to_native_value(std::vector<uint8_t> &value) {
    T result = 0;
    uint32_t value_size = value.size();
    DCHECK(sizeof(T) >= value_size);
    if (value_size == 0) {
        return result;
    }

    const bool is_negative = static_cast<int8_t>(value[0]) < 0;
    for (int i = 0; i < value_size; i++) {
        result = result << 8;
        result = result | (value[i] & 0xFF);
    }
    if (is_negative) {
        typedef typename std::make_unsigned<T>::type UT;
        for (int i = sizeof(result) - 1; i >= value_size; i--) {
            result = result | (static_cast<UT>(0xFF) << (i * 8));
        }
    }
    return result;
}

template<typename T>
static inline std::vector<uint8_t> to_big_endian_bytes(T value) {
    std::vector<uint8_t> result;
    result.resize(sizeof(T));
    // when T is int128_t
    if constexpr (sizeof(T) > sizeof(int64_t)) {
        for (int i = 0; i < sizeof(T); i++) {
            result[i] = static_cast<int8_t>(value >> (sizeof(T) - i - 1) * 8) & 0xFF;
        }
    } else {
        for (int i = sizeof(T) - 1; i >= 0; i--) {
            result[i] = (value >> (sizeof(T) - i - 1) * 8) & 0xFF;
        }
    }

    return result;
}

template<typename T>
static inline T getIntOrDefault(std::unordered_map<std::string, std::string> &options, std::string key,
                                T default_value) {
    auto it = options.find(key);
    if (it != options.end()) {
        std::string value = it->second;
        return std::stoi(value);
    }
    return default_value;
}

static inline arrow::Status to_arrow_status(const starrocks::Status &status) {
    if (status.ok()) {
        return arrow::Status::OK();
    } else {
        return arrow::Status::Invalid(status.to_string());
    }
}

#define FORMAT_ASSIGN_OR_RETURN_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                             \
    if (UNLIKELY(!varname.ok())) {                      \
        return to_arrow_status(varname.status());       \
    }                                                   \
    lhs = std::move(varname).value();

#define FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(lhs, rexpr) \
    FORMAT_ASSIGN_OR_RETURN_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);
