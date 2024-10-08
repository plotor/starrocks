#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# This script is used to build arrow from source.
#
# Arguments:
#    $1 - Base path for logs/artifacts.
#    $2 - type of test (e.g. test or benchmark)
#    $3 - path to executable
#    $ARGN - arguments for executable
#
set -ex

BASE_DIR=`dirname "$0"`
BASE_DIR=`cd "$BASE_DIR"; pwd`

STARROCKS_HOME=${STARROCKS_HOME:-$BASE_DIR/..}

cd $STARROCKS_HOME
git submodule update --init format-sdk/src/main/cpp/arrow/

# build arrow
BUILD_DIR=$STARROCKS_HOME/format-sdk/src/main/cpp/arrow/cpp/build
if [ -d "$BUILD_DIR" ]; then
  echo "Delete build dir $BUILD_DIR"
  rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
cmake .. --preset ninja-release-basic \
      -DARROW_BUILD_STATIC=ON \
      -DARROW_JEMALLOC=OFF
cmake --build .