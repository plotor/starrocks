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

#include <glog/logging.h>
#include <jni.h>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/status.h>

#include "format/starrocks_format_reader.h"
#include "format/starrocks_format_writer.h"
#include "jni_utils.h"
#include "starrocks_format/starrocks_lib.h"

namespace starrocks::lake::format {

#ifdef __cplusplus
    extern "C" {
#endif

    static jint jniVersion = JNI_VERSION_1_8;

    jclass kJniExceptionClass;
    jclass kRuntimeExceptionClass;

    jmethodID kJniExceptionConstructor;

    jint JNI_OnLoad(JavaVM *vm, void *reserved) {
        JNIEnv *env;
        if (vm->GetEnv(reinterpret_cast<void **>(&env), jniVersion) != JNI_OK) {
            return JNI_ERR;
        }

        kRuntimeExceptionClass = find_class(env, "Ljava/lang/RuntimeException;");
        kJniExceptionClass = find_class(env, "Lcom/starrocks/format/jni/JniException;");

        kJniExceptionConstructor = get_method_id(env, kJniExceptionClass, "<init>", "(ILjava/lang/String;)V");

#ifdef DEBUG
        FLAGS_logtostderr = 1;
#endif
        // logging
        google::InitGoogleLogging("starrocks_format");

        LOG(INFO) << "Jni load successful!";
        starrocks_format_initialize();
        return jniVersion;
    }

    void JNI_OnUnload(JavaVM *vm, void *reserved) {
        JNIEnv *env;

        vm->GetEnv(reinterpret_cast<void **>(&env), jniVersion);
        env->DeleteGlobalRef(kRuntimeExceptionClass);
        env->DeleteGlobalRef(kJniExceptionClass);

        LOG(INFO) << "Jni unload successful!";
    }

    JNIEXPORT void JNICALL Java_com_starrocks_format_JniWrapper_releaseWriter(JNIEnv *env, jobject jobj,
                                                                              jlong writerAddress) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(writerAddress);
        LOG(INFO) << "Release writer: " << tablet_writer;
        if (tablet_writer != nullptr) {
            delete tablet_writer;
        }
    }

    JNIEXPORT void JNICALL Java_com_starrocks_format_JniWrapper_releaseReader(JNIEnv *env, jobject jobj,
                                                                              jlong chunkAddress) {
        StarRocksFormatReader *reader = reinterpret_cast<StarRocksFormatReader *>(chunkAddress);
        LOG(INFO) << "Release reader: " << reader;
        if (reader != nullptr) {
            delete reader;
        }
    }

    /* writer functions */

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_createNativeWriter(JNIEnv *env, jobject jobj,
                                                                                         jlong jtablet_id,
                                                                                         jlong jtxn_id,
                                                                                         jlong jschema,
                                                                                         jstring jtable_root_path,
                                                                                         jobject joptions) {
        int64_t tablet_id = jtablet_id;
        int64_t txn_id = jtxn_id;
        // get schema
        if (jschema == 0) {
            LOG(INFO) << "output_schema should not be null";
            env->ThrowNew(kRuntimeExceptionClass, "output_schema should not be null");
            return 0;
        }
        std::string table_root_path = jstring_to_cstring(env, jtable_root_path);
        std::unordered_map<std::string, std::string> options = jhashmap_to_cmap(env, joptions);

        auto &&result = StarRocksFormatWriter::create(tablet_id, txn_id,
                                                      reinterpret_cast<struct ArrowSchema *>(jschema),
                                                      std::move(table_root_path), std::move(options));
        if (!result.ok()) {
            LOG(INFO) << "Create tablet writer failed! " << result.status();
            env->ThrowNew(kRuntimeExceptionClass, result.status().message().c_str());
            return 0;
        }
        StarRocksFormatWriter *format_writer = std::move(result).ValueUnsafe();
        return reinterpret_cast<int64_t>(format_writer);
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_destroyNativeWriter(JNIEnv *env, jobject jobj,
                                                                                          jlong handler) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, { delete tablet_writer; });
        return 0;
    }

    JNIEXPORT jlong JNICALL
    Java_com_starrocks_format_StarRocksWriter_nativeOpen(JNIEnv *env, jobject jobj, jlong handler) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
            arrow::Status st = tablet_writer->open();
            if (!st.ok()) {
                env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
            }
        });
        return 0;
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeClose(JNIEnv *env, jobject jobj,
                                                                                  jlong handler) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, { tablet_writer->close(); });
        return 0;
    }

    JNIEXPORT jlong JNICALL
    Java_com_starrocks_format_StarRocksWriter_nativeWrite(JNIEnv *env, jobject jobj, jlong handler,
                                                          jlong jArrowArray) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
            const struct ArrowArray *c_array_import = reinterpret_cast<struct ArrowArray *>(jArrowArray);
            if (c_array_import != nullptr) {
                arrow::Status st = tablet_writer->write(c_array_import);
                if (!st.ok()) {
                    LOG(INFO) << "Write result " << st;
                    env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
                }
            }
        });

        return 0;
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFlush(JNIEnv *env, jobject jobj,
                                                                                  jlong handler) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
            arrow::Status st = tablet_writer->flush();
            LOG(INFO) << "Flush result " << st;
            if (!st.ok()) {
                env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
            }
        });
        return 0;
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFinish(JNIEnv *env, jobject jobj,
                                                                                   jlong handler) {
        StarRocksFormatWriter *tablet_writer = reinterpret_cast<StarRocksFormatWriter *>(handler);
        SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
            arrow::Status st = tablet_writer->finish();
            LOG(INFO) << "Finish result " << st;
            if (!st.ok()) {
                env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
            }
        });
        return 0;
    }

    /* Reader functions */

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_createNativeReader(
            JNIEnv *env, jobject jobj, jlong jtablet_id, jlong jversion, jlong jrequired_schema, jlong joutput_schema,
            jstring jtable_root_path, jobject joptions) {
        int64_t tablet_id = jtablet_id;
        int64_t version = jversion;
        // get schema
        if (jrequired_schema == 0) {
            LOG(INFO) << "required_schema should not be null";
            env->ThrowNew(kRuntimeExceptionClass, "required_schema should not be null");
            return 0;
        }
        if (joutput_schema == 0) {
            LOG(INFO) << "output_schema should not be null";
            env->ThrowNew(kRuntimeExceptionClass, "output_schema should not be null");
            return 0;
        }
        std::string table_root_path = jstring_to_cstring(env, jtable_root_path);
        std::unordered_map<std::string, std::string> options = jhashmap_to_cmap(env, joptions);

        auto &&result = StarRocksFormatReader::create(
                tablet_id, version, reinterpret_cast<struct ArrowSchema *>(jrequired_schema),
                reinterpret_cast<struct ArrowSchema *>(joutput_schema), std::move(table_root_path), std::move(options));

        if (!result.ok()) {
            LOG(INFO) << "Create tablet reader failed: " << result.status();
            env->ThrowNew(kRuntimeExceptionClass, result.status().message().c_str());
            return 0;
        }
        StarRocksFormatReader *format_Reader = std::move(result).ValueUnsafe();
        return reinterpret_cast<int64_t>(format_Reader);
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_destroyNativeReader(JNIEnv *env, jobject jobj,
                                                                                          jlong handler) {
        StarRocksFormatReader *tablet_reader = reinterpret_cast<StarRocksFormatReader *>(handler);
        SAFE_CALL_READER_FUNCATION(tablet_reader, { delete tablet_reader; });

        return 0;
    }

    JNIEXPORT jlong JNICALL
    Java_com_starrocks_format_StarRocksReader_nativeOpen(JNIEnv *env, jobject jobj, jlong handler) {
        StarRocksFormatReader *tablet_reader = reinterpret_cast<StarRocksFormatReader *>(handler);
        SAFE_CALL_READER_FUNCATION(tablet_reader, {
            arrow::Status st = tablet_reader->open();
            if (!st.ok()) {
                LOG(ERROR) << "Open tablet reader error:" << st;
                env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
            }
        });
        return 0;
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_nativeClose(JNIEnv *env, jobject jobj,
                                                                                  jlong handler) {
        StarRocksFormatReader *tablet_reader = reinterpret_cast<StarRocksFormatReader *>(handler);
        SAFE_CALL_READER_FUNCATION(tablet_reader, { tablet_reader->close(); });
        return 0;
    }

    JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_nativeGetNext(JNIEnv *env, jobject jobj,
                                                                                    jlong handler, jlong jArrowArray) {
        StarRocksFormatReader *tablet_reader = reinterpret_cast<StarRocksFormatReader *>(handler);
        SAFE_CALL_READER_FUNCATION(tablet_reader, {
            struct ArrowArray *c_array_export = reinterpret_cast<struct ArrowArray *>(jArrowArray);
            arrow::Status st = tablet_reader->get_next(c_array_export);
            if (!st.ok()) {
                LOG(INFO) << "Get next data by tablet reader error: " << st;
                env->ThrowNew(kRuntimeExceptionClass, st.message().c_str());
            }
            return 0;
        });
        return 0;
    }

#ifdef __cplusplus
    }
#endif

} // namespace starrocks::lake::format