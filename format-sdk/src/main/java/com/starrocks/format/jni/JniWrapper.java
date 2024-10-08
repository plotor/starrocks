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

package com.starrocks.format.jni;

public class JniWrapper {

    public static final String JNI_LIB_VERSION = "3.2.3-ve-4";

    private static final JniWrapper INSTANCE = new JniWrapper();

    public static JniWrapper get() {
        JniLoader.loadNativeLibrary(JNI_LIB_VERSION);
        return INSTANCE;
    }

    private JniWrapper() {
    }

    public native void releaseWriter(long writerAddress);

    public native void releaseReader(long readerAddress);

    public native void releaseChunk(long chunkAddress);

}
