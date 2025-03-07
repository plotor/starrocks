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

package com.starrocks.authentication;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;

import java.util.List;

public class UserPropertyInfo implements Writable {

    @SerializedName("u")
    private String user;
    @SerializedName("p")
    private List<Pair<String, String>> properties = Lists.newArrayList();

    private UserPropertyInfo() {

    }

    public UserPropertyInfo(String user, List<Pair<String, String>> properties) {
        this.user = user;
        this.properties = properties;
    }

    public String getUser() {
        return user;
    }

    public List<Pair<String, String>> getProperties() {
        return properties;
    }


}
