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

package com.starrocks.format;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Config {

    @Inherited
    @Target({ElementType.FIELD, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Option {

        String value();

    }

    @Option("starrocks.format.writer_type")
    private String writerType;

    @Option("starrocks.format.rows_per_segment")
    private String rowsPerSegment;

    /* FIXME by zhenchao */

    @Option("starrocks.fe.http.url")
    private String srFeHttpUrl = "127.0.0.1:8030";

    @Option("starrocks.fe.jdbc.url")
    private String srFeJdbcUrl = "jdbc:mysql://127.0.0.1:9030";

    @Option("starrocks.database")
    private String srDatabase;

    @Option("starrocks.user")
    private String srUser = "root";

    @Option("starrocks.password")
    private String srPassword = "";

    @Option("fs.s3a.endpoint")
    private String s3Endpoint = "http://127.0.0.1:9000";

    @Option("fs.s3a.endpoint.region")
    private String s3EndpointRegion = "cn-beijing";

    @Option("fs.s3a.connection.ssl.enabled")
    private String s3ConnectionSslEnabled = "false";

    @Option("fs.s3a.path.style.access")
    private String s3PathStyleAccess = "false";

    @Option("fs.s3a.access.key")
    private String s3AccessKey = "minio_access_key";

    @Option("fs.s3a.secret.key")
    private String s3SecretKey = "minio_secret_key";

    @Option("starrocks.format.query_plan")
    private String queryPlan;

    @Option("starrocks.format.using_column_uid")
    private String usingColumnUid;

    public Map<String, String> toMap() {
        Map<String, String> mapping = new HashMap<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Option.class)) {
                Option option = field.getAnnotation(Option.class);
                String propName = option.value();
                try {
                    Optional.ofNullable(field.get(this))
                            .map(Objects::toString)
                            .ifPresent(propValue -> mapping.put(propName, propValue));
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(
                            "Get option[" + propName + "] value error, " + e.getMessage(), e
                    );
                }
            }
        }
        return mapping;
    }

    public String getSrFeHttpUrl() {
        return srFeHttpUrl;
    }

    public void setSrFeHttpUrl(String srFeHttpUrl) {
        this.srFeHttpUrl = srFeHttpUrl;
    }

    public String getSrFeJdbcUrl() {
        return srFeJdbcUrl;
    }

    public void setSrFeJdbcUrl(String srFeJdbcUrl) {
        this.srFeJdbcUrl = srFeJdbcUrl;
    }

    public String getSrDatabase() {
        return srDatabase;
    }

    public void setSrDatabase(String srDatabase) {
        this.srDatabase = srDatabase;
    }

    public String getSrUser() {
        return srUser;
    }

    public void setSrUser(String srUser) {
        this.srUser = srUser;
    }

    public String getSrPassword() {
        return srPassword;
    }

    public void setSrPassword(String srPassword) {
        this.srPassword = srPassword;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public void setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
    }

    public String getS3EndpointRegion() {
        return s3EndpointRegion;
    }

    public void setS3EndpointRegion(String s3EndpointRegion) {
        this.s3EndpointRegion = s3EndpointRegion;
    }

    public String getS3ConnectionSslEnabled() {
        return s3ConnectionSslEnabled;
    }

    public void setS3ConnectionSslEnabled(String s3ConnectionSslEnabled) {
        this.s3ConnectionSslEnabled = s3ConnectionSslEnabled;
    }

    public String getS3PathStyleAccess() {
        return s3PathStyleAccess;
    }

    public void setS3PathStyleAccess(String s3PathStyleAccess) {
        this.s3PathStyleAccess = s3PathStyleAccess;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    public void setQueryPlan(String queryPlan) {
        this.queryPlan = queryPlan;
    }

    public String getUsingColumnUid() {
        return usingColumnUid;
    }

    public void setUsingColumnUid(String usingColumnUid) {
        this.usingColumnUid = usingColumnUid;
    }
}
