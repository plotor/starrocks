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

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Properties;

public class ConnSettings {

    private static final String CONFIG_FILENAME = "starrocks_conn.properties";

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

    private ConnSettings() {
    }

    public static ConnSettings newInstance() throws IOException, IllegalAccessException {
        Properties props = new Properties();
        try (InputStream inputStream = ConnSettings.class.getClassLoader()
                .getResourceAsStream(CONFIG_FILENAME)) {
            if (null != inputStream) {
                System.out.println("Load connection settings from " + CONFIG_FILENAME);
                props.load(inputStream);
            }
        }

        ConnSettings settings = new ConnSettings();
        if (props.isEmpty()) {
            return settings;
        }

        Field[] fields = ConnSettings.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Option.class)) {
                Option option = field.getAnnotation(Option.class);
                Object fieldValue = props.get(option.value());
                field.set(settings, fieldValue);
            }
        }

        return settings;
    }

    public Config toConfig() {
        Config config = new Config();
        config.setSrFeHttpUrl(getSrFeHttpUrl());
        config.setSrFeJdbcUrl(getSrFeJdbcUrl());
        config.setSrDatabase(getSrDatabase());
        config.setSrUser(getSrUser());
        config.setSrPassword(getSrPassword());
        config.setS3Endpoint(getS3Endpoint());
        config.setS3EndpointRegion(getS3EndpointRegion());
        config.setS3ConnectionSslEnabled(getS3ConnectionSslEnabled());
        config.setS3PathStyleAccess(getS3PathStyleAccess());
        config.setS3AccessKey(getS3AccessKey());
        config.setS3SecretKey(getS3SecretKey());
        return config;
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

    @Inherited
    @Target({ElementType.FIELD, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Option {

        String value();

    }

}