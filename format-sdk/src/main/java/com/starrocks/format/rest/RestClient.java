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

package com.starrocks.format.rest;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.format.rest.ResponseContent.PagedResult;
import com.starrocks.format.rest.model.QueryPlan;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TableSchema;
import com.starrocks.format.rest.model.TabletCommitInfo;
import com.starrocks.format.rest.model.TabletFailInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.StatusLine;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

    private static final String HEADER_DATABASE = "db";
    private static final String HEADER_TABLE = "table";
    private static final String HEADER_LABEL = "label";
    private static final String HEADER_TIMEOUT = "timeout";

    private static final String PARAM_TEMPORARY = "temporary";
    private static final String PARAM_PAGE_NUM = "page_num";
    private static final String PARAM_PAGE_SIZE = "page_size";
    private static final String PARAM_SOURCE_TYPE = "source_type";

    private static final String BODY_COMMITTED_TABLETS = "committed_tablets";
    private static final String BODY_FAILED_TABLETS = "failed_tablets";

    private static final int DEFAULT_PAGE_SIZE = 100;

    // FIXME maybe we should extract and import LoadJobSourceType from fe module
    private static final int BYPASS_WRITE_JOB_SOURCE_TYPE = 11;

    private static final String GET_TABLE_SCHEMA_URL_FORMAT =
            "http://%s/api/v2/catalogs/%s/databases/%s/tables/%s/schema";

    private static final String GET_TABLE_PARTITION_URL_FORMAT =
            "http://%s/api/v2/catalogs/%s/databases/%s/tables/%s/partition";

    private static final String GET_QUERY_PLAN_URL_FORMAT = "http://%s/api/%s/%s/_query_plan";

    private static final String OPERATE_TRANSACTION_URL_FORMAT = "http://%s/api/transaction/%s";

    private static final ObjectMapper JSON_PARSER = new ObjectMapper();

    static {
        JSON_PARSER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private List<String> feEndpoints;

    private int connectTimeoutMillis = 30 * 1000;

    private int socketTimeoutMillis = 30 * 1000;

    private Credentials credentials;

    private int retries = 3;

    private RestClient() {
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close rest client.");
    }

    /**
     * Get sql's query plan.
     */
    public QueryPlan getQueryPlan(String dbName,
                                  String tableName,
                                  String selectClause,
                                  String filterClause) throws RequestException {
        String sql = String.format(
                "SELECT %s FROM `%s`.`%s`", Optional.ofNullable(selectClause).orElse("*"), dbName, tableName);
        if (StringUtils.isNotBlank(filterClause)) {
            sql += String.format(" WHERE %s", filterClause);
        }
        return getQueryPlan(dbName, tableName, sql);
    }

    /**
     * Get sql's query plan.
     */
    public QueryPlan getQueryPlan(String dbName, String tableName, final String sql) throws RequestException {
        LOG.info("Get query plan for table[{}.{}] by sql[{}]", dbName, tableName, sql);

        HttpPost request = new HttpPost(toUri(
                String.format(GET_QUERY_PLAN_URL_FORMAT, getRandomFeEndpoint(), dbName, tableName)
        ));

        try {
            StringEntity entity = new StringEntity(
                    JSON_PARSER.writeValueAsString(new HashMap<String, Object>(1) {
                        private static final long serialVersionUID = 4618113091057055830L;

                        {
                            put("sql", sql);
                        }
                    }), StandardCharsets.UTF_8);
            entity.setContentType(ContentType.APPLICATION_JSON.toString());
            request.setEntity(entity);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("write sql as request body error: " + sql, e);
        }

        return this.doRequest(request, httpEntity -> {
            if (null == httpEntity) {
                throw new RequestException(request, "null response entity");
            }

            return JSON_PARSER.readValue(
                    EntityUtils.toString(httpEntity, StandardCharsets.UTF_8),
                    new TypeReference<QueryPlan>() {
                    });
        });
    }

    /**
     * Request to get table schema.
     */
    public TableSchema getTableSchema(String catalog, String database, String table) throws RequestException {
        LOG.info("Get table schema for {}.{}.{}", catalog, database, table);
        HttpGet request = new HttpGet(
                toUri(String.format(GET_TABLE_SCHEMA_URL_FORMAT,
                        getRandomFeEndpoint(), catalog, database, table))
        );
        return this.doRequest(
                request,
                httpEntity -> {
                    if (null == httpEntity) {
                        throw new RequestException(request, "null response entity");
                    }

                    ResponseContent<TableSchema> responseContent = JSON_PARSER.readValue(
                            EntityUtils.toString(httpEntity, StandardCharsets.UTF_8),
                            new TypeReference<ResponseContent<TableSchema>>() {
                            });

                    if (responseContent.isOk()) {
                        return responseContent.getResult();
                    }

                    throw new RequestException(request, responseContent.getMessage());
                });
    }

    /**
     * Request to list all table partitions.
     */
    public List<TablePartition> listTablePartitions(String catalog,
                                                    String database,
                                                    String table,
                                                    boolean temporary) throws RequestException {
        List<TablePartition> partitions = new ArrayList<>();

        int pageNum = 0;
        while (true) {
            PagedResult<TablePartition> pagedResult =
                    listTablePartitions(catalog, database, table, temporary, pageNum, DEFAULT_PAGE_SIZE);
            partitions.addAll(pagedResult.getItems());
            if (++pageNum >= pagedResult.getPages()) {
                break;
            }
        }

        return partitions;
    }

    /**
     * Request to list table partitions by page.
     */
    public PagedResult<TablePartition> listTablePartitions(String catalog,
                                                           String database,
                                                           String table,
                                                           boolean temporary,
                                                           int pageNum,
                                                           int pageSize) throws RequestException {
        Validate.isTrue(pageNum >= 0, "invalid pageNum: " + pageNum);
        Validate.isTrue(pageSize > 0, "invalid pageSize: " + pageSize);

        LOG.info("List table partitions for {}.{}.{}, temporary:{}, pageNum: {}, pageSize: {}",
                catalog, database, table, temporary, pageNum, pageSize);
        HttpGet request = new HttpGet(toTablePartitionUri(catalog, database, table, temporary, pageNum, pageSize));
        return this.doRequest(
                request,
                httpEntity -> {
                    if (null == httpEntity) {
                        throw new RequestException(request, "null response entity");
                    }

                    ResponseContent<PagedResult<TablePartition>> responseContent = JSON_PARSER.readValue(
                            EntityUtils.toString(httpEntity, StandardCharsets.UTF_8),
                            new TypeReference<ResponseContent<PagedResult<TablePartition>>>() {
                            });
                    if (responseContent.isOk()) {
                        return responseContent.getResult();
                    }

                    throw new RequestException(request, responseContent.getMessage());
                });
    }

    private URI toTablePartitionUri(String catalog,
                                    String database,
                                    String table,
                                    boolean temporary,
                                    int pageNum,
                                    int pageSize) {
        String uri = String.format(
                GET_TABLE_PARTITION_URL_FORMAT,
                getRandomFeEndpoint(), catalog, database, table
        );
        return toUri(uri, uriBuilder -> uriBuilder
                .addParameter(PARAM_TEMPORARY, Objects.toString(temporary))
                .addParameter(PARAM_PAGE_NUM, Objects.toString(pageNum))
                .addParameter(PARAM_PAGE_SIZE, Objects.toString(pageSize))
        );
    }

    /**
     * Request to begin transaction.
     */
    public TransactionResult beginTransaction(String catalog,
                                              String database,
                                              String table,
                                              String label) throws RequestException {
        return this.beginTransaction(catalog, database, table, label, null);
    }

    /**
     * Request to begin transaction.
     */
    public TransactionResult beginTransaction(String catalog,
                                              String database,
                                              String table,
                                              String label,
                                              Integer timeoutSecs) throws RequestException {
        return this.doOperateTransaction(TxnOperation.TXN_BEGIN, label, request -> {
            request.addHeader(HEADER_DATABASE, database);
            request.addHeader(HEADER_TABLE, table);
            request.addHeader(HEADER_LABEL, label);

            if (null != timeoutSecs) {
                Validate.isTrue(timeoutSecs > 0, "invalid timeoutSecs: " + timeoutSecs);
                request.addHeader(HEADER_TIMEOUT, Objects.toString(timeoutSecs));
            }
        });
    }

    /**
     * Request to prepare transaction.
     */
    public TransactionResult prepareTransaction(String catalog,
                                                String database,
                                                String label,
                                                List<TabletCommitInfo> successTablets,
                                                List<TabletFailInfo> failureTablets) throws RequestException {
        return this.doOperateTransaction(TxnOperation.TXN_PREPARE, label, request -> {
            request.addHeader(HEADER_DATABASE, database);
            request.addHeader(HEADER_LABEL, label);

            try {
                String body = JSON_PARSER.writeValueAsString(new HashMap<String, Object>(2) {
                    private static final long serialVersionUID = 6981271088717642861L;

                    {
                        Optional.ofNullable(successTablets).ifPresent(tablets -> put(BODY_COMMITTED_TABLETS, tablets));
                        Optional.ofNullable(failureTablets).ifPresent(tablets -> put(BODY_FAILED_TABLETS, tablets));
                    }
                });

                StringEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
                entity.setContentType(ContentType.APPLICATION_JSON.toString());
                request.setEntity(entity);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "write success or failure tablets as json error, " + e.getMessage(), e);
            }
        });
    }

    /**
     * Request to commit transaction.
     */
    public TransactionResult commitTransaction(String catalog,
                                               String database,
                                               String label) throws RequestException {
        return this.doOperateTransaction(TxnOperation.TXN_COMMIT, label, request -> {
            request.addHeader(HEADER_DATABASE, database);
            request.addHeader(HEADER_LABEL, label);
        });
    }

    /**
     * Request to rollback transaction.
     */
    public TransactionResult rollbackTransaction(String catalog,
                                                 String database,
                                                 String label,
                                                 List<TabletFailInfo> failureTablets) throws RequestException {
        return this.doOperateTransaction(TxnOperation.TXN_ROLLBACK, label, request -> {
            request.addHeader(HEADER_DATABASE, database);
            request.addHeader(HEADER_LABEL, label);

            try {
                String body = JSON_PARSER.writeValueAsString(new HashMap<String, Object>(1) {
                    private static final long serialVersionUID = -293525488977240959L;

                    {
                        Optional.ofNullable(failureTablets).ifPresent(tablets -> put(BODY_FAILED_TABLETS, tablets));
                    }
                });

                StringEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
                entity.setContentType(ContentType.APPLICATION_JSON.toString());
                request.setEntity(entity);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "write failure tablets as json error, " + e.getMessage(), e);
            }
        });
    }

    private TransactionResult doOperateTransaction(TxnOperation txnOperation,
                                                   String label,
                                                   Consumer<HttpPost> requestConsumer) throws RequestException {
        HttpPost request = new HttpPost(toUri(
                String.format(OPERATE_TRANSACTION_URL_FORMAT, getRandomFeEndpoint(), txnOperation),
                uriBuilder -> {
                    if (TxnOperation.TXN_BEGIN.equals(txnOperation)) {
                        uriBuilder.addParameter(PARAM_SOURCE_TYPE, Objects.toString(BYPASS_WRITE_JOB_SOURCE_TYPE));
                    }
                }
        ));

        if (null != requestConsumer) {
            requestConsumer.accept(request);
        }

        LOG.info("Request to {} transaction, label: {}", txnOperation, label);
        return this.doRequest(request, httpEntity -> {
            if (null == httpEntity) {
                throw new RequestException(request, "null response entity");
            }

            TransactionResult txnResult = JSON_PARSER.readValue(
                    EntityUtils.toString(httpEntity, StandardCharsets.UTF_8),
                    new TypeReference<TransactionResult>() {
                    });
            if (txnResult.isOk()) {
                return txnResult;
            }

            throw new RequestException(request, txnResult.getMessage());
        });
    }

    private URI toUri(String uriString) {
        return this.toUri(uriString, null);
    }

    private URI toUri(String uriString, Consumer<URIBuilder> uriBuilderConsumer) {
        try {
            URIBuilder uriBuilder = new URIBuilder(uriString);
            if (null != uriBuilderConsumer) {
                uriBuilderConsumer.accept(uriBuilder);
            }
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("invalid uri: " + uriString, e);
        }
    }

    private String getRandomFeEndpoint() {
        return feEndpoints.get(RandomUtils.nextInt(0, feEndpoints.size()));
    }

    private String doRequest(HttpRequestBase request) throws RequestException {
        return this.doRequest(request, httpEntity -> {
            if (null == httpEntity) {
                return null;
            }
            return EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
        });
    }

    private <T> T doRequest(HttpRequestBase request,
                            ThrowingFunction<HttpEntity, T> responseEntityParser) throws RequestException {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeoutMillis)
                .setSocketTimeout(socketTimeoutMillis)
                .setRedirectsEnabled(true)
                .build();

        request.setConfig(requestConfig);

        RedirectStrategy redirectStrategy = new LaxRedirectStrategy() {
            @Override
            public HttpUriRequest getRedirect(HttpRequest req, HttpResponse rep, HttpContext ctx)
                    throws ProtocolException {
                String method = req.getRequestLine().getMethod();
                if (HttpPost.METHOD_NAME.equalsIgnoreCase(method)) {
                    // FIXME deep copy?
                    request.setURI(getLocationURI(req, rep, ctx));
                    return request;
                }
                return super.getRedirect(req, rep, ctx);
            }
        };

        Throwable e = null;
        int retryCnt = 0;
        while (retryCnt++ < retries) {
            try (CloseableHttpClient httpClient = HttpClients.custom()
                    .setRedirectStrategy(redirectStrategy)
                    .addInterceptorFirst((HttpRequestInterceptor) (req, ctx) -> req.removeHeaders(HTTP.CONTENT_LEN))
                    .build()) {
                HttpClientContext context = HttpClientContext.create();
                request.addHeader(new BasicScheme().authenticate(credentials, request, context));
                LOG.debug("Send request: {}", request);
                try (CloseableHttpResponse response = httpClient.execute(request, context)) {
                    StatusLine respStatus = response.getStatusLine();
                    HttpEntity httpEntity = response.getEntity();
                    if (HttpStatus.SC_OK == respStatus.getStatusCode()) {
                        return responseEntityParser.apply(httpEntity);
                    }

                    String message = response.getStatusLine().toString();
                    if (null != httpEntity) {
                        message = message + ", " + EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
                    }
                    e = new RequestException(request, message);
                    LOG.debug("Request {} with retries {} error, reason: {}", request, retryCnt, message);
                }
            } catch (Throwable ex) {
                e = ex;
            }
        }

        LOG.error("Request {} error", request, e);
        if (e instanceof RequestException) {
            throw (RequestException) e;
        }

        throw new RequestException(
                request,
                Optional.ofNullable(e).map(Throwable::getMessage).orElse(null),
                e
        );
    }

    /**
     * Rest client builder.
     */
    public static class Builder {

        private List<String> feEndpoints;

        private Integer connectTimeoutMillis;

        private Integer socketTimeoutMillis;

        private String username;

        private String password;

        private Integer retries;

        public Builder() {
        }

        public Builder setFeEndpoints(String feEndpoints) {
            return this.setFeEndpoints(feEndpoints.split(",\\s*"));
        }

        public Builder setFeEndpoints(String[] feEndpoints) {
            return this.setFeEndpoints(Arrays.asList(feEndpoints));
        }

        public Builder setFeEndpoints(List<String> feEndpoints) {
            this.feEndpoints = feEndpoints.stream()
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .map(elem -> {
                        if (elem.startsWith("http://")) {
                            elem = elem.substring(7);
                        }
                        if (elem.startsWith("https://")) {
                            elem = elem.substring(8);
                        }
                        return elem;
                    })
                    .distinct()
                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(this.feEndpoints)) {
                throw new IllegalArgumentException("invalid fe endpoints: " + StringUtils.join(feEndpoints, ", "));
            }

            return this;
        }

        public Builder setConnectTimeoutMillis(Integer connectTimeoutMillis) {
            if (null == connectTimeoutMillis || connectTimeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "invalid connect timeout value: " + connectTimeoutMillis);
            }
            this.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public Builder setSocketTimeoutMillis(Integer socketTimeoutMillis) {
            if (null == socketTimeoutMillis || socketTimeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "invalid socket timeout value: " + socketTimeoutMillis);
            }
            this.socketTimeoutMillis = socketTimeoutMillis;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setRetries(Integer retries) {
            if (null == retries || retries <= 0) {
                throw new IllegalArgumentException("invalid retries value: " + retries);
            }
            this.retries = retries;
            return this;
        }

        public RestClient build() {
            RestClient restClient = new RestClient();

            if (CollectionUtils.isEmpty(this.feEndpoints)) {
                throw new IllegalArgumentException("missing fe endpoints");
            }
            restClient.setFeEndpoints(this.feEndpoints);

            Optional.ofNullable(this.connectTimeoutMillis)
                    .ifPresent(restClient::setConnectTimeoutMillis);

            Optional.ofNullable(this.socketTimeoutMillis)
                    .ifPresent(restClient::setSocketTimeoutMillis);

            if (StringUtils.isBlank(username)) {
                throw new IllegalArgumentException("missing username");
            }

            restClient.setCredentials(new UsernamePasswordCredentials(this.username, this.password));

            Optional.ofNullable(this.retries).ifPresent(restClient::setRetries);

            return restClient;
        }

    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R> {

        R apply(T t) throws Throwable;

    }

    private void setFeEndpoints(List<String> feEndpoints) {
        this.feEndpoints = feEndpoints;
    }

    private void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    private void setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    private void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    private void setRetries(int retries) {
        this.retries = retries;
    }
}
