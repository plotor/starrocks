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

import com.starrocks.format.rest.TransactionResult;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TabletCommitInfo;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class StarRocksReaderWriterTest extends BaseFormatTest {

    private static final long SLEEP_MILLIS = 0L;

    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }

    private static Stream<Arguments> testReadAfterWrite() {
        return Stream.of(
                Arguments.of("tb_binary_two_key_duplicate"),
                Arguments.of("tb_binary_two_key_primary"),
                Arguments.of("tb_binary_two_key_unique"),
                Arguments.of("tb_read_write_all_type_unique"),
                Arguments.of("tb_read_write_all_type_primary")
        );
    }

    @ParameterizedTest
    @MethodSource("testReadAfterWrite")
    public void testReadAfterWrite(String tableName) throws Exception {
        truncateTable(tableName);
        final String label = String.format("bypass_write_%s_%s_%s",
                dbName, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema schema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        Schema tableSchema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, dbName, tableName, label);
        assertTrue(beginTxnResult.isOk());
        assertEquals(label, beginTxnResult.getLabel());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 300;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(
                            tabletId,
                            partition.getStoragePath(),
                            beginTxnResult.getTxnId(),
                            schema,
                            settings.toConfig());
                    writer.open();
                    // write use arrow interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getAllocator())) {
                        fillSampleData(vsr, startId, 4);
                        System.out.println("write >> \n" + vsr.contentToTSVString());
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, startId + 100, 4);
                        System.out.println("write >> \n" + vsr.contentToTSVString());
                        writer.write(vsr);
                        vsr.clear();
                    }

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult =
                restClient.prepareTransaction(DEFAULT_CATALOG, dbName, label, committedTablets, null);
        assertTrue(prepareTxnResult.isOk());
        assertEquals(label, prepareTxnResult.getLabel());

        TransactionResult commitTxnResult = restClient.commitTransaction(DEFAULT_CATALOG, dbName, label);
        assertTrue(commitTxnResult.isOk());
        assertEquals(label, commitTxnResult.getLabel());

        Thread.sleep(SLEEP_MILLIS);
        queryTable(tableName);

        // read all data test
        int expectedNumRows = 24;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, partition.getStoragePath(), version, tableSchema, tableSchema, settings.toConfig());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.next();
                    numRows = vsr.getRowCount();
                    System.out.println(vsr.contentToTSVString());
                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.next();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }

        assertEquals(expectedNumRows, totalRows);

    }

    private static Stream<Arguments> testReadAfterWriteWithJsonFilter() {
        return Stream.of(
                Arguments.of("tb_json_two_key_primary"),
                Arguments.of("tb_json_two_key_unique")
        );
    }

    @ParameterizedTest
    @MethodSource("testReadAfterWriteWithJsonFilter")
    public void testReadAfterWriteWithJsonFilter(String tableName) throws Exception {
        truncateTable(tableName);
        final String label = String.format("bypass_write_%s_%s_%s",
                dbName, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema tableSchema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, dbName, tableName, label);
        assertTrue(beginTxnResult.isOk());
        assertEquals(label, beginTxnResult.getLabel());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(
                            tabletId,
                            partition.getStoragePath(),
                            beginTxnResult.getTxnId(),
                            tableSchema,
                            settings.toConfig());
                    writer.open();
                    // write use chunk interface
                    VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getAllocator());

                    fillSampleData(vsr, startId, 4);
                    writer.write(vsr);
                    vsr.close();

                    fillSampleData(vsr, startId + 200, 4);
                    writer.write(vsr);
                    vsr.close();

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, dbName, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());
        assertEquals(label, prepareTxnResult.getLabel());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, dbName, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());
        assertEquals(label, commitTxnResult.getLabel());

        Thread.sleep(SLEEP_MILLIS);
        queryTable(tableName);

        // read all data test
        int expectedNumRows = 24;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, partition.getStoragePath(), version, tableSchema, tableSchema, settings.toConfig());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.next();
                    numRows = vsr.getRowCount();
                    System.out.println(vsr.contentToTSVString());
                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.next();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }

        assertEquals(expectedNumRows, totalRows);

        // test with filter
        String requiredColumns = "rowid,c_varchar,c_json";
        String outputColumns = "rowid,c_varchar";
        String sql =
                "select rowId, c_varchar from " + dbName + "." + tableName + " where cast((c_json->'rowid') as int) \\% 2 = 0";
        // get query plan
        String queryPlan = restClient.getQueryPlan(dbName, tableName, sql).getOpaquedQueryPlan();
        Set<String> requiredColumnName = new HashSet<>(Arrays.asList(requiredColumns.split(",")));
        Set<String> outputColumnName = new HashSet<>(Arrays.asList(outputColumns.split(",")));

        // resolve required schema
        List<Field> fields = tableSchema.getFields().stream()
                .filter(col -> requiredColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());
        Schema requiredSchema = new Schema(fields, tableSchema.getCustomMetadata());
        // resolve output schema
        fields = tableSchema.getFields().stream()
                .filter(col -> outputColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());
        Schema outputSchema = new Schema(fields, tableSchema.getCustomMetadata());

        // read chunk
        int expectedTotalRows = 11;
        totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                try {
                    Config config = settings.toConfig();
                    config.setQueryPlan(queryPlan);
                    // read table
                    StarRocksReader reader = new StarRocksReader(
                            tabletId, partition.getStoragePath(), version, requiredSchema, outputSchema, config);
                    reader.open();

                    long numRows;
                    do {
                        VectorSchemaRoot vsr = reader.next();
                        numRows = vsr.getRowCount();

                        checkValue(vsr, numRows);
                        vsr.close();

                        totalRows += numRows;
                    } while (numRows > 0);

                    // should be empty chunk
                    VectorSchemaRoot vsr = reader.next();
                    assertEquals(0, vsr.getRowCount());
                    vsr.close();

                    reader.close();
                    reader.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

            }
        }

        assertEquals(expectedTotalRows, totalRows);

    }

    private static Stream<Arguments> testComplexTypeReadAfterWrite() {
        return Stream.of(
                Arguments.of("tb_map_array_struct")
        );
    }

    @ParameterizedTest
    @MethodSource("testComplexTypeReadAfterWrite")
    public void testComplexTypeReadAfterWrite(String tableName) throws Exception {
        truncateTable(tableName);

        final String label = String.format("bypass_write_%s_%s_%s",
                dbName, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema schema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        Schema tableSchema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, dbName, tableName, label);
        assertTrue(beginTxnResult.isOk());
        assertEquals(label, beginTxnResult.getLabel());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(
                            tabletId,
                            partition.getStoragePath(),
                            beginTxnResult.getTxnId(),
                            tableSchema,
                            settings.toConfig());
                    writer.open();
                    // write use chunk interface
                    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(tableSchema, writer.getAllocator())) {

                        fillSampleData(vsr, startId, 4);
                        writer.write(vsr);
                        vsr.clear();

                        fillSampleData(vsr, startId + 200, 4);
                        writer.write(vsr);
                    }

                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult =
                restClient.prepareTransaction(DEFAULT_CATALOG, dbName, label, committedTablets, null);
        assertTrue(prepareTxnResult.isOk());
        assertEquals(label, prepareTxnResult.getLabel());

        TransactionResult commitTxnResult = restClient.commitTransaction(DEFAULT_CATALOG, dbName, label);
        assertTrue(commitTxnResult.isOk());
        assertEquals(label, commitTxnResult.getLabel());

        Thread.sleep(SLEEP_MILLIS);
        queryTable(tableName);

        // read all data test
        int expectedNumRows = 8;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, partition.getStoragePath(), version, tableSchema, tableSchema, settings.toConfig());
                reader.open();

                long numRows;
                do {
                    VectorSchemaRoot vsr = reader.next();
                    numRows = vsr.getRowCount();

                    checkValue(vsr, numRows);
                    vsr.close();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                VectorSchemaRoot vsr = reader.next();
                assertEquals(0, vsr.getRowCount());
                vsr.close();

                reader.close();
                reader.release();
            }
        }
        assertEquals(expectedNumRows, totalRows);
    }

}
