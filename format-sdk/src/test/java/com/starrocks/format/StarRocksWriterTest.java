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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class StarRocksWriterTest extends BaseFormatTest {

    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }

    private static Stream<Arguments> transactionWriteTables() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate"),
                Arguments.of("tb_all_primitivetype_write_unique"),
                Arguments.of("tb_all_primitivetype_write_aggregate"),
                Arguments.of("tb_all_primitivetype_write_primary")
        );
    }

    @ParameterizedTest
    @MethodSource("transactionWriteTables")
    public void testTransactionWrite(String tableName) throws Exception {
        String label = String.format("bypass_write_%s_%s_%s",
                dbName, tableName, RandomStringUtils.randomAlphabetic(8));
        Schema schema = ArrowUtils.toArrowSchema(restClient.getTableSchema(DEFAULT_CATALOG, dbName, tableName), TZ);
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, dbName, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, dbName, tableName, label);
        assertTrue(beginTxnResult.isOk());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();

                try {
                    StarRocksWriter writer = new StarRocksWriter(
                            tabletId,
                            partition.getStoragePath(),
                            beginTxnResult.getTxnId(),
                            schema,
                            settings.toConfig());
                    writer.open();
                    // write use chunk interface
                    VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());

                    fillSampleData(vsr, 0, 4);

                    writer.write(vsr);
                    vsr.close();

                    // vsr.allocateNew();
                    vsr = VectorSchemaRoot.create(schema, writer.getAllocator());
                    fillSampleData(vsr, 200, 4);
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

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, dbName, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());
    }


}
