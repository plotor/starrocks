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

import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.KeysType;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import com.starrocks.proto.Types.CompressionTypePB;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StarRocksReaderWriterLocalTest extends BaseFormatTest {

    /*@BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }*/

    @Test
    public void testWriteLocal(@TempDir Path tempDir) throws Exception {
        String tabletRootPath = tempDir.toAbsolutePath().toString();
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.BOOLEAN, 4),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DATE, 4),
                new ColumnType(DataType.DATETIME, 8),
                new ColumnType(DataType.DECIMAL32, 8),
                new ColumnType(DataType.DECIMAL64, 16),
                new ColumnType(DataType.VARCHAR, 32 + Integer.SIZE)};

        int colId = 0;
        for (ColumnType columnType : columnTypes) {
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName(toColName(columnType.getDataType()))
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(true)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            if (columnType.getDataType().getLiteral().startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL128")) {
                columnBuilder.setPrecision(38);
                columnBuilder.setFrac(4);
            }
            schemaBuilder.addColumn(columnBuilder.build());
            colId++;
        }
        TabletSchemaPB tabletSchemaPB = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index alway the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long tabletId = 100L;
        long txnId = 4;
        long version = 1;
        setupTabletMeta(tabletRootPath, tabletSchemaPB, tabletId, version);

        Schema schema = toArrowSchema(tabletSchemaPB);
        StarRocksWriter writer = new StarRocksWriter(
                tabletId,
                tabletRootPath,
                txnId,
                schema,
                new Config());
        writer.open();

        // write use chunk interface
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());

        fillSampleData(vsr, 0, 5);
        writer.write(vsr);
        vsr.close();

        writer.flush();
        writer.finish();
        writer.close();
        writer.release();
    }

    @Test
    public void testWriteLongJson(@TempDir Path tempDir) throws Exception {
        String tabletRootPath = tempDir.toAbsolutePath().toString();

        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        // add two key column
        ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                .setName("rowId")
                .setUniqueId(0)
                .setIsKey(true)
                .setIsNullable(false)
                .setType(DataType.INT.getLiteral())
                .setLength(4)
                .setIndexLength(4)
                .setAggregation("none");
        schemaBuilder.addColumn(columnBuilder.build());
        columnBuilder = ColumnPB.newBuilder()
                .setName("rowId2")
                .setUniqueId(1)
                .setIsKey(true)
                .setIsNullable(false)
                .setType(DataType.INT.getLiteral())
                .setLength(4)
                .setIndexLength(4)
                .setAggregation("none");
        schemaBuilder.addColumn(columnBuilder.build());

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.JSON, 4)
        };
        int colId = 2;
        for (ColumnType columnType : columnTypes) {
            columnBuilder = ColumnPB.newBuilder()
                    .setName(toColName(columnType.getDataType()))
                    .setUniqueId(colId)
                    .setIsKey(false)
                    .setIsNullable(true)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            if (DataType.DECIMAL32.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (DataType.DECIMAL64.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (DataType.DECIMAL128.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(38);
                columnBuilder.setFrac(4);
            }
            schemaBuilder.addColumn(columnBuilder.build());
            colId++;
        }
        TabletSchemaPB tabletSchemaPB = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index always the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();


        long tabletId = 100L;
        long txnId = 4;
        long version = 1;
        setupTabletMeta(tabletRootPath, tabletSchemaPB, tabletId, version);

        Schema schema = toArrowSchema(tabletSchemaPB);

        StarRocksWriter writer = new StarRocksWriter(
                tabletId,
                tabletRootPath,
                txnId,
                schema,
                new Config());
        writer.open();

        // write use chunk interface
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());

        VarCharVector fieldVector = (VarCharVector) vsr.getVector(2);
        // normal append
        IntVector vector = (IntVector) vsr.getVector(0);
        vector.setSafe(0, 1);
        vector = (IntVector) vsr.getVector(1);
        vector.setSafe(0, 1);
        String value = generateFixedLengthString(100);
        fieldVector.setSafe(0, value.getBytes());
        vsr.setRowCount(1);
        writer.write(vsr);
        vsr.close();
        // abnormal append
        try {
            vector = (IntVector) vsr.getVector(0);
            vector.setSafe(0, 1);
            vector = (IntVector) vsr.getVector(1);
            vector.setSafe(0, 1);
            value = generateFixedLengthString(16 * 1024 * 1024 + 1);
            fieldVector.setSafe(0, value.getBytes());
            vsr.setRowCount(1);
            writer.write(vsr);
            vsr.close();
            fail("append long string should failed.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("string exceed maximum length"), e.getMessage());
        }
        vsr.close();
        writer.close();
        writer.release();
    }

    @Test
    public void testWriteStringExtendLength(@TempDir Path tempDir) throws Exception {
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.CHAR, 9),
                new ColumnType(DataType.VARCHAR, 17)};

        int colId = 0;
        for (ColumnType columnType : columnTypes) {
            String tabletRootPath = tempDir.toAbsolutePath() + "/" + columnType.getDataType();
            schemaBuilder.clearColumn();
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName(toColName(columnType.getDataType()))
                    .setUniqueId(0)
                    .setIsKey(true)
                    .setIsNullable(true)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            schemaBuilder.addColumn(columnBuilder.build());

            TabletSchemaPB tabletSchemaPB = schemaBuilder
                    .setNextColumnUniqueId(colId)
                    // sort key index alway the key column index
                    .addSortKeyIdxes(0)
                    // short key size is less than sort keys
                    .setNumShortKeyColumns(1)
                    .setNumRowsPerRowBlock(1024)
                    .build();

            long tabletId = 100L;
            long txnId = 4;
            setupTabletMeta(tabletRootPath, tabletSchemaPB, tabletId, 0);

            Schema schema = toArrowSchema(tabletSchemaPB);
            StarRocksWriter writer = new StarRocksWriter(
                    tabletId,
                    tabletRootPath,
                    txnId,
                    schema,
                    new Config());
            writer.open();

            // write use chunk interface
            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());
            // normal append
            String value = generateFixedLengthString(columnType.getLength());
            ((VarCharVector) vsr.getVector(0)).setSafe(0, value.getBytes());
            vsr.setRowCount(1);
            writer.write(vsr);
            vsr.close();

            // abnormal append
            try {
                vsr.allocateNew();
                value = generateFixedLengthString(columnType.getLength() + 1);
                ((VarCharVector) vsr.getVector(0)).setSafe(0, value.getBytes());
                vsr.setRowCount(1);
                writer.write(vsr);
                vsr.close();
                fail("append long string should failed.");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("length of string"), e.getMessage());
                assertTrue(e.getMessage().contains("exceeds limit"), e.getMessage());
            }

            vsr.close();
            writer.release();
        }

    }

    @Disabled
    public void testUnsupportedColumnType(@TempDir Path tempDir) throws Exception {
        String tabletRootPath = tempDir.toAbsolutePath().toString();

        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] types = new ColumnType[] {
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.BOOLEAN, 4),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DATE, 4),
                new ColumnType(DataType.DATETIME, 8),
                new ColumnType(DataType.DECIMAL32, 8),
                new ColumnType(DataType.DECIMAL64, 16),
                new ColumnType(DataType.VARCHAR, 32 + Integer.SIZE),
                new ColumnType(DataType.MAP, 16),
                new ColumnType(DataType.ARRAY, 16),
                new ColumnType(DataType.STRUCT, 16),
                new ColumnType(DataType.JSON, 16)};

        int colId = 0;
        for (ColumnType columnType : types) {
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName(toColName(columnType.getDataType()))
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(false)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            if (columnType.getDataType().getLiteral().startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(3);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(4);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL128")) {
                columnBuilder.setPrecision(29);
                columnBuilder.setFrac(5);
            }
            schemaBuilder.addColumn(columnBuilder.build());
            colId++;
        }
        TabletSchemaPB tabletSchemaPB = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index alway the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long tabletId = 100L;
        long txnId = 4L;
        setupTabletMeta(tabletRootPath, tabletSchemaPB, tabletId, 0);

        Schema schema = toArrowSchema(tabletSchemaPB);

        File dir = new File(tabletRootPath + "/data");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/log");
        assertTrue(dir.mkdirs());

        try {
            StarRocksWriter writer = new StarRocksWriter(
                    tabletId,
                    tabletRootPath,
                    txnId,
                    schema,
                    new Config());
            writer.open();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Unsupported column type"));
        }
    }

    @Test
    public void testComplexTypeReadAfterWriteWithLocal(@TempDir Path tempDir) throws Exception {
        String tabletRootPath = tempDir.toAbsolutePath().toString();
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.ARRAY, 4),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DATE, 4),
                new ColumnType(DataType.DATETIME, 8),
                new ColumnType(DataType.DECIMAL32, 8),
                new ColumnType(DataType.DECIMAL64, 16),
                new ColumnType(DataType.VARCHAR, 32),
                new ColumnType(DataType.BITMAP, 4),
                new ColumnType(DataType.HLL, 4),
                new ColumnType(DataType.MAP, 1),
                new ColumnType(DataType.ARRAY, 1),
                new ColumnType(DataType.STRUCT, 1)
        };

        ColumnPB keyColumn = ColumnPB.newBuilder()
                .setName("rowid")
                .setUniqueId(0)
                .setIsKey(true)
                .setIsNullable(true)
                .setType(DataType.INT.getLiteral())
                .setLength(10)
                .setIndexLength(10)
                .setAggregation("none")
                .build();
        schemaBuilder.addColumn(keyColumn);

        int colId = 1;
        for (ColumnType columnType : columnTypes) {
            String type = columnType.getDataType().getLiteral();
            if (DataType.BITMAP.equals(columnType.getDataType())) {
                type = "OBJECT";
            }
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName(toColName(columnType.getDataType()))
                    .setUniqueId(colId)
                    .setIsKey(false)
                    .setIsNullable(true)
                    .setType(type)
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            if (DataType.DECIMAL32.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (DataType.DECIMAL64.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (DataType.DECIMAL128.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(38);
                columnBuilder.setFrac(4);
            } else if (DataType.ARRAY.equals(columnType.getDataType())) {
                ColumnPB elementCol = ColumnPB.newBuilder()
                        .setName("element")
                        .setUniqueId(-1)
                        .setType(DataType.INT.getLiteral())
                        .setIsNullable(true)
                        .setLength(4)
                        .setIndexLength(4)
                        .setAggregation("none")
                        .build();
                columnBuilder.addChildrenColumns(elementCol);
            } else if (DataType.MAP.equals(columnType.getDataType())) {
                ColumnPB keyCol = ColumnPB.newBuilder()
                        .setName("key")
                        .setUniqueId(-1)
                        .setType(DataType.INT.getLiteral())
                        .setIsNullable(false)
                        .setLength(4)
                        .setIndexLength(4)
                        .setAggregation("none")
                        .build();
                ColumnPB valueCol = ColumnPB.newBuilder()
                        .setName("value")
                        .setUniqueId(-1)
                        .setType(DataType.VARCHAR.getLiteral())
                        .setIsNullable(true)
                        .setLength(1024)
                        .setIndexLength(1024)
                        .setAggregation("none")
                        .build();

                columnBuilder.addChildrenColumns(keyCol);
                columnBuilder.addChildrenColumns(valueCol);
            } else if (DataType.STRUCT.equals(columnType.getDataType())) {
                ColumnPB aCol = ColumnPB.newBuilder()
                        .setName("a")
                        .setUniqueId(-1)
                        .setType(DataType.INT.getLiteral())
                        .setIsNullable(true)
                        .setLength(4)
                        .setIndexLength(4)
                        .setAggregation("none")
                        .build();
                columnBuilder.addChildrenColumns(aCol);
                ColumnPB cCol = ColumnPB.newBuilder()
                        .setName("c")
                        .setUniqueId(-1)
                        .setType(DataType.VARCHAR.getLiteral())
                        .setIsNullable(true)
                        .setLength(1024)
                        .setIndexLength(1024)
                        .setAggregation("none")
                        .build();
                columnBuilder.addChildrenColumns(cCol);
                ColumnPB dCol = ColumnPB.newBuilder()
                        .setName("d")
                        .setUniqueId(-1)
                        .setType(DataType.DATE.getLiteral())
                        .setIsNullable(true)
                        .setLength(4)
                        .setIndexLength(4)
                        .setAggregation("none")
                        .build();
                columnBuilder.addChildrenColumns(dCol);
            }
            schemaBuilder.addColumn(columnBuilder.build());
            colId++;
        }
        TabletSchemaPB tabletSchemaPB = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index always the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long tabletId = 100L;
        long txnId = 4;
        long version = 1;
        setupTabletMeta(tabletRootPath, tabletSchemaPB, tabletId, version);
        Schema schema = toArrowSchema(tabletSchemaPB);

        //write test
        {
            StarRocksWriter writer = new StarRocksWriter(
                    tabletId,
                    tabletRootPath,
                    txnId,
                    schema,
                    new Config());
            writer.open();

            // write use chunk interface
            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());

            fillSampleData(vsr, 0, 10);
            writer.write(vsr);
            vsr.close();

            writer.flush();
            writer.finish();
            writer.close();
            writer.release();
        }
    }


}
