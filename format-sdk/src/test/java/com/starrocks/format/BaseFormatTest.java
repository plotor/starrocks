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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.format.rest.RestClient;
import com.starrocks.proto.LakeTypes;
import com.starrocks.proto.TabletSchema;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseFormatTest {

    protected static final String DEFAULT_CATALOG = "default_catalog";

    protected static final ZoneId TZ = ZoneId.systemDefault();

    protected static ConnSettings settings;

    protected static String dbName = "demo";

    protected static RestClient restClient;

    protected static Connection dbConn;

    protected static ObjectMapper objMapper = new ObjectMapper();

    @BeforeAll
    public static void init() throws Exception {
        settings = ConnSettings.newInstance();

        if (null != settings.getSrDatabase()) {
            dbName = settings.getSrDatabase();
        }

        dbConn = DriverManager.getConnection(
                settings.getSrFeJdbcUrl(), settings.getSrUser(), settings.getSrPassword()
        );

        restClient = new RestClient.Builder()
                .setFeEndpoints(settings.getSrFeHttpUrl())
                .setUsername(settings.getSrUser())
                .setPassword(settings.getSrPassword())
                .build();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (null != dbConn) {
            dbConn.close();
        }
        if (null != restClient) {
            restClient.close();
        }
    }

    public static Schema toArrowSchema(TabletSchemaPB tabletSchema) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(ArrowUtils.STARROKCS_TABLE_ID, String.valueOf(tabletSchema.getId()));
        metadata.put(ArrowUtils.STARROKCS_TABLE_KEY_TYPE, String.valueOf(tabletSchema.getKeysType()));
        metadata.put(ArrowUtils.STARROKCS_TABLE_COMPRESSION, String.valueOf(tabletSchema.getCompressionType()));
        List<Field> fields = tabletSchema.getColumnList().stream()
                .map(BaseFormatTest::toArrowField).collect(Collectors.toList());
        return new Schema(fields, metadata);
    }

    public static Field toArrowField(TabletSchema.ColumnPB column) {
        ArrowType arrowType = ArrowUtils.toArrowType(
                column.getType(),
                TZ,
                column.getPrecision(),
                column.getFrac()
        );
        Map<String, String> metadata = new HashMap<>();
        metadata.put(ArrowUtils.STARROKCS_COLUMN_ID, String.valueOf(column.getUniqueId()));
        metadata.put(ArrowUtils.STARROKCS_COLUMN_TYPE, column.getType());
        metadata.put(ArrowUtils.STARROKCS_COLUMN_IS_KEY, String.valueOf(column.getIsKey()));
        metadata.put(ArrowUtils.STARROKCS_COLUMN_MAX_LENGTH, String.valueOf(column.getLength()));
        metadata.put(ArrowUtils.STARROKCS_COLUMN_AGGREGATION_TYPE,
                StringUtils.defaultIfBlank(column.getAggregation(), "NONE"));
        metadata.put(ArrowUtils.STARROKCS_COLUMN_IS_AUTO_INCREMENT, String.valueOf(column.getIsAutoIncrement()));

        List<Field> children = new ArrayList<>();
        if ("MAP".equals(column.getType())) {
            List<Field> mapChildren = new ArrayList<>();
            for (TabletSchema.ColumnPB child : column.getChildrenColumnsList()) {
                Field childField = toArrowField(child);
                mapChildren.add(childField);
            }
            Field childField = new Field("entries",
                    new FieldType(false, ArrowType.Struct.INSTANCE, null, metadata), mapChildren);
            children.add(childField);
        } else {
            for (TabletSchema.ColumnPB child : column.getChildrenColumnsList()) {
                Field childField = toArrowField(child);
                children.add(childField);
            }
        }
        return new Field(column.getName(), new FieldType(column.getIsNullable(), arrowType, null, metadata), children);
    }


    public static void setupTabletMeta(String tabletRootPath, TabletSchemaPB schema, long tabletId, long version)
            throws IOException {
        File dir = new File(tabletRootPath + "/data");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/log");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/meta");
        assertTrue(dir.mkdirs());

        LakeTypes.TabletMetadataPB metadata = LakeTypes.TabletMetadataPB.newBuilder()
                .setSchema(schema).build();

        File mf1 = new File(dir, String.format("%016x_%016x.meta", tabletId, version));
        try (FileOutputStream out = new FileOutputStream(mf1)) {
            metadata.writeTo(out);
            System.out.println("Write to meta file " + mf1.getAbsolutePath());
        }

        File mf2 = new File(dir, String.format("%016x_%016x.meta", tabletId, version + 1));
        try (FileOutputStream out = new FileOutputStream(mf2)) {
            metadata.writeTo(out);
            System.out.println("Write to meta file " + mf2.getAbsolutePath());
        }
    }

    public static void checkValue(VectorSchemaRoot vsr, long numRows) throws Exception {
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            int rowId = 0;
            for (int colIdx = 0; colIdx < vsr.getSchema().getFields().size(); colIdx++) {
                Field field = vsr.getSchema().getFields().get(colIdx);
                FieldVector fieldVector = vsr.getVector(colIdx);

                if ("rowid".equalsIgnoreCase(field.getName())) {
                    if (field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Utf8) {
                        rowId = Integer.parseInt(new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8));
                    } else {
                        rowId = ((IntVector) fieldVector).get(rowIdx);
                    }
                    break;
                }
            }

            for (int colIdx = 0; colIdx < vsr.getSchema().getFields().size(); colIdx++) {
                Field field = vsr.getSchema().getFields().get(colIdx);
                FieldVector fieldVector = vsr.getVector(colIdx);
                if ("rowid".equalsIgnoreCase(field.getName())) {
                    if (field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Utf8) {
                        rowId = Integer.parseInt(new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8));
                    } else {
                        rowId = ((IntVector) fieldVector).get(rowIdx);
                    }
                    continue;
                }
                if ("rowid2".equalsIgnoreCase(field.getName())) {
                    assertEquals(rowId, ((IntVector) fieldVector).get(rowIdx));
                    continue;
                }
                if (rowId == 2) {
                    assertTrue(fieldVector.isNull(rowIdx),
                            "column " + field.getName() + " row:" + rowId + " should be null."
                    );
                    continue;
                }
                assertFalse(fieldVector.isNull(rowIdx),
                        "column " + field.getName() + " row:" + rowId + " should not be null."
                );
                checkFieldValue(field, rowId, fieldVector, rowIdx, 0);
            }
        }
    }

    protected static void truncateTable(String tableName) throws Exception {
        executeSQL(String.format("TRUNCATE TABLE %s.%s", dbName, tableName));
    }

    protected static void queryTable(String tableName) throws Exception {
        String sql = String.format("SELECT * FROM %s.%s", dbName, tableName);
        System.out.println("Execute SQL: " + sql);
        try (PreparedStatement stat = dbConn.prepareStatement(sql)) {
            printResultSet(stat.executeQuery());
        }
    }

    protected static void printResultSet(ResultSet resultSet) throws Exception {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Print column names
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + "\t");
        }
        System.out.println();

        // Print rows
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
    }

    protected static void executeSQL(String srSql) throws Exception {
        System.out.println("Execute SQL: " + srSql);
        try (PreparedStatement statement = dbConn.prepareStatement(srSql)) {
            statement.execute();
        }
    }

    private static void checkFieldValue(Field field, int rowId, FieldVector fieldVector, int rowIdx, int depth) throws Exception {
        int sign = (rowId % 2 == 0) ? -1 : 1;
        String starRocksTypeName = field.getFieldType().getMetadata().get(ArrowUtils.STARROKCS_COLUMN_TYPE);
        assertFalse(StringUtils.isEmpty(starRocksTypeName),
                "column " + field.getName() + " 's starrocks type should not be null.");
        switch (starRocksTypeName) {
            case "BOOLEAN":
                if (rowId % 2 == 0) {
                    assertTrue(((BitVector) fieldVector).getObject(rowIdx),
                            "rowid: " + rowId + " value is wrong: " + ((BitVector) fieldVector).getObject(rowIdx));
                } else {
                    assertFalse(((BitVector) fieldVector).getObject(rowIdx),
                            "rowid: " + rowId + " value is wrong: " + ((BitVector) fieldVector).getObject(rowIdx));
                }
                break;
            case "TINYINT":
                if (rowId == 0) {
                    assertEquals(Byte.MAX_VALUE, ((TinyIntVector) fieldVector).get(rowIdx));
                } else if (rowId == 1) {
                    assertEquals(Byte.MIN_VALUE, ((TinyIntVector) fieldVector).get(rowIdx));
                } else {
                    byte value = (byte) (rowId * sign);
                    assertEquals(value, ((TinyIntVector) fieldVector).get(rowIdx));
                }
                break;
            case "SMALLINT":
                if (rowId == 0) {
                    assertEquals(Short.MAX_VALUE, ((SmallIntVector) fieldVector).get(rowIdx));
                } else if (rowId == 1) {
                    assertEquals(Short.MIN_VALUE, ((SmallIntVector) fieldVector).get(rowIdx));
                } else {
                    short value = (short) (rowId * 10 * sign);
                    assertEquals(value, ((SmallIntVector) fieldVector).get(rowIdx));
                }
                break;
            case "INT": {
                int intValue;
                if (rowId == 0) {
                    intValue = Integer.MAX_VALUE;
                } else if (rowId == 1) {
                    intValue = Integer.MIN_VALUE;
                } else {
                    intValue = rowId * 100 * sign + depth;
                }
                assertEquals(intValue, ((IntVector) fieldVector).get(rowIdx));
            }
            break;
            case "BIGINT":
                long longValue;
                if (rowId == 0) {
                    longValue = Long.MAX_VALUE;
                } else if (rowId == 1) {
                    longValue = Long.MIN_VALUE;
                } else {
                    longValue = rowId * 1000L * sign;
                }
                assertEquals(longValue, ((BigIntVector) fieldVector).get(rowIdx));
                break;
            case "LARGEINT":
                String largeIntVal;
                if (rowId == 0) {
                    largeIntVal = "99999999999999999999999999999999999999";
                } else if (rowId == 1) {
                    largeIntVal = "-99999999999999999999999999999999999999";
                } else {
                    largeIntVal = String.valueOf(rowId * 10000L * sign);
                }
                assertEquals(largeIntVal, new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8));
                break;
            case "FLOAT":
            case "DOUBLE":
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) field.getFieldType().getType();
                if (floatType.getPrecision() == FloatingPointPrecision.SINGLE) {
                    assertTrue(Math.abs(123.45678901234f * rowId * sign - ((Float4Vector) fieldVector).get(rowIdx)) < 0.0001);
                } else if (floatType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    assertTrue(Math.abs(23456.78901234 * rowId * sign - ((Float8Vector) fieldVector).get(rowIdx)) < 0.0001);
                } else {
                    throw new IllegalStateException("unsupported column type: " + field.getType());
                }
                break;
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getFieldType().getType();
                BigDecimal bd;
                if (rowId == 0) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("9999999.57");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("999999999999999.568");
                    } else {
                        bd = new BigDecimal("9999999999999999999999999999999999.5679");
                    }
                } else if (rowId == 1) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("-9999999.57");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("-999999999999999.568");
                    } else {
                        bd = new BigDecimal("-9999999999999999999999999999999999.5679");
                    }
                } else {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("12345.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("123456789012.56789");
                    } else {
                        bd = new BigDecimal("12345678901234567890123.56789");
                    }
                    bd = bd.multiply(BigDecimal.valueOf(rowId * sign)).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                }
                assertEquals(bd, ((DecimalVector) fieldVector).getObject(rowIdx), "check " + field.getName() + " value failed.");
                break;
            case "CHAR":
            case "VARCHAR":
                if (depth > 0) {
                    assertEquals(field.getName() + ":name" + rowId + ",d:" + depth,
                            new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8));
                } else {
                    assertEquals(field.getName() + ":name" + rowId,
                            new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8));
                }
                break;
            case "BINARY":
            case "VARBINARY":
                String valuePrefix = field.getName() + ":name" + rowId + ":";
                ByteBuffer buffer = ByteBuffer.allocate(valuePrefix.getBytes().length + 4);
                buffer.put(valuePrefix.getBytes());
                buffer.putInt(rowId);
                byte[] value = ((VarBinaryVector) fieldVector).get(rowIdx);
                assertTrue(areByteArraysEqual(buffer.array(), value));
                break;
            case "BITMAP":
                byte[] bitmapValue = ((VarBinaryVector) fieldVector).get(rowIdx);
                switch (rowId % 4) {
                    case 0:
                        assertTrue(areByteArraysEqual(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00}, bitmapValue));
                        break;
                    case 1:
                        assertTrue(areByteArraysEqual(new byte[] {0x01, (byte) 0xE8, 0x03, 0x00, 0x00}, bitmapValue));
                        break;
                    case 3:
                        assertTrue(areByteArraysEqual(new byte[] {0x1, (byte) 0xB8, 0xB, 0x0, 0x0}, bitmapValue));
                        break;
                }
                break;
            case "HLL":
                byte[] hllValue = ((VarBinaryVector) fieldVector).get(rowIdx);
                switch (rowId % 4) {
                    case 0:
                        assertTrue(areByteArraysEqual(new byte[] {0x00}, hllValue));
                        break;
                    case 1:
                        assertTrue(areByteArraysEqual(
                                new byte[] {0x1, 0x1, 0x44, 0x6, (byte) 0xC3, (byte) 0x80, (byte) 0x9E, (byte) 0x9D, (byte) 0xE6,
                                        0x14}, hllValue));
                        break;
                    case 3:
                        assertTrue(areByteArraysEqual(
                                new byte[] {0x1, 0x1, (byte) 0x9A, 0x5, (byte) 0xE4, (byte) 0xE6, 0x65, 0x76, 0x4, 0x28},
                                hllValue));
                        break;
                }
                break;
            case "JSON": {
                String rowStr = new String(((VarCharVector) fieldVector).get(rowIdx), StandardCharsets.UTF_8);
                Map<String, Object> resultMap = objMapper.readValue(rowStr, new TypeReference<Map<String, Object>>() {
                });
                assertEquals(Integer.valueOf(rowId), (Integer) resultMap.get("rowid"));
                boolean boolVal = rowId % 2 == 0;
                assertEquals(boolVal, resultMap.get("bool"));
                Integer intVal = 0;
                if (rowId == 0) {
                    intVal = Integer.MAX_VALUE;
                } else if (rowId == 1) {
                    intVal = Integer.MIN_VALUE;
                } else {
                    intVal = rowId * 100 * sign;
                }
                assertEquals(intVal, (Integer) resultMap.get("int"));
                assertEquals(field.getName() + ":name" + rowId, resultMap.get("varchar"));
            }
            break;
            case "DATE": {
                LocalDate dt;
                if (rowId == 0) {
                    dt = LocalDate.parse("1900-01-01");
                } else if (rowId == 1) {
                    dt = LocalDate.parse("4096-12-31");
                } else {
                    dt = LocalDate.parse("2023-10-31");
                    dt = dt.withYear(1900 + 123 + rowId * sign + depth);
                }
                if (fieldVector instanceof DateDayVector) {
                    assertEquals(dt, LocalDate.ofEpochDay(((DateDayVector) fieldVector).get(rowIdx)));
                } else if (fieldVector instanceof DateMilliVector) {
                    assertEquals(dt, LocalDate.ofEpochDay(((DateMilliVector) fieldVector).get(rowIdx) / 1000 / 24 / 60 / 60));
                } else {
                    throw new IllegalStateException("unsupported column type: " + field.getType());
                }
            }
            break;
            case "DATETIME":
                LocalDateTime ts;
                if (rowId == 0) {
                    ts = LocalDateTime.parse("1800-11-20T12:34:56");
                } else if (rowId == 1) {
                    ts = LocalDateTime.parse("4096-11-30T11:22:33");
                } else {
                    ts = LocalDateTime.parse("2023-12-30T22:33:44");
                    ts = ts.withYear(1900 + 123 + rowId * sign);
                }
                final long micros = ((TimeStampMicroTZVector) fieldVector).getObject(rowIdx);
                LocalDateTime dateTimeValue = DateUtility.getLocalDateTimeFromEpochMicro(micros,
                        ((ArrowType.Timestamp) (field.getFieldType().getType())).getTimezone());
                assertTrue(ts.until(dateTimeValue, ChronoUnit.SECONDS) <= 343,
                        "Expected: " + ts + " , Actual: " + dateTimeValue);
                break;
            case "ARRAY": {
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                assertEquals(1, children.size());
                int elementSize = (rowId + depth) % 4;
                int intVal = rowId * 100 * sign;
                Object arrayValue = ((ListVector) fieldVector).getObject(rowIdx);
                ArrayList<Integer> resultSet = (ArrayList<Integer>) arrayValue;
                for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                    assertEquals(intVal + depth + arrayIndex, resultSet.get(arrayIndex));
                }
            }
            break;
            case "MAP": {
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                assertEquals(1, children.size());
                int elementSize = rowId % 4;
                int intVal = rowId * 100 * sign;
                Object result = ((MapVector) fieldVector).getObject(rowIdx);
                ArrayList<?> resultSet = (ArrayList<?>) result;
                assertEquals(elementSize, resultSet.size());
                for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                    Map<?, ?> resultStruct = (Map<?, ?>) resultSet.get(arrayIndex);
                    assertEquals(intVal + depth + arrayIndex, getResultKey(resultStruct));
                    assertEquals("mapvalue:" + (intVal + depth + arrayIndex), getResultValue(resultStruct).toString());
                }
            }
            break;
            case "STRUCT": {
                List<FieldVector> children = ((StructVector) fieldVector).getChildrenFromFields();
                assertEquals(field.getChildren().size(), children.size());
                for (FieldVector childVector : children) {
                    checkFieldValue(childVector.getField(), rowId, childVector, rowIdx, depth + 1);
                }
            }
            break;
            default:
                throw new IllegalStateException("unsupported column type: " + field.getType());
        }
    }


    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=3 fill the base value * rowId * sign.
    protected static void fillSampleData(VectorSchemaRoot vsr, int startRowId, int numRows) throws Exception {
        for (int colIdx = 0; colIdx < vsr.getSchema().getFields().size(); colIdx++) {
            Field field = vsr.getSchema().getFields().get(colIdx);
            FieldVector fieldVector = vsr.getVector(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("rowid".equalsIgnoreCase(field.getName())) {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId);
                    continue;
                }
                if ("rowid2".equalsIgnoreCase(field.getName())) {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId);
                    continue;
                }
                if (rowId == 2) {
                    fieldVector.setNull(rowIdx);
                    if (CollectionUtils.isNotEmpty(fieldVector.getChildrenFromFields())) {
                        for (FieldVector childVector : fieldVector.getChildrenFromFields()) {
                            childVector.setNull(rowIdx);
                        }
                    }
                    continue;
                }
                fillField(field, rowId, fieldVector, rowIdx, 0);
            }
            fieldVector.setValueCount(numRows);
        }

        vsr.setRowCount(numRows);
    }

    private static void fillField(Field field, int rowId, FieldVector fieldVector, int rowIdx, int depth) throws Exception {
        String starRocksTypeName = field.getFieldType().getMetadata().get(ArrowUtils.STARROKCS_COLUMN_TYPE);
        int sign = (rowId % 2 == 0) ? -1 : 1;
        DataType dataType = DataType.elegantOf(starRocksTypeName).get();
        switch (dataType) {
            case BOOLEAN:
                ((BitVector) fieldVector).setSafe(rowIdx, 1 - (rowId % 2));
                break;
            case TINYINT:
                if (rowId == 0) {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, Byte.MAX_VALUE);
                } else if (rowId == 1) {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, Byte.MIN_VALUE);
                } else {
                    ((TinyIntVector) fieldVector).setSafe(rowIdx, rowId * sign);
                }
                break;
            case SMALLINT:
                if (rowId == 0) {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, Short.MAX_VALUE);
                } else if (rowId == 1) {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, Short.MIN_VALUE);
                } else {
                    ((SmallIntVector) fieldVector).setSafe(rowIdx, (short) (rowId * 10 * sign));
                }
                break;
            case INT:
                if (rowId == 0) {
                    ((IntVector) fieldVector).setSafe(rowIdx, Integer.MAX_VALUE);
                } else if (rowId == 1) {
                    ((IntVector) fieldVector).setSafe(rowIdx, Integer.MIN_VALUE);
                } else {
                    ((IntVector) fieldVector).setSafe(rowIdx, rowId * 100 * sign + depth);
                }
                break;
            case BIGINT:
                if (rowId == 0) {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, Long.MAX_VALUE);
                } else if (rowId == 1) {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, Long.MIN_VALUE);
                } else {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, rowId * 1000L * sign);
                }
                break;
            case LARGEINT:
                VarCharVector largeIntFieldVector = (VarCharVector) fieldVector;
                if (rowId == 0) {
                    largeIntFieldVector.setSafe(rowIdx,
                            "99999999999999999999999999999999999999".getBytes(StandardCharsets.UTF_8));
                } else if (rowId == 1) {
                    largeIntFieldVector.setSafe(rowIdx,
                            "-99999999999999999999999999999999999999".getBytes(StandardCharsets.UTF_8));
                } else {
                    largeIntFieldVector.setSafe(rowIdx, String.valueOf(rowId * 10000L * sign).getBytes(StandardCharsets.UTF_8));
                }
                break;
            case FLOAT:
                ((Float4Vector) fieldVector).setSafe(rowIdx, 123.45678901234f * rowId * sign);
                break;
            case DOUBLE:
                ((Float8Vector) fieldVector).setSafe(rowIdx, 23456.78901234 * rowId * sign);
                break;
            case DECIMAL:
                // decimal v2 type
                BigDecimal bdv2;
                if (rowId == 0) {
                    bdv2 = new BigDecimal("-12345678901234567890123.4567");
                } else if (rowId == 1) {
                    bdv2 = new BigDecimal("999999999999999999999999.9999");
                } else {
                    bdv2 = new BigDecimal("1234.56789");
                    bdv2 = bdv2.multiply(BigDecimal.valueOf(sign));
                }
                ((DecimalVector) fieldVector).setSafe(rowIdx, bdv2);
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getFieldType().getType();
                BigDecimal bd;
                if (rowId == 0) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("999999999999999.56789");
                    } else {
                        bd = new BigDecimal("9999999999999999999999999999999999.56789");
                    }
                } else if (rowId == 1) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("-9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("-999999999999999.56789");
                    } else {
                        bd = new BigDecimal("-9999999999999999999999999999999999.56789");
                    }
                } else {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("12345.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("123456789012.56789");
                    } else {
                        bd = new BigDecimal("12345678901234567890123.56789");
                    }
                    bd = bd.multiply(BigDecimal.valueOf((long) rowId * sign));
                }
                bd = bd.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                ((DecimalVector) fieldVector).setSafe(rowIdx, bd);
                break;
            case CHAR:
            case VARCHAR:
                String strValue = field.getName() + ":name" + rowId;
                if (depth > 0) {
                    strValue += ",d:" + depth;
                }
                ((VarCharVector) fieldVector).setSafe(rowIdx, strValue.getBytes());
                break;
            case OBJECT:
            case BITMAP:
                byte[] bitmapValue = new byte[] {0x00};
                switch (rowId % 4) {
                    case 0:
                        bitmapValue = new byte[] {0x01, 0x00, 0x00, 0x00, 0x00};
                        break;
                    case 1:
                        bitmapValue = new byte[] {0x01, (byte) 0xE8, 0x03, 0x00, 0x00};
                        break;
                    case 3:
                        bitmapValue = new byte[] {0x1, (byte) 0xB8, 0xB, 0x0, 0x0};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, bitmapValue);
                break;
            case HLL: {
                byte[] hllValue = new byte[] {0x00};
                switch (rowId % 4) {
                    case 0:
                        hllValue = new byte[] {0x00};
                        break;
                    case 1:
                        hllValue =
                                new byte[] {0x1, 0x1, 0x44, 0x6, (byte) 0xC3, (byte) 0x80, (byte) 0x9E, (byte) 0x9D, (byte) 0xE6,
                                        0x14};
                        break;
                    case 3:
                        hllValue = new byte[] {0x1, 0x1, (byte) 0x9A, 0x5, (byte) 0xE4, (byte) 0xE6, 0x65, 0x76, 0x4, 0x28};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, hllValue);
            }
            break;
            case BINARY:
            case VARBINARY:
                String valuePrefix = field.getName() + ":name" + rowId + ":";
                ByteBuffer buffer = ByteBuffer.allocate(valuePrefix.getBytes().length + 4);
                buffer.put(valuePrefix.getBytes());
                buffer.putInt(rowId);
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, buffer.array());
                break;
            case JSON: {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("rowid", rowId);
                boolean boolVal = rowId % 2 == 0;
                jsonMap.put("bool", boolVal);
                int intVal = 0;
                if (rowId == 0) {
                    intVal = Integer.MAX_VALUE;
                } else if (rowId == 1) {
                    intVal = Integer.MIN_VALUE;
                } else {
                    intVal = rowId * 100 * sign;
                }
                jsonMap.put("int", intVal);
                jsonMap.put("varchar", field.getName() + ":name" + rowId);
                String json = objMapper.writeValueAsString(jsonMap);
                ((VarCharVector) fieldVector).setSafe(rowIdx, json.getBytes(), 0, json.getBytes().length);
            }
            break;
            case DATE: {
                Date dt;
                if (rowId == 0) {
                    dt = Date.valueOf("1900-1-1");
                } else if (rowId == 1) {
                    dt = Date.valueOf("4096-12-31");
                } else {
                    dt = Date.valueOf("2023-10-31");
                    dt.setYear(123 + rowId * sign + depth);
                }
                if (fieldVector instanceof DateDayVector) {
                    ((DateDayVector) fieldVector).setSafe(rowIdx, (int) dt.toLocalDate().toEpochDay());
                } else if (fieldVector instanceof DateMilliVector) {
                    ((DateMilliVector) fieldVector).setSafe(rowIdx, dt.toLocalDate().toEpochDay() * 24 * 3600 * 1000);
                } else {
                    throw new IllegalStateException("unsupported column type: " + field.getType());
                }
            }
            break;
            case DATETIME:
                LocalDateTime ts;
                if (rowId == 0) {
                    ts = LocalDateTime.parse("1800-11-20T12:34:56");
                } else if (rowId == 1) {
                    ts = LocalDateTime.parse("4096-11-30T11:22:33");
                } else {
                    ts = LocalDateTime.parse("2023-12-30T22:33:44");
                    ts = ts.withYear(1900 + 123 + rowId * sign);
                }
                ((TimeStampMicroTZVector) fieldVector)
                        .setSafe(rowIdx, ts.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() * 1000L);
                break;
            case ARRAY: {
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                int elementSize = (rowId + depth) % 4;
                ((ListVector) fieldVector).startNewValue(rowIdx);
                for (FieldVector childVector : children) {
                    if (childVector instanceof IntVector) {
                        int intVal = rowId * 100 * sign;
                        int startOffset = childVector.getValueCount();
                        for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                            ((IntVector) childVector).setSafe(startOffset + arrayIndex, intVal + depth + arrayIndex);
                        }
                        childVector.setValueCount(startOffset + elementSize);
                    }
                }
                ((ListVector) fieldVector).endValue(rowIdx, elementSize);
            }
            break;
            case MAP: {
                int elementSize = rowId % 4;
                ((ListVector) fieldVector).startNewValue(rowIdx);
                UnionMapWriter mapWriter = ((MapVector) fieldVector).getWriter();
                mapWriter.setPosition(rowIdx);
                mapWriter.startMap();
                int intVal = rowId * 100 * sign;
                for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                    mapWriter.startEntry();
                    mapWriter.key().integer().writeInt(intVal + depth + arrayIndex);
                    mapWriter.value().varChar().writeVarChar("mapvalue:" + (intVal + depth + arrayIndex));
                    mapWriter.endEntry();
                }
                mapWriter.endMap();
            }
            break;
            case STRUCT: {
                List<FieldVector> children = ((StructVector) fieldVector).getChildrenFromFields();
                for (FieldVector childVector : children) {
                    fillField(childVector.getField(), rowId, childVector, rowIdx, depth + 1);
                }
                ((StructVector) fieldVector).setIndexDefined(rowIdx);
            }
            break;
            default:
                throw new IllegalStateException("unsupported column type: " + field.getType());
        }
    }

    public static String generateFixedLengthString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append('a');
        }
        return stringBuilder.toString();
    }

    protected static boolean areByteArraysEqual(byte[] array1, byte[] array2) {
        if (array1 == array2) {
            return true;
        }
        if (array1 == null || array2 == null || array1.length != array2.length) {
            return false;
        }
        for (int i = 0; i < array1.length; ++i) {
            if (array1[i] != array2[i]) {
                return false;
            }
        }
        return true;
    }

    public static <T> T getResultKey(Map<?, T> resultStruct) {
        assertTrue(resultStruct.containsKey(MapVector.KEY_NAME));
        return resultStruct.get(MapVector.KEY_NAME);
    }

    public static <T> T getResultValue(Map<?, T> resultStruct) {
        assertTrue(resultStruct.containsKey(MapVector.VALUE_NAME));
        return resultStruct.get(MapVector.VALUE_NAME);
    }

    protected static String toColName(DataType dataType) {
        return "c_" + dataType.getLiteral().toLowerCase();
    }

    protected static class ColumnType {

        private DataType dataType;
        private Integer length;
        private Integer precision;
        private Integer scale;

        public ColumnType(DataType dataType, Integer length) {
            this.dataType = dataType;
            this.length = length;
        }

        public ColumnType(DataType dataType, Integer length, Integer precision, Integer scale) {
            this.dataType = dataType;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        public DataType getDataType() {
            return dataType;
        }

        public void setDataType(DataType dataType) {
            this.dataType = dataType;
        }

        public Integer getLength() {
            return length;
        }

        public void setLength(Integer length) {
            this.length = length;
        }

        public Integer getPrecision() {
            return precision;
        }

        public void setPrecision(Integer precision) {
            this.precision = precision;
        }

        public Integer getScale() {
            return scale;
        }

        public void setScale(Integer scale) {
            this.scale = scale;
        }
    }

}
