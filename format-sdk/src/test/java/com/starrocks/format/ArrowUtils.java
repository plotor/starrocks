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

import com.starrocks.format.rest.model.Column;
import com.starrocks.format.rest.model.MaterializedIndexMeta;
import com.starrocks.format.rest.model.TableSchema;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ArrowUtils {

    public static final String STARROKCS_TABLE_ID = "starrokcs.table.id";
    public static final String STARROKCS_TABLE_KEY_TYPE = "starrokcs.table.keyType";
    public static final String STARROKCS_TABLE_COMPRESSION = "starrokcs.compression";
    public static final String STARROKCS_TABLE_KEY_INDEX = "starrokcs.table.keyIndex";
    public static final String STARROKCS_TABLE_SHORT_KEY_NUM = "starrokcs.table.shortKeyNum";
    public static final String STARROKCS_TABLE_NUMROWS_PER_BLOCK = "starrokcs.table.numRowsPerBlock";
    public static final String STARROKCS_COLUMN_ID = "starrokcs.column.id";
    public static final String STARROKCS_COLUMN_TYPE = "starrokcs.column.type";
    public static final String STARROKCS_COLUMN_IS_KEY = "starrokcs.column.isKey";
    public static final String STARROKCS_COLUMN_MAX_LENGTH = "starrokcs.column.maxLength";
    public static final String STARROKCS_COLUMN_AGGREGATION_TYPE = "starrokcs.column.aggregationType";
    public static final String STARROKCS_COLUMN_IS_AUTO_INCREMENT = "starrokcs.column.IsAutoIncrement";

    public static Schema toArrowSchema(TableSchema tableSchema, ZoneId tz) {
        MaterializedIndexMeta indexMeta = tableSchema.getIndexMetas().get(0);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_TABLE_ID, String.valueOf(indexMeta.getIndexId()));
        metadata.put(STARROKCS_TABLE_KEY_TYPE, indexMeta.getKeysType());
        List<Field> fields = indexMeta.getColumns().stream()
                .map(column -> toArrowField(column, tz)).collect(Collectors.toList());
        return new Schema(fields, metadata);
    }

    public static Field toArrowField(Column column, ZoneId tz) {
        ArrowType arrowType = toArrowType(
                column.getType().getName(),
                tz,
                Optional.ofNullable(column.getType().getPrecision()).orElse(0),
                Optional.ofNullable(column.getType().getScale()).orElse(0)
        );
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_COLUMN_ID, String.valueOf(column.getUniqueId()));
        metadata.put(STARROKCS_COLUMN_TYPE, column.getType().getName());
        metadata.put(STARROKCS_COLUMN_IS_KEY, String.valueOf(column.getKey()));
        metadata.put(STARROKCS_COLUMN_MAX_LENGTH, String.valueOf(column.getType().getColumnSize()));
        metadata.put(STARROKCS_COLUMN_AGGREGATION_TYPE, StringUtils.defaultIfBlank(column.getAggregationType(), "NONE"));
        metadata.put(STARROKCS_COLUMN_IS_AUTO_INCREMENT, String.valueOf(column.getAutoIncrement()));

        List<Field> children = getChildren(column.getType(), tz);
        return new Field(
                column.getName(),
                new FieldType(Optional.ofNullable(column.getAllowNull()).orElse(false), arrowType, null, metadata),
                children
        );
    }

    public static Field toArrowField(String fieldName, Column.Type columnType, ZoneId tz) {
        ArrowType arrowType = toArrowType(
                columnType.getName(),
                tz,
                Optional.ofNullable(columnType.getPrecision()).orElse(0),
                Optional.ofNullable(columnType.getScale()).orElse(0)
        );
        Map<String, String> metadata = new HashMap<>();
        metadata.put(STARROKCS_COLUMN_TYPE, columnType.getName());
        metadata.put(STARROKCS_COLUMN_MAX_LENGTH, String.valueOf(columnType.getColumnSize()));
        List<Field> children = getChildren(columnType, tz);
        return new Field(fieldName, new FieldType(false, arrowType, null, metadata), children);
    }

    public static List<Field> getChildren(Column.Type columnType, ZoneId tz) {
        if (columnType.isScalar()) {
            return new ArrayList<>(0);
        }

        List<Field> children = new ArrayList<>();
        if (DataType.MAP.is(columnType.getName())) {
            Field keyField = toArrowField("key", columnType.getKeyType(), tz);
            Field valueField = toArrowField("value", columnType.getValueType(), tz);
            Map<String, String> metadata = new HashMap<>();
            metadata.put(STARROKCS_COLUMN_TYPE, "STRUCT");
            Field childField = new Field("entries",
                    new FieldType(false, ArrowType.Struct.INSTANCE, null, metadata), Arrays.asList(keyField, valueField));
            children.add(childField);
        } else if (DataType.ARRAY.is(columnType.getName())) {
            Field itemField = toArrowField("item1", columnType.getItemType(), tz);
            children.add(itemField);
        } else if (DataType.STRUCT.is(columnType.getName())) {
            for (Column child : columnType.getFields()) {
                Field childField = toArrowField(child, tz);
                children.add(childField);
            }
        }
        return children;
    }

    public static ArrowType toArrowType(String srType, ZoneId tz, int precision, int scale) {
        DataType dataType = DataType.of(srType);
        switch (dataType) {
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INT:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new ArrowType.Decimal(precision, scale, 128);
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case DATETIME:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, tz.getId());
            case CHAR:
            case VARCHAR:
            case JSON:
            case LARGEINT:
                return ArrowType.Utf8.INSTANCE;
            case BINARY:
            case VARBINARY:
            case OBJECT:
            case BITMAP:
            case HLL:
                return ArrowType.Binary.INSTANCE;
            case ARRAY:
                return ArrowType.List.INSTANCE;
            case STRUCT:
                return ArrowType.Struct.INSTANCE;
            case MAP:
                return new ArrowType.Map(false);
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

}
