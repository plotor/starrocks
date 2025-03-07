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

package com.starrocks.common.util;

import org.apache.commons.lang3.StringUtils;

public class SqlUtils {

    private static final int SQL_PREFIX_LENGTH = 128;

    public static String escapeUnquote(String ident) {
        return ident.replaceAll("``", "`");
    }

    public static String getIdentSql(String ident) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (char ch : ident.toCharArray()) {
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }

    /**
     * Return the prefix of a sql if it's too long
     */
    public static String sqlPrefix(String sql) {
        if (StringUtils.isEmpty(sql) || sql.length() < SQL_PREFIX_LENGTH) {
            return sql;
        }
        return sql.substring(0, SQL_PREFIX_LENGTH) + "...";
    }
}
