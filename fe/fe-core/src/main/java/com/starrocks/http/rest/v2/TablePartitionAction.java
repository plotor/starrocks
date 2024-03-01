// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest.v2;


import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.v2.RestBaseResultV2.PagedResult;
import com.starrocks.http.rest.v2.vo.TablePartitionView;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static com.starrocks.sql.analyzer.Authorizer.checkTableAction;

public class TablePartitionAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(TablePartitionAction.class);

    private static final String TEMPORARY_KEY = "temporary";

    public TablePartitionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(
                HttpMethod.GET,
                String.format(
                        "/api/v2/catalogs/{%s}/databases/{%s}/tables/{%s}/partition", CATALOG_KEY, DB_KEY, TABLE_KEY),
                new TablePartitionAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request,
                                          BaseResponse response) throws DdlException, AccessDeniedException {
        try {
            this.doExecuteWithoutPassword(request, response);
        } catch (StarRocksHttpException e) {
            HttpResponseStatus status = e.getCode();
            sendResult(request, response, status, new RestBaseResultV2<>(status.code(), e.getMessage()));
        } catch (Exception e) {
            LOG.error("Get table[{}/{}/{}] partition error.",
                    request.getSingleParameter(CATALOG_KEY),
                    request.getSingleParameter(DB_KEY),
                    request.getSingleParameter(TABLE_KEY),
                    e);
            HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            sendResult(request, response, status, new RestBaseResultV2<>(status.code(), e.getMessage()));
        }
    }

    private void doExecuteWithoutPassword(BaseRequest request, BaseResponse response) throws AccessDeniedException {
        String catalogName = getParameter(
                request, CATALOG_KEY, DEFAULT_INTERNAL_CATALOG_NAME, value -> value);
        String dbName = getParameter(request, DB_KEY);
        String tableName = getParameter(request, TABLE_KEY);
        boolean temporary = getParameter(request, TEMPORARY_KEY, false, BooleanUtils::toBoolean);
        int pageNum = getPageNum(request);
        int pageSize = getPageSize(request);

        PagedResult<TablePartitionView> pagedResult = new PagedResult<>();
        pagedResult.setPageNum(pageNum);
        pagedResult.setPageSize(pageSize);
        pagedResult.setPages(0);
        pagedResult.setTotal(0);
        pagedResult.setItems(new ArrayList<>(0));

        OlapTable olapTable = this.getOlapTable(catalogName, dbName, tableName);
        Collection<Partition> partitions =
                temporary ? olapTable.getTempPartitions() : olapTable.getPartitions();
        if (CollectionUtils.isNotEmpty(partitions)) {
            int pages = partitions.size() / pageSize;
            pagedResult.setPages(partitions.size() % pageSize == 0 ? pages : (pages + 1));
            pagedResult.setTotal(partitions.size());
            pagedResult.setItems(partitions.stream()
                    .sorted(Comparator.comparingLong(Partition::getId))
                    .skip((long) pageNum * pageSize)
                    .limit(pageSize)
                    .map(partition ->
                            TablePartitionView.createFrom(olapTable.getPartitionInfo(), partition))
                    .collect(Collectors.toList())
            );
        }

        sendResult(request, response, RestBaseResultV2.ok(pagedResult));
    }

    private OlapTable getOlapTable(String catalogName, String dbName, String tableName) throws AccessDeniedException {
        // check privilege for select, otherwise return 401 HTTP status
        checkTableAction(
                ConnectContext.get().getCurrentUserIdentity(),
                ConnectContext.get().getCurrentRoleIds(),
                catalogName,
                dbName,
                tableName,
                PrivilegeType.SELECT
        );

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (null == db || !Optional.ofNullable(db.getCatalogName())
                .orElse(DEFAULT_INTERNAL_CATALOG_NAME).equalsIgnoreCase(catalogName)) {
            throw new StarRocksHttpException(
                    HttpResponseStatus.NOT_FOUND,
                    String.format("Database[%s] does not exist in catalog %s", dbName, catalogName)
            );
        }

        final Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(tableName);
            if (null == table) {
                throw new StarRocksHttpException(
                        HttpResponseStatus.NOT_FOUND,
                        String.format("Table[%s.%s] does not exist in catalog %s", dbName, tableName, catalogName)
                );
            }

            // just only support OlapTable currently
            if (!(table instanceof OlapTable)) {
                throw new StarRocksHttpException(
                        HttpResponseStatus.FORBIDDEN,
                        String.format(
                                "Table[%s.%s] is a %s, only support olap table currently",
                                dbName, tableName, table.getClass().getSimpleName())
                );
            }

            return (OlapTable) table;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }


}
