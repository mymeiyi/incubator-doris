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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertRequest;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

// Used to generate a plan fragment for a group commit
// we only support OlapTable now.
public class GroupCommitPlanner {
    private static final Logger LOG = LogManager.getLogger(GroupCommitPlanner.class);
    public static final String SCHEMA_CHANGE = " is blocked on schema change";

    protected Database db;
    protected OlapTable table;
    protected TUniqueId loadId;
    protected Backend backend;
    public int baseSchemaVersion;
    private TPipelineFragmentParamsList paramsList;
    private ByteString execPlanFragmentParamsBytes;

    public GroupCommitPlanner(Database db, OlapTable table, List<String> targetColumnNames, TUniqueId queryId,
            String groupCommit)
            throws UserException, TException {
        this.db = db;
        this.table = table;
        this.baseSchemaVersion = table.getBaseSchemaVersion();
        if (Env.getCurrentEnv().getGroupCommitManager().isBlock(this.table.getId())) {
            String msg = "insert table " + this.table.getId() + SCHEMA_CHANGE;
            LOG.info(msg);
            throw new DdlException(msg);
        }
        TStreamLoadPutRequest streamLoadPutRequest = new TStreamLoadPutRequest();
        if (targetColumnNames != null) {
            streamLoadPutRequest.setColumns("`" + String.join("`,`", targetColumnNames) + "`");
            if (targetColumnNames.stream().anyMatch(col -> col.equalsIgnoreCase(Column.SEQUENCE_COL))) {
                streamLoadPutRequest.setSequenceCol(Column.SEQUENCE_COL);
            }
        }
        streamLoadPutRequest
                .setDb(db.getFullName())
                .setMaxFilterRatio(ConnectContext.get().getSessionVariable().enableInsertStrict ? 0
                        : ConnectContext.get().getSessionVariable().insertMaxFilterRatio)
                .setTbl(table.getName())
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(queryId)
                .setTrimDoubleQuotes(true).setGroupCommitMode(groupCommit)
                .setStrictMode(ConnectContext.get().getSessionVariable().enableInsertStrict);
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(streamLoadPutRequest);
        StreamLoadPlanner planner = new StreamLoadPlanner(db, table, streamLoadTask);
        // Will using load id as query id in fragment
        // TODO support pipeline
        TPipelineFragmentParams tRequest = planner.plan(streamLoadTask.getId());
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : tRequest.local_params.get(0)
                .per_node_scan_ranges.entrySet()) {
            for (TScanRangeParams scanRangeParams : entry.getValue()) {
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setFormatType(
                        TFileFormatType.FORMAT_PROTO);
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setCompressType(
                        TFileCompressType.PLAIN);
            }
        }
        List<TScanRangeParams> scanRangeParams = tRequest.local_params.get(0).per_node_scan_ranges.values().stream()
                .flatMap(Collection::stream).collect(Collectors.toList());
        Preconditions.checkState(scanRangeParams.size() == 1);
        loadId = queryId;
        // see BackendServiceProxy#execPlanFragmentsAsync
        paramsList = new TPipelineFragmentParamsList();
        paramsList.addToParamsList(tRequest);
        execPlanFragmentParamsBytes = ByteString.copyFrom(new TSerializer().serialize(paramsList));
    }

    public PGroupCommitInsertResponse executeGroupCommitInsert(ConnectContext ctx,
            List<InternalService.PDataRow> rows, boolean setReturnInfo)
            throws DdlException, RpcException, ExecutionException, InterruptedException {
        selectBackends(ctx);

        PGroupCommitInsertRequest request = PGroupCommitInsertRequest.newBuilder()
                .setExecPlanFragmentRequest(InternalService.PExecPlanFragmentRequest.newBuilder()
                        .setRequest(execPlanFragmentParamsBytes)
                        .setCompact(false).setVersion(InternalService.PFragmentRequestVersion.VERSION_3).build())
                .setLoadId(Types.PUniqueId.newBuilder().setHi(loadId.hi).setLo(loadId.lo)
                .build()).addAllData(rows)
                .build();
        Future<PGroupCommitInsertResponse> future = BackendServiceProxy.getInstance()
                .groupCommitInsert(new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), request);
        PGroupCommitInsertResponse response = future.get();
        if (setReturnInfo) {
            setReturnInfo(ctx, response);
        }
        return response;
    }

    protected void selectBackends(ConnectContext ctx) throws DdlException {
        try {
            backend = Env.getCurrentEnv().getGroupCommitManager()
                    .selectBackendForGroupCommit(this.table.getId(), ctx);
        } catch (LoadException e) {
            throw new DdlException("No suitable backend");
        }
    }

    public Backend getBackend() {
        return backend;
    }

    public InternalService.PDataRow getOneRow(List<Expr> row) throws UserException {
        InternalService.PDataRow data = StmtExecutor.getRowStringValue(row, FormatOptions.getDefault());
        if (LOG.isDebugEnabled()) {
            LOG.debug("add row: [{}]", data.getColList().stream().map(c -> c.getValue())
                    .collect(Collectors.joining(",")));
        }
        return data;
    }

    public List<InternalService.PDataRow> getRows(NativeInsertStmt stmt) throws UserException {
        List<InternalService.PDataRow> rows = new ArrayList<>();
        SelectStmt selectStmt = (SelectStmt) (stmt.getQueryStmt());
        if (selectStmt.getValueList() != null) {
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                rows.add(getOneRow(row));
            }
        } else {
            List<Expr> exprList = new ArrayList<>();
            for (Expr resultExpr : selectStmt.getResultExprs()) {
                if (resultExpr instanceof SlotRef) {
                    exprList.add(((SlotRef) resultExpr).getDesc().getSourceExprs().get(0));
                } else {
                    exprList.add(resultExpr);
                }
            }
            rows.add(getOneRow(exprList));
        }
        return rows;
    }

    private void setReturnInfo(ConnectContext ctx, PGroupCommitInsertResponse response) {
        String labelName = response.getLabel();
        TransactionStatus txnStatus = TransactionStatus.PREPARE;
        long txnId = response.getTxnId();
        long loadedRows = response.getLoadedRows();
        long filteredRows = (int) response.getFilteredRows();
        String errorUrl = response.getErrorUrl();
        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(labelName).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (table.getType() == TableType.MATERIALIZED_VIEW) {
            sb.append("', 'rows':'").append(loadedRows).append("'");
        }
        /*if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }*/
        if (!Strings.isNullOrEmpty(errorUrl)) {
            sb.append(", 'err_url':'").append(errorUrl).append("'");
        }
        sb.append("}");

        ctx.getState().setOk(loadedRows, (int) filteredRows, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, db.getFullName(), table.getName(),
                txnStatus, loadedRows, (int) filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }
}
