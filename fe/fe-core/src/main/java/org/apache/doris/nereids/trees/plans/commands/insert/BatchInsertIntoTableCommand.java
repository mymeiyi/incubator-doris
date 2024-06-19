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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * insert into values with in txn model.
 */
public class BatchInsertIntoTableCommand extends Command implements NoForward, Explainable {

    public static final Logger LOG = LogManager.getLogger(BatchInsertIntoTableCommand.class);

    private LogicalPlan logicalQuery;

    public BatchInsertIntoTableCommand(LogicalPlan logicalQuery) {
        super(PlanType.BATCH_INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) throws Exception {
        return InsertUtils.getPlanForExplain(ctx, this.logicalQuery);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitBatchInsertIntoTableCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        UnboundTableSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) logicalQuery;
        Plan query = unboundTableSink.child();
        if (!(query instanceof LogicalInlineTable)) {
            throw new AnalysisException("Insert into ** select is not supported in a transaction");
        }

        TableIf targetTableIf = InsertUtils.getTargetTable(logicalQuery, ctx);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), targetTableIf.getDatabase().getCatalog().getName(),
                        targetTableIf.getDatabase().getFullName(), targetTableIf.getName(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    targetTableIf.getDatabase().getFullName() + ": " + targetTableIf.getName());
        }

        // reorder cols and values
        List<String> colNames = unboundTableSink.getColNames();
        List<List<NamedExpression>> constantExprsList = ((LogicalInlineTable) query).getConstantExprsList();
        LOG.info("sout: ---------------");
        LOG.info("sout: cols: {}, vals: {}", colNames, constantExprsList);
        if (!colNames.isEmpty()) {
            /*Pair<List<String>, List<List<NamedExpression>>> pair = reorder(targetTableIf.getBaseSchema(true), colNames,
                    constantExprsList);
            colNames = pair.first;
            constantExprsList = pair.second;
            List<String> newColNames = Lists.newArrayList("c1", "c2", "c3");
            unboundTableSink.getColNames().clear();
            unboundTableSink.getColNames().addAll(newColNames);*/
        }

        targetTableIf.readLock();
        try {
            this.logicalQuery = (LogicalPlan) InsertUtils.normalizePlan(logicalQuery, targetTableIf);
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
            executor.checkBlockRules();
            if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }

            Optional<TreeNode<?>> plan = planner.getPhysicalPlan()
                    .<TreeNode<?>>collect(PhysicalOlapTableSink.class::isInstance).stream().findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
            PhysicalOlapTableSink<?> sink = ((PhysicalOlapTableSink<?>) plan.get());
            Table targetTable = sink.getTargetTable();
            // should set columns of sink since we maybe generate some invisible columns
            List<Column> fullSchema = sink.getTargetTable().getFullSchema();
            List<Column> targetSchema = Lists.newArrayList();
            if (sink.isPartialUpdate()) {
                List<String> partialUpdateColumns = sink.getCols().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
                for (Column column : fullSchema) {
                    if (partialUpdateColumns.contains(column.getName())) {
                        targetSchema.add(column);
                    }
                }
            } else {
                targetSchema = fullSchema;
            }

            Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                    .<PhysicalUnion>collect(PhysicalUnion.class::isInstance).stream().findAny();
            if (union.isPresent()) {
                LOG.info("sout: sink_cols={}, target_cols={}, rows={}",
                        sink.getCols().stream().map(Column::getName).collect(Collectors.toList()),
                        targetSchema.stream().map(Column::getName).collect(Collectors.toList()),
                        union.get().getConstantExprsList());
                InsertUtils.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(),
                        targetTable.getName(), targetSchema, union.get().getConstantExprsList());
                // reorder
                List<List<NamedExpression>> valueExprs = reorderValueExprs(targetSchema, sink.getCols(),
                        union.get().getConstantExprsList());
                LOG.info("sout: after sort 2: {}", valueExprs);
                return;
            }
            Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                    .<PhysicalOneRowRelation>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
            if (oneRowRelation.isPresent()) {
                LOG.info("sout: sink_cols={}, target_cols={}, rows={}",
                        sink.getCols().stream().map(Column::getName).collect(Collectors.toList()),
                        targetSchema.stream().map(Column::getName).collect(Collectors.toList()),
                        oneRowRelation.get().getProjects());
                InsertUtils.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(),
                        targetTable.getName(), targetSchema, ImmutableList.of(oneRowRelation.get().getProjects()));
                List<List<NamedExpression>> valueExprs = reorderValueExprs(targetSchema, sink.getCols(),
                        ImmutableList.of(oneRowRelation.get().getProjects()));
                LOG.info("sout: after sort 3: {}", valueExprs);
                return;
            }
            // TODO: update error msg
            throw new AnalysisException("could not run this sql");
        } finally {
            targetTableIf.readUnlock();
        }
    }

    /**
     * target columns is the table column order: such as c0, c1, c2
     * sink columns and value expressions are in the input order: such as insert into t (c0, c2, c1) values(v0, v2, v1)
     */
    private List<List<NamedExpression>> reorderValueExprs(List<Column> targetColumns, List<Column> sinkColumns,
            List<List<NamedExpression>> valueExprs) {
        Preconditions.checkState(targetColumns.size() == sinkColumns.size(),
                "target columns: " + targetColumns + ", sink columns: " + sinkColumns);
        List<Integer> index = new ArrayList<>();
        for (Column targetColumn : targetColumns) {
            boolean found = false;
            for (int i = 0; i < sinkColumns.size(); i++) {
                if (formatColumnName(sinkColumns.get(i)).equalsIgnoreCase(formatColumnName(targetColumn))) {
                    index.add(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new AnalysisException(
                        "column " + targetColumn.getName() + " not found in sink columns " + sinkColumns);
            }
        }
        boolean needReorder = false;
        for (int i = 0; i < index.size(); i++) {
            if (index.get(i) != i) {
                needReorder = true;
                break;
            }
        }
        if (!needReorder) {
            return valueExprs;
        }
        List<List<NamedExpression>> newValueExprs = new ArrayList<>();
        for (List<NamedExpression> exprs : valueExprs) {
            List<NamedExpression> newExprs = new ArrayList<>();
            for (int i : index) {
                newExprs.add(exprs.get(i));
            }
            newValueExprs.add(newExprs);
        }
        LOG.info("sout: after sort 1: {}", newValueExprs);
        return newValueExprs;
    }

    private String formatColumnName(Column column) {
        if (column.getName().startsWith("`") && column.getName().endsWith("`")
                || column.getName().startsWith("\"") && column.getName().endsWith("\"")
                || column.getName().startsWith("'") && column.getName().endsWith("'")) {
            return column.getName().substring(1, column.getName().length() - 1);
        }
        return column.getName();
    }
}
