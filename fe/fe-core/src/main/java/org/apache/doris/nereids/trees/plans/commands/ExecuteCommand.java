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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Exp;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapGroupCommitInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PDataRow;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PointQueryExecutor;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Prepared Statement
 */
public class ExecuteCommand extends Command {
    private static final Logger LOG = LogManager.getLogger(ExecuteCommand.class);

    private final String stmtName;
    private final PrepareCommand prepareCommand;
    private final StatementContext statementContext;

    public ExecuteCommand(String stmtName, PrepareCommand prepareCommand, StatementContext statementContext) {
        super(PlanType.EXECUTE_COMMAND);
        this.stmtName = stmtName;
        this.prepareCommand = prepareCommand;
        this.statementContext = statementContext;
    }

    public String getStmtName() {
        return stmtName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PreparedStatementContext preparedStmtCtx = ctx.getPreparedStementContext(stmtName);
        if (null == preparedStmtCtx) {
            throw new AnalysisException(
                    "prepare statement " + stmtName + " not found,  maybe expired");
        }
        PrepareCommand prepareCommand = (PrepareCommand) preparedStmtCtx.command;
        LogicalPlanAdapter planAdapter = new LogicalPlanAdapter(prepareCommand.getLogicalPlan(), executor.getContext()
                .getStatementContext());
        executor.setParsedStmt(planAdapter);
        // If it's not a short circuit query or schema version is different(indicates schema changed) or
        // has nondeterministic functions in statement, then need to do reanalyze and plan
        if (executor.getContext().getStatementContext().isShortCircuitQuery()
                && preparedStmtCtx.shortCircuitQueryContext.isPresent()
                && preparedStmtCtx.shortCircuitQueryContext.get().tbl.getBaseSchemaVersion()
                == preparedStmtCtx.shortCircuitQueryContext.get().schemaVersion && !executor.getContext()
                .getStatementContext().hasNondeterministic()) {
            PointQueryExecutor.directExecuteShortCircuitQuery(executor, preparedStmtCtx, statementContext);
            return;
        }
        OlapGroupCommitInsertExecutor.analyzeGroupCommit(ctx, prepareCommand);
        if (ctx.isGroupCommit()) {
            InsertIntoTableCommand command = (InsertIntoTableCommand) (prepareCommand.getLogicalPlan());
            OlapTable table = (OlapTable) command.getTable(ctx);
            GroupCommitPlanner groupCommitPlanner;
            if (preparedStmtCtx.groupCommitPlanner.isPresent()
                    && table.getBaseSchemaVersion() == preparedStmtCtx.groupCommitPlanner.get().baseSchemaVersion) {
                groupCommitPlanner = preparedStmtCtx.groupCommitPlanner.get();
            } else {
                List<String> targetColumnNames = command.getTargetColumns();
                if (targetColumnNames != null && targetColumnNames.isEmpty()) {
                    targetColumnNames = null;
                }
                groupCommitPlanner = EnvFactory.getInstance()
                        .createGroupCommitPlanner((Database) table.getDatabase(), table,
                                targetColumnNames, ctx.queryId(),
                                ConnectContext.get().getSessionVariable().getGroupCommit());
            }
            Map<PlaceholderId, Expr> colNameToConjunct = Maps.newTreeMap();
            for (Entry<PlaceholderId, Expression> entry : statementContext.getIdToPlaceholderRealExpr()
                    .entrySet()) {
                Expr conjunctVal = ((Literal) entry.getValue()).toLegacyLiteral();
                colNameToConjunct.put(entry.getKey(), conjunctVal);
            }
            PDataRow oneRow = groupCommitPlanner.getOneRow(
                    colNameToConjunct.values().stream().collect(Collectors.toList()));
            List<InternalService.PDataRow> rows = Lists.newArrayList(oneRow);
            PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(ctx, rows);
            LOG.info("sout: Group commit response: {}", response);
            return;
        }
        // execute real statement
        preparedStmtCtx.shortCircuitQueryContext = Optional.empty();
        statementContext.setShortCircuitQueryContext(null);
        executor.execute();
        if (executor.getContext().getStatementContext().isShortCircuitQuery()) {
            // cache short-circuit plan
            preparedStmtCtx.shortCircuitQueryContext = Optional.of(
                    new ShortCircuitQueryContext(executor.planner(), (Queriable) executor.getParsedStmt()));
            statementContext.setShortCircuitQueryContext(preparedStmtCtx.shortCircuitQueryContext.get());
        }
    }

    /**
     * return the sql representation contains real expr instead of placeholders
     */
    public String toSql() {
        // maybe slow
        List<Expression> realValueExpr = prepareCommand.getPlaceholders().stream()
                .map(placeholder -> statementContext.getIdToPlaceholderRealExpr().get(placeholder.getPlaceholderId()))
                .collect(Collectors.toList());
        return "EXECUTE `" + stmtName + "`"
                + realValueExpr.stream().map(Expression::toSql).collect(Collectors.joining(", ", " USING ", ""));
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXECUTE;
    }
}
