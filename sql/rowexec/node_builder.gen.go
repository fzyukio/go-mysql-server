// Copyright 2023 Dolthub, Inc.
//
// GENERATED
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowexec

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *BaseBuilder) buildNodeExec(ctx *sql.Context, n sql.Node, row sql.LazyRow) (sql.RowIter, error) {
	var iter sql.RowIter
	var err error
	if b.override != nil {
		iter, err = b.override.Build(ctx, n, row, nil)
	}
	if err != nil {
		return nil, err
	}
	if iter == nil {
		iter, err = b.buildNodeExecNoAnalyze(ctx, n, row)
	}
	if err != nil {
		return nil, err
	}
	b.indexer.qFlags.MaxExprCnt = b.indexer.idx
	if withDescribeStats, ok := n.(sql.WithDescribeStats); ok {
		iter = sql.NewCountingRowIter(iter, withDescribeStats)
	}
	return iter, nil
}

func (b *BaseBuilder) buildNodeExecNoAnalyze(ctx *sql.Context, n sql.Node, row sql.LazyRow) (iter sql.RowIter, err error) {
	switch n := n.(type) {
	case *plan.CreateForeignKey:
		iter, err = b.buildCreateForeignKey(ctx, n, row)
	case *plan.AlterTableCollation:
		iter, err = b.buildAlterTableCollation(ctx, n, row)
	case *plan.CreateRole:
		iter, err = b.buildCreateRole(ctx, n, row)
	case *plan.Loop:
		iter, err = b.buildLoop(ctx, n, row)
	case *plan.TransactionCommittingNode:
		iter, err = b.buildTransactionCommittingNode(ctx, n, row)
	case *plan.DropColumn:
		iter, err = b.buildDropColumn(ctx, n, row)
	case *plan.AnalyzeTable:
		iter, err = b.buildAnalyzeTable(ctx, n, row)
	case *plan.UpdateHistogram:
		iter, err = b.buildUpdateHistogram(ctx, n, row)
	case *plan.DropHistogram:
		iter, err = b.buildDropHistogram(ctx, n, row)
	case *plan.QueryProcess:
		iter, err = b.buildQueryProcess(ctx, n, row)
	case *plan.ShowBinlogs:
		iter, err = b.buildShowBinlogs(ctx, n, row)
	case *plan.ShowBinlogStatus:
		iter, err = b.buildShowBinlogStatus(ctx, n, row)
	case *plan.ShowReplicaStatus:
		iter, err = b.buildShowReplicaStatus(ctx, n, row)
	case *plan.UpdateSource:
		iter, err = b.buildUpdateSource(ctx, n, row)
	case plan.ElseCaseError:
		iter, err = b.buildElseCaseError(ctx, n, row)
	case *plan.PrepareQuery:
		iter, err = b.buildPrepareQuery(ctx, n, row)
	case *plan.ResolvedTable:
		iter, err = b.buildResolvedTable(ctx, n, row)
	case *plan.TableCountLookup:
		iter, err = b.buildTableCount(ctx, n, row)
	case *plan.ShowCreateTable:
		iter, err = b.buildShowCreateTable(ctx, n, row)
	case *plan.ShowIndexes:
		iter, err = b.buildShowIndexes(ctx, n, row)
	case *plan.PrependNode:
		iter, err = b.buildPrependNode(ctx, n, row)
	case *plan.UnresolvedTable:
		iter, err = b.buildUnresolvedTable(ctx, n, row)
	case *plan.Use:
		iter, err = b.buildUse(ctx, n, row)
	case *plan.CreateTable:
		iter, err = b.buildCreateTable(ctx, n, row)
	case *plan.CreateProcedure:
		iter, err = b.buildCreateProcedure(ctx, n, row)
	case *plan.CreateTrigger:
		iter, err = b.buildCreateTrigger(ctx, n, row)
	case *plan.IfConditional:
		iter, err = b.buildIfConditional(ctx, n, row)
	case *plan.ShowGrants:
		iter, err = b.buildShowGrants(ctx, n, row)
	case *plan.ShowDatabases:
		iter, err = b.buildShowDatabases(ctx, n, row)
	case *plan.UpdateJoin:
		iter, err = b.buildUpdateJoin(ctx, n, row)
	case *plan.Call:
		iter, err = b.buildCall(ctx, n, row)
	case *plan.Close:
		iter, err = b.buildClose(ctx, n, row)
	case *plan.Describe:
		iter, err = b.buildDescribe(ctx, n, row)
	case *plan.ExecuteQuery:
		iter, err = b.buildExecuteQuery(ctx, n, row)
	case *plan.ProcedureResolvedTable:
		iter, err = b.buildProcedureResolvedTable(ctx, n, row)
	case *plan.ShowTriggers:
		iter, err = b.buildShowTriggers(ctx, n, row)
	case *plan.BeginEndBlock:
		iter, err = b.buildBeginEndBlock(ctx, n, row)
	case *plan.AlterDB:
		iter, err = b.buildAlterDB(ctx, n, row)
	case *plan.Grant:
		iter, err = b.buildGrant(ctx, n, row)
	case *plan.Open:
		iter, err = b.buildOpen(ctx, n, row)
	case *plan.ChangeReplicationFilter:
		iter, err = b.buildChangeReplicationFilter(ctx, n, row)
	case *plan.StopReplica:
		iter, err = b.buildStopReplica(ctx, n, row)
	case *plan.ShowVariables:
		iter, err = b.buildShowVariables(ctx, n, row)
	case *plan.Sort:
		iter, err = b.buildSort(ctx, n, row)
	case *plan.SubqueryAlias:
		iter, err = b.buildSubqueryAlias(ctx, n, row)
	case *plan.SetOp:
		iter, err = b.buildSetOp(ctx, n, row)
	case *plan.IndexedTableAccess:
		iter, err = b.buildIndexedTableAccess(ctx, n, row)
	case *plan.TableAlias:
		iter, err = b.buildTableAlias(ctx, n, row)
	case *plan.AddColumn:
		iter, err = b.buildAddColumn(ctx, n, row)
	case *plan.RenameColumn:
		iter, err = b.buildRenameColumn(ctx, n, row)
	case *plan.DropDB:
		iter, err = b.buildDropDB(ctx, n, row)
	case *plan.Distinct:
		iter, err = b.buildDistinct(ctx, n, row)
	case *plan.Having:
		iter, err = b.buildHaving(ctx, n, row)
	case *plan.Signal:
		iter, err = b.buildSignal(ctx, n, row)
	case *plan.TriggerRollback:
		iter, err = b.buildTriggerRollback(ctx, n, row)
	case *plan.ExternalProcedure:
		iter, err = b.buildExternalProcedure(ctx, n, row)
	case *plan.Into:
		iter, err = b.buildInto(ctx, n, row)
	case *plan.LockTables:
		iter, err = b.buildLockTables(ctx, n, row)
	case *plan.Truncate:
		iter, err = b.buildTruncate(ctx, n, row)
	case *plan.DeclareHandler:
		iter, err = b.buildDeclareHandler(ctx, n, row)
	case *plan.DropProcedure:
		iter, err = b.buildDropProcedure(ctx, n, row)
	case *plan.ChangeReplicationSource:
		iter, err = b.buildChangeReplicationSource(ctx, n, row)
	case *plan.Max1Row:
		iter, err = b.buildMax1Row(ctx, n, row)
	case *plan.Rollback:
		iter, err = b.buildRollback(ctx, n, row)
	case *plan.Limit:
		iter, err = b.buildLimit(ctx, n, row)
	case *plan.RecursiveCte:
		iter, err = b.buildRecursiveCte(ctx, n, row)
	case *plan.ShowColumns:
		iter, err = b.buildShowColumns(ctx, n, row)
	case *plan.ShowTables:
		iter, err = b.buildShowTables(ctx, n, row)
	case *plan.ShowCreateDatabase:
		iter, err = b.buildShowCreateDatabase(ctx, n, row)
	case *plan.DropIndex:
		iter, err = b.buildDropIndex(ctx, n, row)
	case *plan.ResetReplica:
		iter, err = b.buildResetReplica(ctx, n, row)
	case *plan.ShowCreateTrigger:
		iter, err = b.buildShowCreateTrigger(ctx, n, row)
	case *plan.TableCopier:
		iter, err = b.buildTableCopier(ctx, n, row)
	case *plan.DeclareVariables:
		iter, err = b.buildDeclareVariables(ctx, n, row)
	case *plan.Filter:
		iter, err = b.buildFilter(ctx, n, row)
	case *plan.Kill:
		iter, err = b.buildKill(ctx, n, row)
	case *plan.ShowPrivileges:
		iter, err = b.buildShowPrivileges(ctx, n, row)
	case *plan.AlterPK:
		iter, err = b.buildAlterPK(ctx, n, row)
	case plan.Nothing:
		iter, err = b.buildNothing(ctx, n, row)
	case *plan.RevokeAll:
		iter, err = b.buildRevokeAll(ctx, n, row)
	case *plan.DeferredAsOfTable:
		iter, err = b.buildDeferredAsOfTable(ctx, n, row)
	case *plan.CreateUser:
		iter, err = b.buildCreateUser(ctx, n, row)
	case *plan.AlterUser:
		iter, err = b.buildAlterUser(ctx, n, row)
	case *plan.DropView:
		iter, err = b.buildDropView(ctx, n, row)
	case *plan.GroupBy:
		iter, err = b.buildGroupBy(ctx, n, row)
	case *plan.RowUpdateAccumulator:
		iter, err = b.buildRowUpdateAccumulator(ctx, n, row)
	case *plan.Block:
		iter, err = b.buildBlock(ctx, n, row)
	case *plan.InsertDestination:
		iter, err = b.buildInsertDestination(ctx, n, row)
	case *plan.Set:
		iter, err = b.buildSet(ctx, n, row)
	case *plan.TriggerExecutor:
		iter, err = b.buildTriggerExecutor(ctx, n, row)
	case *plan.AlterDefaultDrop:
		iter, err = b.buildAlterDefaultDrop(ctx, n, row)
	case *plan.CachedResults:
		iter, err = b.buildCachedResults(ctx, n, row)
	case *plan.CreateDB:
		iter, err = b.buildCreateDB(ctx, n, row)
	case *plan.CreateSchema:
		iter, err = b.buildCreateSchema(ctx, n, row)
	case *plan.Revoke:
		iter, err = b.buildRevoke(ctx, n, row)
	case *plan.DeclareCondition:
		iter, err = b.buildDeclareCondition(ctx, n, row)
	case *plan.TriggerBeginEndBlock:
		iter, err = b.buildTriggerBeginEndBlock(ctx, n, row)
	case *plan.RecursiveTable:
		iter, err = b.buildRecursiveTable(ctx, n, row)
	case *plan.AlterIndex:
		iter, err = b.buildAlterIndex(ctx, n, row)
	case *plan.TransformedNamedNode:
		iter, err = b.buildTransformedNamedNode(ctx, n, row)
	case *plan.CreateIndex:
		iter, err = b.buildCreateIndex(ctx, n, row)
	case *plan.Procedure:
		iter, err = b.buildProcedure(ctx, n, row)
	case *plan.NoopTriggerRollback:
		iter, err = b.buildNoopTriggerRollback(ctx, n, row)
	case *plan.With:
		iter, err = b.buildWith(ctx, n, row)
	case *plan.Project:
		iter, err = b.buildProject(ctx, n, row)
	case *plan.ModifyColumn:
		iter, err = b.buildModifyColumn(ctx, n, row)
	case *plan.DeclareCursor:
		iter, err = b.buildDeclareCursor(ctx, n, row)
	case *plan.OrderedDistinct:
		iter, err = b.buildOrderedDistinct(ctx, n, row)
	case *plan.SingleDropView:
		iter, err = b.buildSingleDropView(ctx, n, row)
	case *plan.EmptyTable:
		iter, err = b.buildEmptyTable(ctx, n, row)
	case *plan.JoinNode:
		iter, err = b.buildJoinNode(ctx, n, row)
	case *plan.RenameUser:
		iter, err = b.buildRenameUser(ctx, n, row)
	case *plan.ShowCreateProcedure:
		iter, err = b.buildShowCreateProcedure(ctx, n, row)
	case *plan.Commit:
		iter, err = b.buildCommit(ctx, n, row)
	case *plan.DeferredFilteredTable:
		iter, err = b.buildDeferredFilteredTable(ctx, n, row)
	case *plan.Values:
		iter, err = b.buildValues(ctx, n, row)
	case *plan.DropRole:
		iter, err = b.buildDropRole(ctx, n, row)
	case *plan.Fetch:
		iter, err = b.buildFetch(ctx, n, row)
	case *plan.RevokeRole:
		iter, err = b.buildRevokeRole(ctx, n, row)
	case *plan.ShowStatus:
		iter, err = b.buildShowStatus(ctx, n, row)
	case *plan.ShowTableStatus:
		iter, err = b.buildShowTableStatus(ctx, n, row)
	case *plan.ShowCreateEvent:
		iter, err = b.buildShowCreateEvent(ctx, n, row)
	case *plan.SignalName:
		iter, err = b.buildSignalName(ctx, n, row)
	case *plan.StartTransaction:
		iter, err = b.buildStartTransaction(ctx, n, row)
	case *plan.ValueDerivedTable:
		iter, err = b.buildValueDerivedTable(ctx, n, row)
	case *plan.CreateView:
		iter, err = b.buildCreateView(ctx, n, row)
	case *plan.InsertInto:
		iter, err = b.buildInsertInto(ctx, n, row)
	case *plan.TopN:
		iter, err = b.buildTopN(ctx, n, row)
	case *plan.Window:
		iter, err = b.buildWindow(ctx, n, row)
	case *plan.DropCheck:
		iter, err = b.buildDropCheck(ctx, n, row)
	case *plan.DropTrigger:
		iter, err = b.buildDropTrigger(ctx, n, row)
	case *plan.DeallocateQuery:
		iter, err = b.buildDeallocateQuery(ctx, n, row)
	case *plan.RollbackSavepoint:
		iter, err = b.buildRollbackSavepoint(ctx, n, row)
	case *plan.ReleaseSavepoint:
		iter, err = b.buildReleaseSavepoint(ctx, n, row)
	case *plan.Update:
		iter, err = b.buildUpdate(ctx, n, row)
	case plan.ShowWarnings:
		iter, err = b.buildShowWarnings(ctx, n, row)
	case *plan.Releaser:
		iter, err = b.buildReleaser(ctx, n, row)
	case *plan.Concat:
		iter, err = b.buildConcat(ctx, n, row)
	case *plan.DeleteFrom:
		iter, err = b.buildDeleteFrom(ctx, n, row)
	case *plan.DescribeQuery:
		iter, err = b.buildDescribeQuery(ctx, n, row)
	case *plan.ForeignKeyHandler:
		iter, err = b.buildForeignKeyHandler(ctx, n, row)
	case *plan.LoadData:
		iter, err = b.buildLoadData(ctx, n, row)
	case *plan.ShowCharset:
		iter, err = b.buildShowCharset(ctx, n, row)
	case *plan.StripRowNode:
		iter, err = b.buildStripRowNode(ctx, n, row)
	case *plan.DropConstraint:
		iter, err = b.buildDropConstraint(ctx, n, row)
	case *plan.FlushPrivileges:
		iter, err = b.buildFlushPrivileges(ctx, n, row)
	case *plan.Leave:
		iter, err = b.buildLeave(ctx, n, row)
	case *plan.While:
		iter, err = b.buildWhile(ctx, n, row)
	case *plan.ShowProcessList:
		iter, err = b.buildShowProcessList(ctx, n, row)
	case *plan.CreateSavepoint:
		iter, err = b.buildCreateSavepoint(ctx, n, row)
	case *plan.CreateCheck:
		iter, err = b.buildCreateCheck(ctx, n, row)
	case *plan.AlterDefaultSet:
		iter, err = b.buildAlterDefaultSet(ctx, n, row)
	case *plan.DropUser:
		iter, err = b.buildDropUser(ctx, n, row)
	case *plan.IfElseBlock:
		iter, err = b.buildIfElseBlock(ctx, n, row)
	case *plan.NamedWindows:
		iter, err = b.buildNamedWindows(ctx, n, row)
	case *plan.Repeat:
		iter, err = b.buildRepeat(ctx, n, row)
	case *plan.RevokeProxy:
		iter, err = b.buildRevokeProxy(ctx, n, row)
	case *plan.RenameTable:
		iter, err = b.buildRenameTable(ctx, n, row)
	case *plan.CaseStatement:
		iter, err = b.buildCaseStatement(ctx, n, row)
	case *plan.GrantRole:
		iter, err = b.buildGrantRole(ctx, n, row)
	case *plan.GrantProxy:
		iter, err = b.buildGrantProxy(ctx, n, row)
	case *plan.Offset:
		iter, err = b.buildOffset(ctx, n, row)
	case *plan.StartReplica:
		iter, err = b.buildStartReplica(ctx, n, row)
	case *plan.AlterAutoIncrement:
		iter, err = b.buildAlterAutoIncrement(ctx, n, row)
	case *plan.DropForeignKey:
		iter, err = b.buildDropForeignKey(ctx, n, row)
	case *plan.DropTable:
		iter, err = b.buildDropTable(ctx, n, row)
	case *plan.JSONTable:
		iter, err = b.buildJSONTable(ctx, n, row)
	case *plan.UnlockTables:
		iter, err = b.buildUnlockTables(ctx, n, row)
	case *plan.Exchange:
		iter, err = b.buildExchange(ctx, n, row)
	case *plan.ExchangePartition:
		iter, err = b.buildExchangePartition(ctx, n, row)
	case *plan.HashLookup:
		iter, err = b.buildHashLookup(ctx, n, row)
	case *plan.Iterate:
		iter, err = b.buildIterate(ctx, n, row)
	case sql.ExecSourceRel:
		// escape hatch for custom data sources
		iter, err = n.RowIter(ctx, row)
	case *plan.CreateSpatialRefSys:
		iter, err = b.buildCreateSpatialRefSys(ctx, n, row)
	case *plan.RenameForeignKey:
		iter, err = b.buildRenameForeignKey(ctx, n, row)
	default:
		return nil, fmt.Errorf("exec builder found unknown Node type %T", n)
	}
	iter = b.indexer.PostVisit(iter)
	return iter, err
}
