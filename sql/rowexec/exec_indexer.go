package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func newIndexer() *Indexer {
	return &Indexer{}
}

type Indexer struct {
	idx int
	// exprId to output row index
	mapp       map[sql.ColumnId]int
	lastOffset int
	qFlags     *sql.QueryFlags
	lastErr    error
}

func (i *Indexer) Init(qf *sql.QueryFlags) {
	i.idx = 0
	i.lastErr = nil
	i.qFlags = qf
	i.mapp = make(map[sql.ColumnId]int)
}

// add offset to row iterators afte rthet are made
func (i *Indexer) PostVisit(iter sql.RowIter) sql.RowIter {
	if offIter, ok := iter.(sql.OffsetIter); ok {
		iter = offIter.WithOffset(i.lastOffset)
	}
	return iter
}

// we need to fix indexes before we build the row iterators
// DFS up the tree, but called before making the iterator
func (i *Indexer) PreVisit(n sql.Node) (sql.Node, error) {
	// no harm extending mapping ahead of where current node can access
	switch n := n.(type) {
	case *plan.Values:
		for j := range n.ExpressionTuples[0] {
			i.mapp[sql.ColumnId(j+1)] = i.idx
			i.idx++
		}
	case plan.TableIdNode:
		i.lastOffset = i.idx
		tableColSet(n).ForEach(func(col sql.ColumnId) {
			i.mapp[col] = i.idx
			i.idx++
		})
	case *plan.Project:
		i.lastOffset = i.idx
		// case plan.JoinNode:
		for _, p := range n.Projections {
			if idExpr, ok := p.(sql.IdExpression); ok {
				if _, ok := i.mapp[idExpr.Id()]; !ok && idExpr.Id() > 0 {
					i.mapp[idExpr.Id()] = i.idx
				}
				i.idx++
			}
		}
	case *plan.GroupBy:
		i.lastOffset = i.idx
		for _, p := range n.SelectedExprs {
			if idExpr, ok := p.(sql.IdExpression); ok {
				i.mapp[idExpr.Id()] = i.idx
				i.idx++
			}
		}
	case *plan.Window:
		i.lastOffset = i.idx
		for _, p := range n.SelectExprs {
			if idExpr, ok := p.(sql.IdExpression); ok {
				i.mapp[idExpr.Id()] = i.idx
				i.idx++
			}
		}
	}

	if nn, ok := n.(sql.Expressioner); ok {
		// fix indexes
		newExprs, _, err := transform.Exprs(nn.Expressions(), func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if gf, ok := e.(*expression.GetField); ok {
				ret, err := expression.WithExecIndexReference(gf, i.mapp)
				if err != nil {
					return e, transform.SameTree, err
				}
				return ret, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		})
		if err != nil {
			return nil, err
		}
		return nn.WithExpressions(newExprs...)
	}

	return n, nil
}

func tableColSet(n plan.TableIdNode) sql.ColSet {
	if len(n.Schema()) == n.Columns().Len() {
		return n.Columns()
	}
	var tw sql.TableNode
	var ok bool
	for tw, ok = n.(sql.TableNode); !ok; tw, ok = n.Children()[0].(sql.TableNode) {
	}

	var sch sql.Schema
	switch n := tw.UnderlyingTable().(type) {
	case sql.PrimaryKeyTable:
		sch = n.PrimaryKeySchema().Schema
	default:
		sch = n.Schema()
	}
	firstCol, _ := n.Columns().Next(1)

	var colset sql.ColSet
	for _, c := range n.Schema() {
		i := sch.IndexOfColName(c.Name)
		colset.Add(firstCol + sql.ColumnId(i))
	}
	return colset
}
