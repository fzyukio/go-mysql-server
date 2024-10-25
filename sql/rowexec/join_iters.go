// Copyright 2020-2021 Dolthub, Inc.
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
	"errors"
	"fmt"
	"io"
	"reflect"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func newJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.LazyRow) (sql.RowIter, error) {
	var leftName, rightName string
	if leftTable, ok := j.Left().(sql.Nameable); ok {
		leftName = leftTable.Name()
	} else {
		leftName = reflect.TypeOf(j.Left()).String()
	}

	if rightTable, ok := j.Right().(sql.Nameable); ok {
		rightName = rightTable.Name()
	} else {
		rightName = reflect.TypeOf(j.Right()).String()
	}

	span, ctx := ctx.Span("plan.joinIter", trace.WithAttributes(
		attribute.String("left", leftName),
		attribute.String("right", rightName),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &joinIter{
		parentRow:         row,
		primary:           l,
		secondaryProvider: j.Right(),
		cond:              j.Filter,
		joinType:          j.Op,
		rowSize:           row.Count() + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen:          j.ScopeLen,
		b:                 b,
	}), nil
}

// joinIter is an iterator that iterates over every row in the primary table and performs an index lookup in
// the secondary table for each value
type joinIter struct {
	parentRow         sql.LazyRow
	primary           sql.RowIter
	primaryRow        sql.LazyRow
	secondaryProvider sql.Node
	secondary         sql.RowIter
	cond              sql.Expression
	joinType          plan.JoinType

	foundMatch bool
	rowSize    int
	scopeLen   int
	b          sql.NodeExecBuilder
}

func (i *joinIter) loadPrimary(ctx *sql.Context) error {
	if i.primaryRow == nil {
		err := i.primary.Next(ctx, nil)
		if err != nil {
			return err
		}

		// TODO fix join indexing
		//i.primaryRow = i.parentRow.Append(r)
		i.foundMatch = false
	}

	return nil
}

func (i *joinIter) loadSecondary(ctx *sql.Context) (sql.LazyRow, error) {
	if i.secondary == nil {
		rowIter, err := i.b.Build(ctx, i.secondaryProvider, i.primaryRow)

		if err != nil {
			return nil, err
		}
		if plan.IsEmptyIter(rowIter) {
			return nil, plan.ErrEmptyCachedResult
		}
		i.secondary = rowIter
	}

	// TODO fix
	err := i.secondary.Next(ctx, i.primaryRow)
	if err != nil {
		if err == io.EOF {
			err = i.secondary.Close(ctx)
			i.secondary = nil
			if err != nil {
				return nil, err
			}
			i.primaryRow = nil
			return nil, io.EOF
		}
		return nil, err
	}

	// TODO fix
	return i.primaryRow, nil
}

func (i *joinIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	for {
		if err := i.loadPrimary(ctx); err != nil {
			return err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					i.primaryRow = nil
					i.buildRow(row, primary)
					return nil
				}
				continue
			} else if errors.Is(err, plan.ErrEmptyCachedResult) {
				if !i.foundMatch && i.joinType.IsLeftOuter() {
					i.primaryRow = nil
					i.buildRow(row, primary)
					return nil
				}

				return io.EOF
			}
			return err
		}

		i.buildRow(row, secondary)
		i.buildRow(row, primary)
		res, err := sql.EvaluateCondition(ctx, i.cond, row)
		if err != nil {
			return err
		}

		if res == nil && i.joinType.IsExcludeNulls() {
			err = i.secondary.Close(ctx)
			i.secondary = nil
			if err != nil {
				return err
			}
			i.primaryRow = nil
			continue
		}

		if !sql.IsTrue(res) {
			continue
		}

		i.foundMatch = true
		return nil
	}
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func (i *joinIter) buildRow(target, primary sql.LazyRow) {
	off := 0
	for i, v := range primary.SqlValues() {
		// TODO offset
		target.SetSqlValue(off+i, v)
	}
}

func (i *joinIter) Close(ctx *sql.Context) (err error) {
	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			if i.secondary != nil {
				_ = i.secondary.Close(ctx)
			}
			return err
		}
	}

	if i.secondary != nil {
		err = i.secondary.Close(ctx)
		i.secondary = nil
	}

	return err
}

func newExistsIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.LazyRow) (sql.RowIter, error) {
	leftIter, err := b.Build(ctx, j.Left(), row)

	if err != nil {
		return nil, err
	}
	return &existsIter{
		parentRow:         row,
		typ:               j.Op,
		primary:           leftIter,
		secondaryProvider: j.Right(),
		cond:              j.Filter,
		scopeLen:          j.ScopeLen,
		rowSize:           row.Count() + len(j.Left().Schema()) + len(j.Right().Schema()),
		nullRej:           !(j.Filter != nil && plan.IsNullRejecting(j.Filter)),
		b:                 b,
	}, nil
}

type existsIter struct {
	typ               plan.JoinType
	primary           sql.RowIter
	secondaryProvider sql.Node
	cond              sql.Expression

	primaryRow sql.Row

	parentRow         sql.LazyRow
	scopeLen          int
	rowSize           int
	nullRej           bool
	rightIterNonEmpty bool
	b                 sql.NodeExecBuilder
}

type existsState uint8

const (
	esIncLeft existsState = iota
	esIncRight
	esRightIterEOF
	esCompare
	esRejectNull
	esRet
)

func (i *existsIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	var right sql.LazyRow
	var left sql.LazyRow
	var rIter sql.RowIter
	var err error

	// the common sequence is: LOAD_LEFT -> LOAD_RIGHT -> COMPARE -> RET
	// notable exceptions are represented as goto jumps:
	//  - non-null rejecting filters jump to COMPARE with a nil right row
	//    when the secondaryProvider is empty
	//  - antiJoin succeeds to RET when LOAD_RIGHT EOF's
	//  - semiJoin fails when LOAD_RIGHT EOF's, falling back to LOAD_LEFT
	//  - antiJoin fails when COMPARE returns true, falling back to LOAD_LEFT
	nextState := esIncLeft
	for {
		switch nextState {
		case esIncLeft:
			err := i.primary.Next(ctx, nil)
			if err != nil {
				return err
			}
			// TODO offset
			buildRow(0, row, i.parentRow)
			rIter, err = i.b.Build(ctx, i.secondaryProvider, row)
			if err != nil {
				return err
			}
			if plan.IsEmptyIter(rIter) {
				if i.nullRej || i.typ.IsAnti() {
					return io.EOF
				}
				nextState = esCompare
			} else {
				nextState = esIncRight
			}
		case esIncRight:
			err = rIter.Next(ctx, nil)
			if err != nil {
				iterErr := rIter.Close(ctx)
				if iterErr != nil {
					return fmt.Errorf("%w; error on close: %s", err, iterErr)
				}
				if errors.Is(err, io.EOF) {
					nextState = esRightIterEOF
				} else {
					return err
				}
			} else {
				i.rightIterNonEmpty = true
				nextState = esCompare
			}
		case esRightIterEOF:
			if i.typ.IsSemi() {
				// reset iter, no match
				nextState = esIncLeft
			} else {
				nextState = esRet
			}
		case esCompare:
			buildRow(0, row, left)
			buildRow(left.Count(), row, right)
			res, err := sql.EvaluateCondition(ctx, i.cond, row)
			if err != nil {
				return err
			}

			if res == nil && i.typ.IsExcludeNulls() {
				nextState = esRejectNull
				continue
			}

			if !sql.IsTrue(res) {
				nextState = esIncRight
			} else {
				err = rIter.Close(ctx)
				if err != nil {
					return err
				}
				if i.typ.IsAnti() {
					// reset iter, found match -> no return row
					nextState = esIncLeft
				} else {
					nextState = esRet
				}
			}
		case esRejectNull:
			if i.typ.IsAnti() {
				nextState = esIncLeft
			} else {
				nextState = esIncRight
			}
		case esRet:
			return nil
		default:
			return fmt.Errorf("invalid exists join state")
		}
	}
}

func isTrueLit(e sql.Expression) bool {
	if lit, ok := e.(*expression.Literal); ok {
		return lit.Value() == true
	}
	return false
}

// buildRow builds the result set row using the rows from the primary and secondary tables
func buildRow(offset int, target, other sql.LazyRow) {
	for i, v := range other.SqlValues() {
		target.SetSqlValue(offset+i, v)
	}
	return
}

func (i *existsIter) Close(ctx *sql.Context) (err error) {
	if i.primary != nil {
		if err = i.primary.Close(ctx); err != nil {
			return err
		}
	}
	return err
}

func newFullJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.LazyRow) (sql.RowIter, error) {
	leftIter, err := b.Build(ctx, j.Left(), row)

	if err != nil {
		return nil, err
	}
	return &fullJoinIter{
		parentRow: row,
		l:         leftIter,
		rp:        j.Right(),
		cond:      j.Filter,
		scopeLen:  j.ScopeLen,
		rowSize:   row.Count() + len(j.Left().Schema()) + len(j.Right().Schema()),
		seenLeft:  make(map[uint64]struct{}),
		seenRight: make(map[uint64]struct{}),
		b:         b,
	}, nil
}

// fullJoinIter implements full join as a union of left and right join:
// FJ(A,B) => U(LJ(A,B), RJ(A,B)). The current algorithm will have a
// runtime and memory complexity O(m+n).
type fullJoinIter struct {
	l    sql.RowIter
	rp   sql.Node
	b    sql.NodeExecBuilder
	r    sql.RowIter
	cond sql.Expression

	parentRow sql.LazyRow
	leftRow   sql.LazyRow
	scopeLen  int
	rowSize   int

	leftDone  bool
	seenLeft  map[uint64]struct{}
	seenRight map[uint64]struct{}
}

func (i *fullJoinIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	for {
		if i.leftDone {
			break
		}
		if i.leftRow == nil {
			err := i.l.Next(ctx, row)
			if errors.Is(err, io.EOF) {
				i.leftDone = true
				i.l = nil
				i.r = nil
			}
			if err != nil {
				return err
			}

			i.leftRow = row
		}

		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return err
			}
			i.r = iter
		}

		// todo: fix size
		rightRow := sql.NewSqlRow(0)
		err := i.r.Next(ctx, rightRow)
		if err == io.EOF {
			key, err := sql.HashOf(i.leftRow)
			if err != nil {
				return err
			}
			if _, ok := i.seenLeft[key]; !ok {
				// (left, null) only if we haven't matched left
				buildRow(0, i.leftRow, nil)
				i.r = nil
				i.leftRow = nil
				return nil
			}
			i.r = nil
			i.leftRow = nil
		}

		buildRow(0, i.leftRow, rightRow)
		matches, err := sql.EvaluateCondition(ctx, i.cond, row)
		if err != nil {
			return err
		}
		if !sql.IsTrue(matches) {
			continue
		}
		rkey, err := sql.HashOf(rightRow)
		if err != nil {
			return err
		}
		i.seenRight[rkey] = struct{}{}
		lKey, err := sql.HashOf(i.leftRow)
		if err != nil {
			return err
		}
		i.seenLeft[lKey] = struct{}{}
		return nil
	}

	for {
		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return err
			}

			i.r = iter
		}

		// todo fix size
		rightRow := sql.NewSqlRow(0)
		err := i.r.Next(ctx, nil)
		if errors.Is(err, io.EOF) {
			err := i.r.Close(ctx)
			if err != nil {
				return err
			}
			return io.EOF
		}

		key, err := sql.HashOf(rightRow)
		if err != nil {
			return err
		}
		if _, ok := i.seenRight[key]; ok {
			continue
		}
		// (null, right) only if we haven't matched right
		//buildRow(0, nil, rightRow)
		return nil
	}
}

func (i *fullJoinIter) Close(ctx *sql.Context) (err error) {
	if i.l != nil {
		err = i.l.Close(ctx)
	}

	if i.r != nil {
		if err == nil {
			err = i.r.Close(ctx)
		} else {
			i.r.Close(ctx)
		}
	}

	return err
}

func newCrossJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.LazyRow) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := j.Left().(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(j.Left()).String()
	}

	if rightTable, ok := j.Right().(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(j.Right()).String()
	}

	span, ctx := ctx.Span("plan.CrossJoin", trace.WithAttributes(
		attribute.String("left", left),
		attribute.String("right", right),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		b:         b,
		parentRow: row,
		l:         l,
		rp:        j.Right(),
		rowSize:   row.Count() + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen:  j.ScopeLen,
	}), nil
}

type crossJoinIterator struct {
	l  sql.RowIter
	r  sql.RowIter
	rp sql.Node
	b  sql.NodeExecBuilder

	parentRow sql.LazyRow

	rowSize  int
	scopeLen int

	leftRow sql.LazyRow
}

func (i *crossJoinIterator) Next(ctx *sql.Context, row sql.LazyRow) error {
	for {
		if i.leftRow == nil {
			r := sql.NewSqlRow(0)
			err := i.l.Next(ctx, r)
			if err != nil {
				return err
			}

			i.leftRow.CopyRange(0, i.parentRow.SqlValues()...)
			i.leftRow.CopyRange(0, r.SqlValues()...)
		}

		if i.r == nil {
			iter, err := i.b.Build(ctx, i.rp, i.leftRow)
			if err != nil {
				return err
			}

			i.r = iter
		}

		rightRow := sql.NewSqlRow(0)
		err := i.r.Next(ctx, rightRow)
		if err == io.EOF {
			i.r = nil
			i.leftRow = nil
			continue
		}

		if err != nil {
			return err
		}

		row.CopyRange(0, i.leftRow.SqlValues()...)
		row.CopyRange(0, rightRow.SqlValues()...)

		return nil
	}
}

func (i *crossJoinIterator) Close(ctx *sql.Context) (err error) {
	if i.l != nil {
		err = i.l.Close(ctx)
	}

	if i.r != nil {
		if err == nil {
			err = i.r.Close(ctx)
		} else {
			i.r.Close(ctx)
		}
	}

	return err
}

// lateralJoinIter is an iterator that performs a lateral join.
// A LateralJoin is a join where the right side is a subquery that can reference the left side, like through a filter.
// MySQL Docs: https://dev.mysql.com/doc/refman/8.0/en/lateral-derived-tables.html
// Example:
// select * from t;
// +---+
// | i |
// +---+
// | 1 |
// | 2 |
// | 3 |
// +---+
// select * from t1;
// +---+
// | i |
// +---+
// | 1 |
// | 4 |
// | 5 |
// +---+
// select * from t, lateral (select * from t1 where t.i = t1.j) tt;
// +---+---+
// | i | j |
// +---+---+
// | 1 | 1 |
// +---+---+
// cond is passed to the filter iter to be evaluated.
type lateralJoinIterator struct {
	pRow  sql.LazyRow
	lRow  sql.LazyRow
	rRow  sql.LazyRow
	lIter sql.RowIter
	rIter sql.RowIter
	rNode sql.Node
	cond  sql.Expression
	jType plan.JoinType

	rowSize  int
	scopeLen int

	foundMatch bool

	b sql.NodeExecBuilder
}

func newLateralJoinIter(ctx *sql.Context, b sql.NodeExecBuilder, j *plan.JoinNode, row sql.LazyRow) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := j.Left().(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(j.Left()).String()
	}
	if rightTable, ok := j.Right().(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(j.Right()).String()
	}

	span, ctx := ctx.Span("plan.LateralJoin", trace.WithAttributes(
		attribute.String("left", left),
		attribute.String("right", right),
	))

	l, err := b.Build(ctx, j.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &lateralJoinIterator{
		pRow:     row,
		lIter:    l,
		rNode:    j.Right(),
		cond:     j.Filter,
		jType:    j.Op,
		rowSize:  row.Count() + len(j.Left().Schema()) + len(j.Right().Schema()),
		scopeLen: j.ScopeLen,
		b:        b,
	}), nil
}

func (i *lateralJoinIterator) loadLeft(ctx *sql.Context) error {
	if i.lRow == nil {
		lRow := sql.NewSqlRow(0)
		err := i.lIter.Next(ctx, nil)
		if err != nil {
			return err
		}
		i.lRow = lRow
		i.foundMatch = false
	}
	return nil
}

func (i *lateralJoinIterator) buildRight(ctx *sql.Context) error {
	if i.rIter == nil {
		prepended, _, err := transform.Node(i.rNode, plan.PrependRowInPlan(i.lRow.SqlValues(), true))
		if err != nil {
			return err
		}
		iter, err := i.b.Build(ctx, prepended, i.lRow)
		if err != nil {
			return err
		}
		i.rIter = iter
	}
	return nil
}

func (i *lateralJoinIterator) loadRight(ctx *sql.Context) error {
	if i.rRow == nil {
		rRow := sql.NewSqlRow(0)
		err := i.rIter.Next(ctx, nil)
		if err != nil {
			return err
		}
		i.rRow.CopyRange(0, rRow.SqlValues()[i.lRow.Count():]...)
	}
	return nil
}

func (i *lateralJoinIterator) reset(ctx *sql.Context) (err error) {
	if i.rIter != nil {
		err = i.rIter.Close(ctx)
		i.rIter = nil
	}
	i.lRow = nil
	i.rRow = nil
	return
}

func (i *lateralJoinIterator) Next(ctx *sql.Context, row sql.LazyRow) error {
	for {
		if err := i.loadLeft(ctx); err != nil {
			return err
		}
		if err := i.buildRight(ctx); err != nil {
			return err
		}
		if err := i.loadRight(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				if !i.foundMatch && i.jType == plan.JoinTypeLateralLeft {
					//buildRow(i.lRow,  res,nil)
					if rerr := i.reset(ctx); rerr != nil {
						return rerr
					}
					return nil
				}
				if rerr := i.reset(ctx); rerr != nil {
					return rerr
				}
				continue
			}
			return err
		}

		buildRow(0, row, i.lRow)
		buildRow(0, row, i.rRow)
		i.rRow = nil
		if i.cond != nil {
			if res, err := sql.EvaluateCondition(ctx, i.cond, row); err != nil {
				return err
			} else if !sql.IsTrue(res) {
				continue
			}
		}

		i.foundMatch = true
		return nil
	}
}

func (i *lateralJoinIterator) Close(ctx *sql.Context) error {
	var lerr, rerr error
	if i.lIter != nil {
		lerr = i.lIter.Close(ctx)
	}
	if i.rIter != nil {
		rerr = i.rIter.Close(ctx)
	}
	if lerr != nil {
		return lerr
	}
	if rerr != nil {
		return rerr
	}
	return nil
}
