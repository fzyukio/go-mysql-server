// Copyright 2023 Dolthub, Inc.
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
	"context"
	"fmt"
	"io"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type analyzeTableIter struct {
	idx    int
	db     string
	tables []sql.Table
	stats  sql.StatsProvider
}

var _ sql.RowIter = &analyzeTableIter{}

func (itr *analyzeTableIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	if itr.idx >= len(itr.tables) {
		return io.EOF
	}

	t := itr.tables[itr.idx]

	msgType := "status"
	msgText := "OK"
	err := itr.stats.RefreshTableStats(ctx, t, itr.db)
	if err != nil {
		msgType = "Error"
		msgText = err.Error()
	}
	row.SetSqlValue(0, t.Name())
	row.SetSqlValue(1, "analyze")
	row.SetSqlValue(2, msgType)
	row.SetSqlValue(3, msgText)
	itr.idx++
	return nil
}

func (itr *analyzeTableIter) Close(ctx *sql.Context) error {
	return nil
}

type updateHistogramIter struct {
	db      string
	table   string
	columns []string
	stats   sql.Statistic
	prov    sql.StatsProvider
	done    bool
}

var _ sql.RowIter = &updateHistogramIter{}

func (itr *updateHistogramIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	if itr.done {
		return io.EOF
	}
	defer func() {
		itr.done = true
	}()
	err := itr.prov.SetStats(ctx, itr.stats)
	if err != nil {
		return nil
	}
	return nil
}

func (itr *updateHistogramIter) Close(_ *sql.Context) error {
	return nil
}

type dropHistogramIter struct {
	db      string
	table   string
	columns []string
	prov    sql.StatsProvider
	done    bool
}

var _ sql.RowIter = &dropHistogramIter{}

func (itr *dropHistogramIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	if itr.done {
		return io.EOF
	}
	defer func() {
		itr.done = true
	}()
	qual := sql.NewStatQualifier(itr.db, itr.table, "")
	err := itr.prov.DropStats(ctx, qual, itr.columns)
	if err != nil {
		return nil
	}
	return nil
}

func (itr *dropHistogramIter) Close(_ *sql.Context) error {
	return nil
}

// blockIter is a sql.RowIter that iterates over the given rows.
type blockIter struct {
	internalIter sql.RowIter
	repNode      sql.Node
	sch          sql.Schema
}

var _ plan.BlockRowIter = (*blockIter)(nil)

// Next implements the sql.RowIter interface.
func (i *blockIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	return i.internalIter.Next(ctx, row)
}

// Close implements the sql.RowIter interface.
func (i *blockIter) Close(ctx *sql.Context) error {
	return i.internalIter.Close(ctx)
}

// RepresentingNode implements the sql.BlockRowIter interface.
func (i *blockIter) RepresentingNode() sql.Node {
	return i.repNode
}

// Schema implements the sql.BlockRowIter interface.
func (i *blockIter) Schema() sql.Schema {
	return i.sch
}

type prependRowIter struct {
	row       sql.Row
	childIter sql.RowIter
}

func (p *prependRowIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	err := p.childIter.Next(ctx, row)
	if err != nil {
		return err
	}
	return nil
}

func (p *prependRowIter) Close(ctx *sql.Context) error {
	return p.childIter.Close(ctx)
}

type cachedResultsIter struct {
	parent  *plan.CachedResults
	iter    sql.RowIter
	cache   sql.RowsCache
	dispose sql.DisposeFunc
}

func (i *cachedResultsIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	err := i.iter.Next(ctx, row)
	if i.cache != nil {
		if err != nil {
			if err == io.EOF {
				i.saveResultsInGlobalCache()
				i.parent.Finalized = true
			}
			i.cleanUp()
		} else {
			aerr := i.cache.Add(row)
			if aerr != nil {
				i.cleanUp()
				i.parent.Mutex.Lock()
				defer i.parent.Mutex.Unlock()
				i.parent.NoCache = true
			}
		}
	}
	return err
}

func (i *cachedResultsIter) saveResultsInGlobalCache() {
	if plan.CachedResultsGlobalCache.AddNewCache(i.parent.Id, i.cache, i.dispose) {
		i.cache = nil
		i.dispose = nil
	}
}

func (i *cachedResultsIter) cleanUp() {
	if i.dispose != nil {
		i.dispose()
		i.cache = nil
		i.dispose = nil
	}
}

func (i *cachedResultsIter) Close(ctx *sql.Context) error {
	i.cleanUp()
	return i.iter.Close(ctx)
}

type hashLookupGeneratingIter struct {
	n         *plan.HashLookup
	childIter sql.RowIter
	lookup    *map[interface{}][]sql.LazyRow
}

func newHashLookupGeneratingIter(n *plan.HashLookup, chlidIter sql.RowIter) *hashLookupGeneratingIter {
	h := &hashLookupGeneratingIter{
		n:         n,
		childIter: chlidIter,
	}
	lookup := make(map[interface{}][]sql.LazyRow)
	h.lookup = &lookup
	return h
}

func (h *hashLookupGeneratingIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	err := h.childIter.Next(ctx, row)
	if err == io.EOF {
		// We wait until we finish the child iter before caching the Lookup map.
		// This is because some plans may not fully exhaust the iterator.
		h.n.Lookup = h.lookup
		return io.EOF
	}
	if err != nil {
		return err
	}
	// TODO: Maybe do not put nil stuff in here.
	// TODO subselect only the hash columns for hashing
	key, err := h.n.GetHashKey(ctx, h.n.RightEntryKey, row)
	if err != nil {
		return err
	}
	(*(h.lookup))[key] = append((*(h.lookup))[key], row)
	return nil
}

func (h *hashLookupGeneratingIter) Close(c *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*hashLookupGeneratingIter)(nil)

// declareCursorIter is the sql.RowIter of *DeclareCursor.
type declareCursorIter struct {
	*plan.DeclareCursor
}

var _ sql.RowIter = (*declareCursorIter)(nil)

// Next implements the interface sql.RowIter.
func (d *declareCursorIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	if err := d.Pref.InitializeCursor(d.Name, d.Select); err != nil {
		return err
	}
	return io.EOF
}

// Close implements the interface sql.RowIter.
func (d *declareCursorIter) Close(ctx *sql.Context) error {
	return nil
}

// iterPartitions will call Next() on |iter| and send every result it
// finds to |partitions|.  Meant to be run as a goroutine in an
// errgroup, it returns a non-nil error if it gets an error and it
// return |ctx.Err()| if the context becomes Done().
func iterPartitions(ctx *sql.Context, iter sql.PartitionIter, partitions chan<- sql.Partition) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = fmt.Errorf("panic in iterPartitions: %v", r)
		}
	}()
	defer func() {
		cerr := iter.Close(ctx)
		if rerr == nil {
			rerr = cerr
		}
	}()
	for {
		p, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		select {
		case partitions <- p:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type rowIterPartitionFunc func(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error)

// iterPartitionRows is the parallel worker for an Exchange node. It
// is meant to be run as a goroutine in an errgroup.Group. It will
// values read off of |partitions|. For each value it reads, it will
// call |getRowIter| to get a row ProjectIter, and will then call |Next| on
// that row ProjectIter, passing every row it gets into |rows|. If it
// receives an error at any point, it returns it. |iterPartitionRows|
// stops iterating and returns |nil| when |partitions| is closed.
func iterPartitionRows(ctx *sql.Context, getRowIter rowIterPartitionFunc, partitions <-chan sql.Partition, rows chan<- sql.LazyRow) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = fmt.Errorf("panic in ExchangeIterPartitionRows: %v", r)
		}
	}()
	for {
		select {
		case p, ok := <-partitions:
			if !ok {
				return nil
			}
			span, ctx := ctx.Span("exchange.IterPartition")
			iter, err := getRowIter(ctx, p)
			if err != nil {
				return err
			}
			count, err := sendAllRows(ctx, iter, rows)
			span.SetAttributes(attribute.Int("num_rows", count))
			span.End()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sendAllRows(ctx *sql.Context, iter sql.RowIter, rows chan<- sql.LazyRow) (rowCount int, rerr error) {
	defer func() {
		cerr := iter.Close(ctx)
		if rerr == nil {
			rerr = cerr
		}
	}()
	for {
		// TODO appropriate row size
		r := sql.NewSqlRow(0)
		err := iter.Next(ctx, r)
		if err == io.EOF {
			return rowCount, nil
		}
		if err != nil {
			return rowCount, err
		}
		rowCount++
		select {
		case rows <- r:
		case <-ctx.Done():
			return rowCount, ctx.Err()
		}
	}
}

func (b *BaseBuilder) exchangeIterGen(e *plan.Exchange, row sql.LazyRow) func(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return func(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
		node, _, err := transform.Node(e.Child, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			if t, ok := n.(sql.Table); ok {
				return &plan.ExchangePartition{partition, t}, transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		})
		if err != nil {
			return nil, err
		}
		return b.buildNodeExec(ctx, node, row)
	}
}

// exchangeRowIter implements sql.RowIter for an exchange
// node. Calling |Next| reads off of |rows|, while calling |Close|
// calls |shutdownHook| and waits for exchange node workers to
// shutdown. If |rows| is closed, |Next| returns the error returned by
// |waiter|. |Close| returns the error returned by |waiter|, except it
// returns |nil| if |waiter| returns |io.EOF| or |shutdownHookErr|.
type exchangeRowIter struct {
	shutdownHook func()
	waiter       func() error
	rows         <-chan sql.LazyRow
	rows2        <-chan sql.Row2
}

var _ sql.RowIter = (*exchangeRowIter)(nil)

func (i *exchangeRowIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	if i.rows == nil {
		panic("Next called for a Next2 iterator")
	}
	r, ok := <-i.rows
	if !ok {
		return i.waiter()
	}
	row.CopyRange(0, r)
	return nil
}

func (i *exchangeRowIter) Close(ctx *sql.Context) error {
	i.shutdownHook()
	err := i.waiter()
	if err == shutdownHookErr || err == io.EOF {
		return nil
	}
	return err
}

var shutdownHookErr = fmt.Errorf("shutdown hook")

// newShutdownHook returns a |func()| that can be called to cancel the
// |ctx| associated with the supplied |eg|. It is safe to call the
// hook more than once.
//
// If an errgroup is shutdown with a shutdown hook, eg.Wait() will
// return |shutdownHookErr|. This can be used to consider requested
// shutdowns successful in some contexts, for example.
func newShutdownHook(eg *errgroup.Group, ctx context.Context) func() {
	stop := make(chan struct{})
	eg.Go(func() error {
		select {
		case <-stop:
			return shutdownHookErr
		case <-ctx.Done():
			return nil
		}
	})
	shutdownOnce := &sync.Once{}
	return func() {
		shutdownOnce.Do(func() {
			close(stop)
		})
	}
}

type releaseIter struct {
	child   sql.RowIter
	release func()
	once    sync.Once
}

func (i *releaseIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	err := i.child.Next(ctx, row)
	if err != nil {
		_ = i.Close(ctx)
		return err
	}
	return nil
}

func (i *releaseIter) Close(ctx *sql.Context) (err error) {
	i.once.Do(i.release)
	if i.child != nil {
		err = i.child.Close(ctx)
	}
	return err
}

type concatIter struct {
	cur      sql.RowIter
	inLeft   sql.KeyValueCache
	dispose  sql.DisposeFunc
	nextIter func() (sql.RowIter, error)
}

func newConcatIter(ctx *sql.Context, cur sql.RowIter, nextIter func() (sql.RowIter, error)) *concatIter {
	seen, dispose := ctx.Memory.NewHistoryCache()
	return &concatIter{
		cur,
		seen,
		dispose,
		nextIter,
	}
}

var _ sql.Disposable = (*concatIter)(nil)
var _ sql.RowIter = (*concatIter)(nil)

func (ci *concatIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	for {
		err := ci.cur.Next(ctx, row)
		if err == io.EOF {
			if ci.nextIter == nil {
				return io.EOF
			}
			err = ci.cur.Close(ctx)
			if err != nil {
				return err
			}
			ci.cur, err = ci.nextIter()
			ci.nextIter = nil
			if err != nil {
				return err
			}
			err = ci.cur.Next(ctx, row)
		}
		if err != nil {
			return err
		}
		hash, err := sql.HashOf(row)
		if err != nil {
			return err
		}
		if ci.nextIter != nil {
			// On Left
			if err := ci.inLeft.Put(hash, struct{}{}); err != nil {
				return err
			}
		} else {
			// On Right
			if _, err := ci.inLeft.Get(hash); err == nil {
				continue
			}
		}
		return err
	}
}

func (ci *concatIter) Dispose() {
	ci.dispose()
}

func (ci *concatIter) Close(ctx *sql.Context) error {
	ci.Dispose()
	if ci.cur != nil {
		return ci.cur.Close(ctx)
	} else {
		return nil
	}
}

type stripRowIter struct {
	sql.RowIter
	numCols int
}

func (sri *stripRowIter) Next(ctx *sql.Context, row sql.LazyRow) error {
	err := sri.RowIter.Next(ctx, row)
	if err != nil {
		return err
	}
	return nil
}

func (sri *stripRowIter) Close(ctx *sql.Context) error {
	return sri.RowIter.Close(ctx)
}
