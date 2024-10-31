package sql

type OffsetIter interface {
	RowIter
	// SetOffset instructs where this row iter places projections
	WithOffset(int) RowIter
}
