package sql

import (
	"strings"
)

type tableCol struct {
	table string
	col   string
}
type tableColMeta struct {
	Str string
}

func NewColumnInterner() *ColumnInterner {
	return &ColumnInterner{cols: make(map[tableCol]tableColMeta)}
}

type ColumnInterner struct {
	cols map[tableCol]tableColMeta
}

func (i *ColumnInterner) Get(table, col string) tableColMeta {
	tc := tableCol{table, col}
	if m, ok := i.cols[tc]; ok {
		return m
	}
	var m tableColMeta
	if table == "" {
		m = tableColMeta{col}
	} else {
		m = tableColMeta{Str: strings.ToLower(table + "." + col)}
	}
	i.cols[tc] = m
	return m
}
