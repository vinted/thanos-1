// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"math"
	"strings"

	"github.com/pkg/errors"
)

type adjusterFn func(uint64) uint64

func maxStringsPerStore(storeCount uint64) uint64 {
	return math.MaxUint64 / uint64(storeCount)
}

func NewReferenceAdjusterFactory(storeCount uint64) func(storeIndex uint64) adjusterFn {
	// Adjuster adjusts each incoming reference according to the number of stores.
	// Whole label space is stored in uint64 so that's how many
	// strings we are able to store.
	eachStore := maxStringsPerStore(storeCount)

	return func(storeIndex uint64) adjusterFn {
		startFrom := eachStore * storeIndex

		return func(ref uint64) uint64 {
			return startFrom + (ref % eachStore)
		}
	}
}

// LookupTableBuilder provides a way of building
// a lookup table for static strings to compress
// responses better.
type LookupTableBuilder struct {
	maxElements uint64

	current      uint64
	table        map[uint64]string
	reverseTable map[string]uint64
}

func NewLookupTableBuilder(maxElements uint64) *LookupTableBuilder {
	return &LookupTableBuilder{maxElements: maxElements, table: make(map[uint64]string), reverseTable: make(map[string]uint64)}
}

var MaxElementsReached = errors.New("max elements reached in lookup table builder")

func (b *LookupTableBuilder) PutString(s string) (uint64, error) {
	if b.current >= b.maxElements {
		return 0, MaxElementsReached
	}
	if num, ok := b.reverseTable[s]; ok {
		return num, nil
	}

	s = strings.Clone(s)
	b.reverseTable[s] = b.current
	b.table[b.current] = s
	b.current++
	return b.current - 1, nil
}

func (b *LookupTableBuilder) GetTable() map[uint64]string {
	return b.table
}
