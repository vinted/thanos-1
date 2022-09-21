// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// BadgerIndexCache is a memcached-based index cache.
type BadgerIndexCache struct {
	db *badger.DB

	// Fallback to this one if badger contains no keys.
	ic IndexCache

	logger log.Logger
}

func (b *BadgerIndexCache) Close() error {
	return b.db.Close()
}

// NewRemoteIndexCache makes a new RemoteIndexCache.
func NewBadgerIndexCache(logger log.Logger, ic IndexCache) (*BadgerIndexCache, error) {
	db, err := badger.Open(badger.DefaultOptions("/data/test"))
	if err != nil {
		return nil, fmt.Errorf("creating badger: %w", err)
	}

	c := &BadgerIndexCache{
		logger: logger,
		ic:     ic,
		db:     db,
	}

	level.Info(logger).Log("msg", "created index cache")

	return c, nil
}

// StorePostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *BadgerIndexCache) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
	fmt.Println("storing badger postings")
	if l.Name == "app" && l.Value == "core" {
		key := cacheKey{blockID, cacheKeyPostings(l)}.string()

		wb := c.db.NewWriteBatch()
		wb.Set([]byte(key), v)
		wb.Flush()
		fmt.Println("badger stored postings")
	} else {
		c.ic.StorePostings(ctx, blockID, l, v)
	}
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *BadgerIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, lbls []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	fmt.Println("badger is fetching multi postings")
	var (
		extraHits   map[labels.Label][]byte = make(map[labels.Label][]byte)
		extraMisses []labels.Label          = []labels.Label{}
		foundCore   bool
	)
	coreLabel := labels.Label{Name: "app", Value: "core"}

	for i, lbl := range lbls {
		if lbl == coreLabel {
			lbls[i] = lbls[len(lbls)-1]
			lbls = lbls[:len(lbls)-1]
			foundCore = true
			break
		}
	}

	if foundCore {
		c.db.View(func(txn *badger.Txn) error {
			key := cacheKey{blockID, cacheKeyPostings(coreLabel)}.string()

			item, err := txn.Get([]byte(key))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					extraMisses = append(extraMisses, coreLabel)
				}
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			extraHits[coreLabel] = val
			return nil
		})
	}

	hits, misses = c.ic.FetchMultiPostings(ctx, blockID, lbls)

	misses = append(misses, extraMisses...)
	if hits == nil {
		hits = make(map[labels.Label][]byte)
	}
	for k, v := range extraHits {
		hits[k] = v
	}

	return hits, misses
}

// StoreSeries sets the series identified by the ulid and id to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *BadgerIndexCache) StoreSeries(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	c.ic.StoreSeries(ctx, blockID, id, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
// In case of error, it logs and return an empty cache hits map.
func (c *BadgerIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	return c.ic.FetchMultiSeries(ctx, blockID, ids)
}
