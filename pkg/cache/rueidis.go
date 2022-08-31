// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cacheutil"
)

// RueidisCache is a redis cache.
type RueidisCache struct {
	logger      log.Logger
	redisClient *cacheutil.RueidisClient
	name        string

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewRueidisCache makes a new RueidisCache.
func NewRueidisCache(name string, logger log.Logger, redisClient *cacheutil.RueidisClient, reg prometheus.Registerer) *RueidisCache {
	c := &RueidisCache{
		logger:      logger,
		redisClient: redisClient,
		name:        name,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "thanos_cache_redis_requests_total",
		Help:        "Total number of items requests to redis.",
		ConstLabels: prometheus.Labels{"name": name},
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "thanos_cache_redis_hits_total",
		Help:        "Total number of items requests to the cache that were a hit.",
		ConstLabels: prometheus.Labels{"name": name},
	})

	level.Info(logger).Log("msg", "created redis cache")

	return c
}

// Store data identified by keys.
func (c *RueidisCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	c.redisClient.SetMulti(ctx, data, ttl)
}

// Fetch fetches multiple keys and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *RueidisCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	c.requests.Add(float64(len(keys)))
	results := c.redisClient.GetMulti(ctx, keys)
	c.hits.Add(float64(len(results)))
	return results
}

func (c *RueidisCache) Name() string {
	return c.name
}
