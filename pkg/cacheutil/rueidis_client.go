package cacheutil

import (
	"context"
	"net"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rueian/rueidis"
)

// RueidisClient is a wrap of rueidis.Client.
type RueidisClient struct {
	client rueidis.Client
	config RedisClientConfig

	logger           log.Logger
	durationSet      prometheus.Observer
	durationSetMulti prometheus.Observer
	durationGetMulti prometheus.Observer
}

// NewRueidisClient makes a new RueidisClient.
func NewRueidisClient(logger log.Logger, name string, conf []byte, reg prometheus.Registerer) (*RueidisClient, error) {
	config, err := parseRedisClientConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewRueidisClientWithConfig(logger, name, config, reg)
}

// NewRueidisClientWithConfig makes a new RedisClient.
func NewRueidisClientWithConfig(logger log.Logger, name string, config RedisClientConfig,
	reg prometheus.Registerer) (*RueidisClient, error) {

	if err := config.validate(); err != nil {
		return nil, err
	}
	var addrs []string

	if len(config.Addrs) > 0 {
		addrs = config.Addrs
	} else {
		addrs = []string{config.Addr}
	}

	var cacheSize int

	if config.CacheSize != 0 {
		cacheSize = config.CacheSize
	} else {
		cacheSize = 1024 * (1 << 20)
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       addrs,
		ShuffleInit:       true,
		Username:          config.Username,
		Password:          config.Password,
		SelectDB:          config.DB,
		CacheSizeEachConn: cacheSize,
		Dialer:            net.Dialer{Timeout: config.DialTimeout},
		ConnWriteTimeout:  config.WriteTimeout,
	})
	if err != nil {
		return nil, err
	}

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{"name": name}, reg)
	}

	c := &RueidisClient{
		client: client,
		config: config,
		logger: logger,
	}
	duration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_redis_operation_duration_seconds",
		Help:    "Duration of operations against redis.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
	}, []string{"operation"})
	c.durationSet = duration.WithLabelValues(opSet)
	c.durationSetMulti = duration.WithLabelValues(opSetMulti)
	c.durationGetMulti = duration.WithLabelValues(opGetMulti)
	return c, nil
}

// SetAsync implement RemoteCacheClient.
func (c *RueidisClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	if err := c.client.Do(ctx, c.client.B().Set().Key(key).Value(rueidis.BinaryString(value)).ExSeconds(int64(ttl.Seconds())).Build()).Error(); err != nil {
		level.Warn(c.logger).Log("msg", "failed to set item into redis", "err", err, "key", key, "value_size", len(value))
		return nil
	}
	c.durationSet.Observe(time.Since(start).Seconds())
	return nil
}

// SetMulti set multiple keys and value.
func (c *RueidisClient) SetMulti(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	if len(data) == 0 {
		return
	}
	start := time.Now()
	sets := make(rueidis.Commands, 0, len(data))
	ittl := int64(ttl.Seconds())
	for k, v := range data {
		sets = append(sets, c.client.B().Setex().Key(k).Seconds(ittl).Value(rueidis.BinaryString(v)).Build())
	}
	for _, resp := range c.client.DoMulti(ctx, sets...) {
		if err := resp.Error(); err != nil {
			level.Warn(c.logger).Log("msg", "failed to set multi items from redis", "err", err, "items", len(data))
			return
		}
	}
	c.durationSetMulti.Observe(time.Since(start).Seconds())
}

// GetMulti implement RemoteCacheClient.
func (c *RueidisClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	results := make(map[string][]byte, len(keys))

	resps, err := c.client.DoCache(ctx, c.client.B().Mget().Key(keys...).Cache(), 8*time.Hour).ToArray()
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(resps))
	}
	for i, resp := range resps {
		if val, err := resp.ToString(); err == nil {
			results[keys[i]] = stringToBytes(val)
		}
	}
	c.durationGetMulti.Observe(time.Since(start).Seconds())
	return results
}

// Stop implement RemoteCacheClient.
func (c *RueidisClient) Stop() {
	c.client.Close()
}
