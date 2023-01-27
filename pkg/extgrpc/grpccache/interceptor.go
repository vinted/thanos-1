package grpccache

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

func getResponses(c *cache.InMemoryCache, reqSig string) ([]*storepb.SeriesResponse, error) {
	numKeysKey := fmt.Sprintf("%s-num", reqSig)
	numKeysFetchRes := c.Fetch(context.Background(), []string{numKeysKey})

	var numKeys int
	if numKeysBytes, ok := numKeysFetchRes[numKeysKey]; !ok {
		return nil, fmt.Errorf("not found num key")
	} else {
		if n, err := fmt.Sscanf(string(numKeysBytes), "%d", &numKeys); n != 1 || err != nil {
			return nil, fmt.Errorf("parsing %s", string(numKeysBytes))
		}
	}

	var fetchKeys = make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		fetchKeys = append(fetchKeys, fmt.Sprintf("%s-%d", reqSig, i))
	}
	fetchedData := c.Fetch(context.Background(), fetchKeys)

	if len(fetchedData) != numKeys {
		return nil, fmt.Errorf("got wrong number of keys: expected %d, got %d", numKeys, len(fetchedData))
	}
	ret := make([]*storepb.SeriesResponse, 0, numKeys)

	for i := 0; i < numKeys; i++ {
		var r storepb.SeriesResponse
		data := fetchedData[fmt.Sprintf("%s-%d", reqSig, i)]

		if err := r.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("unmarshaling data from cache: %w", err)
		}

		ret = append(ret, &r)
	}

	return ret, nil
}

func putResponses(c *cache.InMemoryCache, reqSig string, responses []*storepb.SeriesResponse) []byte {
	keys := map[string][]byte{
		fmt.Sprintf("%s-num", reqSig): []byte(fmt.Sprintf("%d", len(responses))),
	}

	for i, resp := range responses {
		m, _ := resp.Marshal()
		keys[fmt.Sprintf("%s-%d", reqSig, i)] = m
	}

	c.Store(context.Background(), keys, 5*time.Minute)
	return nil
}

func splitMethodName(fullMethod string) (string, string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/") // remove leading slash
	if i := strings.Index(fullMethod, "/"); i >= 0 {
		return fullMethod[:i], fullMethod[i+1:]
	}
	return "unknown", "unknown"
}

type seriesInterceptor struct {
	grpc.ClientStream
	target string
	c      *cache.InMemoryCache

	hashedReq string

	responses                []*storepb.SeriesResponse
	cachedResponsesAvailable bool

	cachedCalls prometheus.Counter
}

func hashReqTarget(r *storepb.SeriesRequest, target string) string {
	h := xxhash.New()
	_, _ = h.WriteString(target)
	m, _ := r.Marshal()
	_, _ = h.Write(m)

	return string(h.Sum(nil))
}

func (i *seriesInterceptor) RecvMsg(m interface{}) error {
	if i.hashedReq == "" {
		return i.ClientStream.RecvMsg(m)
	}
	if i.cachedResponsesAvailable {
		if len(i.responses) == 0 {
			return io.EOF
		}
		resp, ok := m.(*storepb.SeriesResponse)
		if !ok {
			panic("should be a series response type")
		}
		*resp = *i.responses[0]
		i.responses = i.responses[1:]
		return nil
	}

	if err := i.ClientStream.RecvMsg(m); err != nil {
		if err == io.EOF && len(i.responses) > 0 {
			putResponses(i.c, i.hashedReq, i.responses)
		}
		return err
	}
	if resp, ok := m.(*storepb.SeriesResponse); ok {
		i.responses = append(i.responses, resp)
	}
	return nil
}

func (i *seriesInterceptor) SendMsg(m interface{}) error {
	if req, ok := m.(*storepb.SeriesRequest); ok {
		i.hashedReq = hashReqTarget(req, i.target)

		responses, err := getResponses(i.c, i.hashedReq)
		if err == nil {
			i.responses = responses
			i.cachedResponsesAvailable = true
			i.cachedCalls.Inc()
		}

	}
	return i.ClientStream.SendMsg(m)
}

type SeriesRequestCachingInterceptor struct {
	inmemoryCache *cache.InMemoryCache
	passthrough   bool

	cachedCalls prometheus.Counter
}

func NewSeriesRequestcachingInterceptor(reg *prometheus.Registry, maxSize model.Bytes, maxItemSize model.Bytes) (*SeriesRequestCachingInterceptor, error) {
	if maxSize == 0 {
		maxItemSize = 0
	}
	imc, err := cache.NewInMemoryCacheWithConfig(
		"cachinginterceptor",
		log.NewLogfmtLogger(os.Stderr),
		reg,
		cache.InMemoryCacheConfig{
			MaxSize:     maxSize,
			MaxItemSize: maxItemSize,
		},
	)
	if err != nil {
		return nil, err
	}
	return &SeriesRequestCachingInterceptor{
		inmemoryCache: imc,
		passthrough:   maxSize == 0,
		cachedCalls: promauto.With(prometheus.Registerer(reg)).NewCounter(prometheus.CounterOpts{
			Name: "thanos_grpc_cached_calls_total",
			Help: "How many Series() calls were fully cached",
		}),
	}, nil
}

func (i *SeriesRequestCachingInterceptor) GetInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return cs, err
		}

		if i.passthrough {
			return cs, nil
		}

		if svc, actualMethod := splitMethodName(method); svc == "thanos.Store" && actualMethod == "Series" {
			return &seriesInterceptor{
				ClientStream: cs,
				target:       cc.Target(),
				c:            i.inmemoryCache,
				cachedCalls:  i.cachedCalls,
			}, nil
		}

		return cs, nil
	}
}
