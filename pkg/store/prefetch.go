package store

import (
	"context"
	"fmt"
	"time"

	"github.com/oklog/run"
	"github.com/prometheus/prometheus/model/labels"
)

type PostingsPrefetcher struct {
	b *BucketStore
}

func NewPostingsPrefetcher(b *BucketStore) (*PostingsPrefetcher, error) {

	return &PostingsPrefetcher{b: b}, nil
}

func (p *PostingsPrefetcher) DoFetch() error {
	p.b.mtx.RLock()
	defer p.b.mtx.RUnlock()

	now := time.Now()
	from := now.Add(-2 * 24 * time.Hour)

	var _ = from
	fmt.Println("fetching!")

	g := &run.Group{}

	for _, bs := range p.b.blockSets {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "app", "core")}

		blockMatchers, ok := bs.labelMatchers(matchers...)
		if !ok {
			continue
		}
		n := time.Now()
		from := n.Add(-14 * 24 * time.Hour)

		blocks := bs.getFor(from.UnixMilli(), n.UnixMilli(), 0, []*labels.Matcher{})
		fmt.Println("matched", len(blocks), "blocks", blockMatchers)

		for _, b := range blocks {

			b := b

			g.Add(func() error {
				ir := b.indexReader()

				defer ir.Close()

				fmt.Println("fetching postings")
				postings, err := ir.fetchPostings(context.Background(), []labels.Label{{Name: "app", Value: "core"}})
				if err != nil {
					return err
				}
				fmt.Println("fetchPostings finished")

				var _ = postings

				return nil
			}, func(err error) {
				if err != nil {
					fmt.Println("error happened", err)
				}
			})
		}
	}

	err := g.Run()
	if err != nil {
		return err
	}

	return nil
}
