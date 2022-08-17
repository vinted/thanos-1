// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package drpc

import (
	"context"
	"net"

	"github.com/go-kit/log"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"

	"github.com/prometheus/client_golang/prometheus"
)

type DRPCServer struct {
	s *drpcserver.Server
	m *drpcmux.Mux
}

func (d *DRPCServer) Serve(ctx context.Context, lis net.Listener) error {
	return d.s.Serve(ctx, lis)
}

func NewServer(logger log.Logger, reg prometheus.Registerer) *DRPCServer {
	m := drpcmux.New()

	return &DRPCServer{m: m, s: drpcserver.New(m)}
}

func (d *DRPCServer) GetMux() *drpcmux.Mux {
	return d.m
}
