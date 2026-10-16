// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// lockedBuffer is where the access log goes in these tests: the server writes
// it from its own goroutines.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}

// call serves cfg's server, built with opts, on an in-memory listener, makes
// one health check against it tagged with md, and returns the access line the
// call left in the log.
//
// The listener is reached only through the dial option handed to DialInsecure,
// so a call that succeeds is also the proof that DialInsecure applied it.
func call(t *testing.T, cfg ServerConfig, md metadata.MD, opts ...grpc.ServerOption) map[string]any {
	t.Helper()

	var log lockedBuffer
	saved := Logger
	Logger = zerolog.New(&log)
	t.Cleanup(func() { Logger = saved })

	srv, err := cfg.ServeInsecure(opts...)
	if err != nil {
		t.Fatalf("ServeInsecure: %v", err)
	}
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis := bufconn.Listen(1 << 16)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	// Registered after the Logger's restore, so run before it: the server is
	// gone by the time the variable it reads is written again.
	t.Cleanup(func() {
		srv.Stop()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cnx, err := DialInsecure(ctx, "bufnet", grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatalf("DialInsecure: %v", err)
	}
	defer func() {
		if err := cnx.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	ctx = metadata.NewOutgoingContext(ctx, md)
	if _, err := healthpb.NewHealthClient(cnx).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("the call did not reach the server: %v", err)
	}

	// The interceptor logs before it returns the reply, so the line is there.
	for _, line := range bytes.Split(log.Bytes(), []byte("\n")) {
		var fields map[string]any
		if len(line) == 0 {
			continue
		}
		if err := json.Unmarshal(line, &fields); err != nil {
			t.Fatalf("a log line is not JSON: %q", line)
		}
		if fields["message"] == "access" {
			return fields
		}
	}
	t.Fatalf("no access line in the log:\n%s", log.Bytes())
	return nil
}

func TestServeInsecure_AppliesTheCallerOptions(t *testing.T) {
	var reached atomic.Bool
	witness := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any,
		_ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		reached.Store(true)
		return handler(ctx, req)
	})

	access := call(t, ServerConfig{}, nil, witness)

	if !reached.Load() {
		t.Fatal("the option passed to ServeInsecure was not applied")
	}
	if access["uri"] != "/grpc.health.v1.Health/Check" {
		t.Fatalf("the server's own interceptor is gone: the access line is %v", access)
	}
}

func TestAccessLog_RecordsTheSessionKey(t *testing.T) {
	tagged := metadata.Pairs("session-id", "s-1")

	for _, tc := range []struct {
		name       string
		sessionKey string
		md         metadata.MD
		want       any // nil: no "session" field
	}{
		{"the key the call carries", "session-id", tagged, "s-1"},
		{"no key configured", "", tagged, nil},
		{"a key the call does not carry", "other", tagged, nil},
		{"a call carrying nothing", "session-id", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			access := call(t, ServerConfig{SessionKey: tc.sessionKey}, tc.md)
			if got := access["session"]; got != tc.want {
				t.Fatalf("session = %v, want %v, in %v", got, tc.want, access)
			}
		})
	}
}
