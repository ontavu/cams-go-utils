// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package utils

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
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

// writeKeyPair writes a self-signed certificate and its key, PEM-encoded, into
// dir, and returns their paths.
func writeKeyPair(t *testing.T, dir string) (crt, key string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "cams-go-utils test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	crt = filepath.Join(dir, "crt.pem")
	key = filepath.Join(dir, "key.pem")
	write(t, crt, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	write(t, key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return crt, key
}

func write(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// Every refusal of ServeTLS is told apart with errors.Is, by the constant it
// returns or by the error of the file it could not read.
func TestServeTLS_RefusesWhatItCannotUse(t *testing.T) {
	dir := t.TempDir()
	crt, key := writeKeyPair(t, dir)
	notPEM := filepath.Join(dir, "not.pem")
	write(t, notPEM, []byte("not a certificate"))
	missing := filepath.Join(dir, "missing.pem")

	for _, tc := range []struct {
		name     string
		crt, key string
		want     error
	}{
		{"no certificate path", "", key, ErrNoCertificatePath},
		{"no key path", crt, "", ErrNoKeyPath},
		{"a certificate file that is not there", missing, key, fs.ErrNotExist},
		{"a key file that is not there", crt, missing, fs.ErrNotExist},
		{"a certificate file with no certificate", notPEM, key, ErrNoCertificate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{PathCrt: tc.crt, PathKey: tc.key}
			srv, err := cfg.ServeTLS()
			if !errors.Is(err, tc.want) {
				t.Fatalf("ServeTLS() = %v, want an error that is %v", err, tc.want)
			}
			if srv != nil {
				t.Fatal("a server came back with the error")
			}
		})
	}
}

func TestServeTLS_ServesAValidKeyPair(t *testing.T) {
	crt, key := writeKeyPair(t, t.TempDir())
	cfg := ServerConfig{PathCrt: crt, PathKey: key}
	srv, err := cfg.ServeTLS()
	if err != nil {
		t.Fatalf("ServeTLS: %v", err)
	}
	srv.Stop()
}

func TestDialTLS_IsUnsupported(t *testing.T) {
	cnx, err := DialTLS(context.Background(), "bufnet")
	if !errors.Is(err, errors.ErrUnsupported) || cnx != nil {
		t.Fatalf("DialTLS() = %v, %v, want no connection and errors.ErrUnsupported", cnx, err)
	}
}
