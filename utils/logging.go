// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package utils

import (
	"context"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"os"
	"strings"
	"time"
)

// stderrIsTerminal reports whether the logs are going to a screen.
//
// zerolog's ConsoleWriter colours unconditionally, so a redirected or piped log
// carries the ANSI escapes into whatever reads it: `cams-hls --help 2>log`
// wrote "\x1b[32mINF\x1b[0m" into the file. The colour is for an operator
// watching, and nobody else.
//
// A character device is not strictly a terminal -- /dev/null is one too -- but
// the only cost of that confusion is escapes nobody reads, and it keeps this
// free of a dependency.
func stderrIsTerminal() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

var (
	// LoggerContext is the builder of a zerolog.Logger that is exposed to the application so that
	// options at the CLI might alter the formatting and the output of the logs.
	LoggerContext = zerolog.
			New(zerolog.ConsoleWriter{
			Out: os.Stderr, TimeFormat: time.RFC3339,
			NoColor: !stderrIsTerminal(),
		}).With().Timestamp()

	// Logger is a zerolog logger, that can be safely used from any part of the application.
	// It gathers the format and the output.
	Logger = LoggerContext.Logger()
)

type logEvt struct {
	z     *zerolog.Event
	start time.Time
}

func newEvent(method string) *logEvt {
	return &logEvt{z: Logger.Debug().Str("uri", method), start: time.Now()}
}

func (evt *logEvt) send() { evt.z.Msg("access") }

func (evt *logEvt) setResult(err error) *logEvt {
	evt.z = evt.z.TimeDiff("t", time.Now(), evt.start)
	if err != nil {
		evt.z.Int("rc", 500)
		evt.z.Err(err)
	} else {
		evt.z.Int("rc", 200)
	}
	return evt
}

// patchWithRequest records the caller's authority and, when sessionKey names a
// metadata key the call carries, its first value as "session".
func (evt *logEvt) patchWithRequest(ctx context.Context, sessionKey string) *logEvt {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		auth := md.Get(":authority")
		if len(auth) > 0 {
			evt.z.Str("local", auth[0])
		}
		if sessionKey != "" {
			if sessionID := md.Get(sessionKey); len(sessionID) > 0 {
				evt.z.Str("session", sessionID[0])
			}
		}
	}
	return evt
}

func (evt *logEvt) pathWithReply(ctx context.Context) *logEvt {
	if peer, ok := peer.FromContext(ctx); ok {
		addr := peer.Addr.String()
		if i := strings.LastIndex(addr, ":"); i > -1 {
			addr = addr[:i]
		}
		evt.z.Str("peer", addr)
	}
	return evt
}

// NewStreamServerInterceptorZerolog logs one access line per stream; see
// ServerConfig.SessionKey for sessionKey.
func NewStreamServerInterceptorZerolog(sessionKey string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		evt := newEvent(info.FullMethod)
		err := handler(srv, ss)
		ctx := ss.Context()
		evt.setResult(err).patchWithRequest(ctx, sessionKey).pathWithReply(ctx).send()
		// Returned bare. An interceptor must not rewrap the handler's error:
		// wrapping it costs the gRPC status, because grpc then rebuilds the
		// status from the message and the client gets an annotation chain
		// where the handler's own text should be. The unary interceptor below
		// already had this right.
		return err
	}
}

// NewUnaryServerInterceptorZerolog logs one access line per call; see
// ServerConfig.SessionKey for sessionKey.
func NewUnaryServerInterceptorZerolog(sessionKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		evt := newEvent(info.FullMethod)
		// The request body is deliberately not logged. It was, by reflection,
		// at debug level: that put an account identifier on every registration
		// into the log, and would put a credential there the moment a request
		// message grows one.
		resp, err := handler(ctx, req)
		evt.setResult(err).patchWithRequest(ctx, sessionKey).pathWithReply(ctx).send()
		return resp, err
	}
}
