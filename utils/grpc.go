// Copyright (c) 2022-2024 The authors (see the AUTHORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/juju/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
)

type ServerConfig struct {
	ListenAddr string
	PathCrt    string
	PathKey    string

	// SessionKey is the metadata key whose first value the access log records
	// as "session", so that the logs of both ends of a call can be joined. The
	// key belongs to whoever defines the protocol, not to this package; empty
	// records nothing.
	SessionKey string
}

// ServeTLS returns a server that logs every call, with opts applied after its
// own options. The keepalive, which both ends of a call have to agree on, is
// one of the things the caller passes.
func (srv *ServerConfig) ServeTLS(opts ...grpc.ServerOption) (*grpc.Server, error) {
	if len(srv.PathCrt) <= 0 {
		return nil, errors.NotValidf("invalid TLS/x509 certificate path [%s]", srv.PathCrt)
	}
	if len(srv.PathKey) <= 0 {
		return nil, errors.NotValidf("invalid TLS/x509 key path [%s]", srv.PathKey)
	}
	var certBytes, keyBytes []byte
	var err error

	Logger.Info().Str("key", srv.PathKey).Str("crt", srv.PathCrt).Msg("TLS config")

	if certBytes, err = ioutil.ReadFile(srv.PathCrt); err != nil {
		return nil, errors.Annotate(err, "certificate file error")
	}
	if keyBytes, err = ioutil.ReadFile(srv.PathKey); err != nil {
		return nil, errors.Annotate(err, "key file error")
	}

	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return nil, errors.New("invalid certificates")
	}

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, errors.Annotate(err, "x509 key pair error")
	}

	return grpc.NewServer(append(srv.serverOptions(
		grpc.Creds(credentials.NewServerTLSFromCert(&cert))), opts...)...), nil
}

// ServeInsecure is ServeTLS without the transport security.
func (srv *ServerConfig) ServeInsecure(opts ...grpc.ServerOption) (*grpc.Server, error) {
	return grpc.NewServer(append(srv.serverOptions(), opts...)...), nil
}

func (srv *ServerConfig) serverOptions(head ...grpc.ServerOption) []grpc.ServerOption {
	return append(head,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			NewUnaryServerInterceptorZerolog(srv.SessionKey))),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			NewStreamServerInterceptorZerolog(srv.SessionKey))))
}

func DialTLS(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, errors.NotImplemented
}

// DialInsecure dials endpoint without transport security, with opts applied
// after its own options. As with ServeTLS, the keepalive is the caller's.
func DialInsecure(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, endpoint, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)...)
}
