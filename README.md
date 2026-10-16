# cams-go-utils

What both sides of [Cams](https://github.com/ontavu/cams) use in Go: the LAN side
(`cams-agent`, AGPL-3.0) and the cloud side (`cams-hub`, proprietary). It is under the
MPL-2.0 so that both can link it.

```go
import "github.com/ontavu/cams-go-utils/utils"
```

One package, `utils`:

| What | Where |
|---|---|
| `Logger` and `LoggerContext`: a zerolog logger on stderr, which uses colour only when stderr is a terminal | `utils/logging.go` |
| `ServerConfig.ServeInsecure` / `ServeTLS` and `DialInsecure`: a gRPC server whose every call leaves one access line, and a client dial | `utils/grpc.go` |

## The gRPC helpers do not know the protocol

The keepalive two peers agree on, and the metadata key that ties their logs together, are
part of a contract, and that contract lives elsewhere: for Cams, in `cams-api/wire`. These
helpers therefore take them from the caller:

```go
cfg := utils.ServerConfig{ListenAddr: addr, SessionKey: wire.KeySession}
srv, err := cfg.ServeInsecure(wire.ServerOptions()...)

cnx, err := utils.DialInsecure(ctx, endpoint, wire.DialOptions()...)
```

A caller that passes nothing gets gRPC's defaults, so it has **no keepalive**, and its
access lines have no `session` field.

## Licence

[MPL-2.0](LICENSE). The authors are listed in [AUTHORS](AUTHORS).
