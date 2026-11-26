# go-opengauss

A pure-Go [`database/sql`](https://pkg.go.dev/database/sql) driver for
[openGauss](https://opengauss.org/). It speaks the native openGauss wire
protocol (3.51) with no cgo, no external client libraries and no C compiler at
build time.

The module is pre-release: implementation and offline protocol tests are
complete for the features below, but no live server has been recorded yet. See
[docs/development-progress.md](docs/development-progress.md) for the exact
acceptance state and [docs/feature-matrix.md](docs/feature-matrix.md) for the
per-version evidence.

## Requirements

- Go 1.26 or newer (`go.mod` declares `go 1.26.0`).
- Any openGauss build that accepts protocol 3.51, SHA256, MD5-SHA256, SM3, MD5
  or cleartext password authentication.

## Install

```sh
go get github.com/cocosip/go-opengauss
```

## Usage

Register the driver with a blank import and open a database:

```go
import (
	"context"
	"database/sql"
	"time"

	_ "github.com/cocosip/go-opengauss"
)

db, err := sql.Open("opengauss", "host=127.0.0.1 port=5432 user=app password=secret dbname=app sslmode=prefer")
if err != nil {
	return err
}
defer db.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := db.PingContext(ctx); err != nil {
	return err
}
```

When configuration should be resolved before the first connection, build a
Connector and hand it to `sql.OpenDB`:

```go
connector, err := opengauss.NewConnector(dsn)
if err != nil {
	return err
}
db := sql.OpenDB(connector)
```

`NewConnector` validates local configuration and TLS material without
contacting the server; dial, TLS, authentication and session errors surface on
`PingContext` or the first statement.

## Configuration

Both libpq keyword/value DSNs and PostgreSQL-style URLs are accepted.
Precedence is DSN > service file > environment > `.pgpass`, as in libpq.

| Parameter | Meaning |
| --- | --- |
| `host`, `port` | Target list. A host starting with `/` is a Unix-domain socket directory. |
| `user`, `password` | Login credentials. `password` falls back to the passfile. |
| `dbname`, `database` | Database name; defaults to the user name. |
| `sslmode` | `disable`, `allow`, `prefer` (default), `require`, `verify-ca`, `verify-full`. |
| `sslrootcert`, `sslcert`, `sslkey` | CA file and client certificate key pair. |
| `sslinline` | Treat `sslrootcert`, `sslcert` and `sslkey` values as PEM data, not paths. |
| `sslpassword` | Passphrase for an RFC 1423 encrypted `sslkey`. |
| `connect_timeout` | Per-target connect and startup timeout in seconds. |
| `target_session_attrs` | `any`, `read-write`, `read-only`, `primary`, `standby`, `prefer-standby`. |
| `application_name`, `fallback_application_name` | Startup `application_name`. |
| `options` | Startup `options` command-line string. |
| `service`, `passfile` | Service file name and passfile path. |

Environment variables: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
`PGPASSWORD`, `PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY`,
`PGCONNECT_TIMEOUT`, `PGTARGETSESSIONATTRS`, `PGAPPNAME`, `PGOPTIONS`,
`PGSERVICE`, `PGSERVICEFILE`, `PGPASSFILE`. `PGPASSWORD` takes precedence over
the passfile; a passfile entry supplies a password only when nothing else does.

Parameters that the driver does not implement are rejected with an explicit
error instead of being ignored. That includes `sslcert` material without its
counterpart, `sslcrl`, `gssencmode` and unknown keys.

Examples:

```text
host=db1,db2 port=5432,5433 user=app dbname=app sslmode=verify-ca sslrootcert=/etc/ssl/root.crt
postgres://app:secret@db1:5432/app?sslmode=prefer
host=/var/run/postgresql user=app dbname=app
```

For a TLS policy that the DSN cannot express, register a `*tls.Config` and use
its name as `sslmode`:

```go
opengauss.RegisterTLSConfig("app", &tls.Config{RootCAs: roots})
db, err := sql.Open("opengauss", "host=... user=... dbname=... sslmode=app")
```

The registered config is cloned for every physical connection; an empty
`ServerName` is filled from the target, and the per-connection certificate
parameters are not layered on top.

For a custom network path, use `NewConnectorWithDialer`; the dialer receives
the target network (`tcp` or `unix`) and address for every connection attempt,
including TLS fallbacks and later targets.

## Features

- Authentication: cleartext, MD5, SHA256 (stored as plain, md5 or sha256),
  MD5-SHA256 and SM3. GSS/SPNEGO, Windows SSPI and Lite SHA256_RFC are rejected
  by method name.
- Queries: simple protocol for direct statements, extended protocol for
  prepared statements and parameters, multiple result sets from parameterless
  statement batches, transactions with isolation levels, context cancellation
  with a server-side CancelRequest, and `driver.Validator`/`SessionResetter`
  support for pooled connections.
- TLS: all six `sslmode` values, client certificates, inline PEM material,
  encrypted keys, registered `*tls.Config` policies (`RegisterTLSConfig`) and
  verify-full host-name checks. `verify-full` verifies the certificate chain
  without a host name over a Unix-domain socket, as libpq does.
- Endpoints: multiple hosts tried in order, per-target TLS name checks and
  passfile matching, Unix-domain sockets, custom dialers
  (`NewConnectorWithDialer`) and `target_session_attrs` role selection.
- Values: text parameters and results. The driver decodes bool, integers,
  floats, bytea, date, timestamp and timestamptz; every other type, including
  numeric, JSON/JSONB, UUID, arrays, enums and domains, is returned as the
  exact server text. `ColumnTypes` reports declared lengths and numeric
  precision/scale when the server sends a type modifier.
- `CopyIn` streams text rows with `COPY ... FROM STDIN`; `COPY ... TO STDOUT`
  is read through `QueryContext`.
- `NewListener` keeps a dedicated LISTEN/NOTIFY session and reconnects with
  backoff.
- `ExecuteMultiSearch` runs a vector query per argument set on pinned sessions
  and restores scan parameters afterwards.

## Limits

- No live server acceptance has been recorded; every feature row in the
  tracker is still open for live evidence.
- Binary result format and binary `COPY` are not implemented; use the text
  representation.
- Typed vector codecs and capability/version discovery wait for a live 6.x/7.x
  server.
- GSS/SPNEGO, Windows integrated logon and Lite SHA256_RFC are not
  implemented.
- The driver registers only the `opengauss` name; the upstream `mogdb` alias
  is not registered.
- `LastInsertId` is unsupported, and array/UUID/JSON values are not decoded
  into Go types.

## Testing

```sh
go test ./...
go test -race ./...
golangci-lint run ./...
```

The live acceptance suite runs against a disposable database when
`OPENGAUSS_DSN` is set:

```sh
OPENGAUSS_DSN="host=... user=... password=... dbname=..." go test -run '^TestLive' -v ./...
```

## Documentation

- [Driver design](docs/driver-design.md)
- [Feature matrix](docs/feature-matrix.md)
- [Development tracker](docs/development-progress.md)
- [Protocol research](docs/protocol-research.md)
- [First connection milestone](docs/first-milestone.md)

## License

MIT. See [LICENSE](LICENSE). [NOTICE](NOTICE) reproduces the upstream MIT
notice for the openGauss Go client whose SHA256 exchange the implementation
was verified against.
