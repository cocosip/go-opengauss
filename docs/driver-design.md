# openGauss Go driver design

Design baseline: 2026-11-24. An application imports one module, supplies its connection settings, and uses the same binary against multiple openGauss releases. The [feature matrix](feature-matrix.md) identifies the evidence for each version difference. The [development tracker](development-progress.md) records implementation and live acceptance separately.

## Application contract

| Item | Contract |
| --- | --- |
| Module | `github.com/cocosip/go-opengauss`, with `go 1.26.0` in go.mod and a minimum-toolchain CI job using exactly Go 1.26.0 |
| Entry points | Package and registered driver name `opengauss`. Support a blank import with `sql.Open` and an explicit Connector with `sql.OpenDB`. |
| Configuration | Accept libpq-style keyword/value DSNs and PostgreSQL-style URLs, positional parameters and SQLSTATE. The caller selects endpoints, TLS policy and desired role. The driver determines server capabilities. |
| Migration | Existing standard-library callers using the opengauss name can keep their calls. The upstream package also registers mogdb. Callers of that alias must change the name. Loading both drivers under the same name would cause duplicate registration. Compare signatures for Array, CopyIn, Listener, GSS and vector helpers before promising source compatibility. |
| Endpoints | Start with the native openGauss wire protocol. B-compatible databases, MySQL wire listeners and oGRAC have separate acceptance criteria. The oGRAC release notes list JDBC and ODBC. |
| SQL | Pass application SQL, parameters and server errors through. Indexes, installed extensions and server syntax determine what the database can execute. |

```go
import (
    "database/sql"
    _ "github.com/cocosip/go-opengauss"
)

// ctx and dsn are supplied by the application.
db, err := sql.Open("opengauss", dsn)
if err != nil {
    return err
}
defer db.Close()
return db.PingContext(ctx)
```

## Package layout

The public import path stays a single package, `opengauss`. Supporting code is layered below it:

| Package | Responsibility |
| --- | --- |
| `opengauss` (root) | `database/sql` integration, connection state machine, queries, transactions, TLS policy, public `Config` and `*Error`, DSN/environment/service/passfile resolution |
| `internal/proto` | wire framing, message types and the message reader/writer |
| `internal/auth` | password exchanges (cleartext, MD5, SHA256 proof) |
| `internal/pgpassfile` | libpq passfile parsing |
| `internal/pgservicefile` | libpq service-file parsing |

Dependencies point inward only: the root imports the internal packages, `internal/auth` may use `internal/proto`, and the parsers use only the standard library. Keeping the helpers internal keeps the public surface stable while the driver grows into the remaining feature-matrix rows.

## First public API

The proposed initial surface is `sql.Open("opengauss", dsn)` and `opengauss.NewConnector(dsn string) (*Connector, error)` followed by `sql.OpenDB(connector)`. Both entry points use the same parser and connection state machine. Connector creation may resolve local configuration files but must not contact the database. Syntax and local configuration errors can surface at construction. Dial, TLS, authentication and session errors surface at the first operation requiring a connection, including PingContext. A connector can open multiple physical connections concurrently. Only immutable settings are shared.

For the first milestone, accept `host`, `port`, `dbname`, `user`, `password`, `sslmode`, `sslrootcert` and `connect_timeout` for a single TCP endpoint. Support keyword/value syntax and a single-host URL. Keep explicit settings authoritative. Exercise verify-full first and preserve the DNS name for certificate verification. The compatibility target for omitted sslmode is upstream's prefer. Until a requested mode or DSN option is implemented, reject it explicitly instead of silently ignoring it or selecting a weaker mode. Complete service/passfile, multi-host and the remaining F01 cases before marking F01 complete.

The notification helper keeps the upstream names (`NewListener`, `Listen`, `Unlisten`, `Notify`, `Ping`, `NotificationChannel`, `Notification` with `PID`, `Channel` and `Extra`) so a migrating caller needs no signature change. It differs deliberately where upstream is unsafe or ambiguous: a command fails while the listener is reconnecting instead of queueing silently, the notice handler belongs to the listener session instead of a process-wide variable, and the upstream `ListenerConn` type is not provided. Applications that depended on those details must be adapted; the [tracker](development-progress.md) records the decision with F09.

## Migration notes for unimplemented settings

These upstream options are rejected with an explicit error rather than ignored. A caller migrating from the reference driver must change its configuration:

| Setting | Behaviour here | Migration |
| --- | --- | --- |
| `sslinline` | Rejected as an unsupported parameter | Write the certificate and key to files with owner-only permissions and pass `sslcert`/`sslkey` |
| `sslpassword` | Rejected: only unencrypted key files are read | Decrypt the key once at configuration time, or use `crypto/tls` through a future custom-config entry point |
| Custom `TLSConfig` | Not exposed; the driver derives one policy from `sslmode` plus the certificate parameters | Use one of the six `sslmode` values, `sslrootcert`, `sslcert` and `sslkey`; a programmatic TLS hook is a future extension |
| Custom dialer, `DialFunc` | Not exposed; targets are dialed with `net.Dialer` and the caller's context | Resolve or proxy at the network layer; a dialer hook is a future extension |
| `mogdb` driver name | Not registered | Change the driver name in `sql.Open` to `opengauss`; registering both names would duplicate the same implementation |

Registering only one name is deliberate: `database/sql` keys drivers by name, so a second alias would silently create a second independent pool for the same database.

Expose server failures as `*opengauss.Error` with `SQLState() string`, discoverable via `errors.As`. Preserve the original server code and message. Wrap local connection errors so `errors.Is` can identify context cancellation and deadline expiry. Connector.Connect must not return driver.ErrBadConn. An unsupported authentication method is a local protocol error and must not acquire an invented SQLSTATE. These API choices are project decisions to be tested through both public entry points.

## Pure Go requirement

All driver features, including optional authentication, encryption, COPY and codecs, must work without a C compiler, C client library or native client SDK. Implement the wire protocol in Go using net and crypto/tls. Use reviewed Go cryptographic primitives and implement the openGauss-specific message exchanges in this module. C/C++ libpq and kernel sources serve as protocol references. Do not bind them through cgo, load them dynamically, or invoke a native client executable. A feature that lacks a Go implementation stays explicitly unsupported.

The upstream snapshot `1b2ca1e` provides reusable Go startup, authentication and codec code. The root driver does not link libpq. Its optional Unix Kerberos provider uses gokrb5. Its Windows provider uses SSPI for current-user credentials and its dependency loads secur32.dll through a Go syscall. Implement a Go Kerberos/SPNEGO provider for the supported credential sources on both platforms. Verify ticket acquisition and renewal against a KDC. Windows integrated logon remains a separate capability until it has a conforming implementation. Ordinary OS networking and filesystem services remain available to Go. Build and source audit results are in [F16](feature-matrix.md#pure-go-audit).

Preserve applicable upstream copyrights and licenses when porting files. Review each transitive dependency, platform file and optional feature. CI must build every shipped module with cgo disabled, exercise public entry points, and inspect source and dependency changes for native loading paths that a successful build would miss.

## Connection and capability selection

| Stage | Behavior |
| --- | --- |
| Configuration | Parse once into immutable Connector settings. Resolve explicit DSN, service file, environment and defaults in descending precedence. Match passfile entries per candidate host when no explicit password is supplied. Validate paired hosts and ports. Redact credentials, keys and password-bearing DSNs in errors and logs. |
| Transport | Use the caller's deadline across candidate nodes. Honor the configured TLS mode. Test disable, allow, prefer, require, verify-ca and verify-full, including CA, hostname and client-certificate failures. Certificate or authentication errors do not authorize weakening the caller's policy. |
| Startup | Start with native protocol 3.51. The 1.0.0, 6.0.0 and 7.0.0 server headers all declare it. The 7.0 C client has a Lite-specific V1 branch that reconnects with 3.0 and uppercases the database name. Preserve the supplied name on ordinary connections. Enable a variant only for a documented, reproducible peer signal, on a fresh connection, within the deadline. |
| Authentication | Dispatch on the actual AuthenticationRequest and stored-password method. Check challenge lengths and iteration fields for the selected layout. Verify a server proof when that exchange supplies one. Distinguish request 10 (SHA256), 11 (MD5-SHA256), 12 (Lite SHA256_RFC) and 13 (SM3). Return an error identifying the stage and method for an unsupported exchange. |
| Session readiness | Consume ParameterStatus, BackendKeyData and ReadyForQuery. Preserve timezone, encoding, transaction state, endpoint and raw version strings. Parse an identifiable openGauss product version from a banner such as SELECT version(). Treat server_version as a distinct signal. The upstream parser passes its third version component by value to Sscanf. Test missing components, suffixes and unknown products. |
| Role selection | Check recovery role separately from transaction_read_only. Enforce primary, standby, read-write and read-only when requested. any requires no role. For prefer-standby, search for an available standby before falling back to an eligible node. Upstream groups prefer-standby with any. Test primary-first address ordering. |
| Optional features | Probe on first relevant use. Resolve type identity from namespace and pg_type, including extension or built-in vector types. Select optional catalog queries only when their dependencies are available. Basic queries must work for accounts without access to management views. |

Keep capability state per physical connection as available, unavailable or unknown, together with its evidence. Missing permissions or ambiguous identity leave a capability unknown. Use an equivalent basic path when available, such as a known textual representation. Return a specific error for an unfulfilled explicit driver request. Preserve the server's original error for application SQL. Rebuild observations on reconnect or failover and refresh facts affected by session settings. Keep probes outside user transactions and in-flight results, where a failed or interleaved query could invalidate the session.

## Queries, data and recovery

- Support simple queries, extended Parse/Bind/Describe/Execute/Sync, prepared statements, multiple result sets, command tags, SQL NULL and empty results. Preserve RowsAffected. Use INSERT ... RETURNING for generated keys. Upstream does not provide LastInsertId. Test the official DDL, repeated prepared inserts, updates, scalar function calls, transaction and JSONB examples. [Official Go examples](https://docs.opengauss.org/zh/docs/7.0.0/developer_guide/example_common_operations_go.html)
- Use RowDescription OID, format and typmod to select codecs. Request text for unrecognized types, preserve the original value, and avoid guessing a binary layout. Preserve numeric precision and negative scale. Test B-mode boolean text t/f and 1/0, arrays, UUID, BLOB/bytea, dates, timezones and JSONB. Return unknown column metadata when it cannot be established.
  The implemented mapping is deliberately one-way and total: every column yields an allowed `driver.Value` or the exact server text. bool (t/f and B-mode 1/0), int2/int4/int8/oid, float4/float8, bytea (hex and escape forms), date, timestamp and timestamptz decode to Go values; numeric, JSON, UUID, intervals, arrays, enums, domains and unknown OIDs stay text, which keeps precision and lets the caller choose. A decoder that cannot recognize a value's shape returns that text instead of an error, so infinity, BC dates and undecodable bytea escapes are never lost or guessed. A binary result column is refused rather than decoded as text. Parameters use the same text encoding: `[]byte` carries text and is passed through, so a bytea parameter must already be written as `\x` hex or in the escape form, and bool is written as 1/0, which is accepted by both the native and the dolphin-compatible parsers.
- Start with UTF-8 client text. Distinguish database encoding, client_encoding and raw binary values. Test non-ASCII parameters and results against databases using a different server encoding. Reject unsupported client encodings explicitly. In 7.0, record A, B, C, PG and D databases separately, including the dolphin prerequisite for B. [CREATE DATABASE](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/create_database.html)
- Implement DriverContext, Connector, QueryerContext, ExecerContext, ConnPrepareContext, ConnBeginTx, Pinger and appropriate rows interfaces. Validator and SessionResetter must account for transaction and protocol state. Preserve SQLSTATE, severity and details with errors.As. Return driver.ErrBadConn only when permitted by the Go contract and execution can be ruled out. A lost COMMIT response has an unknown outcome. Never replay that transaction. [Go 1.26.0 driver interfaces](https://github.com/golang/go/blob/go1.26.0/src/database/sql/driver/driver.go)
- Send cancellation only to the original backend endpoint with that connection's key. Coordinate late cancellation with row completion and pool return. Discard the connection if synchronization cannot be confirmed. A transaction remains bound to one physical connection.
  Implemented as the protocol CancelRequest: a separate connection to the exact peer address carries the 16-byte request with the backend process ID and secret key, and the server answers the running statement with SQLSTATE 57014 while the session stays synchronized and returns to the pool. As libpq does, the driver waits for the postmaster to close the cancel connection before the session may run another statement, so a processed request cannot abort the next one; the session is discarded when that acknowledgement or the statement's answer does not arrive within one second, and it is never reused while a cancel may still be pending. `ExecContext` surfaces the server error, while `QueryContext` callers can observe database/sql's context error instead.

## Operations tied to a session

| Operation | Implementation and acceptance |
| --- | --- |
| COPY | Pin the connection through completion and ReadyForQuery. Stream text FROM STDIN first. Check constraints, early cancellation, transaction state and final row count. A separate session can observe pg_stat_progress_copy on 7.0. bytes_total applies only to FROM FILE. Track TO STDOUT and binary input/output independently as proposed extensions. |
| COPY input (implemented) | `CopyIn(tx, table, columns...)` returns a statement whose `Exec`/`ExecContext` append one text-format row and whose `Close`, or an `Exec` without values, sends CopyDone and reads the result. The statement runs with the simple query protocol and is never prepared server-side, so a row costs one CopyData frame. It must run inside a transaction so the stream and its result stay on one session: a COPY prepared outside one is rejected, `ExecContext`/`QueryContext` point the caller to `CopyIn`, and `ResetSession` discards a connection that still has a stream open, so a pooled session cannot be wedged. The context of the opening call governs the stream, every row call first checks its own context, and a client-side row failure (width, encoding, size, deadline) ends the stream with CopyFail and resynchronizes the session, so a later `Close` cannot report success after importing the rows already sent. A stream belongs to the statement that opened it: another statement cannot extend or finish it, and a statement cannot start a second stream after it finished. Options after `FROM STDIN` are rejected because the encoder always writes the default text format; a server error ends the stream and is returned once the session is synchronized; a cancelled stream uses CancelRequest so the SQLSTATE and the session are both preserved; a send failure stops the stream's watcher and discards the connection. Binary-format responses are refused per column as well as as a whole, the row width comes from CopyInResponse, the first command tag is kept when a trailing separator produces an empty statement, and the row encoder reuses one buffer with a protocol size guard. |
| Notifications | Use a dedicated LISTEN connection. Registration and delivery take effect at commit. Test UNLISTEN, rollback and two sessions in the same database. Re-register on reconnect and signal the delivery gap. The 7.0 documentation excludes thread-pool mode. [LISTEN](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/listen.html) |
| Notifications (implemented) | `NewListener` and `NewListenerWithCallback` open one session for LISTEN/NOTIFY and never take it from a `database/sql` pool, where an unrelated statement could receive or lose its state. `Listen`, `Unlisten`, `Notify` and `Ping` run as simple queries outside transactions, so a registration or a notification takes effect at commit, which is immediately in autocommit; a channel name is written as a quoted identifier and a payload as an `E''` string literal. `NotificationChannel` delivers notifications in server order through a buffer of ten: a consumer that stops reading stalls the session instead of losing messages. After a connection failure the listener reconnects with exponential backoff between the caller's intervals (one second to one minute by default), registers every channel whose `LISTEN` had been acknowledged, and reports `ListenerEventDisconnected` and `ListenerEventReconnected` through the callback; the server retains nothing for an absent session, so the application reconciles its state when the session returns. A statement issued while no session exists fails with a reconnecting error instead of waiting for an unknown outage. NOTICE messages are exposed on the listening session through `SetNoticeHandler`; ordinary pooled queries consume notices and notifications without reporting them, so an application that needs them from ordinary statements must run those statements on a listener session, and a listener cannot share a session with an application transaction. Open: live two-session delivery, rollback and commit semantics, notices from pooled sessions, the 7.0 thread-pool restriction, long outages and silent peer loss are not exercised yet. `TestLiveNotifications` implements those live steps. |
| Vector search | Bind and round-trip text before enabling a specialized codec for an identified format. The optional concurrent helper accepts context, reuses the caller's pool, bounds concurrency, preserves input order and returns per-item errors. Keep validated scan settings and query execution on one pinned session, restore session settings and release the connection on error. |
| HA and upgrades | Select the endpoint when opening a physical connection. Preserve candidate-specific errors and the caller's deadline. Test mixed versions and re-detection after switchover. Record actual GUCs on fresh and upgraded installations. Routing does not move a running transaction. |
| `target_session_attrs` | Checked after authentication with one query inside the target's `connect_timeout` window: `SELECT pg_catalog.pg_is_in_recovery(), pg_catalog.current_setting('transaction_read_only')`. All six libpq values are supported; a mismatch closes that attempt and moves to the next endpoint. `prefer-standby` keeps the first acceptable server as the fallback instead of re-dialing every host as libpq does, with the same outcome, and a rejected password always fails the whole attempt. `any` sends no probe, so it costs nothing. Selection happens only when a physical connection is opened; a running transaction keeps its session. |

## Release evidence

Run one driver binary against each target build. Start with 7.0.0 LTS, 6.0.6, 6.0.0 and 5.0.x, then cover the historical and RC lines. Record edition, compatibility mode, plugins, native port, topology, TLS and thread-pool settings. Publish support only for measured build/feature combinations. Future releases use the basic protocol path and acquire specific rules when official changes and reproducible server behavior justify them.
