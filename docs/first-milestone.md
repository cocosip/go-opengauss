# First connection milestone

Build one vertical slice of the [design](driver-design.md) with Go 1.26.0, module path `github.com/cocosip/go-opengauss` and `CGO_ENABLED=0`. The deliverable is an application that connects through database/sql, authenticates, executes a parameterized query and rolls back a transaction. This exercises parts of F01-F06 and F16. The [tracker](development-progress.md) owns completion status.

## Implementation order

1. Create the module, register the driver and implement the shared DSN parser and Connector. Test invalid ports, quoting and password redaction with no network access. When adapting upstream code, record the original path and commit and retain applicable notices. The reference checkout's LICENSE.md requires the copyright and permission notices in copies or substantial portions.
2. Implement TCP, SSLRequest/TLS, StartupMessage 3.51 and the common SHA256 request-10 exchange in Go. Use a read deadline or equivalent cancellation handling throughout startup. An explicit TLS or authentication failure must preserve the cause and release the connection.
3. Wait for AuthenticationOk and ReadyForQuery before handing a connection to the pool. Parse intervening ParameterStatus, BackendKeyData and notices. Keep their state on the physical connection. Consume ErrorResponse and map its fields to the public error type.
4. Implement a simple query and the extended path for a bound integer, plus BeginTx, Rollback and clean connection reuse. Publish the first live result before expanding the authentication and server-version matrix.

## Offline protocol tests

Drive the ordinary Connector through a local scripted peer with synthetic credentials. Split a valid backend message across reads and combine consecutive messages in a single write to verify that parsing respects wire framing. Record fixture provenance (upstream commit or server build, protocol minor, auth method and TLS mode). Keep real passwords, reusable challenge responses and backend cancellation keys out of fixtures.

| Peer behavior | Observable result |
| --- | --- |
| Valid SHA256 exchange, AuthenticationOk, session parameters and ReadyForQuery | PingContext completes and the connection can execute a query. |
| AuthenticationOk followed by disconnect before ReadyForQuery | Connect fails and no partial session enters the pool. |
| Unknown authentication method or truncated response | Bounded local error identifies the startup stage and the connection closes. |
| Server returns ErrorResponse with SQLSTATE | errors.As extracts the same SQLSTATE. Logs contain no password or full DSN. |
| Peer stalls during startup or TLS verification fails | Caller deadline or TLS cause is observable. A verify-full request never falls back to plaintext. |
| Query returns a row followed by CommandComplete and ReadyForQuery | Scan returns the expected value. The next query on the same connection succeeds. |

## Live acceptance

Use a disposable database on a chosen native-port build. The suggested first target is 7.0.0 LTS with SHA256 authentication and a TLS certificate whose hostname matches the DSN. Supply a test account, matching CA and permission to create temporary tables. Record actual availability when provisioning. A reference client can establish that the endpoint accepts the account and TLS policy before investigating a driver failure. [live-acceptance.md](live-acceptance.md) has a container-based provisioning recipe, the DSN commands and a result template; it has not been executed in this workspace.

1. Use both sql.Open and NewConnector/sql.OpenDB with the same DSN. Call PingContext with a deadline, read SELECT version(), then bind the integer 42 to `SELECT $1::integer` and assert Scan returns 42.
2. Reserve one sql.Conn. Create a temporary table on it, begin a transaction on that connection, insert one row using a bound value, then roll back. Assert the row count is zero on the same connection, then close it. This avoids a pool switch hiding session or rollback errors.
3. Run a rejected login and a mismatched-hostname verify-full connection separately. Record the error stage and the server SQLSTATE if supplied. Confirm the valid account and endpoint remain usable.
4. Run the same application binary against 6.0.6 when available. Record binary or commit identity, Go version, target OS/architecture, exact server banner, edition, mode, TLS policy and case results in the tracker. A scripted peer or cross-build alone cannot establish server compatibility.

Once implementation and tests exist, run `go build ./...` and `go test ./...` with Go 1.26.0 and CGO_ENABLED=0 in every shipped module. Run native tests on their matching OS. Keep optional KDC, Lite, extension and HA environments in their respective feature rows. Their absence does not block development of this slice.
