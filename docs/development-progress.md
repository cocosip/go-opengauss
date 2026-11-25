# Development and compatibility tracker

Baseline: 2026-11-24. Target module: `github.com/cocosip/go-opengauss`. Target toolchain: Go 1.26.0. The project is in design. Consult the [feature matrix](feature-matrix.md) for release evidence and the [driver design](driver-design.md) for implementation decisions.

## Updating status

Change `[ ]` to `[x]` when an item's acceptance conditions are met. Append the implementation commit, command or report. Record partial work and missing environments on the unchecked line. Track live server results separately from implementation and cross-builds. For research or proposed extensions, record the scope decision and keep unimplemented capabilities visible.

## Completed research

- [x] Compared local upstream branches, tags, authentication, OID changes and the multi-vector API.
- [x] Cross-checked release features with the official guides, Go examples and relevant kernel/libpq sources.
- [x] Audited upstream cgo usage and Windows SSPI. With Go 1.26.0 and CGO_ENABLED=0, built the root and Kerberos modules for Windows/amd64 and Linux/amd64. See the [audit results](feature-matrix.md#pure-go-audit).
- [x] Published the design, feature matrix and development/acceptance checklists in English.

## Implementation

Start with the [first connection milestone](first-milestone.md). Its checkboxes track a subset of the feature rows below.

- [ ] Compile both public entry points and pass offline startup, error and query cases with Go 1.26.0 and CGO_ENABLED=0.
  Partial, 2026-11-25 (commit 5ae1c7a). `sql.Open`/`Driver.Open` and `NewConnector`/`sql.OpenDB` are covered by offline scripted-peer tests; `go test ./...`, `go test -race ./...`, `go vet ./...` and `golangci-lint run ./...` pass, and `go build ./...` succeeds with `CGO_ENABLED=0` for windows/amd64 and linux/amd64 using Go 1.27.4 against the `go 1.26.0` module target. A Go 1.26.0 CI run is still required before this line is closed.
- [ ] Attach a live report for the first server build covering SHA256, TLS verification, a bound query, rollback and connection reuse.
  Not started: no server endpoint is available in this environment. `live_test.go` implements the acceptance steps and runs when `OPENGAUSS_DSN` names a disposable database.
- [ ] Attach a second server-build report produced with the same application binary before claiming cross-version acceptance.

- [ ] **F01 Connector and TLS.** Create go.mod and build with Go 1.26.0. Test blank import/OpenDB, URL and DSN escaping, DSN/service/environment precedence, passfile host matching, host/port pairing, six sslmode values, client certificates and hostname verification. Record migrations for inline keys, sslpassword, custom TLS config, dialers and mogdb users.
  Partial, 2026-11-25 (commit 5ae1c7a). Implemented single-endpoint keyword/value and URL parsing with quoting, host/port validation, `connect_timeout`, all six sslmode values, `sslrootcert` and verify-full hostname checks. Local CA files are validated when the Connector is built, and verify-ca/verify-full are covered offline. Service/passfile/environment precedence, multi-host, client certificates, inline keys, sslpassword, custom TLS config, dialers and the mogdb alias remain rejected or unimplemented. Omitting `sslrootcert` for a verify-* mode uses the system trust store instead of libpq's `~/.postgresql/root.crt`.
- [ ] **F02 Authentication.** Test protocol 3.51 and each supported authentication exchange, including rejected credentials. Capture GSS and Lite-specific fixtures. Test request 12 on a matching Lite build. Only test the 3.0 retry and database-name conversion against the corresponding V1 peer.
  Partial, 2026-11-25 (commit 5ae1c7a). Protocol 3.51 startup plus requests 3 (cleartext), 5 (MD5) and 10 (SHA256, stored plain/md5/sha256) are implemented and covered offline; the SHA256 proof matches the reference client's published vector. Requests 7/8/9/11/12/13 are rejected with the method name. GSS, Lite and SM3 fixtures are not available.
- [ ] **F03 Discovery.** Retain raw banners and protocol parameters. Test unknown builds, restricted catalogs, absent extensions and one process connected to two different releases.
  Partial, 2026-11-25 (commit 5ae1c7a). Startup ParameterStatus, BackendKeyData and notices are consumed and retained per physical connection. Banner parsing, capabilities, catalogs and mixed-release sessions are not implemented.
- [ ] **F04 Standard SQL.** Reproduce the official DDL, prepared insert loop, update, scalar function, transaction and JSONB examples. Check isolation, multiple results, RowsAffected, unsupported LastInsertId and INSERT RETURNING.
  Partial, 2026-11-25 (commit 5ae1c7a). Simple queries, bound parameters through Parse/Bind/Describe/Execute/Sync, prepared statements, BeginTx/Commit/Rollback, command tags and RowsAffected are implemented and covered offline; LastInsertId returns an explicit error. A simple Query executes one statement and discards any additional result sets. Multiple result sets, JSONB and the official example suite are not yet covered.
- [ ] **F05 Types.** Round-trip parameters and values. Check ColumnTypes, B booleans, precision and negative scale, NULL/empty values, arrays, UUID, BLOB, timezones and unknown OIDs. Record multidimensional array limits.
- [ ] **F06 Cancellation and reuse.** Cancel a running query, close rows early, inspect SQLSTATE, reuse a synchronized connection and confirm no duplicate write on lost COMMIT response.
  Partial, 2026-11-25 (commit 5ae1c7a). Context deadlines and cancellation interrupt startup and row streaming, early Rows.Close resynchronizes the session, SQLSTATE survives errors, a failed transaction reports the server's ROLLBACK tag instead of a false commit, and a lost COMMIT response discards the connection. A protocol CancelRequest is not implemented, and Stmt.Close/Commit/Rollback run without a caller deadline.
- [ ] **F07 High availability.** Test all six target_session_attrs values. Verify actual role when the first address is a primary and a standby exists, and when only a primary remains. Cover strict-role failure, timeout, failover and reconnect during an upgrade.
- [ ] **F08 Text COPY input.** Test transaction-scoped streaming, constraints, interruption and pool reuse. Observe FILE and PIPE separately on LTS. Check bytes_total only for FROM FILE and record view permissions. Check that import works on older servers without the view.
- [ ] **F09 Notifications.** Test NOTICE separately from two-session LISTEN/NOTIFY. Cover registration commit, publisher rollback, UNLISTEN, reconnect and the thread-pool restriction.
- [ ] **F10 Vector data.** Test parameter binding, scans, nearest-neighbor ordering, NULL and invalid dimensions against a 6.x extension and 7.x kernel types. Include an extension-absent database.
- [ ] **F11 Concurrent vector search.** Reuse the application pool, bound concurrency, pin settings to the correct session, preserve order and per-item failures, and release connections on cancellation.
- [ ] **F12 Version-specific SQL.** Test RC3 JSONB paths, LTS identity and INSERT ON CONFLICT. Record parameters, result types and behavior on older servers.
- [ ] **F13 Protocol and edition research.** Investigate fully encrypted databases, logical replication, MySQL listeners and oGRAC separately. Record wire protocol, dependencies and feasibility in pure Go for each.
- [ ] **F14 COPY extensions.** Decide and record scope separately for text output, binary input and binary output. For each implemented path, test large streams, NULL, cancellation and recovery. List any remaining unsupported format.
- [ ] **F15 Encodings and modes.** Inventory legal modes per build. On 7.0 LTS test A, B, C, PG and D as available, including B-mode dolphin requirements. Exercise non-ASCII text, raw bytea, NULL, non-UTF-8 server databases, and unsupported client encodings.
- [ ] **F16 Pure Go.** Build every shipped module with Go 1.26.0 and CGO_ENABLED=0 on supported targets. Inspect dependency graphs and platform-specific code for native SDK loading. Implement and test Go Kerberos/SPNEGO with supported credentials on Linux and Windows. Record Windows integrated-logon limits explicitly.

## Live acceptance

Use the same driver binary and test cases for each target. Record exact builds within each family. Start with the applicable F01-F06 cases, then run the feature-specific cases below. A successful patch or configuration covers only that recorded combination.

- [ ] **7.0.0 LTS.** Native endpoint, COPY progress, notifications, vector types, new SQL, compatibility modes, thread pool on/off and primary/standby.
- [ ] **6.0.6.** Patch baseline. Compare authentication, type metadata, HA and optional dataVec with 6.0.0.
- [ ] **6.0.0 LTS.** Native endpoint, B-mode booleans, negative-scale numeric, and databases with/without dataVec.
- [ ] **5.0.x.** Record 5.0.0 and a maintenance patch separately. Test queries, B-mode codecs and primary/standby.
- [ ] **5.1.x.** Record 5.1.0, readable standby and resource-pooling behavior separately.
- [ ] **7.0.0-RC1.** Discover and round-trip kernel vector types.
- [ ] **7.0.0-RC2.** Check server multi-vector retrieval and Go concurrent search independently.
- [ ] **7.0.0-RC3.** JSONB paths and B-mode authentication defaults on fresh and upgraded installations.
- [ ] **6.0.0-RC1.** Capture startup and test the newly introduced dataVec extension with and without installation.
- [ ] **3.0.x and 3.1.x.** Record each family, handshake, transactions, COPY, types and switchovers.
- [ ] **2.0.x and 2.1.x.** Record each family, authentication, reads/writes, cancellation and standby role.
- [ ] **1.0.x and 1.1.0.** Capture startup and authentication. Verify basic queries, transactions and types.
- [ ] **Mixed releases.** Connect one driver binary to two releases. On a supported upgrade path, verify role changes, refreshed capabilities, transaction errors and write counts.
- [ ] **Lite and KDC environments.** Record exact peer build, authentication method, supported credentials and platform. Track unavailable environments separately.

### Result record

Append one row per run. Use passed, failed, not applicable on server, or environment unavailable. For failures, include the failing case and SQLSTATE or protocol stage. Redact connection secrets.

| Date | Driver commit / Go | Server build / edition | Mode, topology, TLS, auth, extensions, thread pool | Feature / case | Result | Command / report / issue |
| --- | --- | --- | --- | --- | --- | --- |

## Release checks

- [ ] Run builds and unit tests with Go 1.26.0 and cgo disabled. Run any applicable race tests in a separate supported toolchain configuration.
- [ ] Review direct, transitive and optional dependencies after upgrades, including DLL/shared-library loading and external client tools.
- [ ] Link every published version/feature claim to live evidence. List limitations and untested configurations.
- [ ] Run migration cases for upstream DSNs, standard-library calls and named helper APIs. List application changes.
