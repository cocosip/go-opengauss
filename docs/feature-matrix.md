# Server and driver feature matrix

Reviewed on 2026-11-24. A server release note dates a database feature. A driver tag dates a client implementation. A project feature ID identifies work on the new module. No server/feature pair below has passed live acceptance in this repository. See the [design](driver-design.md) and [development tracker](development-progress.md).

## Upstream Go history

The reference checkout is `D:\Code\go-source\openGauss-connector-go-pq`. Its local branches are 5.0.0, 7.0.0 and master. Dates below are commit dates. Server versions must be recorded independently. [Official Go repository](https://gitcode.com/opengauss/openGauss-connector-go-pq)

| Driver reference | Commit | Evidence in the snapshot |
| --- | --- | --- |
| v1.0.0 | 5febca5 | openGauss SHA256 authentication |
| v1.0.1, v1.0.2 | df86a6d | Same commit, with multiple-host support |
| v1.0.3 | 08e63f9 | sslpassword support |
| v3.0.0 | 2095cf3 | BLOB support |
| v1.0.4 | 0632413 | CopyData and SQLState added after the v3 snapshot |
| 5.0.0 branch, v5.0.0, v1.0.5 | dff0b18, 2023-06-13 | OID names for anyset, pg_subscription_rel and gs_sql_patch. A name mapping alone does not establish a codec. |
| v6.0.0, v1.0.6 | 873ff28, 2024-02-21 | B-mode text boolean fix accepting 1 as true. No local 6.0.0 branch. |
| v1.0.7 | 00d6fd5 | Log-level environment variable fix |
| 7.0.0 and master branches, v7.0.0, v1.0.8, v1.0.9 | 1b2ca1e, 2025-08-20 | Multi-node connection timeout fix and client-side ExecuteMultiSearch with subsequent fixes |

To reproduce the comparison in that checkout:

```sh
git branch -avv
git log --all --simplify-by-decoration --format="%h %as %d %s"
git diff v5.0.0 v6.0.0 -- encode.go
git log --no-merges --oneline v6.0.0..v7.0.0
```

## Server release evidence

The table lists changes relevant to clients. It does not infer support on every patch, edition or configuration from a major version. [Historical documentation branches](https://github.com/opengauss-mirror/docs/branches/all) and [official downloads](https://opengauss.org/zh/download/).

| Target | Documented change or baseline | Acceptance consequence | Source |
| --- | --- | --- | --- |
| 1.0.x, 1.1.0 | Documentation branches include 1.0.0, 1.0.1 and 1.1.0. The 1.0.0 protocol header declares 3.51. | Capture startup/authentication and basic type samples for each line. | [1.0 docs](https://github.com/opengauss-mirror/docs/tree/1.0.0), [protocol header](https://raw.gitcode.com/opengauss/openGauss-server/raw/1.0.0/src/include/libpq/pqcomm.h) |
| 2.0.x, 2.1.x | 2.0.0 adds delayed standby, standby logical replication and rolling upgrade capabilities. | Distinguish a readable standby from a writable session. Sample 2.1 independently. | [2.0.0 notes][s20] |
| 3.0.x, 3.1.x | 3.0.0 adds publication/subscription and parallel logical decoding. 3.1.0 enhances subscriptions and rolling upgrades. | Test reconnect and refreshed capabilities. Replication wire support requires its own client. | [3.0.0][s30], [3.1.0][s31] |
| 5.0.x | 5.0.0 expands MySQL syntax compatibility. JSONB, fully encrypted databases and national cryptographic algorithms appear under inherited features. | Exercise B-mode boolean values, parameter binding and metadata. Evaluate client-side encryption independently. | [5.0.0 notes][s50] |
| 5.1.x | 5.1.0 adds readable extreme-RTO standbys and transparent forwarding of standby writes in resource pooling. | Verify server role separately from session write policy. | [5.1.0 notes][s51] |
| 6.0.0-RC1 | Adds the dataVec extension. | Run vector discovery with the extension installed and basic SQL without it. | [RC1 notes][s6rc] |
| 6.0.x LTS | 6.0.0 lists dataVec as inherited and adds HNSW, negative numeric scale and MySQL 8.0 protocol compatibility. The download site separately lists 6.0.6. | Test 6.0.0 and 6.0.6 independently on native ports. Record dataVec configuration and numeric metadata. | [6.0.0 notes][s60], [downloads](https://opengauss.org/zh/download/) |
| 7.0.0-RC1 | Integrates vector storage and IVF-FLAT, IVF-PQ, HNSW and HNSW-PQ indexes into the kernel. | Discover actual types and OIDs for both extension and built-in deployments. | [RC1 notes][s7r1] |
| 7.0.0-RC2 | Adds BM25, DiskANN and multi-vector retrieval. | Test server search results separately from Go-side concurrent queries. | [RC2 notes][s7r2] |
| 7.0.0-RC3 | Adds JSONB path functions and RaBitQ. Changes B-mode user/host authentication defaults, with upgrade-specific behavior. | Test JSONB result types and authentication on fresh and upgraded instances. | [RC3 notes](https://github.com/opengauss-mirror/docs/blob/7.0.0-RC3/docs/zh/release_notes/release_notes.md) |
| 7.0.0 LTS | Lists LISTEN/NOTIFY, pg_stat_progress_copy, INSERT ON CONFLICT, identity columns and BM25 enhancements relative to RC3. | Test two-session delivery, COPY progress, parameterized conflict handling and returned identity values. | [LTS release notes](https://docs.opengauss.org/zh/docs/7.0.0/release_notes/release_notes.html) |
| 7.0.0 oGRAC | The separate release guide lists JDBC and ODBC interfaces. | Investigate the protocol before claiming native Go connectivity. | [oGRAC release notes](https://docs.opengauss.org/zh/docs/7.0.0/ograc/release_notes/release_notes.html) |

## Client capabilities

| ID | Capability | Upstream evidence | New driver work and limits |
| --- | --- | --- | --- |
| F01 | Connector, DSN and TLS | config.go, connector.go, ssl.go, pgpassfile and pgservicefile. The active TLS configuration defaults to prefer. | Port TCP/Unix sockets, URL and keyword DSNs, service/passfile handling, TLS modes, inline certificates, encrypted private keys and custom TLS config. Verify precedence and errors. |
| F02 | Startup and authentication | conn.go handles 0/3/5/7/8/10/11/13. The 6.0 and 7.0 headers also define request 12. The 7.0 C client handles it under ENABLE_LITE_MODE. | Implement supported exchanges in Go. Add Lite SHA256_RFC as a separate target. Test stored-password variants, GSS, unknown methods and edition-specific startup. [6.0 header](https://raw.gitcode.com/opengauss/openGauss-server/raw/6.0.0/src/include/libpq/pqcomm.h), [7.0 header](https://raw.gitcode.com/opengauss/openGauss-server/raw/7.0.0/src/include/libpq/pqcomm.h), [authentication](https://raw.gitcode.com/opengauss/openGauss-server/raw/7.0.0/src/common/interfaces/libpq/fe-auth.cpp), [startup](https://raw.gitcode.com/opengauss/openGauss-server/raw/7.0.0/src/common/interfaces/libpq/fe-connect.cpp) |
| F03 | Capability discovery | conn.go sends a fixed startup version and processes ParameterStatus. | New per-connection product, mode, protocol, type and extension discovery. Handle limited catalog access and unknown future builds. |
| F04 | Queries, results and transactions | conn.go, conn_go18.go, rows.go and the official Go guide | Cover DDL, repeated prepared inserts, updates, scalar functions, row iteration, JSONB, transactions, multiple results, RowsAffected and RETURNING. LastInsertId is unsupported upstream. Scalar SELECT does not establish OUT/INOUT or cursor support. Multiple result sets are implemented for parameterless simple queries. The official Go examples are reproduced offline; a live acceptance test exists but has not run yet; JSONB stays text. [Official examples](https://docs.opengauss.org/zh/docs/7.0.0/developer_guide/example_common_operations_go.html) |
| F05 | Types and metadata | oid/types.go, rows.go, encode.go, array.go, uuid.go and hstore | Extend dynamic OID handling. Test B-mode booleans, negative numeric scale, arrays, NULL, BLOB/bytea, UUID, JSON/JSONB, timezone, column names and typmods. Declared lengths (including nvarchar2 and character-semantic lengths) and numeric precision/scale are reported from the typmod. Upstream GenericArray.Scan rejects multidimensional arrays. |
| F06 | Context, errors and reuse | conn_go18.go and error.go, SQLState in v1.0.4 | Preserve SQLSTATE and cancellation. Test safe pool return and unknown commit outcomes without duplicate writes. Session reuse is gated by `driver.Validator`, and a canceled transaction rolls back and keeps the session. |
| F07 | Multi-host HA | config.go handles prefer-standby like any. conn_validate.go checks transaction_read_only and pg_is_in_recovery(). The README flags read-only limitations. | Implement standby preference, strict roles, overall deadlines and failover. Test primary-first ordering and fallback when only a primary is available. |
| F08 | Text COPY input and progress | copy.go and CopyData support text FROM STDIN. | Test stream completion, cancellation and constraints. 7.0 reports FILE/PIPE and limits bytes_total to FROM FILE. STDIN progress can report processed bytes/rows with unknown total. [Progress view](https://docs.opengauss.org/zh/docs/7.0.0/database_reference/pg_stat_progress_copy.html) |
| F09 | Notices and notifications | notice.go and notify.go predate the 7.0 LTS server announcement. | Test NOTICE separately. Test LISTEN/NOTIFY between sessions after commit, rollback, UNLISTEN, reconnect, and thread-pool restrictions. [LISTEN](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/listen.html), [NOTIFY](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/notify.html) |
| F10 | Vector types and SQL | dataVec in 6.0.0-RC1, kernel integration in 7.0.0-RC1 | New type-aware binding and scans, dynamic OIDs, missing extension, dimensions, NULL and nearest-neighbor ordering. |
| F11 | Concurrent vector queries | v7.0.0 parallel.go accepts a DSN and returns nested slices. | Add context, pool reuse, bounded concurrency and per-input errors. Keep scan settings on the query session. Document signature changes. [Official RC3 Go vector guide](https://github.com/opengauss-mirror/docs/blob/7.0.0-RC3/docs/zh/datavec/integration_go.md) |
| F12 | New SQL features | RC3 JSONB path functions and LTS identity/conflict syntax | Use ordinary parameterized SQL. Verify values, returned metadata and errors against older servers. |
| F13 | Separate protocols and editions | Fully encrypted databases, logical replication, MySQL listeners and oGRAC | Research only. Record wire requirements, client dependencies and viability in pure Go before opening implementation work. |
| F14 | COPY output and binary streams | Text `COPY ... TO STDOUT` is now read through `QueryContext`; upstream copy.go rejects CopyOutResponse and binary CopyInResponse. | Implemented: text output streaming with `column1..N`, NULL and escape decoding, cancel-based early stop and option refusals. Still proposed: binary input, binary output, CSV/HEADER output. [COPY](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/copy.html) |
| F15 | Encoding and database modes | The startup packet requests `client_encoding=UTF8`; every ParameterStatus is stored, and a session reporting any other encoding than UTF8 or SQL_ASCII is rejected. 7.0 lists A, B, C, PG, D and requires dolphin for B. | Implemented: UTF-8 client contract enforced at startup and at runtime, SQL_ASCII byte passthrough, non-ASCII and raw-bytea round trips. Open: live databases with a different server encoding, A/B/C/PG/D modes and the dolphin prerequisite for B. [CREATE DATABASE](https://docs.opengauss.org/zh/docs/7.0.0/sql_reference/create_database.html) |
| F16 | Pure Go implementation | Root and optional Kerberos modules build with cgo disabled. Windows Kerberos still invokes SSPI. | Require pure Go for core, optional features and dependencies. Replace the native authentication path. Build and audit every shipped module. |

## Pure Go audit

At `1b2ca1e`, the tracked upstream source contained no direct cgo import, #cgo directive or tracked C/C++ client library. With Go 1.26.0, CGO_ENABLED=0 and GOARCH=amd64, both `go build -mod=readonly ./...` and the dependency listing succeeded for the root module and the separate auth/kerberos module, targeting Windows and Linux. The selected package graphs had no CgoFiles. These are cross-build results; no authentication or live SQL operation was run.

| Path | Runtime evidence | Treatment |
| --- | --- | --- |
| Core | Native Go wire code in conn.go, TLS in ssl.go, authentication in rfcdigest.go. The Go package name pq does not imply that it links the C libpq library. | Reuse reviewed Go behavior. Keep client C libraries out of the dependency graph. |
| Unix Kerberos | auth/kerberos/krb_unix.go imports gokrb5/v8 and uses configuration and a credential cache. | Evaluate the Go provider on both target platforms with supported credential sources. |
| Windows Kerberos | krb_windows.go calls sspi/negotiate. The pinned sspi dependency's zsyscall_windows.go loads secur32.dll via syscall.NewLazyDLL. | Replace this platform-specific provider with Go Kerberos/SPNEGO. Track current-user Windows logon separately from ticket-cache authentication. |
| Future features | Native dependencies can enter through optional build tags, dynamically loaded SDKs or external tools without appearing in CgoFiles. | Inspect new dependencies and tagged/platform-specific files in addition to building with CGO_ENABLED=0. Apply the same rule to encryption and other optional features. |

[s20]: https://github.com/opengauss-mirror/docs/blob/2.0.0/content/zh/docs/Releasenotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s30]: https://github.com/opengauss-mirror/docs/blob/3.0.0/content/zh/docs/Releasenotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s31]: https://github.com/opengauss-mirror/docs/blob/3.1.0/content/zh/docs/Releasenotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s50]: https://github.com/opengauss-mirror/docs/blob/5.0.0/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s51]: https://github.com/opengauss-mirror/docs/blob/5.1.0/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s6rc]: https://github.com/opengauss-mirror/docs/blob/6.0.0-RC1/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s60]: https://github.com/opengauss-mirror/docs/blob/6.0.0/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s7r1]: https://github.com/opengauss-mirror/docs/blob/7.0.0-RC1/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
[s7r2]: https://github.com/opengauss-mirror/docs/blob/7.0.0-RC2/content/zh/docs/ReleaseNotes/%E7%89%88%E6%9C%AC%E4%BB%8B%E7%BB%8D.md
