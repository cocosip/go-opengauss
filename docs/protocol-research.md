# Protocol and edition research (F13)

This note records the F13 investigation: fully encrypted databases, logical
replication, MySQL listeners and oGRAC. For each area it states what the
feature is, how it reaches the client, which dependencies a pure-Go driver
would need, and whether the project should implement it. Sources are the
official openGauss documentation, mostly the 7.0 line; the 6.0.0-RC1 page is
cited where a client-version detail is stated only there, and the lite-edition
pages are labelled as such.

The native driver targets the openGauss wire protocol on the normal database
port. Nothing below changes that; the note exists so a later milestone does not
start from assumptions.

## Fully encrypted database (全密态)

What it is: data is encrypted on the client before it leaves the machine, stays
encrypted while the server stores and computes on it, and is decrypted by the
client. A client without the encryption switch sees ciphertext such as
`\x011aef...` for an encrypted column.

How a client uses it:

- Only two clients are documented: `gsql -C` and the JDBC driver with
  `enable_ce=1`. It is a client capability: the client creates and stores the
  keys locally and encrypts values before sending them.
- Keys are client objects. `CREATE CLIENT MASTER KEY` creates a CMK in a key
  store (`localkms` is the only supported store; the key files live under
  `$LOCALKMS_FILE_PATH` or `$GAUSSHOME/etc/localkms/`), wrapping the CEK with
  RSA_2048, RSA3072 or SM2. `CREATE COLUMN ENCRYPTION KEY` creates the CEK that
  encrypts column data with AEAD_AES_256_CBC_HMAC_SHA256,
  AEAD_AES_128_CBC_HMAC_SHA256 or SM4_SM3.
- A table declares `ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ..., ENCRYPTION_TYPE = DETERMINISTIC | RANDOMIZED)`.
  `DETERMINISTIC` keeps equality usable in `WHERE`; an encrypted equality
  query is not supported on a `RANDOMIZED` column.
- The server keeps the column metadata in `gs_encrypted_columns` and never sees
  the plaintext or the CMK.

Wire protocol impact: none documented. The statements are ordinary SQL, and the
server stores the key objects and ciphertext that the client sends. The client
work is the key management and the column codec: localkms file format, CMK
unwrap, CEK wrap/unwrap, deterministic IV derivation and the ciphertext
framing.

Pure-Go feasibility: possible in principle (AES-CBC, HMAC-SHA256 and RSA are in
the standard library; SM3 exists in the auth package, while SM2 and SM4 would
add dependencies), but the openGauss
docs describe CE only for `gsql` and JDBC and give no protocol or specification
document, so an implementation would have to be derived from those clients'
sources. That is a large, unverifiable offline effort with its own security
review.

Decision: not planned. A non-CE client can read an encrypted table and sees
ciphertext, which is enough to export or back it up, but the documentation says
inserts and DDL/DML changes on an encrypted table require CE mode, so the driver
cannot write them, and creating a CMK/CEK is documented only for `gsql` and
JDBC. The driver cannot encrypt or decrypt column values. A future milestone
should treat this as a separate security feature with its own threat model, not
as a codec.

## Logical replication

What it is: openGauss decodes its xlog into per-transaction logical changes.
The server provides decoding only; a consumer replays the changes.

How a client uses it: the documented tool-free path is the SQL function
interface. The official overview says openGauss provides logical decoding
"through the SQL function interface" and does not need a separate tool. The
functions are `pg_create_logical_replication_slot`,
`pg_logical_slot_get_changes` (consumes), `pg_logical_slot_peek_changes` (does
not consume) and `pg_drop_replication_slot`. openGauss also documents a
streaming client, `pg_recvlogical --start`, which keeps receiving the decoding
result in real time, and a JDBC streaming mode with parallel decoding; the
replication-protocol messages of that stream are not described in the 7.0 SQL
and operations guides, so a streaming consumer has to be treated as unverified
until a server can be measured.

Prerequisites, from the same pages: `wal_level=logical`, enough
`max_replication_slots`, `ssl=on` for logical replication, and the initial user
or a user with REPLICATION or SYSADMIN permission (or the `gs_role_replication`
role); with separation of powers disabled, the database administrator may also
do it. Only the primary can create or drop a slot; standby decoding is
documented with `enable_slot_log=on` on the primary, and a primary/standby/cascade
deployment is not supported by default. Limits: no DDL decoding, and a DDL
statement can lose physical logs that were not yet decoded; no column-store or
data-page decoding; no decoding on a cascade standby.

Output: the change-reading functions return `(location, xid, data)` rows; `data`
is produced by the output plugin of the slot. The built-in plugin is
`mppdb_decoding`, whose example output is delimited `BEGIN`/`COMMIT` lines
around JSON change objects; openGauss also documents a `wal2json` output plugin
(lite and latest extension references), so a consumer can ask for JSON
directly.

Wire protocol impact: none for the SQL-function path. The existing
`QueryContext` can call the functions today; a helper would poll them, decode
the plugin output and advance the slot. The risk is in the polling loop (slot
retention, backpressure, restart at the last LSN), not in the protocol.

Pure-Go feasibility: high for the SQL-function path, with no new dependency.
Testing needs a live server with `wal_level=logical`; the slot functions are not
exercised by the scripted peer because the server owns the decoding.

Decision: the raw functions are usable now; a dedicated polling helper is a
small future module once a live server can verify slot behaviour. The streaming
path is not planned until its protocol messages are documented or measured.

## MySQL protocol compatibility (dolphin)

What it is: the dolphin plugin adds a MySQL wire listener. Clients that speak
the MySQL protocol (the MySQL JDBC driver or the `mysql` command line) connect
to a separate port and openGauss answers with MySQL framing. The SQL surface of
a B-compatibility database is MySQL-like.

How it is enabled: `enable_dolphin_proto=on`, `dolphin_server_port` set to a
port different from the openGauss port, a database created with
`DBCOMPATIBILITY 'B'`, a user prepared with `set_native_password` for MySQL
native authentication, and `dolphin.default_database_name` when several
B-compatibility databases exist. On a MySQL-protocol connection,
`dolphin.b_compatibility_mode` and `enable_set_variable_b_format` are enabled
automatically.

Documented clients and limits: the 7.0-RC2 page lists the MySQL JDBC driver
8.0.28 and below and the `mysql` command line 8.0.28 and below; the 6.0.0-RC1
page lists the MySQL JDBC driver 5.1.47 and the `mysql` command line
5.7.26–5.7.42. TLS 1.2 only, so clients limited to TLS 1.0 or 1.1 cannot use
SSL; cursors (`useCursorFetch`/`setFetchSize`) and stored procedures are not
supported.

Wire protocol impact: this is the MySQL protocol by definition (handshake,
native password auth, query/resultset commands), not the openGauss protocol. A
Go client for it is a separate implementation; `go-sql-driver/mysql` already
speaks it, so a caller can use that driver against the dolphin port. This
project's native driver has no reason to duplicate it.

Decision: out of scope. The native driver remains the supported path, and the
B-mode SQL semantics it already handles (bool `1/0`, B-mode type text) are what
matters for it. Testing would need a live server with the dolphin plugin
enabled.

## oGRAC

What it is: a shared-storage multi-instance cluster. Every node runs a database
instance, CMS and DSS; instances read the same data files through DSS and
coordinate in-memory state through DMS/DTC. From the application view any
normally opened instance accepts reads and writes: there is no fixed primary
write node.

Client side: the official architecture page lists the JDBC driver, the oGRAC C
driver and the ODBC driver as supported, and says Go and Python drivers "will be
supported in the future". Whether a plain openGauss wire-protocol client can
connect to an oGRAC instance is not stated by that page, so it must be measured,
not assumed.

Driver impact: because there is no fixed primary, the F07 role probe
(`pg_is_in_recovery()` and `transaction_read_only`) does not describe an oGRAC
node the way it describes a primary/standby pair, and `target_session_attrs`
selection needs a cluster-aware design. A multi-writer cluster also changes
failover, retry and transaction-affinity expectations, so this is an HA design
task, not a protocol extension.

Pure-Go feasibility: unknown without a cluster and without a published protocol
description; the upstream project itself plans a Go driver. Not planned here
until that driver or a cluster is available.

Decision: track upstream. No code until a cluster can be tested.

## Summary

| Area | Protocol | Client work | Pure-Go now | Decision |
| --- | --- | --- | --- | --- |
| Fully encrypted database | openGauss wire protocol, client-side crypto | Key store, CMK/CEK management, deterministic/randomized column codec | Possible but unspecified; needs a port and security review | Not planned |
| Logical replication | SQL-function polling on the normal connection; a documented streaming client whose messages are not described | Poll the slot functions, decode plugin output, advance the slot | High for the SQL-function path, no new dependency | Raw functions usable now; polling helper later; streaming unverified |
| MySQL listeners (dolphin) | MySQL protocol on a separate port | None for this driver; use `go-sql-driver/mysql` | n/a | Out of scope; documented client versions differ per release |
| oGRAC | Connection protocol not documented for this client | Cluster-aware HA and role handling | Unknown | Track upstream |

## Sources

- Fully encrypted database, openGauss 7.0:
  [全密态数据库](https://docs.opengauss.org/zh/docs/7.0.0/characteristic_description/advanced_features/full_encrypted_database.html)
- Logical decoding overview, openGauss 7.0:
  [逻辑解码概述](https://docs.opengauss.org/zh/docs/7.0.0-RC3/database_om_guide/logical_decoding_overview.html)
- Logical decoding through SQL functions, openGauss 7.0:
  [使用SQL函数接口进行逻辑解码](https://docs.opengauss.org/zh/docs/7.0.0/database_om_guide/logical_decoding_by_sql_function_interfaces.html)
- Streaming decoding client, openGauss 7.0:
  [pg_recvlogical](https://docs.opengauss.org/zh/docs/7.0.0/tool_and_commandreference/pg_recvlogical.html)
- `wal2json` output plugin, openGauss 7.0 lite and latest:
  [wal2json](https://docs.opengauss.org/zh/docs/7.0.0-lite/extension_reference/extension_reference/server/wal2json.html)
  and [wal2json (latest)](https://docs.opengauss.org/en/docs/latest/extension_reference/extension_reference/server/wal2json.html)
- MySQL protocol compatibility, openGauss 7.0-RC2:
  [MySQL协议兼容](https://docs.opengauss.org/zh/docs/7.0.0-RC2/docs/ExtensionReference/dolphin-MySQL%E5%8D%8F%E8%AE%AE%E5%85%BC%E5%AE%B9.html)
  and openGauss 6.0.0-RC1:
  [MySQL协议兼容](https://docs.opengauss.org/zh/docs/6.0.0-RC1/docs/ExtensionReference/dolphin-MySQL%E5%8D%8F%E8%AE%AE%E5%85%BC%E5%AE%B9.html)
- oGRAC architecture, latest:
  [Architecture Overview](https://docs.opengauss.org/en/docs/latest/ograc/about_ograc/product_architecture/architecture_description.html)
