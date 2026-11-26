// Package opengauss provides a database/sql driver for openGauss.
//
// The driver speaks the native openGauss wire protocol in pure Go. It
// registers itself as "opengauss" for database/sql:
//
//	import (
//		"database/sql"
//		_ "github.com/cocosip/go-opengauss"
//	)
//
//	db, err := sql.Open("opengauss", "host=127.0.0.1 port=5432 user=app password=secret dbname=app")
//
// Applications that need to resolve configuration before the first connection
// can build a Connector with NewConnector and hand it to sql.OpenDB.
//
// # Startup parameters
//
// The startup packet sends user, database and client_encoding=UTF8. Two
// optional libpq settings are supported: application_name, taken from the
// application_name DSN key, the service file or PGAPPNAME (falling back to
// fallback_application_name), and options, taken from the options key or
// PGOPTIONS. A parameter is omitted when its resolved value is empty.
//
// # Custom TLS
//
// RegisterTLSConfig associates a *tls.Config with a name that can be used as
// the sslmode value. The config is cloned for every physical connection, its
// ServerName is used when set and otherwise filled from the target, and the
// sslrootcert, sslcert and sslkey parameters are not applied to it.
//
// # Values
//
// Parameters and results use the server's text representation. The driver
// requests client_encoding=UTF8, so the server transcodes text to UTF-8; a
// session that reports any other client encoding than UTF8 or SQL_ASCII is
// rejected at startup, and a runtime change marks the session unusable. A
// session that never reports client_encoding is treated as UTF8. SQL_ASCII
// sessions pass bytes through unchanged, so their text may not be valid UTF-8.
// A parameter is converted to text as follows: string and []byte pass through
// unchanged, int64 and float64 use their decimal form, bool becomes 1 or 0,
// time.Time uses an ISO timestamp with its zone offset, and nil is SQL NULL. A
// []byte parameter therefore carries text, not a binary value: send a bytea
// column as []byte holding "\x" followed by hex digits, or as a string in the
// legacy escape form.
//
// Results are decoded by the type OID reported for each column: bool becomes
// bool, int2/int4/int8/oid become int64, float4/float8 become float64 (NaN and
// Infinity included), bytea becomes []byte, and date/timestamp/timestamptz
// become time.Time. Every other type, including numeric, JSON, UUID, intervals,
// arrays, enums, domains and unknown OIDs, is returned as the exact server text
// so precision and representation are preserved. A value whose text is not in
// the expected shape, such as infinity, a BC date or an undecodable bytea
// escape, is also returned as text rather than guessed.
//
// ColumnTypes reports the database type name, the Go scan type that this
// mapping produces, the declared length of a varchar, char, bit or varbit
// column, and the precision and scale of a numeric column, both taken from the
// type modifier in the result description. A negative numeric scale, which
// A-compatibility mode on 6.0 and later can declare, is preserved. Nullability
// is not looked up in the catalog, so it stays unknown.
//
// # Multiple statements
//
// A statement executed directly on the connection without parameters is sent
// with the simple query protocol, which the server may run as several
// statements separated by semicolons. The query returns the rows of the first
// statement that produces them; Rows.NextResultSet advances to the following
// sets even when the current one has unread rows, and statements without rows
// such as SET or an INSERT without RETURNING are skipped. Always Close the
// rows: before the last set it drains the remaining sets, so the session stays
// reusable. Prepared statements always use the extended protocol, which carries
// exactly one statement.
//
// # Session-scoped helpers
//
// CopyIn streams rows into a table inside a transaction with COPY ... FROM
// STDIN, using the default text format. The inverse direction is read through
// QueryContext: a COPY ... TO STDOUT statement returns rows whose columns are
// reported as column1..columnN and whose values are strings, with NULL as nil.
//
// NewListener keeps one dedicated session for LISTEN/NOTIFY and reconnects it
// after a failure, re-registering the channels it had accepted. It is separate
// from the sql.DB pool because a notification belongs to the session that
// registered for it.
//
// ExecuteMultiSearch runs one vector query per argument set on sessions taken
// from the caller's pool, applies scan parameters such as hnsw_ef_search to
// each pinned session, restores them afterwards, preserves the input order and
// reports a failure per argument set.
package opengauss
