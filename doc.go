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
package opengauss
