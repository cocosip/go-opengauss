package opengauss

import (
	"bufio"
	"bytes"
	"github.com/cocosip/go-opengauss/internal/proto"
	"testing"
)

// The fuzz targets below assert that hostile backend bytes never panic the
// driver. `go test` runs the seed corpus; `go test -fuzz=Fuzz...` explores.

func FuzzReadMessage(f *testing.F) {
	f.Add([]byte{'Z', 0, 0, 0, 5, proto.TxnIdle})
	f.Add([]byte{'T', 0, 0, 0, 6, 0, 1})
	f.Add([]byte{'E', 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = proto.ReadMessage(bufio.NewReader(bytes.NewReader(data)))
	})
}

func FuzzParseErrorResponse(f *testing.F) {
	f.Add([]byte{'S', 'E', 'R', 'R', 'O', 'R', 0, 'C', '4', '2', 'P', '0', '1', 0, 0})
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = parseErrorResponse(data)
	})
}

func FuzzParseRowDescription(f *testing.F) {
	f.Add([]byte{0, 1, 'a', 0, 0, 0, 0, 0, 0, 0, 0, 0, 23, 0, 4, 0xff, 0xff, 0xff, 0xff, 0, 0})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseRowDescription(data)
	})
}

func FuzzParseDSN(f *testing.F) {
	f.Add("host=127.0.0.1 port=5432 user=u password='a b' dbname=d sslmode=disable")
	f.Add("host=h1,h2 port=5432,5433 user=u dbname=d sslmode=require connect_timeout=5")
	f.Add("postgres://u:p@h1:5432,h2/db?sslmode=verify-full&connect_timeout=5")
	f.Add("postgresql://u@[::1]:5432/db")
	f.Add("user=u password='it''s' dbname=d")
	f.Add("")
	f.Fuzz(func(t *testing.T, dsn string) {
		_, _ = parseDSN(dsn)
	})
}

func FuzzCopyDirection(f *testing.F) {
	f.Add("COPY t FROM STDIN")
	f.Add("COPY t (a, b) FROM STDIN WITH (FORMAT csv)")
	f.Add("COPY t TO STDOUT")
	f.Add("COPY t TO STDOUT WITH (FORMAT csv, HEADER, DELIMITER '|', NULL 'NA')")
	f.Add("COPY t TO STDOUT CSV HEADER QUOTE '''' ESCAPE '\\'")
	f.Add("COPY (SELECT 'COPY x FROM STDIN') TO STDOUT")
	f.Add("SELECT 'COPY t FROM STDIN'")
	f.Add("-- COPY t FROM STDIN\nSELECT 1")
	f.Add("/* COPY t TO STDOUT */ SELECT 1")
	f.Add("copy \"COPY\" from stdin")
	f.Fuzz(func(t *testing.T, statement string) {
		kind, clauseEnd := copyDirection(statement)
		if clauseEnd < 0 || clauseEnd > len(statement) {
			t.Fatalf("copyDirection(%q) returned clause end %d", statement, clauseEnd)
		}
		if kind == copyNone && clauseEnd != 0 {
			t.Fatalf("copyDirection(%q) = none with clause end %d", statement, clauseEnd)
		}
		if kind != copyNone {
			_ = copyInTailError(statement, clauseEnd)
			_, _ = parseCopyOutOptions(statement, clauseEnd)
		}
	})
}

func FuzzValidateVectorQuery(f *testing.F) {
	f.Add("SELECT id FROM t ORDER BY vec <-> $1 LIMIT 10")
	f.Add("SELECT * FROM t WHERE vec <#> $1 > 0.5 ORDER BY vec <=> $1")
	f.Add("select id from t")
	f.Add("SELECT 'x <-> y' FROM t WHERE id = $1")
	f.Add("/* <-> */ SELECT 1")
	f.Add("-- <->\nSELECT 1")
	f.Add("")
	f.Fuzz(func(t *testing.T, query string) {
		if statements, _ := inspectVectorQuery(query); statements < 0 {
			t.Fatalf("inspectVectorQuery(%q) returned %d statements", query, statements)
		}
		_ = validateVectorQuery(query)
	})
}
