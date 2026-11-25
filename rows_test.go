package opengauss

import (
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/oid"
	"github.com/cocosip/go-opengauss/internal/proto"
)

// testRowDescription builds a RowDescription body with one column per entry and
// the given result format code.
func testRowDescription(format int16, columns ...column) []byte {
	m := proto.NewMessageWriter(proto.MsgRowDescription)
	m.Int16(int16(len(columns)))
	for _, column := range columns {
		m.String(column.name)
		m.Int32(0) // table OID
		m.Int16(0) // attribute number
		m.Int32(column.oid)
		m.Int16(4)  // type size
		m.Int32(-1) // type modifier
		m.Int16(format)
	}
	// The parser receives the message body, without the type byte and length.
	return m.Finish()[5:]
}

func TestParseRowDescriptionKeepsOIDs(t *testing.T) {
	body := testRowDescription(0,
		column{name: "id", oid: oid.Int8},
		column{name: "raw", oid: oid.Bytea},
	)
	columns, err := parseRowDescription(body)
	if err != nil {
		t.Fatalf("parseRowDescription: %v", err)
	}
	if len(columns) != 2 {
		t.Fatalf("columns = %v, want 2", columns)
	}
	if columns[0].name != "id" || columns[0].oid != oid.Int8 {
		t.Errorf("columns[0] = %+v, want id/int8", columns[0])
	}
	if columns[1].name != "raw" || columns[1].oid != oid.Bytea {
		t.Errorf("columns[1] = %+v, want raw/bytea", columns[1])
	}
}

func TestParseRowDescriptionRejectsBinaryFormat(t *testing.T) {
	// A server must not answer with a binary column, because the driver would
	// decode its bytes as text and silently report a wrong value.
	body := testRowDescription(1, column{name: "id", oid: oid.Int8})

	_, err := parseRowDescription(body)
	if err == nil {
		t.Fatal("parseRowDescription accepted a binary column")
	}
	if !strings.Contains(err.Error(), "binary format") {
		t.Errorf("error = %v, want a binary format error", err)
	}
}

func TestRowsColumnMetadataBounds(t *testing.T) {
	result := &rows{columns: []column{{name: "a", oid: oid.Bool}}}
	if got := result.ColumnTypeDatabaseTypeName(-1); got != "" {
		t.Errorf("DatabaseTypeName(-1) = %q, want empty", got)
	}
	if got := result.ColumnTypeDatabaseTypeName(1); got != "" {
		t.Errorf("DatabaseTypeName(1) = %q, want empty", got)
	}
	if got := result.ColumnTypeDatabaseTypeName(0); got != "bool" {
		t.Errorf("DatabaseTypeName(0) = %q, want bool", got)
	}
}
