package opengauss

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/cocosip/go-opengauss/internal/oid"
	"github.com/cocosip/go-opengauss/internal/proto"
)

var (
	_ driver.RowsColumnTypeLength         = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale = (*rows)(nil)
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
		m.Int16(4) // type size
		typeMod := column.typeMod
		if typeMod == 0 {
			typeMod = -1
		}
		m.Int32(typeMod) // type modifier
		m.Int16(format)
	}
	// The parser receives the message body, without the type byte and length.
	return m.Finish()[5:]
}

func TestParseRowDescriptionKeepsOIDs(t *testing.T) {
	body := testRowDescription(0,
		column{name: "id", oid: oid.Int8},
		column{name: "raw", oid: oid.Bytea},
		column{name: "name", oid: oid.Varchar, typeMod: 14},
	)
	columns, err := parseRowDescription(body)
	if err != nil {
		t.Fatalf("parseRowDescription: %v", err)
	}
	if len(columns) != 3 {
		t.Fatalf("columns = %v, want 3", columns)
	}
	if columns[0].name != "id" || columns[0].oid != oid.Int8 {
		t.Errorf("columns[0] = %+v, want id/int8", columns[0])
	}
	if columns[1].name != "raw" || columns[1].oid != oid.Bytea {
		t.Errorf("columns[1] = %+v, want raw/bytea", columns[1])
	}
	if columns[2].typeMod != 14 {
		t.Errorf("columns[2].typeMod = %d, want 14", columns[2].typeMod)
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
	if _, ok := result.ColumnTypeLength(-1); ok {
		t.Error("ColumnTypeLength(-1) reported a length")
	}
	if _, ok := result.ColumnTypeLength(1); ok {
		t.Error("ColumnTypeLength(1) reported a length")
	}
	if _, _, ok := result.ColumnTypePrecisionScale(-1); ok {
		t.Error("ColumnTypePrecisionScale(-1) reported a precision")
	}
	if _, _, ok := result.ColumnTypePrecisionScale(1); ok {
		t.Error("ColumnTypePrecisionScale(1) reported a precision")
	}
}

// TestColumnTypesReportDeclaredModifiers checks the typmod-derived metadata:
// declared lengths for character and bit types and precision/scale for numeric.
func TestColumnTypesReportDeclaredModifiers(t *testing.T) {
	const query = "SELECT v, c, b, b1, n, neg, t FROM typmods"
	server := newFakeServer(t, func(s *fakeSession) {
		s.completeStartup(nil)
		if got := s.expectSimpleQuery(); got != query {
			s.fail("query = %q", got)
			return
		}
		s.sendRowDescription(
			fakeColumn{name: "v", oid: oid.Varchar, typmod: 14},                             // varchar(10)
			fakeColumn{name: "c", oid: oid.Bpchar, typmod: 7},                               // char(3)
			fakeColumn{name: "b", oid: oid.Varbit, typmod: 32},                              // varbit(32)
			fakeColumn{name: "b1", oid: oid.Bit, typmod: 1},                                 // bit(1)
			fakeColumn{name: "n", oid: oid.Numeric, typmod: 4 + (12<<16 | 4)},               // numeric(12,4)
			fakeColumn{name: "neg", oid: oid.Numeric, typmod: 4 + (10<<16 | (-2 & 0xffff))}, // numeric(10,-2)
			fakeColumn{name: "t", oid: oid.Text},
		)
		s.sendCommandComplete("SELECT 0")
		s.sendReady(proto.TxnIdle)
		s.expectType(proto.MsgTerminate)
	})

	db := openTestDB(t, testDSN(server, ""))
	rows, err := db.QueryContext(testContext(t), query)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer func() { _ = rows.Close() }()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	if len(types) != 7 {
		t.Fatalf("ColumnTypes returned %d columns", len(types))
	}
	if name := types[0].DatabaseTypeName(); name != "varchar" {
		t.Errorf("varchar type name = %q", name)
	}
	if length, ok := types[0].Length(); !ok || length != 10 {
		t.Errorf("varchar length = %d, %v, want 10", length, ok)
	}
	if _, _, ok := types[0].DecimalSize(); ok {
		t.Error("varchar reported a numeric precision")
	}
	if length, ok := types[1].Length(); !ok || length != 3 {
		t.Errorf("char length = %d, %v, want 3", length, ok)
	}
	if length, ok := types[2].Length(); !ok || length != 32 {
		t.Errorf("varbit length = %d, %v, want 32", length, ok)
	}
	if length, ok := types[3].Length(); !ok || length != 1 {
		t.Errorf("bit(1) length = %d, %v, want 1", length, ok)
	}
	if precision, scale, ok := types[4].DecimalSize(); !ok || precision != 12 || scale != 4 {
		t.Errorf("numeric precision/scale = %d/%d, %v, want 12/4", precision, scale, ok)
	}
	if _, ok := types[4].Length(); ok {
		t.Error("numeric reported a character length")
	}
	if precision, scale, ok := types[5].DecimalSize(); !ok || precision != 10 || scale != -2 {
		t.Errorf("numeric(10,-2) precision/scale = %d/%d, %v, want 10/-2", precision, scale, ok)
	}
	if _, ok := types[6].Length(); ok {
		t.Error("text reported a declared length")
	}
	if _, _, ok := types[6].DecimalSize(); ok {
		t.Error("text reported a numeric precision")
	}
	// Nullability needs a catalog lookup, so it stays unknown rather than
	// guessed from the wire data.
	for i, typ := range types {
		if _, ok := typ.Nullable(); ok {
			t.Errorf("types[%d].Nullable() reported a known value", i)
		}
	}
}
