package oid

import (
	"database/sql/driver"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestDecode(t *testing.T) {
	utc := func(hour, min, sec int) time.Time {
		return time.Date(2026, 1, 2, hour, min, sec, 123456000, time.UTC)
	}
	zoned := func(hour, min, sec, offset int) time.Time {
		return time.Date(2026, 1, 2, hour, min, sec, 123456000, time.FixedZone("", offset))
	}
	tests := []struct {
		name string
		oid  int32
		text string
		want driver.Value
	}{
		{"bool true", Bool, "t", true},
		{"bool false", Bool, "f", false},
		{"bool one", Bool, "1", true},
		{"bool zero", Bool, "0", false},
		{"bool malformed", Bool, "yes", "yes"},
		{"int2", Int2, "-7", int64(-7)},
		{"int4", Int4, "42", int64(42)},
		{"int8 beyond float precision", Int8, "9007199254740993", int64(9007199254740993)},
		{"oid", Oid, "26", int64(26)},
		{"int malformed", Int4, "abc", "abc"},
		{"float4", Float4, "1.5", 1.5},
		{"float8", Float8, "-2.25e10", -2.25e10},
		{"float infinity", Float8, "Infinity", math.Inf(1)},
		{"float negative infinity", Float8, "-Infinity", math.Inf(-1)},
		{"float malformed", Float8, "1.5x", "1.5x"},
		{"numeric keeps text", Numeric, "12345678901234567890.123", "12345678901234567890.123"},
		{"bytea hex", Bytea, `\x00ff`, []byte{0x00, 0xff}},
		{"bytea empty", Bytea, `\x`, []byte{}},
		{"bytea escape octal", Bytea, `\001`, []byte{0x01}},
		{"bytea escape backslash", Bytea, `a\\b`, []byte(`a\b`)},
		{"bytea escape mixed", Bytea, `A\001B`, []byte{'A', 0x01, 'B'}},
		{"bytea invalid escape stays text", Bytea, `\9`, `\9`},
		{"bytea truncated escape stays text", Bytea, `ab\01`, `ab\01`},
		{"date", Date, "2026-01-02", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"date infinity", Date, "infinity", "infinity"},
		{"timestamp", Timestamp, "2026-01-02 15:04:05.123456", utc(15, 4, 5)},
		{"timestamp no fraction", Timestamp, "2026-01-02 15:04:05", time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)},
		{"timestamp malformed", Timestamp, "yesterday", "yesterday"},
		{"timestamptz +08", TimestampTZ, "2026-01-02 15:04:05.123456+08", zoned(15, 4, 5, 8*3600)},
		{"timestamptz utc suffix", TimestampTZ, "2026-01-02 15:04:05.123456Z", utc(15, 4, 5)},
		{"timestamptz half hour", TimestampTZ, "2026-01-02 15:04:05.123456+05:30", zoned(15, 4, 5, 5*3600+1800)},
		{"timestamptz negative", TimestampTZ, "2026-01-02 15:04:05.123456-04", zoned(15, 4, 5, -4*3600)},
		{"timestamptz offsets with seconds", TimestampTZ, "2026-01-02 15:04:05.123456+05:30:30", zoned(15, 4, 5, 5*3600+1830)},
		{"timestamp at midnight 24:00", Timestamp, "2026-01-02 24:00:00", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
		{"timestamp leap second", Timestamp, "2026-01-02 23:59:60", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
		{"timestamp 24:00 with minutes stays text", Timestamp, "2026-01-02 24:00:01", "2026-01-02 24:00:01"},
		{"timestamp out-of-range hour stays text", Timestamp, "2026-01-02 99:00:00", "2026-01-02 99:00:00"},
		{"timestamp compact offset stays text", TimestampTZ, "2026-01-02 15:04:05+0530", "2026-01-02 15:04:05+0530"},
		{"timestamp short offset stays text", TimestampTZ, "2026-01-02 15:04:05+8", "2026-01-02 15:04:05+8"},
		{"timestamp out-of-range offset hour stays text", TimestampTZ, "2026-01-02 15:04:05+19:00", "2026-01-02 15:04:05+19:00"},
		{"timestamp out-of-range offset minute stays text", TimestampTZ, "2026-01-02 15:04:05+08:99", "2026-01-02 15:04:05+08:99"},
		{"timestamp with offset is not a zone", Timestamp, "2026-01-02 15:04:05+08", "2026-01-02 15:04:05+08"},
		{"bc timestamp stays text", Timestamp, "0001-01-01 00:00:00 BC", "0001-01-01 00:00:00 BC"},
		{"bc date stays text", Date, "0001-01-01 BC", "0001-01-01 BC"},
		{"timestamp infinity", Timestamp, "infinity", "infinity"},
		{"text", Text, "hi", "hi"},
		{"empty text", Text, "", ""},
		{"unknown oid", 999999, "x", "x"},
		{"json", JSON, `{"a":1}`, `{"a":1}`},
		{"uuid", UUID, "936da01f-9abd-4d9d-80c7-02af85c822a8", "936da01f-9abd-4d9d-80c7-02af85c822a8"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decode(tc.oid, []byte(tc.text))
			if want, ok := tc.want.(time.Time); ok {
				gotTime, isTime := got.(time.Time)
				if !isTime {
					t.Fatalf("Decode = %T(%v), want time.Time", got, got)
				}
				if !gotTime.Equal(want) || gotTime.Location().String() != want.Location().String() {
					t.Errorf("Decode = %v, want %v", gotTime, want)
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Decode = %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestTypeName(t *testing.T) {
	tests := []struct {
		oid  int32
		want string
	}{
		{Bool, "bool"},
		{Bytea, "bytea"},
		{Int8, "int8"},
		{Int2, "int2"},
		{Int4, "int4"},
		{Oid, "oid"},
		{Raw, "raw"},
		{Blob, "blob"},
		{Float4, "float4"},
		{Float8, "float8"},
		{Numeric, "numeric"},
		{Text, "text"},
		{Varchar, "varchar"},
		{Bpchar, "bpchar"},
		{Bit, "bit"},
		{Varbit, "varbit"},
		{Nvarchar2, "nvarchar2"},
		{Date, "date"},
		{Timestamp, "timestamp"},
		{TimestampTZ, "timestamptz"},
		{UUID, "uuid"},
		{JSONB, "jsonb"},
		{0, ""},
		{999999, ""},
	}
	for _, tc := range tests {
		if got := TypeName(tc.oid); got != tc.want {
			t.Errorf("TypeName(%d) = %q, want %q", tc.oid, got, tc.want)
		}
	}
}

func TestScanType(t *testing.T) {
	tests := []struct {
		oid  int32
		want reflect.Type
	}{
		{Bool, reflect.TypeOf(false)},
		{Int4, reflect.TypeOf(int64(0))},
		{Float8, reflect.TypeOf(float64(0))},
		{Bytea, reflect.TypeOf([]byte(nil))},
		{Date, reflect.TypeOf(time.Time{})},
		{TimestampTZ, reflect.TypeOf(time.Time{})},
		{Text, reflect.TypeOf("")},
		{999999, reflect.TypeOf("")},
	}
	for _, tc := range tests {
		if got := ScanType(tc.oid); got != tc.want {
			t.Errorf("ScanType(%d) = %v, want %v", tc.oid, got, tc.want)
		}
	}
}

func TestColumnLength(t *testing.T) {
	tests := []struct {
		name   string
		oid    int32
		typmod int32
		want   int64
		ok     bool
	}{
		{"varchar(10)", Varchar, 14, 10, true},
		{"char(3)", Bpchar, 7, 3, true},
		{"nvarchar2(10)", Nvarchar2, 14, 10, true},
		{"varchar(10 char)", Varchar, 4 + (10 | charSemanticBit), 10, true},
		{"nvarchar2(10 char)", Nvarchar2, 4 + (10 | charSemanticBit), 10, true},
		{"bit(5)", Bit, 5, 5, true},
		{"varbit(32)", Varbit, 32, 32, true},
		{"varchar unbounded", Varchar, -1, 0, false},
		{"nvarchar2 unbounded", Nvarchar2, -1, 0, false},
		{"bit unbounded", Bit, -1, 0, false},
		{"text", Text, 14, 0, false},
		{"numeric", Numeric, 4 + (12<<16 | 4), 0, false},
	}
	for _, tc := range tests {
		got, ok := ColumnLength(tc.oid, tc.typmod)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ColumnLength(%s) = %d, %v, want %d, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestNumericPrecisionScale(t *testing.T) {
	tests := []struct {
		name      string
		oid       int32
		typmod    int32
		precision int64
		scale     int64
		ok        bool
	}{
		{"numeric(12,4)", Numeric, 4 + (12<<16 | 4), 12, 4, true},
		{"numeric(10,0)", Numeric, 4 + (10 << 16), 10, 0, true},
		{"numeric(1,1)", Numeric, 4 + (1<<16 | 1), 1, 1, true},
		{"numeric(12,-2)", Numeric, 4 + (12<<16 | (-2 & 0xffff)), 12, -2, true},
		{"numeric(12,-84)", Numeric, 4 + (12<<16 | (-84 & 0xffff)), 12, -84, true},
		{"numeric(1000,1000)", Numeric, 4 + (1000<<16 | 1000), 1000, 1000, true},
		{"numeric precision without scale", Numeric, 4 + (10<<16 | 0x8000), 10, 0, false},
		{"unconstrained numeric", Numeric, -1, 0, 0, false},
		{"varchar", Varchar, 14, 0, 0, false},
	}
	for _, tc := range tests {
		precision, scale, ok := NumericPrecisionScale(tc.oid, tc.typmod)
		if precision != tc.precision || scale != tc.scale || ok != tc.ok {
			t.Errorf("NumericPrecisionScale(%s) = %d, %d, %v, want %d, %d, %v",
				tc.name, precision, scale, ok, tc.precision, tc.scale, tc.ok)
		}
	}
}

func TestDecodeDoesNotAliasInput(t *testing.T) {
	// The connection allocates a fresh body per message today, but Decode must
	// not depend on that: returned values may outlive the input buffer.
	text := []byte("hello")
	got := Decode(Text, text)
	text[0] = 'j'
	if got != "hello" {
		t.Errorf("Decode aliased the input buffer: %q", got)
	}

	raw := []byte(`\x00ff`)
	decoded := Decode(Bytea, raw).([]byte)
	raw[2] = '0'
	if len(decoded) != 2 || decoded[0] != 0 || decoded[1] != 0xff {
		t.Errorf("Decode aliased the input buffer: %v", decoded)
	}
}

// TestTypeNameArrays covers the array and vector names, whose OIDs the server's
// catalog defines and which a caller reads through ColumnTypes.
func TestTypeNameArrays(t *testing.T) {
	tests := []struct {
		oid  int32
		want string
	}{
		{1000, "bool[]"},
		{1007, "int4[]"},
		{1009, "text[]"},
		{1231, "numeric[]"},
		{2951, "uuid[]"},
		{3201, "blob[]"},
		{3968, "nvarchar2[]"},
		{8308, "vector[]"},
		{Vector, "vector"},
		{Halfvec, "halfvec"},
		{Sparsevec, "sparsevec"},
		{86, "raw"},
		{999999, ""},
	}
	for _, tc := range tests {
		if got := TypeName(tc.oid); got != tc.want {
			t.Errorf("TypeName(%d) = %q, want %q", tc.oid, got, tc.want)
		}
	}
}

// TestArrayOIDsDoNotCollide keeps the array table from shadowing a scalar name or
// repeating an OID, which would mislabel a column.
func TestArrayOIDsDoNotCollide(t *testing.T) {
	seen := make(map[int32]string, len(arrayTypeNames))
	for arrayOID, element := range arrayTypeNames {
		if existing, ok := typeNames[arrayOID]; ok {
			t.Errorf("array OID %d is also the scalar %q", arrayOID, existing)
		}
		if previous, ok := seen[arrayOID]; ok {
			t.Errorf("array OID %d appears twice: %q and %q", arrayOID, previous, element)
		}
		seen[arrayOID] = element
		if _, ok := typeNames[elementOID(t, element)]; !ok {
			t.Errorf("array OID %d names element %q, which has no scalar entry", arrayOID, element)
		}
	}
}

// elementOID returns the OID of a scalar type name, failing the test when the
// name is not in the table.
func elementOID(t *testing.T, name string) int32 {
	t.Helper()
	for typeOID, candidate := range typeNames {
		if candidate == name {
			return typeOID
		}
	}
	t.Fatalf("no scalar type is named %q", name)
	return 0
}
