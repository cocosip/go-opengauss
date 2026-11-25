// Package oid maps openGauss type OIDs to the Go values this driver hands to
// database/sql. Results are read in the server's text format, so every decoder
// either returns a canonical Go value or the original text.
package oid

import (
	"database/sql/driver"
	"encoding/hex"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Type OIDs used by the decoders and the column metadata.
const (
	Bool        int32 = 16
	Bytea       int32 = 17
	Char        int32 = 18
	Name        int32 = 19
	Int8        int32 = 20
	Int2        int32 = 21
	Int4        int32 = 23
	Text        int32 = 25
	Oid         int32 = 26
	JSON        int32 = 114
	Float4      int32 = 700
	Float8      int32 = 701
	Bpchar      int32 = 1042
	Varchar     int32 = 1043
	Date        int32 = 1082
	Timestamp   int32 = 1114
	TimestampTZ int32 = 1184
	Interval    int32 = 1186
	Timetz      int32 = 1266
	Bit         int32 = 1560
	Varbit      int32 = 1562
	Numeric     int32 = 1700
	UUID        int32 = 2950
	JSONB       int32 = 3802
	Nvarchar2   int32 = 3969
)

var typeNames = map[int32]string{
	Bool:        "bool",
	Bytea:       "bytea",
	Char:        "char",
	Name:        "name",
	Int8:        "int8",
	Int2:        "int2",
	Int4:        "int4",
	Text:        "text",
	Oid:         "oid",
	JSON:        "json",
	Float4:      "float4",
	Float8:      "float8",
	Bpchar:      "bpchar",
	Varchar:     "varchar",
	Date:        "date",
	Timestamp:   "timestamp",
	TimestampTZ: "timestamptz",
	Interval:    "interval",
	Timetz:      "timetz",
	Bit:         "bit",
	Varbit:      "varbit",
	Numeric:     "numeric",
	UUID:        "uuid",
	JSONB:       "jsonb",
	Nvarchar2:   "nvarchar2",
}

// TypeName returns the openGauss name of a type OID, or "" when it is unknown.
func TypeName(typeOID int32) string {
	return typeNames[typeOID]
}

// ScanType returns the Go type that Decode produces for a type OID. Unknown
// types are returned as text.
func ScanType(typeOID int32) reflect.Type {
	switch typeOID {
	case Bool:
		return reflect.TypeOf(false)
	case Int2, Int4, Int8, Oid:
		return reflect.TypeOf(int64(0))
	case Float4, Float8:
		return reflect.TypeOf(float64(0))
	case Bytea:
		return reflect.TypeOf([]byte(nil))
	case Date, Timestamp, TimestampTZ:
		return reflect.TypeOf(time.Time{})
	default:
		return reflect.TypeOf("")
	}
}

// charSemanticBit marks a length declared in characters rather than bytes, as
// in varchar(10 char). It is stored in the type modifier by openGauss.
const charSemanticBit = int32(0x40000000)

// ColumnLength returns the declared length of a character or bit column from
// the type modifier in RowDescription. A character type stores VARHDRSZ plus
// the length, while a bit type stores the bit count itself; a modifier that
// carries no length means the column is unconstrained.
func ColumnLength(typeOID, typeMod int32) (int64, bool) {
	switch typeOID {
	case Varchar, Bpchar, Nvarchar2:
		// Postgres keeps VARHDRSZ + characters for historical reasons.
		if typeMod > 4 {
			length := int64(typeMod-4) &^ int64(charSemanticBit)
			return length, true
		}
	case Bit, Varbit:
		// The modifier is the bit count itself, without VARHDRSZ.
		if typeMod > 0 {
			return int64(typeMod), true
		}
	}
	return 0, false
}

// NumericPrecisionScale decodes the type modifier of a numeric column: VARHDRSZ
// plus (precision << 16 | scale). openGauss stores the scale as a signed 16-bit
// field, so a negative scale (rounding before the decimal point, available in
// A-compatibility mode) is preserved. The -32768 scale is the sentinel that
// float_as_numeric uses for a precision without a scale, so no scale is
// reported for it. A negative modifier means an unconstrained numeric.
func NumericPrecisionScale(typeOID, typeMod int32) (precision, scale int64, ok bool) {
	if typeOID != Numeric || typeMod < 4 {
		return 0, 0, false
	}
	mod := uint32(typeMod - 4)
	precision = int64((mod >> 16) & 0xffff)
	scale = int64(int16(mod & 0xffff))
	if scale == math.MinInt16 {
		return precision, 0, false
	}
	return precision, scale, true
}

// Decode converts one text-format value. Values whose text is not in the shape
// the driver expects, such as infinity, BC dates or a legacy bytea escape, are
// returned unchanged so no data is lost and scanning still reports the problem.
func Decode(typeOID int32, text []byte) driver.Value {
	switch typeOID {
	case Bool:
		return decodeBool(text)
	case Int2, Int4, Int8, Oid:
		return decodeInt(text)
	case Float4, Float8:
		return decodeFloat(text)
	case Bytea:
		return decodeBytea(text)
	case Date:
		return decodeDate(text)
	case Timestamp:
		return decodeTimestamp(text, false)
	case TimestampTZ:
		return decodeTimestamp(text, true)
	default:
		// Strings are copied out of the message buffer by the conversion.
		return string(text)
	}
}

func decodeBool(text []byte) driver.Value {
	// openGauss databases in dolphin-compatible B mode print 1/0, while the
	// native modes print t/f; both spellings mean the same value.
	switch string(text) {
	case "t", "1":
		return true
	case "f", "0":
		return false
	}
	return string(text)
}

func decodeInt(text []byte) driver.Value {
	value, err := strconv.ParseInt(string(text), 10, 64)
	if err != nil {
		return string(text)
	}
	return value
}

func decodeFloat(text []byte) driver.Value {
	value, err := strconv.ParseFloat(string(text), 64)
	if err != nil {
		return string(text)
	}
	return value
}

// decodeBytea handles both text representations: the hex form the server uses
// by default and the escape form used when bytea_output=escape.
func decodeBytea(text []byte) driver.Value {
	if len(text) >= 2 && text[0] == '\\' && text[1] == 'x' {
		decoded := make([]byte, hex.DecodedLen(len(text)-2))
		n, err := hex.Decode(decoded, text[2:])
		if err != nil {
			return string(text)
		}
		return decoded[:n]
	}
	return decodeEscapeBytea(text)
}

// decodeEscapeBytea decodes the legacy escape representation, where a doubled
// backslash means one backslash and a backslash plus three octal digits means
// one byte.
func decodeEscapeBytea(text []byte) driver.Value {
	decoded := make([]byte, 0, len(text))
	for i := 0; i < len(text); i++ {
		if text[i] != '\\' {
			decoded = append(decoded, text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == '\\' {
			decoded = append(decoded, '\\')
			i++
			continue
		}
		if i+3 >= len(text) {
			return string(text)
		}
		value, ok := parseOctal(text[i+1 : i+4])
		if !ok {
			return string(text)
		}
		decoded = append(decoded, value)
		i += 3
	}
	return decoded
}

// parseOctal converts exactly three octal digits, the only escape width the
// server emits.
func parseOctal(digits []byte) (byte, bool) {
	if len(digits) != 3 {
		return 0, false
	}
	value := 0
	for _, digit := range digits {
		if digit < '0' || digit > '7' {
			return 0, false
		}
		value = value<<3 | int(digit-'0')
	}
	if value > 0xff {
		return 0, false
	}
	return byte(value), true
}

func decodeDate(text []byte) driver.Value {
	s := string(text)
	if s == "infinity" || s == "-infinity" {
		return s
	}
	day, ok := parseDate(s)
	if !ok {
		return s
	}
	return day
}

// parseDate parses the ISO date the server always emits in text mode.
func parseDate(s string) (time.Time, bool) {
	if len(s) < 10 {
		return time.Time{}, false
	}
	day, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, false
	}
	return day, true
}

func decodeTimestamp(text []byte, withZone bool) driver.Value {
	s := string(text)
	if s == "infinity" || s == "-infinity" {
		return s
	}
	datePart, timePart, found := strings.Cut(s, " ")
	if !found || strings.HasSuffix(timePart, " BC") {
		return s
	}
	day, ok := parseDate(datePart)
	if !ok {
		return s
	}
	hour, minute, second, nanos, zone, ok := parseClock(timePart)
	if !ok {
		return s
	}
	location := time.UTC
	if zone != "" {
		if !withZone {
			return s
		}
		offset, ok := parseOffset(zone)
		if !ok {
			return s
		}
		if offset != 0 {
			location = time.FixedZone("", offset)
		}
	}
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, second, nanos, location)
}

// parseClock splits "HH:MM:SS[.fraction][zone]".
func parseClock(s string) (hour, minute, second, nanos int, zone string, ok bool) {
	clock := s
	if i := strings.IndexAny(s, "+-Z"); i >= 0 {
		clock, zone = s[:i], s[i:]
	}
	whole, fraction, _ := strings.Cut(clock, ".")
	fields := strings.Split(whole, ":")
	if len(fields) != 3 {
		return 0, 0, 0, 0, "", false
	}
	values := [3]int{}
	for i, field := range fields {
		value, err := strconv.Atoi(field)
		if err != nil || value < 0 {
			return 0, 0, 0, 0, "", false
		}
		values[i] = value
	}
	if fraction != "" {
		if len(fraction) > 9 {
			fraction = fraction[:9]
		}
		digits, err := strconv.Atoi(fraction + strings.Repeat("0", 9-len(fraction)))
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		nanos = digits
	}
	if values[0] > 24 || values[1] > 59 || values[2] > 60 {
		return 0, 0, 0, 0, "", false
	}
	if values[0] == 24 && (values[1] != 0 || values[2] != 0 || nanos != 0) {
		return 0, 0, 0, 0, "", false
	}
	return values[0], values[1], values[2], nanos, zone, true
}

// parseOffset converts "Z", "+08", "-04" or "+05:30[:00]" to seconds east of
// UTC.
func parseOffset(zone string) (int, bool) {
	if zone == "Z" {
		return 0, true
	}
	if len(zone) < 2 || (zone[0] != '+' && zone[0] != '-') {
		return 0, false
	}
	sign := 1
	if zone[0] == '-' {
		sign = -1
	}
	fields := strings.Split(zone[1:], ":")
	if len(fields) > 3 {
		return 0, false
	}
	seconds := 0
	for i, field := range fields {
		// The server always writes two digits per field; anything else is not
		// a timestamp this driver understands, so it stays text.
		if len(field) != 2 {
			return 0, false
		}
		value, err := strconv.Atoi(field)
		if err != nil || value < 0 || value > 59 {
			return 0, false
		}
		switch i {
		case 0:
			if value > 18 {
				return 0, false
			}
			seconds += value * 3600
		case 1:
			seconds += value * 60
		case 2:
			seconds += value
		}
	}
	return sign * seconds, true
}
