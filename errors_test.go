package opengauss

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// errorResponseBody encodes ErrorResponse fields in wire order and appends the
// terminating zero byte.
func errorResponseBody(fields ...string) []byte {
	var b []byte
	for i := 0; i+1 < len(fields); i += 2 {
		b = append(b, fields[i][0])
		b = append(b, fields[i+1]...)
		b = append(b, 0)
	}
	return append(b, 0)
}

func TestParseErrorResponseFields(t *testing.T) {
	body := errorResponseBody(
		"S", "ERROR",
		"V", "ERROR",
		"C", "28P01",
		"M", `password authentication failed for user "alice"`,
		"D", "detail text",
		"H", "hint text",
		"P", "42",
		"p", "7",
		"q", "SELECT 1",
		"W", "where text",
		"s", "public",
		"t", "t1",
		"c", "c1",
		"d", "int4",
		"n", "constraint_name",
		"F", "auth.c",
		"L", "42",
		"R", "auth_failed",
	)
	e := parseErrorResponse(body)
	if e == nil {
		t.Fatal("parseErrorResponse returned nil")
	}
	if e.Severity != "ERROR" {
		t.Errorf("Severity = %q", e.Severity)
	}
	if e.Code != "28P01" {
		t.Errorf("Code = %q", e.Code)
	}
	if e.SQLState() != "28P01" {
		t.Errorf("SQLState = %q", e.SQLState())
	}
	if !strings.Contains(e.Message, "password authentication failed") {
		t.Errorf("Message = %q", e.Message)
	}
	if e.Detail != "detail text" || e.Hint != "hint text" {
		t.Errorf("Detail/Hint = %q/%q", e.Detail, e.Hint)
	}
	if e.Position != 42 || e.InternalPosition != 7 {
		t.Errorf("positions = %d/%d", e.Position, e.InternalPosition)
	}
	if e.InternalQuery != "SELECT 1" || e.Where != "where text" {
		t.Errorf("InternalQuery/Where = %q/%q", e.InternalQuery, e.Where)
	}
	if e.Schema != "public" || e.Table != "t1" || e.Column != "c1" || e.DataType != "int4" {
		t.Errorf("schema/table/column/type = %q/%q/%q/%q", e.Schema, e.Table, e.Column, e.DataType)
	}
	if e.Constraint != "constraint_name" || e.File != "auth.c" || e.Line != "42" || e.Routine != "auth_failed" {
		t.Errorf("constraint/file/line/routine = %q/%q/%q/%q", e.Constraint, e.File, e.Line, e.Routine)
	}
}

func TestParseErrorResponseIgnoresUnknownAndTruncatedFields(t *testing.T) {
	body := []byte{'Z', 'x', 0, 'M', 'm', 's', 'g', 0, 0}
	e := parseErrorResponse(body)
	if e == nil {
		t.Fatal("parseErrorResponse returned nil")
	}
	if e.Message != "msg" {
		t.Errorf("Message = %q, want msg", e.Message)
	}
}

func TestErrorTextIncludesSQLState(t *testing.T) {
	e := &Error{Severity: "ERROR", Code: "42P01", Message: `relation "missing" does not exist`}
	text := e.Error()
	for _, want := range []string{"ERROR", "42P01", "missing"} {
		if !strings.Contains(text, want) {
			t.Errorf("Error() = %q, missing %q", text, want)
		}
	}
}

func TestErrorIsDiscoverableThroughWrap(t *testing.T) {
	e := &Error{Severity: "ERROR", Code: "42501", Message: "permission denied"}
	wrapped := fmt.Errorf("opengauss: exec: %w", e)
	var target *Error
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As did not find *Error")
	}
	if target.SQLState() != "42501" {
		t.Errorf("SQLState = %q", target.SQLState())
	}
}
