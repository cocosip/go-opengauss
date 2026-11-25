package opengauss

import (
	"fmt"
	"strconv"
)

// Error is a server-reported error. Its fields mirror the ErrorResponse
// message, so applications can reproduce the server's severity, SQLSTATE and
// message. Use errors.As to extract it.
type Error struct {
	// Severity is the localized severity, for example "ERROR" or "FATAL".
	Severity string
	// Code is the SQLSTATE code, for example "28P01".
	Code string
	// Message is the primary human-readable message.
	Message string

	Detail     string
	Hint       string
	Position   int
	Where      string
	Schema     string
	Table      string
	Column     string
	DataType   string
	Constraint string
	File       string
	Line       string
	Routine    string

	InternalPosition int
	InternalQuery    string
}

// SQLState returns the SQLSTATE code supplied by the server.
func (e *Error) SQLState() string { return e.Code }

// Error implements the error interface.
func (e *Error) Error() string {
	severity := e.Severity
	if severity == "" {
		severity = "ERROR"
	}
	if e.Code == "" {
		return fmt.Sprintf("opengauss: %s: %s", severity, e.Message)
	}
	return fmt.Sprintf("opengauss: %s: %s (SQLSTATE %s)", severity, e.Message, e.Code)
}

// parseErrorResponse decodes an ErrorResponse body. Unknown field codes are
// skipped so a newer server does not break the driver.
func parseErrorResponse(body []byte) *Error {
	e := &Error{}
	r := &readBuf{b: body}
	for r.remaining() > 0 {
		field := r.byte()
		if r.err != nil || field == 0 {
			break
		}
		value := r.string()
		if r.err != nil {
			break
		}
		switch field {
		case 'S':
			e.Severity = value
		case 'C':
			e.Code = value
		case 'M':
			e.Message = value
		case 'D':
			e.Detail = value
		case 'H':
			e.Hint = value
		case 'P':
			e.Position, _ = strconv.Atoi(value)
		case 'p':
			e.InternalPosition, _ = strconv.Atoi(value)
		case 'q':
			e.InternalQuery = value
		case 'W':
			e.Where = value
		case 's':
			e.Schema = value
		case 't':
			e.Table = value
		case 'c':
			e.Column = value
		case 'd':
			e.DataType = value
		case 'n':
			e.Constraint = value
		case 'F':
			e.File = value
		case 'L':
			e.Line = value
		case 'R':
			e.Routine = value
		}
	}
	if e.Message == "" {
		e.Message = "server returned an error without a message"
	}
	return e
}
