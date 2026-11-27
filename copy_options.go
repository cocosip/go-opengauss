package opengauss

import (
	"errors"
	"fmt"
	"strings"
)

// copyOutFormat is the representation the driver must decode for a COPY ... TO
// STDOUT statement. The server echoes only text or binary in CopyOutResponse,
// so the statement's own options decide where a row ends and what a field
// means. A format the decoder cannot honour is refused instead of being read
// under the wrong rules.
type copyOutFormat struct {
	csv       bool
	binary    bool
	header    bool
	delimiter byte
	quote     byte
	escape    byte
	null      []byte
}

// textCopyOutFormat is the default COPY text format: tab-separated fields,
// terminated by a newline, with \N for NULL.
func textCopyOutFormat() copyOutFormat {
	return copyOutFormat{delimiter: '\t', null: []byte(`\N`)}
}

// useCSVDefaults switches the format to CSV with its documented defaults: a
// comma separator, the double quote for both quoting and escaping, and an
// unquoted empty field for NULL.
func (f *copyOutFormat) useCSVDefaults() {
	f.csv = true
	f.delimiter = ','
	f.quote = '"'
	f.escape = '"'
	f.null = nil
}

// errCopyInBinary is reported when a COPY ... FROM STDIN statement asks for a
// binary stream, whose row encoding the driver does not implement.
var errCopyInBinary = errors.New("opengauss: COPY ... FROM STDIN uses binary format, which this driver does not support; use FORMAT text or FORMAT csv")

// CopyFormat is the representation of a COPY stream.
type CopyFormat int

const (
	// CopyFormatText is the default COPY text format: tab-separated fields and
	// \N for NULL.
	CopyFormatText CopyFormat = iota
	// CopyFormatCSV is the CSV format: comma-separated fields, a double quote
	// for quoting and an unquoted empty field for NULL by default.
	CopyFormatCSV
)

// CopyInOptions selects the representation of a COPY ... FROM STDIN stream.
// The zero value is the default text format, so CopyIn needs no options.
//
// For CopyFormatCSV a zero Delimiter or Quote means the CSV default (a comma
// and a double quote), a zero Escape follows Quote, and an empty Null means
// NULL is the unquoted empty field. Header asks the server to expect a header
// line; the caller still writes that line as its first row, because the COPY
// protocol carries rows only.
type CopyInOptions struct {
	Format    CopyFormat
	Header    bool
	Delimiter byte
	Quote     byte
	Escape    byte
	Null      string
}

// copyOption is one name/value pair from a COPY option list.
type copyOption struct {
	name     string
	value    string
	hasValue bool
}

// parseCopyOutOptions reads the option list of a COPY ... TO STDOUT statement
// and returns the format its rows must be decoded in. The modern WITH (...) form
// and the legacy bare form are both accepted, and the default text format is
// used when no option is present.
func parseCopyOutOptions(statement string, clauseEnd int) (copyOutFormat, error) {
	options, err := parseCopyOptions(statement, clauseEnd)
	if err != nil {
		return copyOutFormat{}, err
	}
	return applyCopyOutOptions(options)
}

// parseCopyOptions returns the options that follow the TO STDOUT clause. Only
// whitespace, comments, one trailing separator and another trailing comment may
// follow the last option, because a second statement would have to share the
// copy stream.
func parseCopyOptions(statement string, clauseEnd int) ([]copyOption, error) {
	i := skipSpaceAndComments(statement, clauseEnd)
	if i < len(statement) && statement[i] == ';' {
		i = skipSpaceAndComments(statement, i+1)
	}
	if i >= len(statement) {
		return nil, nil
	}
	if end, ok := matchKeyword(statement, i, "WITH"); ok {
		i = skipSpaceAndComments(statement, end)
		if i < len(statement) && statement[i] == '(' {
			return parseParenthesizedCopyOptions(statement, i)
		}
		return parseBareCopyOptions(statement, i)
	}
	return parseBareCopyOptions(statement, i)
}

// parseBareCopyOptions reads the legacy form, where options are words separated
// by whitespace, for example CSV HEADER or DELIMITER ','.
func parseBareCopyOptions(statement string, i int) ([]copyOption, error) {
	var options []copyOption
	for {
		i = skipSpaceAndComments(statement, i)
		if i >= len(statement) || statement[i] == ';' {
			return options, nil
		}
		option, end, err := parseBareCopyOption(statement, i)
		if err != nil {
			return nil, err
		}
		options = append(options, option)
		i = end
	}
}

// parseBareCopyOption reads one legacy option and its optional value. Whether a
// value belongs to the option comes from the option's own grammar, so HEADER
// stays valueless when another option follows it.
func parseBareCopyOption(statement string, i int) (copyOption, int, error) {
	name, end, err := readCopyOptionName(statement, i)
	if err != nil {
		return copyOption{}, end, err
	}
	option := copyOption{name: name}
	if copyOptionNeedsValue(name) {
		value, next, err := readCopyOptionValue(statement, skipSpaceAndComments(statement, end))
		if err != nil {
			return copyOption{}, next, fmt.Errorf("opengauss: COPY option %s: %w", strings.ToUpper(name), err)
		}
		option.value, option.hasValue = value, true
		return option, next, nil
	}
	if name == "header" {
		// HEADER may be followed by a boolean, but the next option word must
		// not be swallowed when it is not one.
		if value, next, ok := readCopyBoolValue(statement, skipSpaceAndComments(statement, end)); ok {
			option.value, option.hasValue = value, true
			return option, next, nil
		}
	}
	return option, end, nil
}

// parseParenthesizedCopyOptions reads the WITH (name value, ...) form.
func parseParenthesizedCopyOptions(statement string, start int) ([]copyOption, error) {
	var options []copyOption
	i := start + 1
	for {
		i = skipSpaceAndComments(statement, i)
		if i >= len(statement) {
			return nil, errors.New("opengauss: the COPY option list is not closed")
		}
		if statement[i] == ')' {
			return options, checkCopyOptionTail(statement, i+1)
		}
		name, end, err := readCopyOptionName(statement, i)
		if err != nil {
			return nil, err
		}
		option := copyOption{name: name}
		i = skipSpaceAndComments(statement, end)
		if i < len(statement) && statement[i] != ',' && statement[i] != ')' {
			value, next, err := readCopyOptionValue(statement, i)
			if err != nil {
				return nil, fmt.Errorf("opengauss: COPY option %s: %w", strings.ToUpper(name), err)
			}
			option.value, option.hasValue = value, true
			i = next
		}
		options = append(options, option)
		i = skipSpaceAndComments(statement, i)
		if i < len(statement) && statement[i] == ',' {
			i++
		}
	}
}

// checkCopyOptionTail verifies that only whitespace, comments and one trailing
// separator follow the option list.
func checkCopyOptionTail(statement string, i int) error {
	i = skipSpaceAndComments(statement, i)
	if i < len(statement) && statement[i] == ';' {
		i = skipSpaceAndComments(statement, i+1)
	}
	if i < len(statement) {
		return errors.New("opengauss: COPY ... TO STDOUT must be the only statement in a query")
	}
	return nil
}

// readCopyOptionName reads one option name and lower-cases it.
func readCopyOptionName(statement string, i int) (string, int, error) {
	start := i
	for i < len(statement) && isCopyOptionNameByte(statement[i]) {
		i++
	}
	if i == start {
		return "", i, fmt.Errorf("opengauss: unexpected %q in the COPY option list", statement[i])
	}
	return strings.ToLower(statement[start:i]), i, nil
}

func isCopyOptionNameByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		return true
	}
	return false
}

// readCopyOptionValue reads a quoted string, a parenthesized column list or an
// unquoted word. A quoted value uses the standard SQL rule that a doubled quote
// stands for one quote, and an E” string additionally expands the backslash
// escapes the driver emits for a control character or a quote.
func readCopyOptionValue(statement string, i int) (string, int, error) {
	if i >= len(statement) {
		return "", i, errors.New("a value is missing")
	}
	if statement[i] == '\'' {
		return readCopyQuotedValue(statement, i+1, false)
	}
	if statement[i] == '(' {
		return readCopyColumnList(statement, i)
	}
	if (statement[i] == 'E' || statement[i] == 'e') && i+1 < len(statement) && statement[i+1] == '\'' {
		return readCopyQuotedValue(statement, i+2, true)
	}
	start := i
	for i < len(statement) {
		c := statement[i]
		if isSpace(c) || c == ',' || c == ')' || c == ';' {
			break
		}
		i++
	}
	if i == start {
		return "", i, errors.New("a value is missing")
	}
	return statement[start:i], i, nil
}

// readCopyQuotedValue reads the body of a quoted option value starting just
// after its opening quote, with escape reporting whether backslash escapes
// apply.
func readCopyQuotedValue(statement string, i int, escape bool) (string, int, error) {
	var value strings.Builder
	for {
		if i >= len(statement) {
			return "", i, errors.New("the quoted value is not terminated")
		}
		if statement[i] == '\'' {
			if i+1 < len(statement) && statement[i+1] == '\'' {
				value.WriteByte('\'')
				i += 2
				continue
			}
			return value.String(), i + 1, nil
		}
		if escape && statement[i] == '\\' && i+1 < len(statement) {
			value.WriteByte(unescapeCopyOptionByte(statement[i+1]))
			i += 2
			continue
		}
		value.WriteByte(statement[i])
		i++
	}
}

// unescapeCopyOptionByte maps the backslash escapes an E” value can carry. Any
// other escaped character stands for itself.
func unescapeCopyOptionByte(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'b':
		return '\b'
	case 'f':
		return '\f'
	}
	return c
}

// readCopyColumnList skips a parenthesized column list, such as the one
// FORCE_QUOTE takes, and returns its body. The list is not interpreted: the
// server is the authority on its validity, and a list of columns only changes
// how values are formatted, not how they are decoded.
func readCopyColumnList(statement string, start int) (string, int, error) {
	i := start + 1
	for {
		i = skipSpaceAndComments(statement, i)
		if i >= len(statement) {
			return "", i, errors.New("the column list is not closed")
		}
		switch statement[i] {
		case ')':
			return statement[start+1 : i], i + 1, nil
		case '"', '\'':
			i = skipQuoted(statement, i, statement[i])
		default:
			i++
		}
	}
}

// readCopyBoolValue reads a boolean option value when one is present.
func readCopyBoolValue(statement string, i int) (string, int, bool) {
	start := i
	for i < len(statement) && isCopyOptionNameByte(statement[i]) {
		i++
	}
	word := strings.ToLower(statement[start:i])
	if _, err := parseCopyBool(word); err != nil {
		return "", start, false
	}
	return word, i, true
}

// copyOptionNeedsValue reports whether an option is always followed by a value.
func copyOptionNeedsValue(name string) bool {
	switch name {
	case "format", "delimiter", "quote", "escape", "null", "encoding", "force_quote", "force_not_null", "force_null":
		return true
	}
	return false
}

// checkCopyEncoding accepts an ENCODING value only when it names the UTF-8 or
// SQL_ASCII text the driver already speaks: any other encoding changes the
// bytes of every field without changing the decoder.
func checkCopyEncoding(value string) error {
	normalized := strings.ToLower(value)
	normalized = strings.NewReplacer("_", "", "-", "", " ", "").Replace(normalized)
	switch normalized {
	case "utf8", "unicode", "sqlascii":
		return nil
	}
	return fmt.Errorf("opengauss: COPY option ENCODING %q is not implemented: the driver exchanges UTF-8 and SQL_ASCII text only", value)
}

// parseCopyBool accepts the spellings SQL uses for a boolean option value.
func parseCopyBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "true", "on", "1", "yes", "t":
		return true, nil
	case "false", "off", "0", "no", "f":
		return false, nil
	}
	return false, fmt.Errorf("opengauss: %q is not a boolean", value)
}

// copyOptionKind is the representation an option list selects.
type copyOptionKind int

const (
	copyOptionText copyOptionKind = iota
	copyOptionCSV
	copyOptionBinary
)

// copyOptionFormat reads the FORMAT option, including the legacy CSV and BINARY
// words, so the rest of the option list can be checked against it.
func copyOptionFormat(options []copyOption) (copyOptionKind, error) {
	kind := copyOptionText
	for _, option := range options {
		switch option.name {
		case "format":
			if !option.hasValue {
				return copyOptionText, errors.New("opengauss: COPY option FORMAT needs a value")
			}
			switch strings.ToLower(option.value) {
			case "csv":
				kind = copyOptionCSV
			case "text":
				kind = copyOptionText
			case "binary":
				kind = copyOptionBinary
			default:
				return copyOptionText, fmt.Errorf("opengauss: unknown COPY format %q: use text, csv or binary", option.value)
			}
		case "csv":
			kind = copyOptionCSV
		case "binary":
			kind = copyOptionBinary
		}
	}
	return kind, nil
}

// applyCopyOutOptions turns the parsed options into a decoder format. FORMAT is
// applied first so the other options see the right defaults, and then every
// option is checked against what the decoder actually implements.
func applyCopyOutOptions(options []copyOption) (copyOutFormat, error) {
	format := textCopyOutFormat()
	kind, err := copyOptionFormat(options)
	if err != nil {
		return copyOutFormat{}, err
	}
	switch kind {
	case copyOptionCSV:
		format.useCSVDefaults()
	case copyOptionBinary:
		format.binary = true
	}
	// A single-byte option that does not default to the quote character yet.
	escapeSet := false
	for _, option := range options {
		switch option.name {
		case "format", "csv", "binary":
			// Already applied.
		case "header":
			value := true
			if option.hasValue {
				parsed, err := parseCopyBool(option.value)
				if err != nil {
					return copyOutFormat{}, err
				}
				value = parsed
			}
			if !format.csv {
				return copyOutFormat{}, errors.New("opengauss: COPY option HEADER is available only with FORMAT csv")
			}
			format.header = value
		case "delimiter", "quote", "escape":
			value, err := singleByteOption(option)
			if err != nil {
				return copyOutFormat{}, err
			}
			if !format.csv {
				return copyOutFormat{}, fmt.Errorf("opengauss: COPY option %s is available only with FORMAT csv", strings.ToUpper(option.name))
			}
			switch option.name {
			case "delimiter":
				format.delimiter = value
			case "quote":
				format.quote = value
			case "escape":
				format.escape = value
				escapeSet = true
			}
		case "null":
			if !format.csv {
				return copyOutFormat{}, errors.New("opengauss: COPY option NULL is available only with FORMAT csv")
			}
			if !option.hasValue {
				return copyOutFormat{}, errors.New("opengauss: COPY option NULL needs a value")
			}
			format.null = []byte(option.value)
		case "encoding":
			if !option.hasValue {
				return copyOutFormat{}, errors.New("opengauss: COPY option ENCODING needs a value")
			}
			if err := checkCopyEncoding(option.value); err != nil {
				return copyOutFormat{}, err
			}
		case "force_quote":
			if !format.csv {
				return copyOutFormat{}, errors.New("opengauss: COPY option FORCE_QUOTE is available only with FORMAT csv")
			}
			if !option.hasValue {
				return copyOutFormat{}, errors.New("opengauss: COPY option FORCE_QUOTE needs a value")
			}
			// FORCE_QUOTE only adds quotes around the listed columns, which the
			// CSV decoder already reads, so the list needs no interpretation.
		case "force_not_null", "force_null":
			return copyOutFormat{}, forceNullOptionError(option.name)
		case "force":
			return copyOutFormat{}, errLegacyForceOption
		default:
			return copyOutFormat{}, fmt.Errorf("opengauss: COPY option %s is not implemented", strings.ToUpper(option.name))
		}
	}
	// ESCAPE follows QUOTE unless the statement names it, as the server does.
	if format.csv && !escapeSet {
		format.escape = format.quote
	}
	return format, nil
}

// singleByteOption reads a one-character option such as DELIMITER.
func singleByteOption(option copyOption) (byte, error) {
	if !option.hasValue {
		return 0, fmt.Errorf("opengauss: COPY option %s needs a value", strings.ToUpper(option.name))
	}
	if len(option.value) != 1 {
		return 0, fmt.Errorf("opengauss: COPY option %s must be one single-byte character, got %q", strings.ToUpper(option.name), option.value)
	}
	return option.value[0], nil
}

// parseCopyInOptions reads the option list of a COPY ... FROM STDIN statement
// and returns the representation its rows must be written in. A statement
// without options keeps the default text format.
func parseCopyInOptions(statement string, clauseEnd int) (CopyInOptions, error) {
	options, err := parseCopyOptions(statement, clauseEnd)
	if err != nil {
		return CopyInOptions{}, err
	}
	return applyCopyInOptions(options)
}

// applyCopyInOptions turns the parsed options into the representation the row
// encoder must write.
func applyCopyInOptions(options []copyOption) (CopyInOptions, error) {
	kind, err := copyOptionFormat(options)
	if err != nil {
		return CopyInOptions{}, err
	}
	if kind == copyOptionBinary {
		return CopyInOptions{}, errCopyInBinary
	}
	result := CopyInOptions{Format: CopyFormatText}
	if kind == copyOptionCSV {
		result.Format = CopyFormatCSV
	}
	for _, option := range options {
		switch option.name {
		case "format", "csv", "binary":
			// Already applied.
		case "header":
			value := true
			if option.hasValue {
				parsed, err := parseCopyBool(option.value)
				if err != nil {
					return CopyInOptions{}, err
				}
				value = parsed
			}
			if kind != copyOptionCSV {
				return CopyInOptions{}, errors.New("opengauss: COPY option HEADER is available only with FORMAT csv")
			}
			result.Header = value
		case "delimiter", "quote", "escape":
			value, err := singleByteOption(option)
			if err != nil {
				return CopyInOptions{}, err
			}
			if kind != copyOptionCSV {
				return CopyInOptions{}, fmt.Errorf("opengauss: COPY option %s is available only with FORMAT csv", strings.ToUpper(option.name))
			}
			switch option.name {
			case "delimiter":
				result.Delimiter = value
			case "quote":
				result.Quote = value
			case "escape":
				result.Escape = value
			}
		case "null":
			if kind != copyOptionCSV {
				return CopyInOptions{}, errors.New("opengauss: COPY option NULL is available only with FORMAT csv")
			}
			if !option.hasValue {
				return CopyInOptions{}, errors.New("opengauss: COPY option NULL needs a value")
			}
			result.Null = option.value
		case "encoding":
			if !option.hasValue {
				return CopyInOptions{}, errors.New("opengauss: COPY option ENCODING needs a value")
			}
			if err := checkCopyEncoding(option.value); err != nil {
				return CopyInOptions{}, err
			}
		case "force_quote":
			return CopyInOptions{}, errors.New("opengauss: COPY option FORCE_QUOTE applies to COPY ... TO STDOUT only")
		case "force_not_null", "force_null":
			return CopyInOptions{}, forceNullOptionError(option.name)
		case "force":
			return CopyInOptions{}, errLegacyForceOption
		default:
			return CopyInOptions{}, fmt.Errorf("opengauss: COPY option %s is not implemented", strings.ToUpper(option.name))
		}
	}
	return checkCopyInOptions(result)
}

// forceNullOptionError explains why the per-column NULL options are refused:
// they change how a quoted or unquoted empty field reads, and the COPY protocol
// reports no column names to apply them to.
func forceNullOptionError(name string) error {
	return fmt.Errorf("opengauss: COPY option %s is not implemented: it changes how an empty field reads, and the COPY protocol reports no column names to apply it to", strings.ToUpper(name))
}

// errLegacyForceOption reports the two-word FORCE QUOTE spelling that the
// modern WITH (FORCE_QUOTE ...) form replaces.
var errLegacyForceOption = errors.New("opengauss: the legacy FORCE QUOTE syntax is not supported: use WITH (FORCE_QUOTE ...)")

// checkCopyInOptions fills the CSV defaults and rejects a combination the row
// encoder cannot write.
func checkCopyInOptions(options CopyInOptions) (CopyInOptions, error) {
	switch options.Format {
	case CopyFormatText:
		if options.Header || options.Delimiter != 0 || options.Quote != 0 ||
			options.Escape != 0 || options.Null != "" {
			return CopyInOptions{}, errors.New("opengauss: DELIMITER, QUOTE, ESCAPE, NULL and HEADER are available only with CopyFormatCSV")
		}
		return options, nil
	case CopyFormatCSV:
	default:
		return CopyInOptions{}, fmt.Errorf("opengauss: unknown COPY format %d", int(options.Format))
	}
	if options.Delimiter == 0 {
		options.Delimiter = ','
	}
	if options.Quote == 0 {
		options.Quote = '"'
	}
	if options.Escape == 0 {
		options.Escape = options.Quote
	}
	if options.Delimiter == options.Quote {
		return CopyInOptions{}, fmt.Errorf("opengauss: the COPY delimiter %q and quote %q must differ", options.Delimiter, options.Quote)
	}
	for _, c := range []byte{options.Delimiter, options.Quote, options.Escape} {
		if c == '\n' || c == '\r' {
			return CopyInOptions{}, errors.New("opengauss: a COPY delimiter, quote or escape cannot be a line break")
		}
	}
	return options, nil
}

// quoteCopyOptionLiteral writes a COPY option value as a SQL string literal. A
// value with a backslash switches to the E” form, so it keeps its meaning
// whatever standard_conforming_strings is set to.
func quoteCopyOptionLiteral(text string) string {
	if strings.Contains(text, `\`) {
		escaped := strings.ReplaceAll(text, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, "'", "''")
		return "E'" + escaped + "'"
	}
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}
