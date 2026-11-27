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

// errCopyOutBinary is reported when a copy-out stream would be binary, which the
// driver never requests and cannot decode.
var errCopyOutBinary = errors.New("opengauss: COPY ... TO STDOUT uses binary format, which this driver does not support; request FORMAT text or FORMAT csv")

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

// readCopyOptionValue reads a quoted string or an unquoted word. A quoted value
// uses the standard SQL rule that a doubled quote stands for one quote.
func readCopyOptionValue(statement string, i int) (string, int, error) {
	if i >= len(statement) {
		return "", i, errors.New("a value is missing")
	}
	if statement[i] == '\'' {
		var value strings.Builder
		for j := i + 1; ; {
			if j >= len(statement) {
				return "", j, errors.New("the quoted value is not terminated")
			}
			if statement[j] != '\'' {
				value.WriteByte(statement[j])
				j++
				continue
			}
			if j+1 < len(statement) && statement[j+1] == '\'' {
				value.WriteByte('\'')
				j += 2
				continue
			}
			return value.String(), j + 1, nil
		}
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
	case "format", "delimiter", "quote", "escape", "null":
		return true
	}
	return false
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

// applyCopyOutOptions turns the parsed options into a decoder format. FORMAT is
// applied first so the other options see the right defaults, and then every
// option is checked against what the decoder actually implements.
func applyCopyOutOptions(options []copyOption) (copyOutFormat, error) {
	format := textCopyOutFormat()
	// The format comes first so the other options see the right defaults; the
	// legacy CSV and BINARY words are format synonyms.
	for _, option := range options {
		switch option.name {
		case "format":
			if !option.hasValue {
				return copyOutFormat{}, errors.New("opengauss: COPY option FORMAT needs a value")
			}
			switch strings.ToLower(option.value) {
			case "csv":
				format.useCSVDefaults()
			case "text":
			case "binary":
				return copyOutFormat{}, errCopyOutBinary
			default:
				return copyOutFormat{}, fmt.Errorf("opengauss: unknown COPY format %q: use text or csv", option.value)
			}
		case "csv":
			format.useCSVDefaults()
		case "binary":
			return copyOutFormat{}, errCopyOutBinary
		}
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
