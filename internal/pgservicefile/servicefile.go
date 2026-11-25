// Package pgservicefile parses libpq service files (.pg_service.conf).
package pgservicefile

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Parse returns the settings of the named service. Like libpq, it validates
// only the lines inside the requested section and stops when that section
// ends, so malformed content in unrelated sections is ignored.
func Parse(r io.Reader, service string) (map[string]string, error) {
	settings := make(map[string]string)
	inSection := false
	found := false
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("line %d: malformed section header %q", lineNo, line)
			}
			if inSection {
				break // libpq stops at the end of the matching section
			}
			name := strings.TrimSpace(line[1 : len(line)-1])
			if name == service {
				inSection = true
				found = true
			}
			continue
		}
		if !inSection {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("line %d: expected key=value, got %q", lineNo, line)
		}
		settings[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("service %q not found", service)
	}
	return settings, nil
}
