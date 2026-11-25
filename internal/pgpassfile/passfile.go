// Package pgpassfile parses libpq password files (.pgpass) and matches
// entries against connection parameters.
package pgpassfile

import (
	"bufio"
	"io"
	"strings"
)

// Entry is one password-file line.
type Entry struct {
	Hostname string
	Port     string
	Database string
	Username string
	Password string
}

// Passfile is a parsed password file. Malformed lines are skipped, as libpq
// does.
type Passfile struct {
	Entries []Entry
}

// Parse reads a passfile. Malformed lines are skipped; the error reports an\n// I/O failure or an over-long line.
func Parse(r io.Reader) (*Passfile, error) {
	passfile := &Passfile{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		entry, ok := parseLine(line)
		if !ok {
			continue
		}
		passfile.Entries = append(passfile.Entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return passfile, nil
}

// FindPassword returns the password of the first entry matching all four
// fields. "*" matches anything; an empty result means no entry matched.
func (p *Passfile) FindPassword(hostname, port, database, username string) string {
	for _, entry := range p.Entries {
		if match(entry.Hostname, hostname) &&
			match(entry.Port, port) &&
			match(entry.Database, database) &&
			match(entry.Username, username) {
			return entry.Password
		}
	}
	return ""
}

func match(pattern, value string) bool { return pattern == "*" || pattern == value }

// parseLine splits host:port:database:user:password. Only "\:" and "\\" are
// escapes, matching libpq, so "\*" stays literal rather than becoming a
// wildcard. The password ends at the first unescaped colon.
func parseLine(line string) (Entry, bool) {
	var fields [5]string
	field := 0
	var value strings.Builder
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == '\\' && i+1 < len(line) {
			if next := line[i+1]; next == ':' || next == '\\' {
				value.WriteByte(next)
				i++
				continue
			}
		}
		if c == ':' {
			if field == len(fields)-1 {
				break
			}
			fields[field] = value.String()
			value.Reset()
			field++
			continue
		}
		value.WriteByte(c)
	}
	fields[field] = value.String()
	if field != len(fields)-1 {
		return Entry{}, false
	}
	return Entry{
		Hostname: fields[0],
		Port:     fields[1],
		Database: fields[2],
		Username: fields[3],
		Password: fields[4],
	}, true
}
