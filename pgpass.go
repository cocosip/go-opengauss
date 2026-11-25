package opengauss

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// lookupPassfile returns the password that matches the connection parameters
// from a libpq passfile. A missing default file is not an error; a file named
// explicitly must be readable, and on Unix it must not be accessible to group
// or others.
func lookupPassfile(path string, explicit bool, cfg *Config) (string, error) {
	if path == "" {
		path = defaultPassfile()
	}
	if path == "" {
		return "", nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && !explicit {
			return "", nil
		}
		return "", fmt.Errorf("opengauss: read passfile %q: %w", path, err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("opengauss: passfile %q has insecure permissions %#o: restrict it to the owner", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("opengauss: read passfile %q: %w", path, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields, ok := splitPassfileLine(line)
		if !ok {
			continue
		}
		if matchPassfileField(fields[0], cfg.Host) &&
			(fields[1] == "*" || fields[1] == strconv.Itoa(cfg.Port)) &&
			matchPassfileField(fields[2], cfg.Database) &&
			matchPassfileField(fields[3], cfg.User) {
			password := fields[4]
			if strings.IndexByte(password, 0) >= 0 {
				return "", fmt.Errorf("opengauss: passfile %q contains a NUL byte in a matched password", path)
			}
			return password, nil
		}
	}
	return "", nil
}

func matchPassfileField(pattern, value string) bool {
	return pattern == "*" || pattern == value
}

// splitPassfileLine splits host:port:database:user:password. Only "\:" and
// "\\" are escapes, matching libpq, so "\*" stays literal rather than becoming
// a wildcard. The password ends at the first unescaped colon; anything after
// it is ignored.
func splitPassfileLine(line string) ([5]string, bool) {
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
	return fields, field == len(fields)-1
}

// defaultPassfile returns libpq's default passfile location, or "" when the
// home directory is unknown.
func defaultPassfile() string {
	if runtime.GOOS == "windows" {
		if dir, err := os.UserConfigDir(); err == nil {
			return filepath.Join(dir, "postgresql", "pgpass.conf")
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".pgpass")
	}
	return ""
}
