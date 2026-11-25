package opengauss

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// applyServiceFile fills parameters that the DSN did not set, using the named
// section of a libpq service file. Service-file entries override environment
// variables, so this runs before applyEnvironment. PGSERVICEFILE selects the
// file; otherwise the platform default is used.
func applyServiceFile(values map[string]string, service string) error {
	path, explicit := os.Getenv("PGSERVICEFILE"), true
	if path == "" {
		path, explicit = defaultServiceFile(), false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !explicit && errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("opengauss: definition of service %q not found: no service file at %q", service, path)
		}
		return fmt.Errorf("opengauss: read service file %q: %w", path, err)
	}
	serviceValues, err := parseServiceFile(data, service)
	if err != nil {
		return fmt.Errorf("opengauss: service file %q: %w", path, err)
	}
	for key, value := range serviceValues {
		if _, set := values[key]; set {
			continue
		}
		values[key] = value
	}
	return nil
}

// defaultServiceFile returns libpq's default service file location.
func defaultServiceFile() string {
	if runtime.GOOS == "windows" {
		if dir, err := os.UserConfigDir(); err == nil {
			return filepath.Join(dir, "postgresql", ".pg_service.conf")
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".pg_service.conf")
	}
	return ".pg_service.conf"
}

// parseServiceFile returns the parameters of the first section named service.
func parseServiceFile(data []byte, service string) (map[string]string, error) {
	values := make(map[string]string)
	matched := false
	for lineNo, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("line %d: malformed section header %q", lineNo+1, line)
			}
			if matched {
				break // libpq stops at the end of the first matching section
			}
			matched = strings.TrimSpace(line[1:len(line)-1]) == service
			continue
		}
		if !matched {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("line %d: expected key=value, got %q", lineNo+1, line)
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if !matched {
		return nil, fmt.Errorf("service %q not found", service)
	}
	return values, nil
}
