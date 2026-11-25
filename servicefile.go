package opengauss

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"github.com/cocosip/go-opengauss/internal/pgservicefile"
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
	serviceValues, err := pgservicefile.Parse(bytes.NewReader(data), service)
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
