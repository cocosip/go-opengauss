package opengauss

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cocosip/go-opengauss/internal/pgpassfile"
)

// lookupPassfile returns the password that matches target from a libpq
// passfile. A missing default file is not an error; a file named explicitly
// must be readable, and on Unix it must not be accessible to group or others.
func lookupPassfile(path string, explicit bool, target Target, database, user string) (string, error) {
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
	passfile, err := pgpassfile.Parse(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("opengauss: parse passfile %q: %w", path, err)
	}
	password := passfile.FindPassword(target.Host, strconv.Itoa(target.Port), database, user)
	if strings.IndexByte(password, 0) >= 0 {
		return "", fmt.Errorf("opengauss: passfile %q contains a NUL byte in a matched password", path)
	}
	return password, nil
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
