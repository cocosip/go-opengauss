package opengauss

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// ServerVersion is the product version a server reported in the banner of
// SELECT version(). openGauss keeps a PostgreSQL-compatible server_version
// (9.2.4), so the banner is the only reliable product signal: a decision that
// depends on the release must use this value, not the server_version parameter.
type ServerVersion struct {
	// Product is the first word of the banner, for example openGauss, MogDB or
	// PostgreSQL, and is empty when the banner does not name a product.
	Product string
	Major   int
	Minor   int
	Patch   int
	// Raw is the banner exactly as the server sent it.
	Raw string
}

// String renders the product and its version, for example "openGauss 7.0.0". A
// banner without a product name renders as "unknown 0.0.0"; compare the fields
// when that distinction matters.
func (v ServerVersion) String() string {
	product := v.Product
	if product == "" {
		product = "unknown"
	}
	return fmt.Sprintf("%s %d.%d.%d", product, v.Major, v.Minor, v.Patch)
}

// DetectServerVersion asks conn for SELECT version() and parses the product
// version from the banner. The answer is cached on the physical session, so
// later calls cost no round trip. A session inside a transaction is refused:
// a failed probe there would abort the caller's transaction, so run it before
// BeginTx or on a connection of its own.
func DetectServerVersion(ctx context.Context, session *sql.Conn) (ServerVersion, error) {
	if session == nil {
		return ServerVersion{}, errors.New("opengauss: DetectServerVersion needs a connection taken with db.Conn")
	}
	var version ServerVersion
	err := session.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("opengauss: DetectServerVersion needs an openGauss connection, got %T", driverConn)
		}
		detected, err := c.detectServerVersion(ctx)
		version = detected
		return err
	})
	if err != nil {
		return ServerVersion{}, err
	}
	return version, nil
}

// detectServerVersion reads the banner once per physical connection.
func (c *conn) detectServerVersion(ctx context.Context) (ServerVersion, error) {
	if c.versionKnown {
		return c.version, nil
	}
	if c.txnStatus != proto.TxnIdle {
		return ServerVersion{}, errors.New("opengauss: DetectServerVersion cannot run inside a transaction")
	}
	rows, err := c.QueryContext(ctx, "SELECT version()", nil)
	if err != nil {
		return ServerVersion{}, err
	}
	values := make([]driver.Value, 1)
	nextErr := rows.Next(values)
	closeErr := rows.Close()
	if nextErr != nil {
		if errors.Is(nextErr, io.EOF) {
			return ServerVersion{}, errors.New("opengauss: SELECT version() returned no row")
		}
		return ServerVersion{}, nextErr
	}
	if closeErr != nil {
		return ServerVersion{}, closeErr
	}
	banner, ok := bannerText(values[0])
	if !ok {
		return ServerVersion{}, fmt.Errorf("opengauss: SELECT version() returned a %T, not text", values[0])
	}
	c.version = parseVersionBanner(banner)
	c.versionKnown = true
	return c.version, nil
}

// bannerText reads the banner whether the column arrived as text or bytes.
func bannerText(value driver.Value) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	}
	return "", false
}

// parseVersionBanner reads the product name and the leading major.minor.patch
// numbers of a server banner. A missing component is zero and a suffix such as
// -RC1 or a build hash does not change the numbers, so an unknown build still
// reports the version it advertises. A banner without a product word directly
// followed by a version leaves the product empty and keeps Raw.
func parseVersionBanner(banner string) ServerVersion {
	version := ServerVersion{Raw: banner}
	fields := strings.Fields(strings.TrimLeft(strings.TrimSpace(banner), "("))
	if len(fields) < 2 || !isVersionProductToken(fields[0]) || !startsWithDigit(fields[1]) {
		return version
	}
	version.Product = fields[0]
	version.Major, version.Minor, version.Patch = parseVersionNumbers(fields[1])
	return version
}

// isVersionProductToken reports whether token can be a product name: a word
// made of letters, digits, underscore or hyphen.
func isVersionProductToken(token string) bool {
	if token == "" || !isLetterByte(token[0]) {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if !isLetterByte(c) && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func isLetterByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func startsWithDigit(text string) bool {
	return text != "" && text[0] >= '0' && text[0] <= '9'
}

// parseVersionNumbers reads up to three dot-separated components, stopping at
// the first that does not start with digits.
func parseVersionNumbers(token string) (major, minor, patch int) {
	parts := strings.SplitN(token, ".", 3)
	targets := []*int{&major, &minor, &patch}
	for i, part := range parts {
		digits := leadingDigits(part)
		if digits == "" {
			return major, minor, patch
		}
		value, err := strconv.Atoi(digits)
		if err != nil || value < 0 {
			return major, minor, patch
		}
		*targets[i] = value
	}
	return major, minor, patch
}

// leadingDigits returns the digit prefix of text.
func leadingDigits(text string) string {
	i := 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	return text[:i]
}
