package pgpassfile

import (
	"strings"
	"testing"
)

func TestParseAndFindPassword(t *testing.T) {
	passfile, err := Parse(strings.NewReader(`# host:port:database:user:password
127.0.0.1:5432:app:alice:topsecret
127.0.0.1:5433:*:bob:otherpass
*:5432:app:carol:starpass
127.0.0.1:5432:esc:alice:p\:a\\ss
bad line
short:line
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tests := []struct {
		name                       string
		host, port, database, user string
		want                       string
	}{
		{"exact", "127.0.0.1", "5432", "app", "alice", "topsecret"},
		{"database wildcard", "127.0.0.1", "5433", "any", "bob", "otherpass"},
		{"host wildcard", "elsewhere", "5432", "app", "carol", "starpass"},
		{"escaped separators", "127.0.0.1", "5432", "esc", "alice", `p:a\ss`},
		{"no match", "127.0.0.1", "9999", "app", "alice", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := passfile.FindPassword(tt.host, tt.port, tt.database, tt.user); got != tt.want {
				t.Errorf("FindPassword = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseLineStopsPasswordAtUnescapedColon(t *testing.T) {
	entry, ok := parseLine("host:5432:app:alice:pa:ss")
	if !ok {
		t.Fatal("parseLine reported a malformed line")
	}
	if entry.Password != "pa" {
		t.Errorf("Password = %q, want libpq's stop at the first unescaped colon", entry.Password)
	}
}

func TestParseLineKeepsEscapedStarLiteral(t *testing.T) {
	entry, ok := parseLine(`\*:5432:app:alice:\*`)
	if !ok {
		t.Fatal("parseLine reported a malformed line")
	}
	if entry.Hostname != `\*` {
		t.Errorf("Hostname = %q, want a literal backslash-star", entry.Hostname)
	}
}

func TestParseHandlesCRLF(t *testing.T) {
	passfile, err := Parse(strings.NewReader("host:1:db:user:crlfpass\r\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := passfile.FindPassword("host", "1", "db", "user"); got != "crlfpass" {
		t.Errorf("FindPassword = %q, want %q", got, "crlfpass")
	}
}
