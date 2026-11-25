package pgservicefile

import (
	"strings"
	"testing"
)

func TestParseSection(t *testing.T) {
	settings, err := Parse(strings.NewReader(`# global comment
[other]
host=ignored

[svc]
host=svchost
port=5433
`), "svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if settings["host"] != "svchost" || settings["port"] != "5433" {
		t.Errorf("settings = %v", settings)
	}
}

func TestParseStopsAtEndOfSection(t *testing.T) {
	// libpq ignores unrelated sections, including malformed lines in them.
	settings, err := Parse(strings.NewReader("[svc]\nhost=a\n[unrelated]\nbadline\n"), "svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if settings["host"] != "a" {
		t.Errorf("settings = %v", settings)
	}
}

func TestParseDuplicateSectionKeepsFirst(t *testing.T) {
	settings, err := Parse(strings.NewReader("[svc]\nhost=first\n[svc]\nhost=second\n"), "svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if settings["host"] != "first" {
		t.Errorf("host = %q, want the first section's value", settings["host"])
	}
}

func TestParseErrors(t *testing.T) {
	if _, err := Parse(strings.NewReader("[svc\nhost=x\n"), "svc"); err == nil ||
		!strings.Contains(err.Error(), "malformed section header") {
		t.Errorf("malformed header error = %v", err)
	}
	if _, err := Parse(strings.NewReader("[svc]\nhost\n"), "svc"); err == nil ||
		!strings.Contains(err.Error(), "expected key=value") {
		t.Errorf("missing = error = %v", err)
	}
}

func TestServiceNotFound(t *testing.T) {
	if _, err := Parse(strings.NewReader("[other]\nhost=x\n"), "svc"); err == nil ||
		!strings.Contains(err.Error(), `service "svc"`) {
		t.Errorf("error = %v, want a missing-service error", err)
	}
}
