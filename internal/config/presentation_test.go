package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPresentationRejectsTerminalInjectionAndUnsafeLinks(t *testing.T) {
	t.Parallel()
	for _, presentation := range []Presentation{
		{MOTD: "\x1b]52;c;secret\a"},
		{Organization: "one\ntwo"},
		{MOTD: "unsafe\u202e"},
		{SupportURL: "http://example.com"},
		{DocumentationURL: "https://user:secret@example.com"}, // #nosec G101 -- synthetic URL verifies credentials are rejected.
		{MOTD: strings.Repeat("x", 4097)},
	} {
		if err := presentation.Validate(); err == nil {
			t.Errorf("accepted unsafe presentation: %#v", presentation)
		}
	}
	loaded, err := Parse([]byte("schema_version: 1\npresentation:\n  organization: Example Organization\n  motd: |\n    First line\n    Second line\n"))
	if err != nil || loaded.Presentation.Organization != "Example Organization" {
		t.Fatalf("presentation parse: %v", err)
	}
	schema, err := json.Marshal(JSONSchema())
	if err != nil || !strings.Contains(string(schema), "presentation") || !strings.Contains(string(schema), "additionalProperties") {
		t.Fatalf("schema: %s, %v", schema, err)
	}
}
