package jobman

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryancswallace/jobman/internal/config"
)

func TestPresentationIsExplicitAndDoesNotDecoratePipes(t *testing.T) {
	t.Parallel()
	deps := dependenciesFor(newFakeBackend(t))
	deps.LoadConfig = func(*rootOptions) (config.Loaded, error) {
		return config.Loaded{Config: config.Config{Presentation: config.Presentation{Organization: "Example Organization", MOTD: "Scheduled maintenance"}}}, nil
	}
	output, err := executeCommand(t, deps, nil)
	if err != nil || strings.Contains(output, "Scheduled maintenance") {
		t.Fatalf("pipe received presentation: %q, %v", output, err)
	}
	output, err = executeCommand(t, deps, []string{"about"})
	if err != nil || !strings.Contains(output, "Scheduled maintenance") {
		t.Fatalf("about: %q, %v", output, err)
	}
	output, err = executeCommand(t, deps, []string{"about", "--json"})
	if err != nil || !json.Valid([]byte(output)) {
		t.Fatalf("JSON presentation: %q, %v", output, err)
	}
}

func TestIsolatedConfigValidationIgnoresEnvironment(t *testing.T) {
	t.Setenv("JOBMAN_RETENTION_MAX_JOBS", "invalid")
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("schema_version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := NewCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetArgs([]string{"config", "validate", "--isolated", path})
	if err := command.Execute(); err != nil || output.String() != "valid\n" {
		t.Fatalf("isolated validation: %q, %v", output.String(), err)
	}
}
