package agent

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAgentConfigurationOverridesAndServiceReference(t *testing.T) {
	t.Setenv(agentConfigEnvironment, "")
	path := filepath.Join(t.TempDir(), "agent.yml")
	state := filepath.Join(t.TempDir(), "state")
	body := fmt.Sprintf("schema_version: 1\nstate_dir: %q\npoll_interval: 4s\ncontainer_engine: podman\ncontainer_host_network: true\n", state)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	flags.String("state-dir", "", "")
	interval := flags.Duration("poll-interval", time.Second, "")
	host := flags.Bool("container-host-network", true, "")
	if err := flags.Parse([]string{"--poll-interval=7s", "--container-host-network=false"}); err != nil {
		t.Fatal(err)
	}
	overrides := agentRunOverrides(flags)
	selected, err := applyAgentConfiguration(flags, path)
	if err != nil {
		t.Fatal(err)
	}
	if *interval != 7*time.Second || *host || flags.Lookup("state-dir").Value.String() != state {
		t.Fatal("explicit flags did not win over configuration")
	}
	arguments, err := serviceRunArguments(InstallServiceOptions{ConfigPath: selected, ConfigOverrides: overrides}, state)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"run", "--config", path, "--container-host-network=false", "--poll-interval=7s"}
	if !reflect.DeepEqual(arguments, expected) {
		t.Fatalf("service arguments = %v", arguments)
	}
	configuration, err := loadAgentConfiguration(path)
	if err != nil || configuration.Origins["poll_interval"] != path || configuration.Origins["max_log_bytes"] != "built-in" {
		t.Fatalf("provenance: %v, %v", configuration.Origins, err)
	}
}

func TestAgentConfigurationFailsClosedWithoutResolvingReferences(t *testing.T) {
	t.Parallel()
	for _, invalid := range []string{
		"schema_version: 1\npoll_interval: 0s\n", "schema_version: 1\nunknown: secret\n",
		"schema_version: 1\nserver: http://example.com\n", "schema_version: 1\nmax_log_bytes: 0\n",
		"schema_version: 1\nstate_dir: &state /tmp/state\n", "schema_version: 1\nstate_dir: /tmp/one\nstate_dir: /tmp/two\n",
		"schema_version: 1\n---\nschema_version: 1\n", "schema_version: 1\ncontainer_host_network: true\n",
		"schema_version: 1\nslurm_root: /tmp/slurm\n", "schema_version: 1\ntoken_file: relative\n",
		"schema_version: 1\ncontainer_host_network: yes\n", "state_dir: /tmp/state\n",
	} {
		path := filepath.Join(t.TempDir(), "invalid.yml")
		if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadAgentConfiguration(path); err == nil {
			t.Errorf("accepted %q", invalid)
		}
	}
	path := filepath.Join(t.TempDir(), "refs.yml")
	missing := filepath.Join(t.TempDir(), "not-present")
	body := fmt.Sprintf("schema_version: 1\ntoken_file: %q\nserver_ca: %q\n", filepath.Join(missing, "token"), filepath.Join(missing, "ca.pem"))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := executeAgentConfig([]string{"validate", path}, &output); err != nil || output.String() != "valid\n" {
		t.Fatalf("validation opened references: %v", err)
	}
	if err := executeAgentConfig([]string{"show", path}, &output); err != nil {
		t.Fatal(err)
	}
	if err := executeAgentConfig([]string{"schema"}, &output); err != nil || !strings.Contains(output.String(), "additionalProperties") {
		t.Fatalf("schema: %v", err)
	}
}

func TestAgentConfigurationExplicitPathWinsOverEnvironment(t *testing.T) {
	t.Setenv(agentConfigEnvironment, "/not-present/agent.yml")
	path := filepath.Join(t.TempDir(), "agent.yml")
	if err := os.WriteFile(path, []byte("schema_version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	if _, err := applyAgentConfiguration(flags, path); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAgentConfiguration(flags, ""); err == nil {
		t.Fatal("missing environment-selected file did not fail closed")
	}
}
