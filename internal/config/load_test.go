package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestDefaultConfiguration(t *testing.T) {
	t.Parallel()

	configuration := Default()
	if configuration.SchemaVersion != SchemaVersion {
		t.Fatalf("SchemaVersion = %d, want %d", configuration.SchemaVersion, SchemaVersion)
	}
	if !configuration.Concurrency.MaxActiveSlots.IsUnlimited() {
		t.Fatal("MaxActiveSlots is finite, want unlimited")
	}
	if !configuration.Retention.CompletedMetadataMaxAge.IsUnlimited() {
		t.Fatal("CompletedMetadataMaxAge is finite, want unlimited")
	}
	logAge, finite := configuration.Retention.CompletedLogMaxAge.Value()
	if !finite || logAge != 30*24*time.Hour {
		t.Fatalf("CompletedLogMaxAge = (%v, %v), want (720h, true)", logAge, finite)
	}
	if configuration.JobSpecs == nil || configuration.Notifiers == nil || configuration.Profiles == nil ||
		configuration.Shared.Profiles == nil {
		t.Fatal("Default() returned nil named-object maps")
	}
	specification := baseJobSpec()
	if failures, finite := specification.Completion.MaxFailures.Value(); !finite || failures != 1 {
		t.Fatalf("default MaxFailures = (%d, %v), want (1, true)", failures, finite)
	}
}

func TestParseCompleteConfiguration(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configurationYAML := fmt.Sprintf(`
schema_version: 1
trusted_project_roots: [%q]
secrets:
  api-token: env:JOBMAN_API_TOKEN
wait_conditions:
  morning:
    type: until
    until: "2026-07-15T08:00:00Z"
  settle:
    type: delay
    delay: 5s
  ready:
    type: file-exists
    file_exists:
      path: ./input.ready
      type: file
  healthy:
    type: probe
    probe:
      command: [healthcheck, --quiet]
      timeout: 4s
      poll_interval: 250ms
      output_limit: 8KiB
      environment:
        secrets:
          TOKEN: api-token
concurrency:
  max_active_slots: 4
  pools:
    experiments: 2
retention:
  completed_log_max_age: 2w
  max_jobs: 100
notifiers:
  hook:
    type: command
    events: [job_failed]
    command:
      command: [notify-hook]
      output_limit: 16KiB
  webhook:
    type: http
    events: [job_succeeded, job_failed]
    http:
      url: example.com/hooks/jobman
      signing_secret: api-token
      headers:
        X-Client: jobman
      secret_headers:
        X-API-Key: api-token
  email:
    type: smtp
    smtp:
      address: smtp.example.com:465
      tls: implicit
      username: jobman
      password_secret: api-token
      from: jobman@example.com
      to: [operator@example.com]
job_specs:
  analyze:
    command: [analyze, --input, input.dat]
    name: analysis
    groups: [science]
    working_directory: %q
    environment:
      set:
        MODE: batch
      unset: [DEBUG]
      secrets:
        TOKEN: api-token
    dependencies:
      - job: 01980f4c-7b2a-7a6f-8c10-0123456789ab
        outcomes: [success]
    wait:
      mode: all
      conditions: [ready, healthy]
    admission:
      pool: experiments
      slots: 2
    completion:
      max_runs: 4
      failure_limit: 3
      success_target: 1
      success_exit_codes: [0]
      retryable_exit_codes: [75]
    delay:
      strategy: exponential
      initial: 1s
      max_delay: 30s
      base: 2
      jitter: 200ms
    timeouts:
      run: 10m
      job: 1h
    logging:
      capture: both
      segment_bytes: 10MiB
      segments_per_run: 5
      completed_log_max_age: 7d
    notification:
      notifiers: [hook, webhook]
      events: [job_failed]
profiles:
  production:
    job_spec: analyze
    overrides:
      name: production-analysis
      environment:
        set:
          MODE: production
      admission:
        pool: experiments
        slots: 1
redaction:
  names: [session_id]
  patterns: ['acct-[0-9]+']
`, root, root)

	configuration, err := Parse([]byte(configurationYAML))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if capacity, finite := configuration.Concurrency.Pools["experiments"].Value(); !finite || capacity != 2 {
		t.Fatalf("experiments capacity = (%d, %v), want (2, true)", capacity, finite)
	}
	if got := configuration.Notifiers["webhook"].HTTP.EffectiveURL(); got != "https://example.com/hooks/jobman" {
		t.Fatalf("EffectiveURL() = %q", got)
	}
	probeTimeout, set := configuration.WaitConditions["healthy"].Probe.Timeout.Value()
	if !set || probeTimeout != 4*time.Second {
		t.Fatalf("probe timeout = (%v, %v), want (4s, true)", probeTimeout, set)
	}

	resolved, err := configuration.ResolveJobSpec("", "production")
	if err != nil {
		t.Fatalf("ResolveJobSpec() error = %v", err)
	}
	if resolved.Name != "production-analysis" || resolved.Environment.Set["MODE"] != "production" || resolved.Admission.Slots != 1 {
		t.Fatalf("ResolveJobSpec() = %#v", resolved)
	}
	resolved.Dependencies[0].Outcomes[0] = "failure"
	if configuration.JobSpecs["analyze"].Dependencies[0].Outcomes[0] != "success" {
		t.Fatal("ResolveJobSpec() exposed base dependency slices")
	}
	if configuration.JobSpecs["analyze"].Environment.Set["MODE"] != "batch" {
		t.Fatal("ResolveJobSpec() mutated the configured base specification")
	}

	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), `"max_active_slots":{}`) || !strings.Contains(string(encoded), `"completed_log_max_age":"336h0m0s"`) {
		t.Fatalf("json.Marshal() emitted unusable scalar values: %s", encoded)
	}
	effectiveYAML, err := yaml.Marshal(configuration)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	if _, err := Parse(effectiveYAML); err != nil {
		t.Fatalf("Parse(yaml.Marshal(configuration)) error = %v\n%s", err, effectiveYAML)
	}
}

func TestLoadMergesSourcesAndTracksOrigins(t *testing.T) {
	t.Parallel()

	low := BytesSource(SourceUser, "user", []byte(`
job_specs:
  build:
    command: [go, build, ./...]
    environment:
      set:
        FIRST: one
        SHARED: low
`))
	high := BytesSource(SourceExplicit, "explicit", []byte(`
job_specs:
  build:
    command: [go, test, ./...]
    environment:
      set:
        SECOND: two
        SHARED: high
`))

	loaded, err := Load(low, high)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	specification := loaded.Config.JobSpecs["build"]
	if got := strings.Join(specification.Command, " "); got != "go test ./..." {
		t.Fatalf("Command = %q", got)
	}
	if specification.Environment.Set["FIRST"] != "one" || specification.Environment.Set["SECOND"] != "two" || specification.Environment.Set["SHARED"] != "high" {
		t.Fatalf("Environment.Set = %#v", specification.Environment.Set)
	}
	if origin := loaded.Origins["job_specs.build.command"]; origin.Kind != SourceExplicit {
		t.Fatalf("command origin = %#v, want explicit", origin)
	}
	if origin := loaded.Origins["job_specs.build.environment.set.FIRST"]; origin.Kind != SourceUser {
		t.Fatalf("FIRST origin = %#v, want user", origin)
	}
}

func TestLoadEnforcesNormativeSourceOrder(t *testing.T) {
	t.Parallel()

	if _, err := Load(
		BytesSource(SourceEnvironment, "environment", []byte("{}\n")),
		BytesSource(SourceUser, "user", []byte("{}\n")),
	); err == nil {
		t.Fatal("Load() accepted descending source precedence")
	}
	if flags, ok := SourceFlags.Precedence(); !ok || flags <= 0 {
		t.Fatalf("SourceFlags.Precedence() = (%d, %v)", flags, ok)
	}
	if _, ok := SourceKind("unknown").Precedence(); ok {
		t.Fatal("unknown source kind has a precedence")
	}
}

func TestEnvironmentScalarAndSourceErrors(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		value string
		kind  environmentValueKind
	}{
		{value: "", kind: environmentIntegerLimit},
		{value: "bad", kind: environmentIntegerLimit},
		{value: "bad", kind: environmentDurationLimit},
		{value: "bad", kind: environmentByteLimit},
		{value: "1", kind: environmentValueKind(255)},
	} {
		if _, err := environmentScalar(test.value, test.kind); err == nil {
			t.Fatalf("environmentScalar(%q, %d) error = nil", test.value, test.kind)
		}
	}
	if _, _, err := EnvironmentSource([]string{
		"JOBMAN_RETENTION_MAX_JOBS=1",
		"JOBMAN_RETENTION_MAX_JOBS=2",
	}); err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("EnvironmentSource(duplicate) error = %v, want invalid configuration", err)
	}
	if _, _, err := EnvironmentSource([]string{"JOBMAN_RETENTION_MAX_JOBS=bad"}); err == nil ||
		!errors.Is(err, ErrInvalid) {
		t.Fatalf("EnvironmentSource(invalid) error = %v, want invalid configuration", err)
	}
	if source, found, err := EnvironmentSource([]string{"IGNORED=value", "malformed"}); err != nil || found || source.Kind != "" {
		t.Fatalf("EnvironmentSource(ignored) = %#v, %t, %v", source, found, err)
	}
}

func TestParseRejectsMalformedYAML(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"unknown key":        "mystery: true\n",
		"duplicate key":      "concurrency:\n  max_active_slots: 1\n  max_active_slots: 2\n",
		"multiple documents": "schema_version: 1\n---\nschema_version: 1\n",
		"alias":              "value: &shared x\nother: *shared\n",
		"merge key":          "base: &base {name: x}\nprofiles:\n  x:\n    <<: *base\n",
		"non-string key":     "1: value\n",
		"new schema":         "schema_version: 2\n",
		"wrong type":         "concurrency:\n  max_active_slots: false\n",
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Parse([]byte(input)); err == nil || !errors.Is(err, ErrInvalid) {
				t.Fatalf("Parse(%q) error = %v, want invalid configuration", input, err)
			}
		})
	}
}

func TestLoadSourcePolicyAndFileHandling(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	missing := OptionalFileSource(SourceUser, filepath.Join(root, "missing.yml"))
	loaded, err := Load(missing)
	if err != nil {
		t.Fatalf("Load(optional missing) error = %v", err)
	}
	if len(loaded.Sources) != 1 {
		t.Fatalf("len(Sources) = %d, want 1", len(loaded.Sources))
	}

	if _, err := Load(BytesSource(SourceSystem, "system", []byte("trusted_project_roots: [/tmp]\n"))); err == nil ||
		!errors.Is(err, ErrInvalid) {
		t.Fatalf("system source policy error = %v, want invalid configuration", err)
	}
	if _, err := Load(BytesSource(
		SourceProject, "project", []byte("shared:\n  current_profile: department\n"),
	)); err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("project shared-profile policy error = %v, want invalid configuration", err)
	}
	if _, err := Parse(nil); err != nil {
		t.Fatalf("Parse(empty) error = %v", err)
	}
	if _, err := Load(FileSource(SourceExplicit, root)); err == nil || errors.Is(err, ErrInvalid) {
		t.Fatalf("directory source error = %v, want non-validation failure", err)
	}

	large := make([]byte, maxConfigBytes+1)
	if _, err := Load(BytesSource(SourceExplicit, "large", large)); err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversize source error = %v, want invalid configuration", err)
	}
	if _, err := Load(FileSource(SourceExplicit, filepath.Join(root, "missing.yml"))); err == nil ||
		errors.Is(err, ErrInvalid) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source error = %v, want filesystem not-exist failure", err)
	}
}

func TestParseSharedProfiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	loaded, err := Parse([]byte(fmt.Sprintf(`
shared:
  current_profile: department
  profiles:
    department:
      endpoint: https://jobman.example.edu
      namespace: research
      token_file: %q
      ca_file: %q
      artifact_roots:
        department-nfs:
          version: 3
          path: %q
`, filepath.Join(root, "token"), filepath.Join(root, "ca.pem"), filepath.Join(root, "artifacts"))))
	if err != nil {
		t.Fatalf("Parse(shared profiles) error = %v", err)
	}
	profile := loaded.Shared.Profiles["department"]
	if loaded.Shared.CurrentProfile != "department" || profile.Namespace != "research" ||
		profile.Endpoint != "https://jobman.example.edu" ||
		profile.ArtifactRoots["department-nfs"].Version != 3 {
		t.Fatalf("shared configuration = %#v", loaded.Shared)
	}
}

func TestEnvironmentSourceOverridesDefaults(t *testing.T) {
	t.Parallel()

	source, found, err := EnvironmentSource([]string{
		"IGNORED=value",
		"JOBMAN_STATE_DIR=/somewhere",
		"JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS=6",
		"JOBMAN_RETENTION_COMPLETED_LOG_MAX_AGE=2w",
		"JOBMAN_RETENTION_MAX_TOTAL_LOG_BYTES=2GiB",
	})
	if err != nil {
		t.Fatalf("EnvironmentSource() error = %v", err)
	}
	if !found {
		t.Fatal("EnvironmentSource() found = false")
	}
	loaded, err := Load(source)
	if err != nil {
		t.Fatalf("Load(environment) error = %v", err)
	}
	if slots, finite := loaded.Config.Concurrency.MaxActiveSlots.Value(); !finite || slots != 6 {
		t.Fatalf("MaxActiveSlots = (%d, %v), want (6, true)", slots, finite)
	}
	if age, finite := loaded.Config.Retention.CompletedLogMaxAge.Value(); !finite || age != 14*24*time.Hour {
		t.Fatalf("CompletedLogMaxAge = (%v, %v), want (336h, true)", age, finite)
	}
	if size, finite := loaded.Config.Retention.MaxTotalLogBytes.Value(); !finite || size != 2<<30 {
		t.Fatalf("MaxTotalLogBytes = (%d, %v), want (%d, true)", size, finite, uint64(2<<30))
	}

	bindings := EnvironmentBindings()
	bindings["JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS"] = "modified"
	if path, ok := EnvironmentPath("JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS"); !ok || path != "concurrency.max_active_slots" {
		t.Fatalf("EnvironmentPath() = (%q, %v)", path, ok)
	}
}

func TestEnvironmentSourceRejectsInvalidAndDuplicateValues(t *testing.T) {
	t.Parallel()

	tests := [][]string{
		{"JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS=0"},
		{"JOBMAN_RETENTION_COMPLETED_LOG_MAX_AGE=-1s"},
		{"JOBMAN_RETENTION_MAX_TOTAL_LOG_BYTES=2MB"},
		{"JOBMAN_RETENTION_MAX_JOBS=1", "JOBMAN_RETENTION_MAX_JOBS=2"},
	}
	for _, environ := range tests {
		source, found, err := EnvironmentSource(environ)
		if err == nil && found {
			_, err = Load(source)
		}
		if err == nil {
			t.Fatalf("EnvironmentSource(%v) succeeded", environ)
		}
	}
}

func TestCurrentEnvironmentSourceAndUnsupportedBinding(t *testing.T) {
	t.Setenv("JOBMAN_RETENTION_MAX_JOBS", "12")

	source, found, err := CurrentEnvironmentSource()
	if err != nil || !found || source.Kind != SourceEnvironment {
		t.Fatalf("CurrentEnvironmentSource() = %#v, %t, %v", source, found, err)
	}
	if _, err := environmentScalar("12", environmentValueKind(255)); err == nil {
		t.Fatal("environmentScalar(unsupported kind) error = nil")
	}
}

func TestFileSourceReadsRegularYAML(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("concurrency:\n  max_active_slots: 3\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	loaded, err := Load(FileSource(SourceExplicit, path))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if slots, finite := loaded.Config.Concurrency.MaxActiveSlots.Value(); !finite || slots != 3 {
		t.Fatalf("MaxActiveSlots = (%d, %v), want (3, true)", slots, finite)
	}
}

func FuzzParseConfiguration(fuzz *testing.F) {
	fuzz.Add([]byte("{}\n"))
	fuzz.Add([]byte("concurrency:\n  max_active_slots: unlimited\n"))
	fuzz.Add([]byte("x: &x [*x]\n"))

	fuzz.Fuzz(func(t *testing.T, data []byte) {
		configuration, err := Parse(data)
		if err != nil {
			return
		}
		if err := configuration.Validate(); err != nil {
			t.Fatalf("Parse() returned invalid configuration: %v", err)
		}
	})
}
