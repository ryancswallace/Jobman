package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigurationValidationFailures(t *testing.T) {
	t.Parallel()
	tokenPath := filepath.Join(t.TempDir(), "token")

	tests := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"invalid trusted root": {
			mutate: func(configuration *Config) { configuration.TrustedProjectRoots = []string{"relative"} },
			want:   "clean absolute path",
		},
		"unknown wait": {
			mutate: func(configuration *Config) {
				configuration.JobSpecs["job"] = baseJobSpec()
				specification := configuration.JobSpecs["job"]
				specification.Wait.Conditions = []string{"missing"}
				configuration.JobSpecs["job"] = specification
			},
			want: "unknown wait condition",
		},
		"unknown pool": {
			mutate: func(configuration *Config) {
				specification := baseJobSpec()
				specification.Admission.Pool = "missing"
				configuration.JobSpecs["job"] = specification
			},
			want: "unknown pool",
		},
		"capacity impossible": {
			mutate: func(configuration *Config) {
				configuration.Concurrency.MaxActiveSlots = SlotLimit{value: 1, set: true}
				specification := baseJobSpec()
				specification.Admission.Slots = 2
				configuration.JobSpecs["job"] = specification
			},
			want: "exceeds store-wide capacity",
		},
		"secret unknown": {
			mutate: func(configuration *Config) {
				specification := baseJobSpec()
				specification.Environment.Secrets["TOKEN"] = "missing"
				configuration.JobSpecs["job"] = specification
			},
			want: "unknown secret",
		},
		"exit overlap": {
			mutate: func(configuration *Config) {
				specification := baseJobSpec()
				specification.Completion.RetryableExitCodes = []int{0}
				configuration.JobSpecs["job"] = specification
			},
			want: "both successful and retryable",
		},
		"contradictory dependency": {
			mutate: func(configuration *Config) {
				specification := baseJobSpec()
				specification.Dependencies = []Dependency{
					{Job: "prior", Outcomes: []string{"success"}},
					{Job: "prior", Outcomes: []string{"failure"}},
				}
				configuration.JobSpecs["job"] = specification
			},
			want: "contradictory outcome requirements",
		},
		"wait union": {
			mutate: func(configuration *Config) {
				configuration.WaitConditions["bad"] = WaitCondition{
					Type:  "delay",
					Delay: durationMust(time.Second),
					Until: "2026-01-01T00:00:00Z",
				}
			},
			want: "requires only the delay field",
		},
		"sensitive literal header": {
			mutate: func(configuration *Config) {
				configuration.Notifiers["hook"] = baseHTTPNotifier()
				notifier := configuration.Notifiers["hook"]
				notifier.HTTP.Headers["Authorization"] = "Bearer literal"
				configuration.Notifiers["hook"] = notifier
			},
			want: "must use secret_headers",
		},
		"insecure HTTP": {
			mutate: func(configuration *Config) {
				configuration.Notifiers["hook"] = baseHTTPNotifier()
				notifier := configuration.Notifiers["hook"]
				notifier.HTTP.URL = "http://example.com/hook"
				configuration.Notifiers["hook"] = notifier
			},
			want: "must use HTTPS",
		},
		"private HTTP host": {
			mutate: func(configuration *Config) {
				configuration.Notifiers["hook"] = baseHTTPNotifier()
				notifier := configuration.Notifiers["hook"]
				notifier.HTTP.URL = "https://127.0.0.1/hook"
				configuration.Notifiers["hook"] = notifier
			},
			want: "allow_private_hosts",
		},
		"invalid HTTP header": {
			mutate: func(configuration *Config) {
				configuration.Notifiers["hook"] = baseHTTPNotifier()
				notifier := configuration.Notifiers["hook"]
				notifier.HTTP.Headers["Bad:Header"] = "value"
				configuration.Notifiers["hook"] = notifier
			},
			want: "header name or value is invalid",
		},
		"profile base missing": {
			mutate: func(configuration *Config) {
				configuration.Profiles["bad"] = Profile{JobSpec: "missing"}
			},
			want: "unknown job spec",
		},
		"shared current profile missing": {
			mutate: func(configuration *Config) {
				configuration.Shared.CurrentProfile = "missing"
			},
			want: "unknown profile",
		},
		"shared namespace invalid": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "https://jobman.example.edu", Namespace: "Not Portable",
				}
			},
			want: "invalid namespace",
		},
		"shared endpoint insecure": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "http://jobman.example.edu", Namespace: "research",
				}
			},
			want: "must use HTTPS",
		},
		"shared token over HTTP": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "http://127.0.0.1:8080", Namespace: "research", TokenFile: tokenPath,
				}
			},
			want: "cannot send a token over HTTP",
		},
		"shared credential path relative": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "https://jobman.example.edu", Namespace: "research", TokenFile: "token",
				}
			},
			want: "clean absolute path",
		},
		"shared artifact version invalid": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "https://jobman.example.edu", Namespace: "research",
					ArtifactRoots: map[string]SharedArtifactRoot{
						"department-nfs": {Path: "/nfs/jobman"},
					},
				}
			},
			want: "version must be positive",
		},
		"shared artifact path relative": {
			mutate: func(configuration *Config) {
				configuration.Shared.Profiles["department"] = SharedProfile{
					Endpoint: "https://jobman.example.edu", Namespace: "research",
					ArtifactRoots: map[string]SharedArtifactRoot{
						"department-nfs": {Version: 1, Path: "relative"},
					},
				}
			},
			want: "clean absolute path",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			configuration := Default()
			test.mutate(&configuration)
			err := configuration.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestWaitConditionVariants(t *testing.T) {
	t.Parallel()

	configuration := Default()
	configuration.Secrets["probe-token"] = SecretRef{provider: "env", locator: "PROBE_TOKEN"}
	configuration.WaitConditions = map[string]WaitCondition{
		"until": {
			Type:  "until",
			Until: "2026-07-15T08:00:00.123456789Z",
		},
		"delay": {
			Type:  "delay",
			Delay: durationMust(0),
		},
		"file": {
			Type:       "file-exists",
			FileExists: &FileCondition{Path: "relative.ready", Type: "file"},
		},
		"probe": {
			Type: "probe",
			Probe: &ProbeCondition{
				Command:      []string{"probe", "arg with spaces"},
				Environment:  Environment{Secrets: map[string]string{"TOKEN": "probe-token"}},
				Timeout:      durationMust(time.Second),
				PollInterval: durationMust(time.Millisecond),
				OutputLimit:  byteLimit(1024),
			},
		},
	}
	normalize(&configuration)
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestEquivalentDependenciesAreCoalesced(t *testing.T) {
	t.Parallel()

	configuration := Default()
	specification := JobSpec{
		Command: []string{"true"},
		Dependencies: []Dependency{
			{Job: "prior", Outcomes: []string{"failure", "success"}},
			{Job: "prior", Outcomes: []string{"success", "failure"}},
		},
	}
	normalizeJobSpec(&specification)
	configuration.JobSpecs["job"] = specification
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if len(specification.Dependencies) != 1 {
		t.Fatalf("len(Dependencies) = %d, want 1", len(specification.Dependencies))
	}
}

func TestNotifierVariants(t *testing.T) {
	t.Parallel()

	configuration := Default()
	configuration.Secrets["credential"] = SecretRef{provider: "env", locator: "NOTIFIER_PASSWORD"}
	configuration.Notifiers["command"] = Notifier{
		Type:   "command",
		Events: []string{"job_lost"},
		Command: &CommandNotifier{
			Command:     []string{"notify", "--json"},
			Environment: Environment{Secrets: map[string]string{"TOKEN": "credential"}},
		},
	}
	configuration.Notifiers["http"] = Notifier{
		Type: "http",
		HTTP: &HTTPNotifier{
			URL:               "http://127.0.0.1:8080/hook",
			AllowHTTP:         true,
			AllowPrivateHosts: true,
			SigningSecret:     "credential",
		},
	}
	configuration.Notifiers["smtp"] = Notifier{
		Type: "smtp",
		SMTP: &SMTPNotifier{
			Address:        "smtp.example.com:587",
			TLS:            "starttls",
			Username:       "jobman",
			PasswordSecret: "credential",
			From:           "Jobman <jobman@example.com>",
			To:             []string{"ops@example.com"},
		},
	}
	normalize(&configuration)
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestRedactorRedactsNamesValuesAndPatterns(t *testing.T) {
	t.Parallel()

	redactor, err := NewRedactor(
		RedactionConfig{Names: []string{"session"}, Patterns: []string{`acct-[0-9]+`}},
		map[string]string{"short": "abc", "long": "abcdef"},
	)
	if err != nil {
		t.Fatalf("NewRedactor() error = %v", err)
	}
	if got := redactor.RedactField("session", "visible"); got != Redacted {
		t.Fatalf("RedactField(session) = %q", got)
	}
	if got := redactor.RedactField("api_token", "visible"); got != Redacted {
		t.Fatalf("RedactField(api_token) = %q", got)
	}
	if got := redactor.RedactString("abcdef abc acct-123"); got != "[REDACTED] [REDACTED] [REDACTED]" {
		t.Fatalf("RedactString() = %q", got)
	}
	if got := (*Redactor)(nil).RedactString("unchanged"); got != "unchanged" {
		t.Fatalf("nil RedactString() = %q", got)
	}
}

func TestNewRedactorRejectsInvalidPolicies(t *testing.T) {
	t.Parallel()

	if _, err := NewRedactor(RedactionConfig{Patterns: []string{"["}}, nil); err == nil {
		t.Fatal("NewRedactor() accepted invalid regular expression")
	}
	patterns := make([]string, maxRedactionPatterns+1)
	for index := range patterns {
		patterns[index] = "x"
	}
	if _, err := NewRedactor(RedactionConfig{Patterns: patterns}, nil); err == nil {
		t.Fatal("NewRedactor() accepted too many patterns")
	}
}

func TestResolveJobSpecRejectsConflictingProfiles(t *testing.T) {
	t.Parallel()

	configuration := Default()
	configuration.JobSpecs["first"] = baseJobSpec()
	configuration.JobSpecs["second"] = baseJobSpec()
	configuration.Profiles["first"] = Profile{JobSpec: "first"}
	configuration.Profiles["second"] = Profile{JobSpec: "second"}
	if _, err := configuration.ResolveJobSpec("", "first", "second"); err == nil {
		t.Fatal("ResolveJobSpec() accepted conflicting bases")
	}
	if _, err := configuration.ResolveJobSpec("", "missing"); err == nil {
		t.Fatal("ResolveJobSpec() accepted unknown profile")
	}
}

func baseJobSpec() JobSpec {
	specification := JobSpec{Command: []string{"true"}}
	normalizeJobSpec(&specification)

	return specification
}

func baseHTTPNotifier() Notifier {
	notifier := Notifier{
		Type: "http",
		HTTP: &HTTPNotifier{
			URL:     "https://example.com/hook",
			Headers: map[string]string{},
		},
	}
	normalizeNotifier(&notifier)

	return notifier
}
