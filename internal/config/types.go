package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// SchemaVersion is the current YAML configuration schema version.
const SchemaVersion = 1

const (
	// Unlimited is the only textual representation of an unbounded limit.
	Unlimited = "unlimited"
	// Redacted is substituted for sensitive field values and matched secrets.
	Redacted       = "[REDACTED]"
	yamlTagMap     = "!!map"
	yamlTagString  = "!!str"
	yamlTagInteger = "!!int"
	goosWindows    = "windows"
	goosDarwin     = "darwin"
	stdinNull      = "null"
	waitModeAll    = "all"
	waitModeAny    = "any"
	fileKind       = "file"
	httpScheme     = "http"
	httpsScheme    = "https"
)

// Config is Jobman's complete, effective configuration.
type Config struct {
	SchemaVersion       int                      `yaml:"schema_version" json:"schema_version"`
	TrustedProjectRoots []string                 `yaml:"trusted_project_roots" json:"trusted_project_roots"`
	JobSpecs            map[string]JobSpec       `yaml:"job_specs" json:"job_specs"`
	WaitConditions      map[string]WaitCondition `yaml:"wait_conditions" json:"wait_conditions"`
	Secrets             map[string]SecretRef     `yaml:"secrets" json:"secrets"`
	Concurrency         Concurrency              `yaml:"concurrency" json:"concurrency"`
	Retention           Retention                `yaml:"retention" json:"retention"`
	Notifiers           map[string]Notifier      `yaml:"notifiers" json:"notifiers"`
	Profiles            map[string]Profile       `yaml:"profiles" json:"profiles"`
	Shared              SharedConfig             `yaml:"shared" json:"shared"`
	Redaction           RedactionConfig          `yaml:"redaction" json:"redaction"`
}

// SharedConfig contains explicitly selected pre-release control-service profiles.
// It is separate from Profiles, which remains the frozen v1 job-spec overlay feature.
type SharedConfig struct {
	CurrentProfile string                   `yaml:"current_profile,omitempty" json:"current_profile,omitempty"`
	Profiles       map[string]SharedProfile `yaml:"profiles" json:"profiles"`
}

// SharedProfile selects one Jobman Control state universe and credential source.
// Bearer tokens are read from TokenFile and are never embedded in configuration.
type SharedProfile struct {
	Endpoint      string                        `yaml:"endpoint" json:"endpoint"`
	Namespace     string                        `yaml:"namespace" json:"namespace"`
	TokenFile     string                        `yaml:"token_file,omitempty" json:"token_file,omitempty"`
	CAFile        string                        `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	ArtifactRoots map[string]SharedArtifactRoot `yaml:"artifact_roots,omitempty" json:"artifact_roots,omitempty"`
}

// SharedArtifactRoot maps one versioned logical filesystem store to its mount
// path on the current client host.
type SharedArtifactRoot struct {
	Version int64  `yaml:"version" json:"version"`
	Path    string `yaml:"path" json:"path"`
}

// JobSpec is a reusable, direct-execution job specification.
type JobSpec struct {
	Command          []string           `yaml:"command" json:"command"`
	Name             string             `yaml:"name,omitempty" json:"name,omitempty"`
	Tags             []string           `yaml:"tags" json:"tags"`
	Groups           []string           `yaml:"groups" json:"groups"`
	WorkingDirectory string             `yaml:"working_directory,omitempty" json:"working_directory,omitempty"`
	Environment      Environment        `yaml:"environment" json:"environment"`
	Stdin            string             `yaml:"stdin,omitempty" json:"stdin,omitempty"`
	Stop             StopPolicy         `yaml:"stop" json:"stop"`
	Dependencies     []Dependency       `yaml:"dependencies" json:"dependencies"`
	Wait             WaitPolicy         `yaml:"wait" json:"wait"`
	Admission        Admission          `yaml:"admission" json:"admission"`
	Completion       CompletionPolicy   `yaml:"completion" json:"completion"`
	Delay            DelayPolicy        `yaml:"delay" json:"delay"`
	Timeouts         TimeoutPolicy      `yaml:"timeouts" json:"timeouts"`
	Logging          LoggingPolicy      `yaml:"logging" json:"logging"`
	Notification     NotificationPolicy `yaml:"notification" json:"notification"`
}

// Dependency gates the first run on terminal outcomes of a submitted job selector.
type Dependency struct {
	Job      string   `yaml:"job" json:"job"`
	Outcomes []string `yaml:"outcomes" json:"outcomes"`
}

// Environment describes explicit non-secret and secret environment changes.
type Environment struct {
	Set     map[string]string `yaml:"set" json:"set"`
	Unset   []string          `yaml:"unset" json:"unset"`
	Secrets map[string]string `yaml:"secrets" json:"secrets"`
}

// StopPolicy controls graceful and forced target-tree termination.
type StopPolicy struct {
	GracePeriod     Duration `yaml:"grace_period" json:"grace_period"`
	ForceAfterGrace bool     `yaml:"force_after_grace" json:"force_after_grace"`
}

// WaitPolicy selects reusable wait conditions for a job.
type WaitPolicy struct {
	Mode       string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	Conditions []string `yaml:"conditions" json:"conditions"`
	AbortAt    string   `yaml:"abort_at,omitempty" json:"abort_at,omitempty"`
}

// Admission selects the optional concurrency pool and requested slots.
type Admission struct {
	Pool  string `yaml:"pool,omitempty" json:"pool,omitempty"`
	Slots int    `yaml:"slots,omitempty" json:"slots,omitempty"`
}

// CompletionPolicy defines bounded or explicitly unbounded repeated-run behavior.
type CompletionPolicy struct {
	MaxRuns            IntegerLimit `yaml:"max_runs" json:"max_runs"`
	SuccessTarget      IntegerLimit `yaml:"success_target" json:"success_target"`
	MaxFailures        IntegerLimit `yaml:"failure_limit" json:"failure_limit"`
	SuccessExitCodes   []int        `yaml:"success_exit_codes" json:"success_exit_codes"`
	RetryableExitCodes []int        `yaml:"retryable_exit_codes" json:"retryable_exit_codes"`
	RetryTimeouts      bool         `yaml:"retry_timeouts" json:"retry_timeouts"`
	RetryStartFailures bool         `yaml:"retry_start_failures" json:"retry_start_failures"`
}

// DelayPolicy controls delay between repeated runs.
type DelayPolicy struct {
	Strategy string        `yaml:"strategy,omitempty" json:"strategy,omitempty"`
	Initial  Duration      `yaml:"initial" json:"initial"`
	MaxDelay DurationLimit `yaml:"max_delay" json:"max_delay"`
	Base     float64       `yaml:"base,omitempty" json:"base,omitempty"`
	Jitter   Duration      `yaml:"jitter" json:"jitter"`
}

// TimeoutPolicy defines per-run and whole-job deadlines.
type TimeoutPolicy struct {
	Run DurationLimit `yaml:"run" json:"run"`
	Job DurationLimit `yaml:"job" json:"job"`
}

// LoggingPolicy controls stream capture and per-job rotation overrides.
type LoggingPolicy struct {
	Capture            string        `yaml:"capture,omitempty" json:"capture,omitempty"`
	SegmentBytes       ByteLimit     `yaml:"segment_bytes" json:"segment_bytes"`
	SegmentsPerRun     IntegerLimit  `yaml:"segments_per_run" json:"segments_per_run"`
	CompletedLogMaxAge DurationLimit `yaml:"completed_log_max_age" json:"completed_log_max_age"`
}

// NotificationPolicy selects notifier names and subscribed events.
type NotificationPolicy struct {
	Notifiers []string `yaml:"notifiers" json:"notifiers"`
	Events    []string `yaml:"events" json:"events"`
}

// WaitCondition is one reusable prerequisite predicate.
type WaitCondition struct {
	Type       string          `yaml:"type" json:"type"`
	Until      string          `yaml:"until,omitempty" json:"until,omitempty"`
	Delay      Duration        `yaml:"delay,omitempty" json:"delay,omitempty"`
	FileExists *FileCondition  `yaml:"file_exists,omitempty" json:"file_exists,omitempty"`
	Probe      *ProbeCondition `yaml:"probe,omitempty" json:"probe,omitempty"`
}

// FileCondition waits for a path and, optionally, a particular file type.
type FileCondition struct {
	Path string `yaml:"path" json:"path"`
	Type string `yaml:"type,omitempty" json:"type,omitempty"`
}

// ProbeCondition waits for a direct executable to return success.
type ProbeCondition struct {
	Command          []string    `yaml:"command" json:"command"`
	WorkingDirectory string      `yaml:"working_directory,omitempty" json:"working_directory,omitempty"`
	Environment      Environment `yaml:"environment" json:"environment"`
	Timeout          Duration    `yaml:"timeout" json:"timeout"`
	PollInterval     Duration    `yaml:"poll_interval" json:"poll_interval"`
	OutputLimit      ByteLimit   `yaml:"output_limit" json:"output_limit"`
	FatalOnError     bool        `yaml:"fatal_on_error" json:"fatal_on_error"`
}

// Concurrency configures store-wide and named-pool slot capacities.
type Concurrency struct {
	MaxActiveSlots SlotLimit            `yaml:"max_active_slots" json:"max_active_slots"`
	Pools          map[string]SlotLimit `yaml:"pools" json:"pools"`
}

// Retention configures cleanup limits for completed metadata and logs.
type Retention struct {
	CompletedMetadataMaxAge DurationLimit `yaml:"completed_metadata_max_age" json:"completed_metadata_max_age"`
	CompletedLogMaxAge      DurationLimit `yaml:"completed_log_max_age" json:"completed_log_max_age"`
	MaxJobs                 IntegerLimit  `yaml:"max_jobs" json:"max_jobs"`
	MaxRunsPerJob           IntegerLimit  `yaml:"max_runs_per_job" json:"max_runs_per_job"`
	MaxLogBytesPerJob       ByteLimit     `yaml:"max_log_bytes_per_job" json:"max_log_bytes_per_job"`
	MaxTotalLogBytes        ByteLimit     `yaml:"max_total_log_bytes" json:"max_total_log_bytes"`
}

// Notifier is one reusable notification delivery definition.
type Notifier struct {
	Type    string           `yaml:"type" json:"type"`
	Events  []string         `yaml:"events" json:"events"`
	Timeout Duration         `yaml:"timeout" json:"timeout"`
	Retry   NotifierRetry    `yaml:"retry" json:"retry"`
	Command *CommandNotifier `yaml:"command,omitempty" json:"command,omitempty"`
	HTTP    *HTTPNotifier    `yaml:"http,omitempty" json:"http,omitempty"`
	SMTP    *SMTPNotifier    `yaml:"smtp,omitempty" json:"smtp,omitempty"`
}

// NotifierRetry configures bounded at-least-once delivery attempts.
type NotifierRetry struct {
	MaxAttempts int      `yaml:"max_attempts" json:"max_attempts"`
	Delay       Duration `yaml:"delay" json:"delay"`
	MaxDelay    Duration `yaml:"max_delay" json:"max_delay"`
}

// CommandNotifier executes a direct command with versioned JSON on stdin.
type CommandNotifier struct {
	Command     []string    `yaml:"command" json:"command"`
	Environment Environment `yaml:"environment" json:"environment"`
	OutputLimit ByteLimit   `yaml:"output_limit" json:"output_limit"`
}

// HTTPNotifier delivers event JSON to an HTTP endpoint.
type HTTPNotifier struct {
	URL               string            `yaml:"url" json:"url"`
	Headers           map[string]string `yaml:"headers" json:"headers"`
	SecretHeaders     map[string]string `yaml:"secret_headers" json:"secret_headers"`
	SigningSecret     string            `yaml:"signing_secret,omitempty" json:"signing_secret,omitempty"`
	AllowHTTP         bool              `yaml:"allow_http" json:"allow_http"`
	AllowPrivateHosts bool              `yaml:"allow_private_hosts" json:"allow_private_hosts"`
	FollowRedirects   bool              `yaml:"follow_redirects" json:"follow_redirects"`
}

// EffectiveURL returns the notifier URL with the default HTTPS scheme applied.
func (notifier HTTPNotifier) EffectiveURL() string {
	if strings.Contains(notifier.URL, "://") {
		return notifier.URL
	}

	return "https://" + notifier.URL
}

// SMTPNotifier delivers an email using a secret-referenced credential.
type SMTPNotifier struct {
	Address        string   `yaml:"address" json:"address"`
	TLS            string   `yaml:"tls" json:"tls"`
	Username       string   `yaml:"username,omitempty" json:"username,omitempty"`
	PasswordSecret string   `yaml:"password_secret,omitempty" json:"password_secret,omitempty"`
	From           string   `yaml:"from" json:"from"`
	To             []string `yaml:"to" json:"to"`
	SubjectPrefix  string   `yaml:"subject_prefix,omitempty" json:"subject_prefix,omitempty"`
}

// Profile is an explicitly selected bundle based on an optional named job spec.
type Profile struct {
	JobSpec   string          `yaml:"job_spec,omitempty" json:"job_spec,omitempty"`
	Overrides JobSpecOverride `yaml:"overrides" json:"overrides"`
}

// JobSpecOverride preserves field presence while applying a selected profile.
type JobSpecOverride struct {
	Command          []string            `yaml:"command,omitempty" json:"command,omitempty"`
	Name             *string             `yaml:"name,omitempty" json:"name,omitempty"`
	Tags             []string            `yaml:"tags,omitempty" json:"tags,omitempty"`
	Groups           []string            `yaml:"groups,omitempty" json:"groups,omitempty"`
	WorkingDirectory *string             `yaml:"working_directory,omitempty" json:"working_directory,omitempty"`
	Environment      *Environment        `yaml:"environment,omitempty" json:"environment,omitempty"`
	Stdin            *string             `yaml:"stdin,omitempty" json:"stdin,omitempty"`
	Stop             *StopPolicy         `yaml:"stop,omitempty" json:"stop,omitempty"`
	Dependencies     []Dependency        `yaml:"dependencies,omitempty" json:"dependencies,omitempty"`
	Wait             *WaitPolicy         `yaml:"wait,omitempty" json:"wait,omitempty"`
	Admission        *Admission          `yaml:"admission,omitempty" json:"admission,omitempty"`
	Completion       *CompletionPolicy   `yaml:"completion,omitempty" json:"completion,omitempty"`
	Delay            *DelayPolicy        `yaml:"delay,omitempty" json:"delay,omitempty"`
	Timeouts         *TimeoutPolicy      `yaml:"timeouts,omitempty" json:"timeouts,omitempty"`
	Logging          *LoggingPolicy      `yaml:"logging,omitempty" json:"logging,omitempty"`
	Notification     *NotificationPolicy `yaml:"notification,omitempty" json:"notification,omitempty"`
}

// RedactionConfig lists sensitive field names and bounded RE2 patterns.
type RedactionConfig struct {
	Names    []string `yaml:"names" json:"names"`
	Patterns []string `yaml:"patterns" json:"patterns"`
}

// Duration is a present or absent nonnegative duration supporting d and w units.
type Duration struct {
	value time.Duration
	set   bool
}

// NewDuration constructs a configured duration.
func NewDuration(value time.Duration) (Duration, error) {
	if value < 0 {
		return Duration{}, errors.New("duration must not be negative")
	}

	return Duration{value: value, set: true}, nil
}

// Value returns the parsed duration and whether it was explicitly configured.
func (duration Duration) Value() (time.Duration, bool) {
	return duration.value, duration.set
}

// IsSet reports whether a duration was explicitly configured or defaulted.
func (duration Duration) IsSet() bool { return duration.set }

// IsZero reports whether a duration is absent for YAML omission.
func (duration Duration) IsZero() bool { return !duration.set }

// String returns the canonical duration or an empty string when absent.
func (duration Duration) String() string {
	if !duration.set {
		return ""
	}

	return duration.value.String()
}

// IntegerLimit is a nonnegative integer or the explicit unlimited value.
type IntegerLimit struct {
	value     uint64
	unlimited bool
	set       bool
}

// NewIntegerLimit constructs a finite nonnegative integer limit.
func NewIntegerLimit(value uint64) IntegerLimit { return IntegerLimit{value: value, set: true} }

// UnlimitedIntegerLimit constructs an explicitly unbounded integer limit.
func UnlimitedIntegerLimit() IntegerLimit { return IntegerLimit{unlimited: true, set: true} }

// Value returns the finite value and whether this limit is finite.
func (limit IntegerLimit) Value() (uint64, bool) { return limit.value, limit.set && !limit.unlimited }

// IsUnlimited reports whether the explicit unlimited value is configured.
func (limit IntegerLimit) IsUnlimited() bool { return limit.set && limit.unlimited }

// IsSet reports whether an integer limit was explicitly configured or defaulted.
func (limit IntegerLimit) IsSet() bool { return limit.set }

// IsZero reports whether an integer limit is absent for YAML omission.
func (limit IntegerLimit) IsZero() bool { return !limit.set }

// SlotLimit is a positive integer capacity or the explicit unlimited value.
type SlotLimit struct {
	value     uint32
	unlimited bool
	set       bool
}

// NewSlotLimit constructs a positive finite slot limit.
func NewSlotLimit(value uint32) (SlotLimit, error) {
	if value == 0 {
		return SlotLimit{}, errors.New("slot limit must be positive")
	}

	return SlotLimit{value: value, set: true}, nil
}

// UnlimitedSlotLimit constructs an explicitly unbounded slot limit.
func UnlimitedSlotLimit() SlotLimit { return SlotLimit{unlimited: true, set: true} }

// Value returns the finite capacity and whether this limit is finite.
func (limit SlotLimit) Value() (uint32, bool) { return limit.value, limit.set && !limit.unlimited }

// IsUnlimited reports whether the explicit unlimited value is configured.
func (limit SlotLimit) IsUnlimited() bool { return limit.set && limit.unlimited }

// IsSet reports whether a slot limit was explicitly configured or defaulted.
func (limit SlotLimit) IsSet() bool { return limit.set }

// IsZero reports whether a slot limit is absent for YAML omission.
func (limit SlotLimit) IsZero() bool { return !limit.set }

// DurationLimit is a nonnegative duration or the explicit unlimited value.
type DurationLimit struct {
	value     time.Duration
	unlimited bool
	set       bool
}

// NewDurationLimit constructs a finite nonnegative duration limit.
func NewDurationLimit(value time.Duration) (DurationLimit, error) {
	if value < 0 {
		return DurationLimit{}, errors.New("duration limit must not be negative")
	}

	return DurationLimit{value: value, set: true}, nil
}

// UnlimitedDurationLimit constructs an explicitly unbounded duration limit.
func UnlimitedDurationLimit() DurationLimit { return DurationLimit{unlimited: true, set: true} }

// Value returns the finite duration and whether this limit is finite.
func (limit DurationLimit) Value() (time.Duration, bool) {
	return limit.value, limit.set && !limit.unlimited
}

// IsUnlimited reports whether the explicit unlimited value is configured.
func (limit DurationLimit) IsUnlimited() bool { return limit.set && limit.unlimited }

// IsSet reports whether a duration limit was explicitly configured or defaulted.
func (limit DurationLimit) IsSet() bool { return limit.set }

// IsZero reports whether a duration limit is absent for YAML omission.
func (limit DurationLimit) IsZero() bool { return !limit.set }

// ByteLimit is a nonnegative byte count or the explicit unlimited value.
type ByteLimit struct {
	value     uint64
	unlimited bool
	set       bool
}

// NewByteLimit constructs a finite nonnegative byte limit.
func NewByteLimit(value uint64) ByteLimit { return ByteLimit{value: value, set: true} }

// UnlimitedByteLimit constructs an explicitly unbounded byte limit.
func UnlimitedByteLimit() ByteLimit { return ByteLimit{unlimited: true, set: true} }

// Value returns the finite byte count and whether this limit is finite.
func (limit ByteLimit) Value() (uint64, bool) { return limit.value, limit.set && !limit.unlimited }

// IsUnlimited reports whether the explicit unlimited value is configured.
func (limit ByteLimit) IsUnlimited() bool { return limit.set && limit.unlimited }

// IsSet reports whether a byte limit was explicitly configured or defaulted.
func (limit ByteLimit) IsSet() bool { return limit.set }

// IsZero reports whether a byte limit is absent for YAML omission.
func (limit ByteLimit) IsZero() bool { return !limit.set }

// SecretRef identifies a re-resolvable secret without containing its value.
type SecretRef struct {
	provider string
	locator  string
}

// ParseSecretRef validates a stable provider:locator secret reference.
func ParseSecretRef(value string) (SecretRef, error) { return parseSecretReference(value) }

// Provider returns the credential-provider name.
func (reference SecretRef) Provider() string { return reference.provider }

// Locator returns the provider-specific non-secret locator.
func (reference SecretRef) Locator() string { return reference.locator }

// String returns the stable provider:locator form.
func (reference SecretRef) String() string {
	if reference.provider == "" {
		return ""
	}

	return reference.provider + ":" + reference.locator
}

// IsZero reports whether a secret reference is absent for YAML omission.
func (reference SecretRef) IsZero() bool { return reference.provider == "" }

// Redactor applies field-aware and value-aware redaction without persisting secrets.
type Redactor struct {
	names    map[string]struct{}
	values   []string
	patterns []*regexp.Regexp
}

// ProtectsValues reports whether the redactor has a configured pattern or
// resolved secret value that can match content independently of a field name.
func (redactor *Redactor) ProtectsValues() bool {
	return redactor != nil && (len(redactor.values) != 0 || len(redactor.patterns) != 0)
}

// RedactField fully redacts sensitive fields and otherwise applies value redaction.
func (redactor *Redactor) RedactField(name, value string) string {
	if redactor == nil {
		return value
	}
	if redactor.sensitiveName(name) {
		return Redacted
	}

	return redactor.RedactString(value)
}

// RedactString replaces resolved secrets and configured RE2 matches.
func (redactor *Redactor) RedactString(value string) string {
	if redactor == nil {
		return value
	}
	for _, secret := range redactor.values {
		value = strings.ReplaceAll(value, secret, Redacted)
	}
	for _, pattern := range redactor.patterns {
		value = pattern.ReplaceAllString(value, Redacted)
	}

	return value
}

func (redactor *Redactor) sensitiveName(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if _, ok := redactor.names[normalized]; ok {
		return true
	}

	for _, fragment := range []string{
		"password", "passwd", "secret", "token", "credential", "private_key", "private-key",
		"api_key", "api-key", "authorization", "cookie",
	} {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}

	return false
}

func validateHTTPURL(notifier HTTPNotifier) error {
	parsed, err := url.Parse(notifier.EffectiveURL())
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return errors.New("URL must have a host and must not contain user information or a fragment")
	}
	if parsed.Scheme != httpsScheme && (parsed.Scheme != httpScheme || !notifier.AllowHTTP) {
		return errors.New("URL must use HTTPS unless allow_http is true")
	}
	if !notifier.AllowPrivateHosts && privateHTTPHost(parsed.Hostname()) {
		return errors.New("URL host is local or private; allow_private_hosts is required")
	}

	return nil
}

func sortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}
