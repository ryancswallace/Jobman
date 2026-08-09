package diagnostic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maximumCommandArguments     = 1024
	maximumCommandBytes         = 128 * 1024
	maximumEnvironmentNames     = 2048
	maximumEnvironmentNameBytes = 1024
	maximumExecutionPolicyBytes = 128 * 1024
)

// Command is one direct, shell-free executable and its ordered argument
// vector. Argument boundaries are preserved exactly; environment is described
// separately and never contains values.
type Command struct {
	Executable string   `json:"executable"`
	Arguments  []string `json:"arguments"`
}

// Validate checks the bounded public command representation.
func (command Command) Validate() error {
	if command.Executable == "" || len(command.Arguments) > maximumCommandArguments ||
		!validContextText(command.Executable, maximumCommandBytes) {
		return errors.New("validate diagnostic command: invalid executable or argument count")
	}
	total := len(command.Executable)
	for _, argument := range command.Arguments {
		if !validContextText(argument, maximumCommandBytes) || total > maximumCommandBytes-len(argument) {
			return errors.New("validate diagnostic command: invalid or oversized argument vector")
		}
		total += len(argument)
	}

	return nil
}

// EnvironmentNames identifies only variable names and their roles. Secret is
// the destination variable name backed by a secret reference; neither the
// reference identifier nor any environment value is included.
type EnvironmentNames struct {
	Inheritance string   `json:"inheritance,omitempty"`
	Set         []string `json:"set"`
	Unset       []string `json:"unset"`
	Secret      []string `json:"secret"`
}

// Validate checks the bounded name-only environment representation. Names may
// contain configured redaction markers, so validation deliberately does not
// require shell identifier syntax after sanitization.
func (environment EnvironmentNames) Validate() error {
	if !validOptionalContextText(environment.Inheritance, maximumEnvironmentNameBytes) {
		return errors.New("validate diagnostic environment names: invalid inheritance policy")
	}
	count := len(environment.Set) + len(environment.Unset) + len(environment.Secret)
	if count > maximumEnvironmentNames {
		return errors.New("validate diagnostic environment names: too many names")
	}
	for _, names := range [][]string{environment.Set, environment.Unset, environment.Secret} {
		for _, name := range names {
			if name == "" || !validContextText(name, maximumEnvironmentNameBytes) {
				return errors.New("validate diagnostic environment names: invalid name")
			}
		}
	}

	return nil
}

// ExecutionPolicy is Jobman's immutable, non-secret execution-policy summary.
// It intentionally excludes environment values, secret-reference identifiers,
// notifier destinations, command bodies, and filesystem paths.
type ExecutionPolicy struct {
	StdinPolicy                   string               `json:"stdin_policy"`
	Foreground                    bool                 `json:"foreground"`
	StopGracePeriod               string               `json:"stop_grace_period"`
	ForceAfterGrace               bool                 `json:"force_after_grace"`
	RunTimeout                    string               `json:"run_timeout"`
	JobTimeout                    string               `json:"job_timeout"`
	Completion                    CompletionPolicy     `json:"completion"`
	Classification                ClassificationPolicy `json:"classification"`
	FailureDelay                  DelayPolicy          `json:"failure_delay"`
	SuccessDelay                  DelayPolicy          `json:"success_delay"`
	WaitMode                      string               `json:"wait_mode"`
	WaitConditionKinds            []string             `json:"wait_condition_kinds"`
	DependencyCount               uint64               `json:"dependency_count"`
	ConcurrencyPool               string               `json:"concurrency_pool,omitempty"`
	ConcurrencySlots              uint64               `json:"concurrency_slots"`
	NotificationSubscriptionCount uint64               `json:"notification_subscription_count"`
	NotifierKinds                 []string             `json:"notifier_kinds"`
	Tags                          []string             `json:"tags"`
	Groups                        []string             `json:"groups"`
	LogCapture                    string               `json:"log_capture"`
	LogRotateBytes                int64                `json:"log_rotate_bytes"`
	LogMaximumSegmentsPerStream   int                  `json:"log_maximum_segments_per_stream"`
	LogRetention                  string               `json:"log_retention"`
}

// CompletionPolicy summarizes the finite or unlimited run-completion bounds.
type CompletionPolicy struct {
	MaximumRuns   CountLimit `json:"maximum_runs"`
	SuccessTarget CountLimit `json:"success_target"`
	FailureLimit  CountLimit `json:"failure_limit"`
	RetryAbortAt  *time.Time `json:"retry_abort_at,omitempty"`
}

// CountLimit is either a positive finite count or an explicit unlimited value.
type CountLimit struct {
	Value     uint64 `json:"value,omitempty"`
	Unlimited bool   `json:"unlimited"`
}

// ClassificationPolicy summarizes how factual process results are classified
// for success and retry.
type ClassificationPolicy struct {
	SuccessExitCodes                  []int           `json:"success_exit_codes"`
	RetryableExitCodes                []ExitCodeRange `json:"retryable_exit_codes"`
	UnlistedNonzeroExitCodesRetryable bool            `json:"unlisted_nonzero_exit_codes_retryable"`
	RetryableSignals                  []string        `json:"retryable_signals"`
	RetryablePlatformReasons          []string        `json:"retryable_platform_reasons"`
	RetryTimeout                      bool            `json:"retry_timeout"`
	RetryStartFailure                 bool            `json:"retry_start_failure"`
	RetryCancellation                 bool            `json:"retry_cancellation"`
}

// ExitCodeRange is one inclusive retryable process-exit range.
type ExitCodeRange struct {
	First int `json:"first"`
	Last  int `json:"last"`
}

// DelayPolicy summarizes one retry or successful-repetition delay algorithm.
type DelayPolicy struct {
	Base            string `json:"base"`
	Backoff         string `json:"backoff"`
	ExponentialBase uint64 `json:"exponential_base,omitempty"`
	Maximum         string `json:"maximum,omitempty"`
	Jitter          string `json:"jitter"`
}

// Validate checks that an execution-policy item remains a bounded JSON value.
// The internal model already validated policy semantics before persistence;
// this public boundary additionally prevents malformed imported evidence from
// smuggling unbounded or invalid text.
func (policy ExecutionPolicy) Validate() error {
	encoded, err := json.Marshal(policy)
	if err != nil || len(encoded) > maximumExecutionPolicyBytes || !utf8.Valid(encoded) ||
		policy.StdinPolicy == "" || policy.StopGracePeriod == "" || policy.RunTimeout == "" ||
		policy.JobTimeout == "" || policy.WaitMode == "" || policy.ConcurrencySlots == 0 ||
		policy.LogCapture == "" || policy.LogRetention == "" {
		return errors.New("validate diagnostic execution policy: invalid or oversized summary")
	}
	if !validExecutionPolicyText(policy) {
		return errors.New("validate diagnostic execution policy: summary contains invalid text")
	}

	return nil
}

func validExecutionPolicyText(policy ExecutionPolicy) bool {
	values := make([]string, 0, 16+
		len(policy.Classification.RetryableSignals)+
		len(policy.Classification.RetryablePlatformReasons)+
		len(policy.WaitConditionKinds)+
		len(policy.NotifierKinds)+
		len(policy.Tags)+
		len(policy.Groups))
	values = append(values,
		policy.StdinPolicy,
		policy.StopGracePeriod,
		policy.RunTimeout,
		policy.JobTimeout,
		policy.FailureDelay.Base,
		policy.FailureDelay.Backoff,
		policy.FailureDelay.Maximum,
		policy.FailureDelay.Jitter,
		policy.SuccessDelay.Base,
		policy.SuccessDelay.Backoff,
		policy.SuccessDelay.Maximum,
		policy.SuccessDelay.Jitter,
		policy.WaitMode,
		policy.ConcurrencyPool,
		policy.LogCapture,
		policy.LogRetention,
	)
	values = append(values, policy.Classification.RetryableSignals...)
	values = append(values, policy.Classification.RetryablePlatformReasons...)
	values = append(values, policy.WaitConditionKinds...)
	values = append(values, policy.NotifierKinds...)
	values = append(values, policy.Tags...)
	values = append(values, policy.Groups...)
	for _, value := range values {
		if !validOptionalContextText(value, maximumExecutionPolicyBytes) {
			return false
		}
	}

	return true
}

func validContextText(value string, maximum int) bool {
	return len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func validOptionalContextText(value string, maximum int) bool {
	return value == "" || validContextText(value, maximum)
}

// ResourceObservation is one portable, integer-valued operating-system fact.
// Scope prevents a process-only measurement from being presented as a process
// tree total.
type ResourceObservation struct {
	Metric       string `json:"metric"`
	Value        uint64 `json:"value"`
	Unit         string `json:"unit"`
	Scope        string `json:"scope"`
	Source       string `json:"source"`
	Completeness string `json:"completeness"`
}

// Supported resource metric codes.
const (
	ResourceCPUUserTime   = "cpu_user_time"
	ResourceCPUSystemTime = "cpu_system_time"
	ResourcePeakRSS       = "peak_resident_memory"
)

// Supported resource units.
const (
	ResourceUnitNanoseconds = "nanoseconds"
	ResourceUnitBytes       = "bytes"
)

// Supported resource scopes.
const (
	ResourceScopeProcess = "process"
	ResourceScopeTree    = "tree"
)

// Supported resource observation sources.
const (
	ResourceSourceProcessState = "process_state"
	ResourceSourceWaitRusage   = "wait_rusage"
)

// Supported resource completeness values.
const (
	ResourceCompleteAtExit = "complete_at_exit"
	ResourcePartial        = "partial"
)

// Validate checks the controlled shape and metric/unit relationship.
func (observation ResourceObservation) Validate() error {
	if observation.Scope != ResourceScopeProcess && observation.Scope != ResourceScopeTree {
		return errors.New("validate resource observation: invalid scope")
	}
	if observation.Source != ResourceSourceProcessState && observation.Source != ResourceSourceWaitRusage {
		return errors.New("validate resource observation: invalid source")
	}
	if observation.Completeness != ResourceCompleteAtExit && observation.Completeness != ResourcePartial {
		return errors.New("validate resource observation: invalid completeness")
	}
	switch observation.Metric {
	case ResourceCPUUserTime, ResourceCPUSystemTime:
		if observation.Unit != ResourceUnitNanoseconds {
			return errors.New("validate resource observation: CPU time must use nanoseconds")
		}
	case ResourcePeakRSS:
		if observation.Unit != ResourceUnitBytes {
			return errors.New("validate resource observation: resident memory must use bytes")
		}
	default:
		return fmt.Errorf("validate resource observation: unsupported metric %q", observation.Metric)
	}

	return nil
}

// FailureFingerprint is an opaque, store-local grouping key over safe factual
// inputs. Value is not portable between state stores.
type FailureFingerprint struct {
	Algorithm          string `json:"algorithm"`
	InputSchemaVersion int    `json:"input_schema_version"`
	Value              string `json:"value"`
	Scope              string `json:"scope"`
}

// Failure fingerprint constants.
const (
	FingerprintAlgorithmHMACSHA256 = "hmac-sha256"
	FingerprintInputSchemaVersion  = 1
	FingerprintScopeStoreLocal     = "store_local"
)

// Validate checks a public failure fingerprint without exposing its key or
// original inputs.
func (fingerprint FailureFingerprint) Validate() error {
	if fingerprint.Algorithm != FingerprintAlgorithmHMACSHA256 ||
		fingerprint.InputSchemaVersion != FingerprintInputSchemaVersion ||
		fingerprint.Scope != FingerprintScopeStoreLocal ||
		len(fingerprint.Value) != 64 {
		return errors.New("validate failure fingerprint: invalid metadata")
	}
	for _, character := range fingerprint.Value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return errors.New("validate failure fingerprint: value is not lowercase hexadecimal")
		}
	}

	return nil
}

// SimilarFailure is a deliberately minimal summary of one indexed matching
// run. It excludes job names, specifications, paths, environment, logs, and
// notifier data.
type SimilarFailure struct {
	JobID          string             `json:"job_id"`
	RunID          string             `json:"run_id"`
	RunNumber      uint64             `json:"run_number"`
	CompletedAt    time.Time          `json:"completed_at"`
	Outcome        string             `json:"outcome"`
	FailureClass   string             `json:"failure_class"`
	Fingerprint    FailureFingerprint `json:"fingerprint"`
	LaterSucceeded bool               `json:"later_succeeded"`
}

// Validate checks the bounded public similarity summary.
func (failure SimilarFailure) Validate() error {
	if !validIdentifier(failure.JobID) || !validIdentifier(failure.RunID) || failure.RunNumber == 0 ||
		failure.CompletedAt.IsZero() || failure.CompletedAt.Location() != time.UTC ||
		!validSimilarOutcome(failure.Outcome) || !validFailureClass(failure.FailureClass) {
		return errors.New("validate similar failure: incomplete summary")
	}

	return failure.Fingerprint.Validate()
}

func validSimilarOutcome(value string) bool {
	switch value {
	case "failure", "timed_out", "cancelled", "start_failed", "lost": //nolint:misspell // Stable Jobman outcome spelling.
		return true
	default:
		return false
	}
}

func validFailureClass(value string) bool {
	if value == "" || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' {
			continue
		}

		return false
	}

	return true
}
