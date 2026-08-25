package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/mail"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	defaultStopGrace       = 10 * time.Second
	defaultNotifierTimeout = 10 * time.Second
	defaultProbeTimeout    = 30 * time.Second
	defaultPollInterval    = time.Second
	defaultOutputLimit     = 64 * 1024
	maxConfiguredName      = 128
	maxRedactionPatterns   = 64
	maxRedactionPattern    = 1024
	maxConfiguredSecrets   = 256
)

var (
	configuredNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	sharedNamePattern     = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$`)
	artifactStorePattern  = regexp.MustCompile(`^[a-z](?:[a-z0-9._-]{0,62}[a-z0-9])?$`)
)

var knownNotificationEvents = map[string]struct{}{
	"job_started":           {},
	"run_started":           {},
	"run_succeeded":         {},
	"run_failed":            {},
	"run_timed_out":         {},
	"run_cancelled":         {}, //nolint:misspell // This event spelling is fixed by the specification.
	"run_lost":              {},
	"retry_scheduled":       {},
	"job_succeeded":         {},
	"job_failed":            {},
	"job_timed_out":         {},
	"job_cancelled":         {}, //nolint:misspell // This event spelling is fixed by the specification.
	"job_aborted":           {},
	"job_lost":              {},
	"job_submission_failed": {},
}

// Validate checks the complete effective configuration and all named references.
func (configuration Config) Validate() error {
	if configuration.SchemaVersion != SchemaVersion {
		return fmt.Errorf("configuration schema_version must be %d", SchemaVersion)
	}
	if err := validateTrustedRoots(configuration.TrustedProjectRoots); err != nil {
		return err
	}
	if !configuration.Concurrency.MaxActiveSlots.set {
		return errors.New("concurrency.max_active_slots must be configured")
	}
	if err := validateNamedLimits(configuration.Concurrency.Pools); err != nil {
		return err
	}
	if err := validateRetention(configuration.Retention); err != nil {
		return err
	}
	if err := validateNamedSecrets(configuration.Secrets); err != nil {
		return err
	}
	if err := validateWaitConditions(configuration); err != nil {
		return err
	}
	if err := validateNotifiers(configuration); err != nil {
		return err
	}
	if err := validateJobSpecs(configuration); err != nil {
		return err
	}
	if err := validateProfiles(configuration); err != nil {
		return err
	}
	if err := validateShared(configuration.Shared); err != nil {
		return err
	}
	if _, err := NewRedactor(configuration.Redaction, nil); err != nil {
		return fmt.Errorf("redaction: %w", err)
	}

	return nil
}

// NewRedactor validates redaction policy and incorporates provided resolved values.
func NewRedactor(configuration RedactionConfig, resolvedSecrets map[string]string) (*Redactor, error) {
	if len(configuration.Patterns) > maxRedactionPatterns {
		return nil, fmt.Errorf("at most %d redaction patterns are allowed", maxRedactionPatterns)
	}

	redactor := &Redactor{names: make(map[string]struct{}, len(configuration.Names))}
	if len(resolvedSecrets) > maxConfiguredSecrets {
		return nil, fmt.Errorf("at most %d resolved secrets are allowed", maxConfiguredSecrets)
	}
	for _, name := range configuration.Names {
		normalized := strings.ToLower(strings.TrimSpace(name))
		if normalized == "" {
			return nil, errors.New("redaction field names must not be empty")
		}
		redactor.names[normalized] = struct{}{}
	}
	for _, pattern := range configuration.Patterns {
		if pattern == "" || len(pattern) > maxRedactionPattern {
			return nil, fmt.Errorf("redaction patterns must contain between 1 and %d bytes", maxRedactionPattern)
		}
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("compile redaction pattern: %w", err)
		}
		redactor.patterns = append(redactor.patterns, compiled)
	}
	for _, value := range resolvedSecrets {
		if len(value) > maxSecretBytes {
			return nil, fmt.Errorf("resolved secret exceeds %d bytes", maxSecretBytes)
		}
		if value != "" {
			redactor.values = append(redactor.values, value)
		}
	}
	sort.Slice(redactor.values, func(first, second int) bool {
		return len(redactor.values[first]) > len(redactor.values[second])
	})

	return redactor, nil
}

func normalize(configuration *Config) {
	ensureTopLevelMaps(configuration)
	for name, specification := range configuration.JobSpecs {
		normalizeJobSpec(&specification)
		configuration.JobSpecs[name] = specification
	}
	for name, condition := range configuration.WaitConditions {
		normalizeWaitCondition(&condition)
		configuration.WaitConditions[name] = condition
	}
	for name, notifier := range configuration.Notifiers {
		normalizeNotifier(&notifier)
		configuration.Notifiers[name] = notifier
	}
}

func ensureTopLevelMaps(configuration *Config) {
	if configuration.TrustedProjectRoots == nil {
		configuration.TrustedProjectRoots = []string{}
	}
	if configuration.JobSpecs == nil {
		configuration.JobSpecs = map[string]JobSpec{}
	}
	if configuration.WaitConditions == nil {
		configuration.WaitConditions = map[string]WaitCondition{}
	}
	if configuration.Secrets == nil {
		configuration.Secrets = map[string]SecretRef{}
	}
	if configuration.Concurrency.Pools == nil {
		configuration.Concurrency.Pools = map[string]SlotLimit{}
	}
	if configuration.Notifiers == nil {
		configuration.Notifiers = map[string]Notifier{}
	}
	if configuration.Profiles == nil {
		configuration.Profiles = map[string]Profile{}
	}
	if configuration.Shared.Profiles == nil {
		configuration.Shared.Profiles = map[string]SharedProfile{}
	}
	for name, profile := range configuration.Shared.Profiles {
		if profile.ArtifactRoots == nil {
			profile.ArtifactRoots = map[string]SharedArtifactRoot{}
			configuration.Shared.Profiles[name] = profile
		}
	}
	if configuration.Redaction.Names == nil {
		configuration.Redaction.Names = []string{}
	}
	if configuration.Redaction.Patterns == nil {
		configuration.Redaction.Patterns = []string{}
	}
}

//nolint:cyclop,gocognit // Defaults are intentionally explicit for every independent policy group.
func normalizeJobSpec(specification *JobSpec) {
	if specification.Tags == nil {
		specification.Tags = []string{}
	}
	if specification.Groups == nil {
		specification.Groups = []string{}
	}
	normalizeEnvironment(&specification.Environment)
	if specification.Stdin == "" {
		specification.Stdin = stdinNull
	}
	if !specification.Stop.GracePeriod.set {
		specification.Stop.GracePeriod = durationMust(defaultStopGrace)
		specification.Stop.ForceAfterGrace = true
	}
	if specification.Wait.Mode == "" {
		specification.Wait.Mode = waitModeAll
	}
	if specification.Wait.Conditions == nil {
		specification.Wait.Conditions = []string{}
	}
	if specification.Dependencies == nil {
		specification.Dependencies = []Dependency{}
	} else {
		specification.Dependencies = normalizeDependencies(specification.Dependencies)
	}
	if specification.Admission.Slots == 0 {
		specification.Admission.Slots = 1
	}
	if !specification.Completion.MaxRuns.set {
		specification.Completion.MaxRuns = integerLimit(1)
	}
	if !specification.Completion.MaxFailures.set {
		specification.Completion.MaxFailures = integerLimit(1)
	}
	if !specification.Completion.SuccessTarget.set {
		specification.Completion.SuccessTarget = integerLimit(1)
	}
	if specification.Completion.SuccessExitCodes == nil {
		specification.Completion.SuccessExitCodes = []int{0}
	}
	if specification.Completion.RetryableExitCodes == nil {
		specification.Completion.RetryableExitCodes = []int{}
	}
	if specification.Delay.Strategy == "" {
		specification.Delay.Strategy = "constant"
	}
	if !specification.Delay.Initial.set {
		specification.Delay.Initial = durationMust(0)
	}
	if !specification.Delay.MaxDelay.set {
		specification.Delay.MaxDelay = UnlimitedDurationLimit()
	}
	if specification.Delay.Base == 0 {
		specification.Delay.Base = 2
	}
	if !specification.Delay.Jitter.set {
		specification.Delay.Jitter = durationMust(0)
	}
	if !specification.Timeouts.Run.set {
		specification.Timeouts.Run = UnlimitedDurationLimit()
	}
	if !specification.Timeouts.Job.set {
		specification.Timeouts.Job = UnlimitedDurationLimit()
	}
	if specification.Logging.Capture == "" {
		specification.Logging.Capture = "both"
	}
	if !specification.Logging.SegmentBytes.set {
		specification.Logging.SegmentBytes = UnlimitedByteLimit()
	}
	if !specification.Logging.SegmentsPerRun.set {
		specification.Logging.SegmentsPerRun = UnlimitedIntegerLimit()
	}
	if specification.Notification.Notifiers == nil {
		specification.Notification.Notifiers = []string{}
	}
	if specification.Notification.Events == nil {
		specification.Notification.Events = []string{}
	}
}

func normalizeWaitCondition(condition *WaitCondition) {
	if condition.Probe == nil {
		return
	}
	normalizeEnvironment(&condition.Probe.Environment)
	if !condition.Probe.Timeout.set {
		condition.Probe.Timeout = durationMust(defaultProbeTimeout)
	}
	if !condition.Probe.PollInterval.set {
		condition.Probe.PollInterval = durationMust(defaultPollInterval)
	}
	if !condition.Probe.OutputLimit.set {
		condition.Probe.OutputLimit = byteLimit(defaultOutputLimit)
	}
}

func normalizeNotifier(notifier *Notifier) {
	if !notifier.Timeout.set {
		notifier.Timeout = durationMust(defaultNotifierTimeout)
	}
	if notifier.Retry.MaxAttempts == 0 {
		notifier.Retry.MaxAttempts = 3
	}
	if !notifier.Retry.Delay.set {
		notifier.Retry.Delay = durationMust(time.Second)
	}
	if !notifier.Retry.MaxDelay.set {
		notifier.Retry.MaxDelay = durationMust(time.Minute)
	}
	if notifier.Events == nil {
		notifier.Events = []string{}
	}
	if notifier.Command != nil {
		normalizeEnvironment(&notifier.Command.Environment)
		if !notifier.Command.OutputLimit.set {
			notifier.Command.OutputLimit = byteLimit(defaultOutputLimit)
		}
	}
	if notifier.HTTP != nil {
		if notifier.HTTP.Headers == nil {
			notifier.HTTP.Headers = map[string]string{}
		}
		if notifier.HTTP.SecretHeaders == nil {
			notifier.HTTP.SecretHeaders = map[string]string{}
		}
	}
}

func normalizeEnvironment(environment *Environment) {
	if environment.Set == nil {
		environment.Set = map[string]string{}
	}
	if environment.Unset == nil {
		environment.Unset = []string{}
	}
	if environment.Secrets == nil {
		environment.Secrets = map[string]string{}
	}
}

func validateTrustedRoots(roots []string) error {
	seen := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
			return fmt.Errorf("trusted_project_roots entry %q must be a clean absolute path", root)
		}
		if _, duplicate := seen[root]; duplicate {
			return fmt.Errorf("trusted_project_roots contains duplicate %q", root)
		}
		seen[root] = struct{}{}
	}

	return nil
}

func validateNamedLimits(pools map[string]SlotLimit) error {
	for _, name := range sortedMapKeys(pools) {
		if err := validateConfiguredName("concurrency pool", name); err != nil {
			return err
		}
		if !pools[name].set {
			return fmt.Errorf("concurrency pool %q has no capacity", name)
		}
	}

	return nil
}

func validateRetention(retention Retention) error {
	limits := map[string]bool{
		"completed_metadata_max_age": retention.CompletedMetadataMaxAge.set,
		"completed_log_max_age":      retention.CompletedLogMaxAge.set,
		"max_jobs":                   retention.MaxJobs.set,
		"max_runs_per_job":           retention.MaxRunsPerJob.set,
		"max_log_bytes_per_job":      retention.MaxLogBytesPerJob.set,
		"max_total_log_bytes":        retention.MaxTotalLogBytes.set,
	}
	for name, set := range limits {
		if !set {
			return fmt.Errorf("retention.%s must be configured", name)
		}
	}

	return nil
}

func validateNamedSecrets(secrets map[string]SecretRef) error {
	if len(secrets) > maxConfiguredSecrets {
		return fmt.Errorf("at most %d secrets may be configured", maxConfiguredSecrets)
	}
	for _, name := range sortedMapKeys(secrets) {
		if err := validateConfiguredName("secret", name); err != nil {
			return err
		}
		if secrets[name].provider == "" {
			return fmt.Errorf("secret %q has an empty reference", name)
		}
	}

	return nil
}

func validateWaitConditions(configuration Config) error {
	for _, name := range sortedMapKeys(configuration.WaitConditions) {
		if err := validateConfiguredName("wait condition", name); err != nil {
			return err
		}
		if err := validateWaitCondition(configuration.WaitConditions[name], configuration.Secrets); err != nil {
			return fmt.Errorf("wait condition %q: %w", name, err)
		}
	}

	return nil
}

//nolint:cyclop // Each branch validates one mutually exclusive wait-condition schema.
func validateWaitCondition(condition WaitCondition, secrets map[string]SecretRef) error {
	switch condition.Type {
	case "until":
		if condition.Until == "" || condition.Delay.set || condition.FileExists != nil || condition.Probe != nil {
			return errors.New("type until requires only the until field")
		}
		if _, err := time.Parse(time.RFC3339Nano, condition.Until); err != nil {
			return fmt.Errorf("until must be an RFC 3339 timestamp: %w", err)
		}
	case "delay":
		if !condition.Delay.set || condition.Until != "" || condition.FileExists != nil || condition.Probe != nil {
			return errors.New("type delay requires only the delay field")
		}
	case "file-exists":
		if condition.FileExists == nil || condition.Until != "" || condition.Delay.set || condition.Probe != nil {
			return errors.New("type file-exists requires only the file_exists field")
		}
		if err := validateFileCondition(*condition.FileExists); err != nil {
			return err
		}
	case "probe":
		if condition.Probe == nil || condition.Until != "" || condition.Delay.set || condition.FileExists != nil {
			return errors.New("type probe requires only the probe field")
		}
		if err := validateProbe(*condition.Probe, secrets); err != nil {
			return err
		}
	default:
		return errors.New("type must be until, delay, file-exists, or probe")
	}

	return nil
}

func validateFileCondition(condition FileCondition) error {
	if condition.Path == "" || strings.ContainsRune(condition.Path, '\x00') {
		return errors.New("file_exists.path must be nonempty and contain no NUL")
	}
	switch condition.Type {
	case "", waitModeAny, fileKind, "directory", "symlink":
		return nil
	default:
		return errors.New("file_exists.type must be any, file, directory, or symlink")
	}
}

func validateProbe(probe ProbeCondition, secrets map[string]SecretRef) error {
	if err := validateCommand(probe.Command); err != nil {
		return err
	}
	if err := validateOptionalAbsolutePath("probe working_directory", probe.WorkingDirectory); err != nil {
		return err
	}
	if err := validateEnvironment(probe.Environment, secrets); err != nil {
		return err
	}
	if duration, _ := probe.Timeout.Value(); duration <= 0 {
		return errors.New("probe timeout must be positive")
	}
	if interval, _ := probe.PollInterval.Value(); interval <= 0 {
		return errors.New("probe poll_interval must be positive")
	}
	if output, finite := probe.OutputLimit.Value(); !finite || output == 0 {
		return errors.New("probe output_limit must be a positive finite size")
	}

	return nil
}

func validateNotifiers(configuration Config) error {
	for _, name := range sortedMapKeys(configuration.Notifiers) {
		if err := validateConfiguredName("notifier", name); err != nil {
			return err
		}
		if err := validateNotifier(configuration.Notifiers[name], configuration.Secrets); err != nil {
			return fmt.Errorf("notifier %q: %w", name, err)
		}
	}

	return nil
}

//nolint:cyclop // The notifier union requires type-to-payload cardinality checks.
func validateNotifier(notifier Notifier, secrets map[string]SecretRef) error {
	if timeout, set := notifier.Timeout.Value(); !set || timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if notifier.Retry.MaxAttempts < 1 || notifier.Retry.MaxAttempts > 100 {
		return errors.New("retry.max_attempts must be between 1 and 100")
	}
	delay, delaySet := notifier.Retry.Delay.Value()
	maximum, maximumSet := notifier.Retry.MaxDelay.Value()
	if !delaySet || !maximumSet || delay < 0 || maximum < delay {
		return errors.New("retry delay and max_delay must be present and max_delay must not be smaller")
	}
	if err := validateEvents(notifier.Events); err != nil {
		return err
	}

	configuredVariants := 0
	if notifier.Command != nil {
		configuredVariants++
	}
	if notifier.HTTP != nil {
		configuredVariants++
	}
	if notifier.SMTP != nil {
		configuredVariants++
	}
	if configuredVariants != 1 {
		return errors.New("exactly one command, http, or smtp configuration is required")
	}

	switch notifier.Type {
	case "command":
		if notifier.Command == nil {
			return errors.New("type command requires command configuration")
		}
		return validateCommandNotifier(*notifier.Command, secrets)
	case httpScheme:
		if notifier.HTTP == nil {
			return errors.New("type http requires http configuration")
		}
		return validateHTTPNotifier(*notifier.HTTP, secrets)
	case "smtp":
		if notifier.SMTP == nil {
			return errors.New("type smtp requires smtp configuration")
		}
		return validateSMTPNotifier(*notifier.SMTP, secrets)
	default:
		return errors.New("type must be command, http, or smtp")
	}
}

func validateCommandNotifier(notifier CommandNotifier, secrets map[string]SecretRef) error {
	if err := validateCommand(notifier.Command); err != nil {
		return err
	}
	if err := validateEnvironment(notifier.Environment, secrets); err != nil {
		return err
	}
	if output, finite := notifier.OutputLimit.Value(); !finite || output == 0 {
		return errors.New("command output_limit must be a positive finite size")
	}

	return nil
}

func validateHTTPNotifier(notifier HTTPNotifier, secrets map[string]SecretRef) error {
	if err := validateHTTPURL(notifier); err != nil {
		return err
	}
	for name, value := range notifier.Headers {
		if !validHTTPHeaderName(name) || !validHTTPHeaderValue(value) {
			return errors.New("HTTP header name or value is invalid")
		}
		if isSensitiveHTTPHeader(name) {
			return fmt.Errorf("HTTP header %q must use secret_headers", name)
		}
	}
	for name, secret := range notifier.SecretHeaders {
		if !validHTTPHeaderName(name) {
			return errors.New("HTTP secret header name is invalid")
		}
		if err := requireSecret(secret, secrets); err != nil {
			return fmt.Errorf("secret header %q: %w", name, err)
		}
	}
	if notifier.SigningSecret != "" {
		if err := requireSecret(notifier.SigningSecret, secrets); err != nil {
			return fmt.Errorf("signing_secret: %w", err)
		}
	}

	return nil
}

func validateSMTPNotifier(notifier SMTPNotifier, secrets map[string]SecretRef) error {
	host, portText, err := net.SplitHostPort(notifier.Address)
	if err != nil {
		return fmt.Errorf("smtp address must use host:port syntax: %w", err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if host == "" || err != nil || port == 0 {
		return errors.New("smtp address must contain a host and a port from 1 through 65535")
	}
	if notifier.TLS != "starttls" && notifier.TLS != "implicit" {
		return errors.New("smtp tls must be starttls or implicit")
	}
	if _, err := mail.ParseAddress(notifier.From); err != nil {
		return fmt.Errorf("smtp from address is invalid: %w", err)
	}
	if len(notifier.To) == 0 {
		return errors.New("smtp to must contain at least one recipient")
	}
	for _, recipient := range notifier.To {
		if _, err := mail.ParseAddress(recipient); err != nil {
			return fmt.Errorf("smtp recipient is invalid: %w", err)
		}
	}
	if notifier.PasswordSecret != "" {
		if err := requireSecret(notifier.PasswordSecret, secrets); err != nil {
			return fmt.Errorf("password_secret: %w", err)
		}
	}
	if (notifier.Username == "") != (notifier.PasswordSecret == "") {
		return errors.New("smtp username and password_secret must be configured together")
	}

	return nil
}

func validateJobSpecs(configuration Config) error {
	for _, name := range sortedMapKeys(configuration.JobSpecs) {
		if err := validateConfiguredName("job spec", name); err != nil {
			return err
		}
		if err := validateJobSpec(configuration.JobSpecs[name], configuration); err != nil {
			return fmt.Errorf("job spec %q: %w", name, err)
		}
	}

	return nil
}

//nolint:cyclop,gocognit // A job specification deliberately composes all independent policy validators.
func validateJobSpec(specification JobSpec, configuration Config) error {
	if err := validateCommand(specification.Command); err != nil {
		return err
	}
	if err := validateDisplayName(specification.Name); err != nil {
		return err
	}
	if err := validateUniqueNames("tag", specification.Tags); err != nil {
		return err
	}
	if err := validateUniqueNames("group", specification.Groups); err != nil {
		return err
	}
	if err := validateOptionalAbsolutePath("working_directory", specification.WorkingDirectory); err != nil {
		return err
	}
	if err := validateEnvironment(specification.Environment, configuration.Secrets); err != nil {
		return err
	}
	if err := validateDependencies(specification.Dependencies); err != nil {
		return err
	}
	if specification.Stdin != stdinNull && specification.Stdin != "live" {
		return errors.New("stdin must be null or live")
	}
	if _, set := specification.Stop.GracePeriod.Value(); !set {
		return errors.New("stop.grace_period must be configured")
	}
	if specification.Wait.Mode != waitModeAll && specification.Wait.Mode != waitModeAny {
		return errors.New("wait.mode must be all or any")
	}
	if err := validateReferences("wait condition", specification.Wait.Conditions, configuration.WaitConditions); err != nil {
		return err
	}
	if specification.Wait.AbortAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, specification.Wait.AbortAt); err != nil {
			return fmt.Errorf("wait.abort_at must be an RFC 3339 timestamp: %w", err)
		}
	}
	if err := validateAdmission(specification.Admission, configuration.Concurrency); err != nil {
		return err
	}
	if err := validateCompletion(specification.Completion); err != nil {
		return err
	}
	if err := validateDelay(specification.Delay); err != nil {
		return err
	}
	if !specification.Timeouts.Run.set || !specification.Timeouts.Job.set {
		return errors.New("timeouts.run and timeouts.job must be configured")
	}
	if err := validateLogging(specification.Logging); err != nil {
		return err
	}
	if err := validateReferences("notifier", specification.Notification.Notifiers, configuration.Notifiers); err != nil {
		return err
	}
	if err := validateEvents(specification.Notification.Events); err != nil {
		return err
	}
	if len(specification.Notification.Events) == 0 {
		for _, notifierName := range specification.Notification.Notifiers {
			if len(configuration.Notifiers[notifierName].Events) == 0 {
				return fmt.Errorf("notification notifier %q has no subscribed events", notifierName)
			}
		}
	}

	return nil
}

func validateProfiles(configuration Config) error {
	for _, name := range sortedMapKeys(configuration.Profiles) {
		if err := validateConfiguredName("profile", name); err != nil {
			return err
		}
		profile := configuration.Profiles[name]
		if profile.JobSpec != "" {
			if _, found := configuration.JobSpecs[profile.JobSpec]; !found {
				return fmt.Errorf("profile %q references unknown job spec %q", name, profile.JobSpec)
			}
		}
		if err := validateOverride(profile.Overrides, configuration); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
		specification := JobSpec{Command: []string{"profile-validation-placeholder"}}
		if profile.JobSpec != "" {
			specification = cloneJobSpec(configuration.JobSpecs[profile.JobSpec])
		}
		applyOverride(&specification, profile.Overrides)
		normalizeJobSpec(&specification)
		if err := validateJobSpec(specification, configuration); err != nil {
			return fmt.Errorf("profile %q produces an invalid job spec: %w", name, err)
		}
	}

	return nil
}

func validateShared(shared SharedConfig) error {
	if shared.CurrentProfile != "" {
		if _, found := shared.Profiles[shared.CurrentProfile]; !found {
			return fmt.Errorf("shared.current_profile references unknown profile %q", shared.CurrentProfile)
		}
	}
	for _, name := range sortedMapKeys(shared.Profiles) {
		if err := validateSharedProfile(name, shared.Profiles[name]); err != nil {
			return err
		}
	}

	return nil
}

func validateSharedProfile(name string, profile SharedProfile) error {
	if err := validateConfiguredName("shared profile", name); err != nil {
		return err
	}
	if !sharedNamePattern.MatchString(profile.Namespace) {
		return fmt.Errorf("shared profile %q has invalid namespace", name)
	}
	parsed, err := validateSharedEndpoint(name, profile.Endpoint)
	if err != nil {
		return err
	}
	if err := validateSharedPath(name, "token_file", profile.TokenFile); err != nil {
		return err
	}
	if err := validateSharedPath(name, "ca_file", profile.CAFile); err != nil {
		return err
	}
	if parsed.Scheme == httpScheme && profile.TokenFile != "" {
		return fmt.Errorf("shared profile %q cannot send a token over HTTP", name)
	}
	for _, storeName := range sortedMapKeys(profile.ArtifactRoots) {
		mapping := profile.ArtifactRoots[storeName]
		if !artifactStorePattern.MatchString(storeName) {
			return fmt.Errorf("shared profile %q has invalid artifact store name %q", name, storeName)
		}
		if mapping.Version < 1 {
			return fmt.Errorf("shared profile %q artifact store %q version must be positive", name, storeName)
		}
		if err := validateSharedPath(name, "artifact_roots."+storeName+".path", mapping.Path); err != nil {
			return err
		}
		if mapping.Path == "" {
			return fmt.Errorf("shared profile %q artifact store %q path is required", name, storeName)
		}
	}

	return nil
}

func validateSharedEndpoint(name, endpoint string) (*url.URL, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("shared profile %q has invalid endpoint", name)
	}
	if parsed.Scheme != httpsScheme && (parsed.Scheme != httpScheme || !loopbackHost(parsed.Hostname())) {
		return nil, fmt.Errorf("shared profile %q endpoint must use HTTPS or loopback HTTP", name)
	}

	return parsed, nil
}

func validateSharedPath(profile, field, value string) error {
	if value != "" && (!filepath.IsAbs(value) || filepath.Clean(value) != value) {
		return fmt.Errorf("shared profile %q %s must be a clean absolute path", profile, field)
	}

	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)

	return address != nil && address.IsLoopback()
}

//nolint:cyclop,gocognit // Optional override fields must be validated only when present.
func validateOverride(override JobSpecOverride, configuration Config) error {
	if override.Command != nil {
		if err := validateCommand(override.Command); err != nil {
			return err
		}
	}
	if override.Name != nil {
		if err := validateDisplayName(*override.Name); err != nil {
			return err
		}
	}
	if override.Tags != nil {
		if err := validateUniqueNames("tag", override.Tags); err != nil {
			return err
		}
	}
	if override.Groups != nil {
		if err := validateUniqueNames("group", override.Groups); err != nil {
			return err
		}
	}
	if override.WorkingDirectory != nil {
		if err := validateOptionalAbsolutePath("working_directory", *override.WorkingDirectory); err != nil {
			return err
		}
	}
	if override.Environment != nil {
		if err := validateEnvironment(*override.Environment, configuration.Secrets); err != nil {
			return err
		}
	}
	if override.Stdin != nil && *override.Stdin != stdinNull && *override.Stdin != "live" {
		return errors.New("stdin must be null or live")
	}
	if override.Dependencies != nil {
		if err := validateDependencies(override.Dependencies); err != nil {
			return err
		}
	}
	if override.Wait != nil {
		if override.Wait.Mode != "" && override.Wait.Mode != waitModeAll && override.Wait.Mode != waitModeAny {
			return errors.New("wait.mode must be all or any")
		}
		if err := validateReferences("wait condition", override.Wait.Conditions, configuration.WaitConditions); err != nil {
			return err
		}
	}
	if override.Admission != nil {
		admission := *override.Admission
		if admission.Slots == 0 {
			admission.Slots = 1
		}
		if err := validateAdmission(admission, configuration.Concurrency); err != nil {
			return err
		}
	}
	if override.Notification != nil {
		if err := validateReferences("notifier", override.Notification.Notifiers, configuration.Notifiers); err != nil {
			return err
		}
		if err := validateEvents(override.Notification.Events); err != nil {
			return err
		}
	}

	return nil
}

func validateAdmission(admission Admission, concurrency Concurrency) error {
	if admission.Slots < 1 {
		return errors.New("admission.slots must be positive")
	}
	if finite, ok := concurrency.MaxActiveSlots.Value(); ok && uint64(admission.Slots) > uint64(finite) {
		return errors.New("admission.slots exceeds store-wide capacity")
	}
	if admission.Pool == "" {
		return nil
	}
	pool, found := concurrency.Pools[admission.Pool]
	if !found {
		return fmt.Errorf("admission.pool references unknown pool %q", admission.Pool)
	}
	if finite, ok := pool.Value(); ok && uint64(admission.Slots) > uint64(finite) {
		return fmt.Errorf("admission.slots exceeds pool %q capacity", admission.Pool)
	}

	return nil
}

//nolint:cyclop // Completion limits have independent reachability and exit-code invariants.
func validateCompletion(policy CompletionPolicy) error {
	if !policy.MaxRuns.set || !policy.MaxFailures.set || !policy.SuccessTarget.set {
		return errors.New("completion max_runs, success_target, and failure_limit must be configured")
	}
	if runs, finite := policy.MaxRuns.Value(); finite && runs == 0 {
		return errors.New("completion.max_runs must be positive or unlimited")
	}
	if failures, finite := policy.MaxFailures.Value(); finite && failures == 0 {
		return errors.New("completion.failure_limit must be positive or unlimited")
	}
	successes, finiteSuccesses := policy.SuccessTarget.Value()
	if finiteSuccesses && successes == 0 {
		return errors.New("completion.success_target must be positive or unlimited")
	}
	if runs, finiteRuns := policy.MaxRuns.Value(); finiteRuns && finiteSuccesses && successes > runs {
		return errors.New("completion.success_target exceeds max_runs")
	}
	if err := validateExitCodes("success_exit_codes", policy.SuccessExitCodes); err != nil {
		return err
	}
	if err := validateExitCodes("retryable_exit_codes", policy.RetryableExitCodes); err != nil {
		return err
	}
	success := make(map[int]struct{}, len(policy.SuccessExitCodes))
	for _, code := range policy.SuccessExitCodes {
		success[code] = struct{}{}
	}
	for _, code := range policy.RetryableExitCodes {
		if _, overlap := success[code]; overlap {
			return fmt.Errorf("completion exit code %d is both successful and retryable", code)
		}
	}

	return nil
}

func privateHTTPHost(host string) bool {
	normalized := strings.TrimSuffix(strings.ToLower(host), ".")
	if normalized == "localhost" || strings.HasSuffix(normalized, ".localhost") || strings.HasSuffix(normalized, ".local") {
		return true
	}
	address, _, _ := strings.Cut(normalized, "%")
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}

	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func validHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", character) {
			continue
		}
		return false
	}

	return true
}

func validHTTPHeaderValue(value string) bool {
	for _, character := range value {
		if character == '\t' || character >= ' ' && character != '\x7f' {
			continue
		}
		return false
	}

	return true
}

func validateDelay(policy DelayPolicy) error {
	switch policy.Strategy {
	case "constant", "linear", "exponential":
	default:
		return errors.New("delay.strategy must be constant, linear, or exponential")
	}
	initial, initialSet := policy.Initial.Value()
	if !initialSet || !policy.MaxDelay.set || !policy.Jitter.set {
		return errors.New("delay initial, max_delay, and jitter must be configured")
	}
	if maximum, finite := policy.MaxDelay.Value(); finite && maximum < initial {
		return errors.New("delay.max_delay must not be smaller than initial")
	}
	if policy.Base < 1 || math.Trunc(policy.Base) != policy.Base {
		return errors.New("delay.base must be an integer of at least 1")
	}

	return nil
}

func validateLogging(policy LoggingPolicy) error {
	switch policy.Capture {
	case "both", "stdout", "stderr", "none":
	default:
		return errors.New("logging.capture must be both, stdout, stderr, or none")
	}
	if !policy.SegmentBytes.set || !policy.SegmentsPerRun.set {
		return errors.New("logging segment limits must be configured")
	}
	if segments, finite := policy.SegmentsPerRun.Value(); finite && segments == 0 {
		return errors.New("logging.segments_per_run must be positive or unlimited")
	}
	if segmentBytes, finite := policy.SegmentBytes.Value(); finite && segmentBytes == 0 {
		if !policy.SegmentsPerRun.IsUnlimited() {
			return errors.New("logging finite segment count requires positive segment_bytes")
		}
	}

	return nil
}

func validateEnvironment(environment Environment, secrets map[string]SecretRef) error {
	unset := make(map[string]struct{}, len(environment.Unset))
	for _, name := range environment.Unset {
		if !validEnvironmentName(name) {
			return fmt.Errorf("environment unset name %q is invalid", name)
		}
		if _, duplicate := unset[name]; duplicate {
			return fmt.Errorf("environment unset contains duplicate %q", name)
		}
		unset[name] = struct{}{}
	}
	for name, value := range environment.Set {
		if !validEnvironmentName(name) || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("environment set entry %q is invalid", name)
		}
		if _, overlap := unset[name]; overlap {
			return fmt.Errorf("environment variable %q is both set and unset", name)
		}
	}
	for name, secret := range environment.Secrets {
		if !validEnvironmentName(name) {
			return fmt.Errorf("secret environment name %q is invalid", name)
		}
		if _, overlap := unset[name]; overlap {
			return fmt.Errorf("environment variable %q is both secret and unset", name)
		}
		if _, overlap := environment.Set[name]; overlap {
			return fmt.Errorf("environment variable %q is both literal and secret", name)
		}
		if err := requireSecret(secret, secrets); err != nil {
			return fmt.Errorf("secret environment %q: %w", name, err)
		}
	}

	return nil
}

func validateDependencies(dependencies []Dependency) error {
	seen := make(map[string][]string, len(dependencies))
	for _, dependency := range dependencies {
		if strings.TrimSpace(dependency.Job) == "" || strings.ContainsRune(dependency.Job, '\x00') {
			return errors.New("dependency job selector must be nonempty and contain no NUL")
		}
		if len(dependency.Outcomes) == 0 {
			return fmt.Errorf("dependency %q must contain at least one outcome", dependency.Job)
		}
		outcomes := make(map[string]struct{}, len(dependency.Outcomes))
		for _, outcome := range dependency.Outcomes {
			switch outcome {
			case "success", "failure", "timed_out", "cancelled", "aborted", "lost", "submission_failed": //nolint:misspell // Persisted outcome spelling follows the specification.
			default:
				return fmt.Errorf("dependency %q has unknown outcome %q", dependency.Job, outcome)
			}
			if _, duplicate := outcomes[outcome]; duplicate {
				return fmt.Errorf("dependency %q contains duplicate outcome %q", dependency.Job, outcome)
			}
			outcomes[outcome] = struct{}{}
		}
		sorted := append([]string(nil), dependency.Outcomes...)
		sort.Strings(sorted)
		if prior, duplicate := seen[dependency.Job]; duplicate {
			if strings.Join(prior, "\x00") != strings.Join(sorted, "\x00") {
				return fmt.Errorf("dependency %q has contradictory outcome requirements", dependency.Job)
			}
			continue
		}
		seen[dependency.Job] = sorted
	}

	return nil
}

func normalizeDependencies(dependencies []Dependency) []Dependency {
	normalized := make([]Dependency, 0, len(dependencies))
	seen := make(map[string]string, len(dependencies))
	for _, dependency := range dependencies {
		outcomes := append([]string(nil), dependency.Outcomes...)
		sort.Strings(outcomes)
		fingerprint := strings.Join(outcomes, "\x00")
		if previous, found := seen[dependency.Job]; found && previous == fingerprint {
			continue
		}
		seen[dependency.Job] = fingerprint
		normalized = append(normalized, Dependency{Job: dependency.Job, Outcomes: outcomes})
	}

	return normalized
}

func validateCommand(command []string) error {
	if len(command) == 0 || command[0] == "" {
		return errors.New("command must contain an executable")
	}
	for _, argument := range command {
		if strings.ContainsRune(argument, '\x00') {
			return errors.New("command arguments must not contain NUL")
		}
	}

	return nil
}

func validateDisplayName(name string) error {
	if name == "" {
		return nil
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("name must contain a non-space character")
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return errors.New("name must not contain control characters")
		}
	}

	return nil
}

func validateOptionalAbsolutePath(description, value string) error {
	if value == "" {
		return nil
	}
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must be a clean absolute path", description)
	}

	return nil
}

func validateConfiguredName(kind, name string) error {
	if len(name) > maxConfiguredName || !configuredNamePattern.MatchString(name) {
		return fmt.Errorf("%s name %q is invalid", kind, name)
	}

	return nil
}

func validateUniqueNames(kind string, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if err := validateConfiguredName(kind, name); err != nil {
			return err
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("%s list contains duplicate %q", kind, name)
		}
		seen[name] = struct{}{}
	}

	return nil
}

func validateReferences[T any](kind string, names []string, definitions map[string]T) error {
	if err := validateUniqueNames(kind, names); err != nil {
		return err
	}
	for _, name := range names {
		if _, found := definitions[name]; !found {
			return fmt.Errorf("unknown %s %q", kind, name)
		}
	}

	return nil
}

func validateEvents(events []string) error {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if _, known := knownNotificationEvents[event]; !known {
			return fmt.Errorf("unknown notification event %q", event)
		}
		if _, duplicate := seen[event]; duplicate {
			return fmt.Errorf("duplicate notification event %q", event)
		}
		seen[event] = struct{}{}
	}

	return nil
}

func validateExitCodes(description string, codes []int) error {
	seen := make(map[int]struct{}, len(codes))
	for _, code := range codes {
		if code < 0 || code > 255 {
			return fmt.Errorf("completion.%s entry %d must be between 0 and 255", description, code)
		}
		if _, duplicate := seen[code]; duplicate {
			return fmt.Errorf("completion.%s contains duplicate %d", description, code)
		}
		seen[code] = struct{}{}
	}

	return nil
}

func requireSecret(name string, secrets map[string]SecretRef) error {
	if _, found := secrets[name]; !found {
		return fmt.Errorf("references unknown secret %q", name)
	}

	return nil
}

func isSensitiveHTTPHeader(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "cookie", "proxy-authorization", "set-cookie", "x-api-key":
		return true
	default:
		return false
	}
}

func validEnvironmentName(name string) bool {
	if name == "" || strings.ContainsAny(name, "=\x00") {
		return false
	}
	for index, character := range name {
		if (character >= 'A' && character <= 'Z') || character == '_' {
			continue
		}
		if index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}

	return true
}

func durationMust(value time.Duration) Duration {
	return Duration{value: value, set: true}
}

func integerLimit(value uint64) IntegerLimit {
	return IntegerLimit{value: value, set: true}
}

func byteLimit(value uint64) ByteLimit {
	return ByteLimit{value: value, set: true}
}
