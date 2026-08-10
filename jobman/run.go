package jobman

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
)

type runOptions struct {
	name               string
	rerun              string
	directory          string
	environment        []string
	unsetEnvironment   []string
	secretEnvironment  []string
	groups             []string
	tags               []string
	jobSpec            string
	profiles           []string
	stdin              string
	stdinFile          string
	stopGrace          time.Duration
	forceAfterGrace    bool
	retries            uint64
	maxRuns            string
	successTarget      string
	failureLimit       string
	successExitCodes   []int
	retryableExitCodes []int
	retryTimeouts      bool
	retryStartFailures bool
	retryDelay         time.Duration
	repeatDelay        time.Duration
	retryBackoff       string
	retryJitter        time.Duration
	retryMaxDelay      time.Duration
	retryAbortAt       string
	runTimeout         time.Duration
	jobTimeout         time.Duration
	afterSuccess       []string
	afterFinish        []string
	afterFailed        []string
	afterOutcome       []string
	pool               string
	slots              uint64
	waitConditions     []string
	waitUntil          []string
	waitDelay          []string
	waitFile           []string
	waitMode           string
	waitAbortAt        string
	waitPoll           time.Duration
	logSegmentBytes    uint64
	logSegments        uint64
	logCapture         string
	logRetention       string
	notifiers          []string
	notificationEvents []string
	waitForCompletion  bool
	foreground         bool
}

func newRunCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	options := &runOptions{forceAfterGrace: true, slots: 1}
	command := &cobra.Command{
		Use:   "run [OPTIONS] (-- COMMAND [ARG...] | --job-spec NAME | --rerun JOB)",
		Short: "Submit a command as a managed job",
		Args:  usageArgs(validateRunArguments),
		RunE: func(command *cobra.Command, arguments []string) error {
			return run(command, dependencies, root, options, arguments)
		},
	}
	command.Flags().SetInterspersed(false)
	flags := command.Flags()
	flags.StringVar(&options.name, "name", "", "assign a display name")
	flags.StringVar(&options.rerun, "rerun", "", "copy a prior job specification into a new job")
	flags.StringArrayVar(&options.groups, "group", nil, "add the job to a group")
	flags.StringArrayVar(&options.tags, "tag", nil, "attach a tag")
	flags.StringVar(&options.directory, "cwd", "", "set the target working directory")
	flags.StringArrayVar(&options.environment, "env", nil, "set NAME=VALUE in the target environment")
	flags.StringArrayVar(&options.unsetEnvironment, "unset-env", nil, "remove NAME from the target environment")
	flags.StringArrayVar(&options.secretEnvironment, "secret-env", nil, "set NAME from a configured secret")
	flags.StringVar(&options.jobSpec, "job-spec", "", "use a named job specification")
	flags.StringArrayVar(&options.profiles, "profile", nil, "apply a named profile in argument order")
	flags.StringVar(&options.stdin, "stdin", "", "select null or live standard input")
	flags.StringVar(&options.stdinFile, "stdin-file", "", "read target standard input from a file")
	durationFlag(flags, &options.stopGrace, "stop-grace", "wait this long before forced termination")
	flags.BoolVar(&options.forceAfterGrace, "force-after-grace", true, "force termination after the grace period")
	flags.Uint64Var(&options.retries, "retries", 0, "permit N retries after the initial run")
	flags.StringVar(&options.maxRuns, "max-runs", "", "set a positive run limit or unlimited")
	flags.StringVar(&options.successTarget, "success-target", "", "set a positive success target or unlimited")
	flags.StringVar(&options.failureLimit, "failure-limit", "", "set a positive failure limit or unlimited")
	flags.IntSliceVar(&options.successExitCodes, "success-exit-code", nil, "classify an exit code as successful")
	flags.IntSliceVar(&options.retryableExitCodes, "retryable-exit-code", nil, "classify an exit code as retryable")
	flags.BoolVar(&options.retryTimeouts, "retry-timeouts", false, "permit retry after a run timeout")
	flags.BoolVar(&options.retryStartFailures, "retry-start-failures", false, "permit retry after process start failure")
	durationFlag(flags, &options.retryDelay, "retry-delay", "set the base failed-run delay")
	durationFlag(flags, &options.repeatDelay, "repeat-delay", "set the base successful-run delay")
	flags.StringVar(&options.retryBackoff, "retry-backoff", "", "select constant, linear, or exponential backoff")
	durationFlag(flags, &options.retryJitter, "retry-jitter", "set the full width of bounded delay jitter")
	durationFlag(flags, &options.retryMaxDelay, "retry-max-delay", "cap retry and repetition delay")
	flags.StringVar(&options.retryAbortAt, "retry-abort-at", "", "prevent a run after this RFC3339 timestamp")
	durationFlag(flags, &options.runTimeout, "run-timeout", "limit each target-command run")
	durationFlag(flags, &options.jobTimeout, "job-timeout", "limit the entire job")
	flags.StringArrayVar(&options.afterSuccess, "after-success", nil, "require another job to succeed first")
	flags.StringArrayVar(&options.afterFinish, "after-finish", nil, "require another job to finish first")
	flags.StringArrayVar(&options.afterFailed, "after-failed", nil, "require another job to fail first")
	flags.StringArrayVar(&options.afterOutcome, "after-outcome", nil, "require JOB=OUTCOME[,OUTCOME...]")
	flags.StringVar(&options.pool, "pool", "", "reserve slots from a named concurrency pool")
	flags.Uint64Var(&options.slots, "slots", 1, "reserve this many global and pool slots")
	flags.StringArrayVar(&options.waitConditions, "wait-condition", nil, "use a named wait condition")
	flags.StringArrayVar(&options.waitUntil, "wait-until", nil, "wait until an RFC3339 timestamp")
	flags.StringArrayVar(&options.waitDelay, "wait-delay", nil, "wait for a duration after acceptance")
	flags.StringArrayVar(&options.waitFile, "wait-file", nil, "wait for a path to exist")
	flags.StringVar(&options.waitMode, "wait-mode", "", "combine wait conditions with all or any")
	flags.StringVar(&options.waitAbortAt, "wait-abort-at", "", "abort waits at this RFC3339 timestamp")
	durationFlag(flags, &options.waitPoll, "wait-poll", "set the wait-condition polling interval")
	byteSizeFlag(flags, &options.logSegmentBytes, "log-segment-bytes", "rotate streams after this many bytes")
	flags.Uint64Var(&options.logSegments, "log-segments", 0, "cap captured segments per stream")
	flags.StringVar(&options.logCapture, "log-capture", "", "capture both, stdout, stderr, or none")
	flags.StringVar(&options.logRetention, "log-retention", "", "retain logs for a duration or unlimited")
	flags.StringArrayVar(&options.notifiers, "notify", nil, "deliver events through a named notifier")
	flags.StringArrayVar(&options.notificationEvents, "notify-on", nil, "subscribe selected notifiers to an event")
	flags.BoolVar(&options.waitForCompletion, "wait", false, "wait for the terminal job outcome")
	flags.BoolVar(&options.foreground, "foreground", false, "attach input and output and wait for completion")
	registerJobSelectorFlagCompletion(
		command,
		dependencies,
		root,
		"rerun",
		"after-success",
		"after-finish",
		"after-failed",
	)
	if err := command.RegisterFlagCompletionFunc(
		"after-outcome",
		jobSelectorOutcomeCompletion(dependencies, root),
	); err != nil {
		panic(err)
	}

	return command
}

func run(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	options *runOptions,
	arguments []string,
) error {
	if options.rerun != "" {
		if err := validateRerunOptions(command, options); err != nil {
			return usageError(err)
		}
	}
	return withLoadedBackend(command, dependencies, root, func(backend app.Backend, loaded config.Loaded) error {
		if options.rerun != "" {
			return submitRerun(command, backend, loaded.Config, options)
		}

		return submitConfiguredJob(command, backend, loaded.Config, options, arguments)
	})
}

func submitRerun(
	command *cobra.Command,
	backend app.Backend,
	configuration config.Config,
	options *runOptions,
) error {
	if err := applyBackendConfiguration(command.Context(), backend, configuration); err != nil {
		return err
	}
	lifecycle, ok := backend.(app.LifecycleBackend)
	if !ok {
		return errors.New("application backend does not support rerunning jobs")
	}
	job, err := lifecycle.Rerun(command.Context(), options.rerun, app.RerunRequest{Name: options.name})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(command.OutOrStdout(), job.ID); err != nil {
		return fmt.Errorf("write rerun job ID: %w", err)
	}
	if options.waitForCompletion {
		return waitForSubmittedJob(command.Context(), backend, job.ID.String())
	}

	return nil
}

func submitConfiguredJob(
	command *cobra.Command,
	backend app.Backend,
	configuration config.Config,
	options *runOptions,
	arguments []string,
) error {
	var configured config.JobSpec
	var err error
	if len(arguments) > 0 {
		configured, err = configuration.ResolveJobSpecWithCommand(options.jobSpec, arguments, options.profiles...)
	} else {
		configured, err = configuration.ResolveJobSpec(options.jobSpec, options.profiles...)
	}
	if err != nil {
		return usageError(err)
	}
	request, err := submitRequestFromConfig(configuration, configured)
	if err != nil {
		return usageError(err)
	}
	if applyErr := applyRunOptions(command, options, arguments, configuration, &request); applyErr != nil {
		return usageError(applyErr)
	}
	if configurationErr := applyBackendConfiguration(command.Context(), backend, configuration); configurationErr != nil {
		return configurationErr
	}
	job, err := backend.Submit(command.Context(), request)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(command.OutOrStdout(), job.ID); err != nil {
		return fmt.Errorf("write submitted job ID: %w", err)
	}
	if options.foreground {
		return attachForeground(command, backend, job)
	}
	if options.waitForCompletion {
		return waitForSubmittedJob(command.Context(), backend, job.ID.String())
	}

	return nil
}

//nolint:gocognit,cyclop,funlen,maintidx // Each independent flag overlays one immutable policy field explicitly.
func applyRunOptions(
	command *cobra.Command,
	options *runOptions,
	arguments []string,
	configuration config.Config,
	request *app.SubmitRequest,
) error {
	if len(arguments) > 0 {
		request.Executable = arguments[0]
		request.Arguments = slices.Clone(arguments[1:])
	}
	if request.Executable == "" {
		return errors.New("a target command or named job specification is required")
	}
	if flagChanged(command, "name") {
		request.Name = options.name
	}
	if flagChanged(command, "group") {
		request.ExecutionPolicy.Groups = slices.Clone(options.groups)
	}
	if flagChanged(command, "tag") {
		request.ExecutionPolicy.Tags = slices.Clone(options.tags)
	}
	if flagChanged(command, "cwd") {
		request.WorkingDirectory = options.directory
	}
	if request.WorkingDirectory == "" {
		workingDirectory, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolve working directory: %w", err)
		}
		request.WorkingDirectory = workingDirectory
	}
	absoluteDirectory, err := filepath.Abs(request.WorkingDirectory)
	if err != nil {
		return fmt.Errorf("resolve working directory: %w", err)
	}
	request.WorkingDirectory = filepath.Clean(absoluteDirectory)

	environment, err := parseEnvironment(options.environment)
	if err != nil {
		return err
	}
	for name, value := range environment {
		request.Environment[name] = value
	}
	if flagChanged(command, "unset-env") {
		request.UnsetEnvironment = slices.Clone(options.unsetEnvironment)
	}
	for _, encoded := range options.secretEnvironment {
		name, secretName, ok := strings.Cut(encoded, "=")
		if !ok || name == "" || secretName == "" {
			return fmt.Errorf("invalid --secret-env %q: expected NAME=SECRET", encoded)
		}
		reference, found := configuration.Secrets[secretName]
		if !found {
			return fmt.Errorf("--secret-env %q references unknown secret %q", name, secretName)
		}
		if request.ExecutionPolicy.SecretEnv == nil {
			request.ExecutionPolicy.SecretEnv = map[string]model.SecretReference{}
		}
		request.ExecutionPolicy.SecretEnv[name] = model.SecretReference{
			Provider: reference.Provider(), Name: reference.Locator(),
		}
	}

	if options.stdinFile != "" && flagChanged(command, "stdin") {
		return errors.New("--stdin and --stdin-file are mutually exclusive")
	}
	if options.stdinFile != "" {
		path, pathErr := filepath.Abs(options.stdinFile)
		if pathErr != nil {
			return fmt.Errorf("resolve --stdin-file: %w", pathErr)
		}
		request.StdinPolicy = model.StdinFile
		request.ExecutionPolicy.StdinPath = filepath.Clean(path)
	} else if flagChanged(command, "stdin") {
		request.StdinPolicy = model.StdinPolicy(options.stdin)
	}
	if options.foreground {
		if flagChanged(command, "stdin") && options.stdin != "inherit" && options.stdin != "live" {
			return errors.New("--foreground requires inherited input; omit --stdin or select inherit")
		}
		if options.stdinFile != "" {
			return errors.New("--foreground and --stdin-file are mutually exclusive")
		}
		// The per-job supervisor owns the target even while this client is
		// attached. Its private live-input transport provides inherited terminal
		// semantics without leaking a terminal file descriptor into detachment.
		request.StdinPolicy = model.StdinLive
		request.ExecutionPolicy.Foreground = false
	}
	if request.StdinPolicy != model.StdinNull && request.StdinPolicy != model.StdinLive &&
		request.StdinPolicy != model.StdinFile {
		return fmt.Errorf("unsupported stdin policy %q", request.StdinPolicy)
	}
	if flagChanged(command, "stop-grace") || flagChanged(command, "force-after-grace") {
		stopPolicy := request.StopPolicy
		if flagChanged(command, "stop-grace") {
			stopPolicy.GracePeriod = options.stopGrace
		}
		if flagChanged(command, "force-after-grace") {
			stopPolicy.ForceAfterGrace = options.forceAfterGrace
		}
		request.StopPolicy = stopPolicy
		request.StopPolicySet = true
	}

	if flagChanged(command, "retries") {
		if flagChanged(command, "max-runs") || flagChanged(command, "failure-limit") {
			return errors.New("--retries cannot be combined with --max-runs or --failure-limit")
		}
		if options.retries == ^uint64(0) {
			return errors.New("--retries is too large")
		}
		limit, limitErr := policy.FiniteLimit(options.retries + 1)
		if limitErr != nil {
			return fmt.Errorf("--retries: %w", limitErr)
		}
		request.ExecutionPolicy.Completion.MaxRuns = limit
		request.ExecutionPolicy.Completion.FailureLimit = limit
		successTarget, limitErr := policy.FiniteLimit(1)
		if limitErr != nil {
			return fmt.Errorf("construct retry success target: %w", limitErr)
		}
		request.ExecutionPolicy.Completion.SuccessTarget = successTarget
		request.ExecutionPolicy.Classification.RetryableExitCodes = nil
	}
	for name, value := range map[string]string{
		"max-runs": options.maxRuns, "success-target": options.successTarget, "failure-limit": options.failureLimit,
	} {
		if !flagChanged(command, name) {
			continue
		}
		limit, parseErr := parseLimitFlag(name, value)
		if parseErr != nil {
			return parseErr
		}
		switch name {
		case "max-runs":
			request.ExecutionPolicy.Completion.MaxRuns = limit
		case "success-target":
			request.ExecutionPolicy.Completion.SuccessTarget = limit
		case "failure-limit":
			request.ExecutionPolicy.Completion.FailureLimit = limit
		}
	}
	if flagChanged(command, "success-exit-code") {
		request.ExecutionPolicy.Classification.SuccessExitCodes = slices.Clone(options.successExitCodes)
	}
	if flagChanged(command, "retryable-exit-code") {
		request.ExecutionPolicy.Classification.RetryableExitCodes = make([]policy.ExitCodeRange, len(options.retryableExitCodes))
		for index, code := range options.retryableExitCodes {
			request.ExecutionPolicy.Classification.RetryableExitCodes[index] = policy.ExitCodeRange{First: code, Last: code}
		}
	}
	if flagChanged(command, "retry-timeouts") {
		request.ExecutionPolicy.Classification.RetryTimeout = options.retryTimeouts
	}
	if flagChanged(command, "retry-start-failures") {
		request.ExecutionPolicy.Classification.RetryStartFailure = options.retryStartFailures
	}
	if flagChanged(command, "retry-delay") {
		request.ExecutionPolicy.FailureDelay.Base = options.retryDelay
	}
	if flagChanged(command, "repeat-delay") {
		request.ExecutionPolicy.SuccessDelay.Base = options.repeatDelay
	}
	if flagChanged(command, "retry-backoff") {
		request.ExecutionPolicy.FailureDelay.Backoff = policy.Backoff(options.retryBackoff)
		request.ExecutionPolicy.SuccessDelay.Backoff = policy.Backoff(options.retryBackoff)
	}
	if flagChanged(command, "retry-jitter") {
		request.ExecutionPolicy.FailureDelay.Jitter = options.retryJitter
		request.ExecutionPolicy.SuccessDelay.Jitter = options.retryJitter
	}
	if flagChanged(command, "retry-max-delay") {
		request.ExecutionPolicy.FailureDelay.MaxDelay = options.retryMaxDelay
		request.ExecutionPolicy.FailureDelay.HasMaxDelay = true
		request.ExecutionPolicy.SuccessDelay.MaxDelay = options.retryMaxDelay
		request.ExecutionPolicy.SuccessDelay.HasMaxDelay = true
	}
	if flagChanged(command, "retry-abort-at") {
		request.ExecutionPolicy.Completion.RetryAbortAt, err = parseOptionalTimestamp(options.retryAbortAt)
		if err != nil {
			return fmt.Errorf("--retry-abort-at %w", err)
		}
		request.ExecutionPolicy.Completion.HasRetryAbortAt = true
	}
	if flagChanged(command, "run-timeout") {
		request.ExecutionPolicy.RunTimeout = options.runTimeout
	}
	if flagChanged(command, "job-timeout") {
		request.ExecutionPolicy.JobTimeout = options.jobTimeout
	}

	flagDependencies, err := appendDependencyFlags(options)
	if err != nil {
		return err
	}
	request.Dependencies = append(request.Dependencies, flagDependencies...)
	if flagChanged(command, "pool") {
		request.ExecutionPolicy.Concurrency.Pool = options.pool
	}
	if flagChanged(command, "slots") {
		request.ExecutionPolicy.Concurrency.Slots = options.slots
	}
	if request.ExecutionPolicy.Concurrency.Slots == 0 {
		return errors.New("--slots must be positive")
	}

	if flagChanged(command, "wait-condition") {
		configured := config.WaitPolicy{Mode: string(request.ExecutionPolicy.WaitMode), Conditions: options.waitConditions}
		if options.waitAbortAt != "" {
			configured.AbortAt = options.waitAbortAt
		}
		waits, waitErr := waitsFromConfig(configuration, configured)
		if waitErr != nil {
			return waitErr
		}
		request.ExecutionPolicy.WaitConditions = append(request.ExecutionPolicy.WaitConditions, waits...)
	}
	waitAbort, err := parseOptionalTimestamp(options.waitAbortAt)
	if err != nil {
		return fmt.Errorf("--wait-abort-at %w", err)
	}
	poll := options.waitPoll
	if poll == 0 {
		poll = defaultWaitPollInterval
	}
	for _, encoded := range options.waitUntil {
		until, parseErr := parseOptionalTimestamp(encoded)
		if parseErr != nil || until.IsZero() {
			return fmt.Errorf("invalid --wait-until %q: expected RFC3339 timestamp", encoded)
		}
		request.ExecutionPolicy.WaitConditions = append(request.ExecutionPolicy.WaitConditions, model.WaitCondition{
			Kind: model.WaitUntil, Until: until, AbortAt: waitAbort, PollInterval: poll,
		})
	}
	for _, encoded := range options.waitDelay {
		delay, parseErr := config.ParseDuration(encoded)
		if parseErr != nil || delay < 0 {
			return fmt.Errorf("invalid --wait-delay %q: expected nonnegative duration", encoded)
		}
		request.ExecutionPolicy.WaitConditions = append(request.ExecutionPolicy.WaitConditions, model.WaitCondition{
			Kind: model.WaitDelay, Delay: delay, AbortAt: waitAbort, PollInterval: poll,
		})
	}
	for _, path := range options.waitFile {
		request.ExecutionPolicy.WaitConditions = append(request.ExecutionPolicy.WaitConditions, model.WaitCondition{
			Kind: model.WaitFileExists, Path: path, FileKind: policy.FileKindAny,
			AbortAt: waitAbort, PollInterval: poll,
		})
	}
	if flagChanged(command, "wait-poll") {
		for index := range request.ExecutionPolicy.WaitConditions {
			request.ExecutionPolicy.WaitConditions[index].PollInterval = poll
		}
	}
	if flagChanged(command, "wait-mode") {
		request.ExecutionPolicy.WaitMode = policy.WaitMode(options.waitMode)
	}
	if flagChanged(command, "log-segment-bytes") {
		if options.logSegmentBytes > math.MaxInt64 {
			return errors.New("--log-segment-bytes exceeds the supported range")
		}
		request.ExecutionPolicy.LogRotateSize = int64(options.logSegmentBytes)
	}
	if flagChanged(command, "log-segments") {
		if options.logSegments == 0 || options.logSegments > uint64(^uint16(0)) {
			return errors.New("--log-segments must be between 1 and 65535")
		}
		request.ExecutionPolicy.LogMaxSegmentsPerStream = int(options.logSegments)
	}
	if flagChanged(command, "log-capture") {
		request.ExecutionPolicy.LogCapture = options.logCapture
	}
	if flagChanged(command, "log-retention") {
		request.ExecutionPolicy.LogRetentionMaxAge = 0
		request.ExecutionPolicy.LogRetentionUnlimited = options.logRetention == config.Unlimited
		if !request.ExecutionPolicy.LogRetentionUnlimited {
			retention, parseErr := config.ParseDuration(options.logRetention)
			if parseErr != nil || retention < 0 {
				return fmt.Errorf("--log-retention must be a nonnegative duration or %q", config.Unlimited)
			}
			request.ExecutionPolicy.LogRetentionMaxAge = retention
		}
	}
	if flagChanged(command, "notify") {
		request.ExecutionPolicy.Notifications = make([]model.NotificationSubscription, 0, len(options.notifiers))
		for _, name := range options.notifiers {
			if _, found := configuration.Notifiers[name]; !found {
				return fmt.Errorf("unknown notifier %q", name)
			}
			events := slices.Clone(options.notificationEvents)
			if len(events) == 0 {
				events = slices.Clone(configuration.Notifiers[name].Events)
			}
			request.ExecutionPolicy.Notifications = append(request.ExecutionPolicy.Notifications,
				model.NotificationSubscription{Notifier: name, Events: events})
		}
	} else if flagChanged(command, "notify-on") {
		if len(request.ExecutionPolicy.Notifications) == 0 {
			return errors.New("--notify-on requires an effective notifier selected by --notify or configuration")
		}
		for index := range request.ExecutionPolicy.Notifications {
			request.ExecutionPolicy.Notifications[index].Events = slices.Clone(options.notificationEvents)
		}
	}
	request.ExecutionPolicy.NotifierDefinitions, err = notifierDefinitions(
		configuration,
		request.ExecutionPolicy.Notifications,
	)
	if err != nil {
		return err
	}

	return request.ExecutionPolicy.Validate(request.StdinPolicy)
}

func attachForeground(command *cobra.Command, backend app.Backend, job model.JobState) error {
	lifecycle, lifecycleOK := backend.(app.LifecycleBackend)
	follower, followerOK := backend.(app.FollowBackend)
	input, inputOK := backend.(app.InputBackend)
	if !lifecycleOK || !followerOK || !inputOK {
		return errors.New("application backend does not support foreground attachment")
	}
	followCtx, cancelFollow := context.WithCancel(command.Context())
	defer cancelFollow()
	inputCtx, cancelInput := context.WithCancel(command.Context())
	defer cancelInput()
	followErrors := make(chan error, 2)
	inputErrors := make(chan error, 1)
	go func() {
		followErrors <- follower.FollowLogs(followCtx, job.ID.String(), app.LogStdout, 0, command.OutOrStdout())
	}()
	go func() {
		followErrors <- follower.FollowLogs(followCtx, job.ID.String(), app.LogStderr, 0, command.ErrOrStderr())
	}()
	go func() { inputErrors <- pumpForegroundInput(inputCtx, input, job.ID.String(), command.InOrStdin()) }()

	completed, err := lifecycle.Wait(command.Context(), job.ID.String())
	cancelInput()
	if err != nil {
		cancelFollow()
	}
	for range 2 {
		followErr := <-followErrors
		if followErr != nil && !errors.Is(followErr, context.Canceled) {
			err = errors.Join(err, followErr)
		}
	}
	select {
	case inputErr := <-inputErrors:
		if inputErr != nil && !errors.Is(inputErr, context.Canceled) {
			err = errors.Join(err, inputErr)
		}
	default:
		// A terminal read cannot necessarily be interrupted by context
		// cancellation. The buffered result channel lets that goroutine finish
		// later without delaying delivery of already-durable target output.
	}
	if err != nil {
		return err
	}
	return completedJobError(completed)
}

func pumpForegroundInput(ctx context.Context, backend app.InputBackend, selector string, source io.Reader) error {
	buffer := make([]byte, 64*1024)
	for {
		count, err := source.Read(buffer)
		if count > 0 {
			if _, sendErr := backend.SendInput(ctx, selector, bytes.NewReader(buffer[:count]), false); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			_, sendErr := backend.SendInput(ctx, selector, strings.NewReader(""), true)
			return sendErr
		}
		if err != nil {
			return fmt.Errorf("read foreground input: %w", err)
		}
	}
}

func waitForSubmittedJob(ctx context.Context, backend app.Backend, selector string) error {
	lifecycle, ok := backend.(app.LifecycleBackend)
	if !ok {
		return errors.New("application backend does not support waiting")
	}
	job, err := lifecycle.Wait(ctx, selector)
	if err != nil {
		return err
	}
	return completedJobError(job)
}

func completedJobError(job model.JobState) error {
	if job.Outcome != model.JobOutcomeSuccess {
		return fmt.Errorf("job %s completed with outcome %s", job.ID, job.Outcome)
	}

	return nil
}

//nolint:cyclop // Source exclusivity and Cobra retrieval failures are all reported as usage errors.
func validateRunArguments(command *cobra.Command, arguments []string) error {
	if len(arguments) > 0 && command.ArgsLenAtDash() < 0 {
		return errors.New("target command must follow --")
	}
	jobSpec, err := command.Flags().GetString("job-spec")
	if err != nil {
		return fmt.Errorf("read --job-spec: %w", err)
	}
	rerun, err := command.Flags().GetString("rerun")
	if err != nil {
		return fmt.Errorf("read --rerun: %w", err)
	}
	profiles, err := command.Flags().GetStringArray("profile")
	if err != nil {
		return fmt.Errorf("read --profile: %w", err)
	}
	if rerun != "" && (len(arguments) > 0 || jobSpec != "" || len(profiles) > 0) {
		return errors.New("--rerun is mutually exclusive with a command, --job-spec, and --profile")
	}
	if len(arguments) > 0 && jobSpec != "" {
		return errors.New("a target command and --job-spec are mutually exclusive")
	}
	if len(arguments) == 0 && jobSpec == "" && len(profiles) == 0 && rerun == "" {
		return errors.New("target command must follow -- or be supplied by --job-spec, --profile, or --rerun")
	}

	return nil
}

func validateRerunOptions(command *cobra.Command, options *runOptions) error {
	if options.foreground {
		return errors.New("--foreground cannot be combined with --rerun; use logs --follow and input explicitly")
	}
	allowed := map[string]struct{}{
		"config": {}, "name": {}, "rerun": {}, "state-dir": {}, "wait": {},
	}
	var incompatible []string
	command.Flags().Visit(func(flag *pflag.Flag) {
		if _, ok := allowed[flag.Name]; !ok {
			incompatible = append(incompatible, "--"+flag.Name)
		}
	})
	if len(incompatible) > 0 {
		return fmt.Errorf("--rerun copies the prior effective specification and cannot be combined with %s", strings.Join(incompatible, ", "))
	}

	return nil
}

func parseEnvironment(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, value := range values {
		name, content, ok := strings.Cut(value, "=")
		if !ok || name == "" || strings.ContainsRune(name, '\x00') || strings.ContainsRune(name, '=') {
			return nil, fmt.Errorf("invalid --env %q: expected NAME=VALUE", value)
		}
		result[name] = content
	}

	return result, nil
}
