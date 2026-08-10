package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/failureclass"
	"github.com/ryancswallace/jobman/internal/logstore"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
	"github.com/ryancswallace/jobman/internal/store"
	"github.com/ryancswallace/jobman/internal/systemcontext"
)

const (
	defaultEvidenceLogBytes     = 64 * 1024
	maximumEvidenceLogBytes     = 1024 * 1024
	maximumEvidenceSimilar      = 20
	maximumEvidenceRuns         = 100
	maximumEvidenceEvents       = 500
	maximumEvidenceNotifies     = 200
	maximumEvidenceCommandBytes = 128 * 1024
	maximumEvidencePathBytes    = 4096
	jobSnapshotSourceKind       = "job_snapshot"
)

// DiagnosticEvidence returns a sealed factual evidence snapshot. It performs
// bounded lifecycle reconciliation before taking the final metadata snapshot,
// but it never interprets target output or invokes a model.
func (service *Service) DiagnosticEvidence(
	ctx context.Context,
	request diagnostic.EvidenceRequest,
	sanitizer diagnostic.Sanitizer,
) (diagnostic.Evidence, error) {
	request, err := normalizeEvidenceRequest(request)
	if err != nil {
		return diagnostic.Evidence{}, err
	}
	limits := store.DiagnosticSnapshotLimits{
		SelectedRun: request.Run, MaxRuns: maximumEvidenceRuns,
		MaxEvents: maximumEvidenceEvents, MaxNotifications: maximumEvidenceNotifies,
		MaxSimilar: int(request.Similar), // #nosec G115 -- normalizeEvidenceRequest bounds this to 20.
	}
	snapshot, err := service.store.GetDiagnosticSnapshot(ctx, request.Selector, limits)
	if err != nil {
		return diagnostic.Evidence{}, translateStoreError("collect diagnostic snapshot", err)
	}
	reconciled, err := service.reconcileDiagnosticSnapshot(ctx, snapshot)
	if err != nil {
		return diagnostic.Evidence{}, err
	}
	if reconciled {
		snapshot, err = service.store.GetDiagnosticSnapshot(ctx, request.Selector, limits)
		if err != nil {
			return diagnostic.Evidence{}, translateStoreError("reload diagnostic snapshot", err)
		}
	}

	capturedAt := service.now().UTC()
	selected, err := selectEvidenceRuns(snapshot.Job, snapshot.Runs, snapshot.TotalRuns, request)
	if err != nil {
		return diagnostic.Evidence{}, err
	}
	collector := newEvidenceCollector(service, snapshot, selected, capturedAt, sanitizer)
	if collectErr := collector.collect(ctx, request); collectErr != nil {
		return diagnostic.Evidence{}, collectErr
	}
	sealed, err := diagnostic.Seal(collector.finish())
	if err != nil {
		return diagnostic.Evidence{}, fmt.Errorf("seal diagnostic evidence: %w", err)
	}

	return sealed, nil
}

func normalizeEvidenceRequest(request diagnostic.EvidenceRequest) (diagnostic.EvidenceRequest, error) {
	if request.Selector == "" {
		return diagnostic.EvidenceRequest{}, errors.New("collect diagnostic evidence: selector must not be empty")
	}
	if request.Run != 0 && request.AllRuns {
		return diagnostic.EvidenceRequest{}, errors.New("collect diagnostic evidence: run and all-runs are mutually exclusive")
	}
	if request.Logs == "" {
		request.Logs = diagnostic.LogsMetadata
	}
	switch request.Logs {
	case diagnostic.LogsMetadata, diagnostic.LogsTail, diagnostic.LogsNone:
	default:
		return diagnostic.EvidenceRequest{}, fmt.Errorf("collect diagnostic evidence: invalid log mode %q", request.Logs)
	}
	if request.LogBytes > maximumEvidenceLogBytes {
		return diagnostic.EvidenceRequest{}, fmt.Errorf(
			"collect diagnostic evidence: log byte limit exceeds %d", maximumEvidenceLogBytes,
		)
	}
	if request.Logs == diagnostic.LogsTail && request.LogBytes == 0 {
		request.LogBytes = defaultEvidenceLogBytes
	}
	if request.Logs != diagnostic.LogsTail && request.LogBytes != 0 {
		return diagnostic.EvidenceRequest{}, errors.New(
			"collect diagnostic evidence: log bytes may be set only when logs=tail",
		)
	}
	if request.Similar > maximumEvidenceSimilar {
		return diagnostic.EvidenceRequest{}, fmt.Errorf(
			"collect diagnostic evidence: similar history limit exceeds %d", maximumEvidenceSimilar,
		)
	}

	return request, nil
}

func (service *Service) reconcileDiagnosticSnapshot(
	ctx context.Context,
	snapshot store.DiagnosticSnapshot,
) (bool, error) {
	if reconciled, err := service.reconcileExpiredSubmission(ctx, snapshot.Job); err != nil {
		return false, err
	} else if reconciled {
		return true, nil
	}

	return service.reconcileStaleOwnership(ctx, snapshot.Job, snapshot.Runs)
}

func selectEvidenceRuns(
	job model.JobState,
	runs []model.RunState,
	totalRuns uint64,
	request diagnostic.EvidenceRequest,
) ([]model.RunState, error) {
	if request.AllRuns {
		return slices.Clone(runs), nil
	}
	if request.Run != 0 {
		wanted := requestedRunNumber(request.Run, totalRuns)
		for _, run := range runs {
			if run.Number == wanted {
				return []model.RunState{run}, nil
			}
		}

		return nil, fmt.Errorf("collect diagnostic evidence: run %d: %w", request.Run, ErrNotFound)
	}
	if job.ActiveRunID.Valid() {
		for _, run := range runs {
			if run.ID == job.ActiveRunID {
				return []model.RunState{run}, nil
			}
		}
	}
	for index := len(runs) - 1; index >= 0; index-- {
		if runs[index].Outcome != "" && runs[index].Outcome != model.RunOutcomeSuccess {
			return []model.RunState{runs[index]}, nil
		}
	}
	if len(runs) > 0 {
		return []model.RunState{runs[len(runs)-1]}, nil
	}

	return []model.RunState{}, nil
}

func requestedRunNumber(selected int64, total uint64) uint64 {
	if selected > 0 {
		return uint64(selected)
	}
	if selected == 0 || total > math.MaxInt64 {
		return 0
	}
	index := int64(total) + selected + 1
	if index < 1 {
		return 0
	}

	return uint64(index)
}

type evidenceCollector struct {
	service     *Service
	snapshot    store.DiagnosticSnapshot
	selected    []model.RunState
	capturedAt  modelTime
	sanitizer   diagnostic.Sanitizer
	encodeValue func(any) (json.RawMessage, error)
	evidence    diagnostic.Evidence
	omissions   map[string]map[string]struct{}
	redactions  map[string]map[string]struct{}
	redacted    map[string]uint64
}

// modelTime aliases time.Time only to keep the collector initializer compact.
type modelTime = time.Time

func newEvidenceCollector(
	service *Service,
	snapshot store.DiagnosticSnapshot,
	selected []model.RunState,
	capturedAt modelTime,
	sanitizer diagnostic.Sanitizer,
) *evidenceCollector {
	selectedNumbers := make([]uint64, 0, len(selected))
	for _, run := range selected {
		selectedNumbers = append(selectedNumbers, run.Number)
	}
	active := snapshot.Job.Phase != model.JobPhaseCompleted
	capabilities := []string{
		"diagnostic_records_v1", "failure_fingerprints_v1", "lifecycle_events",
		"log_metadata", "log_tail", "notification_history", "resource_observations_v1",
		"execution_context_v1", "execution_policy_v1", "similar_history_v1",
		"system_context_v1", "transactional_snapshot", "wait_evaluations",
	}
	if reporter, ok := sanitizer.(diagnostic.ValueRedactionReporter); ok && reporter.ValueRedactionConfigured() {
		capabilities = append(capabilities, "configured_value_redaction_v1")
	}
	return &evidenceCollector{
		service: service, snapshot: snapshot, selected: selected,
		capturedAt: capturedAt, sanitizer: sanitizer, encodeValue: diagnostic.JSONValue,
		evidence: diagnostic.Evidence{
			CapturedAt: capturedAt,
			Source: diagnostic.Source{
				JobmanVersion: buildinfo.Version, CollectorVersion: diagnostic.CollectorVersion,
				StoreSchemaVersion: service.store.SchemaVersion(), Platform: runtime.GOOS,
				Capabilities: capabilities,
			},
			Subject: diagnostic.Subject{
				JobID: snapshot.Job.ID.String(), JobRevision: snapshot.Job.Revision,
				SelectedRuns: selectedNumbers, Phase: string(snapshot.Job.Phase),
				Outcome: string(snapshot.Job.Outcome),
			},
			Consistency: diagnostic.Consistency{
				Metadata:  diagnostic.MetadataTransactionalSnapshot,
				Artifacts: diagnostic.ArtifactsNotCollected, ActiveStateMayHaveAdvanced: active,
			},
			Items: []diagnostic.Item{}, Artifacts: []diagnostic.Artifact{},
			Omissions: []diagnostic.Omission{}, RedactionNotices: []diagnostic.RedactionNotice{},
		},
		omissions:  make(map[string]map[string]struct{}),
		redactions: make(map[string]map[string]struct{}), redacted: make(map[string]uint64),
	}
}

//nolint:gocognit,cyclop // Collection keeps the stable domain order explicit while delegating each bounded domain.
func (collector *evidenceCollector) collect(ctx context.Context, request diagnostic.EvidenceRequest) error {
	if err := collector.collectJob(); err != nil {
		return err
	}
	if err := collector.collectExecutionPolicy(); err != nil {
		return err
	}
	if request.IncludeCommand {
		if err := collector.collectCommands(); err != nil {
			return err
		}
	}
	if request.IncludePaths {
		if err := collector.collectPaths(); err != nil {
			return err
		}
	}
	if request.IncludeEnvironmentNames {
		if err := collector.collectEnvironmentNames(); err != nil {
			return err
		}
	}
	if request.IncludeSystem {
		if err := collector.collectSystem(); err != nil {
			return err
		}
	}
	for _, run := range collector.snapshot.Runs {
		if err := collector.collectRun(run, request.Logs != diagnostic.LogsNone); err != nil {
			return err
		}
		if err := collector.collectRunFacts(run); err != nil {
			return err
		}
	}
	if err := collector.collectRuntime(); err != nil {
		return err
	}
	if err := collector.collectDependencies(); err != nil {
		return err
	}
	if err := collector.collectWaits(); err != nil {
		return err
	}
	if err := collector.collectAdmission(); err != nil {
		return err
	}
	if err := collector.collectNotifications(); err != nil {
		return err
	}
	if err := collector.collectEvents(); err != nil {
		return err
	}
	if err := collector.collectSimilar(); err != nil {
		return err
	}
	if err := collector.collectLogs(ctx, request); err != nil {
		return err
	}
	collector.addCollectionOmissions(request)

	return nil
}

func (collector *evidenceCollector) collectSystem() error {
	value := systemcontext.Observe(collector.service.stateDir)
	if valid := value.Validate() == nil; !valid {
		collector.omit(diagnostic.OmissionSystemContextUnavailable, "system_context")

		return nil
	}

	return collector.add(
		"ev:system:context",
		diagnostic.CodeSystemContext,
		value,
		nil,
		diagnostic.ItemSource{Kind: "system_probe"},
		diagnostic.QualityPointInTime,
	)
}

func (collector *evidenceCollector) collectCommands() error {
	job := collector.snapshot.Job
	source := diagnostic.ItemSource{Kind: jobSnapshotSourceKind, EntityID: job.ID.String(), Revision: job.Revision}
	if err := collector.addCommand(
		"ev:job:target:command", diagnostic.CodeTargetCommand, "job.spec.command",
		job.Spec.Executable(), job.Spec.Arguments(), &job.SubmittedAt, source,
	); err != nil {
		return err
	}
	configuration := job.Spec.ExecutionPolicy()
	for index, condition := range configuration.WaitConditions {
		if condition.Kind != model.WaitProbe {
			continue
		}
		id := fmt.Sprintf("ev:wait:%06d:command", index)
		if err := collector.addCommand(id, diagnostic.CodeWaitCommand, fmt.Sprintf("job.policy.wait.%d.command", index),
			condition.Probe.Executable, condition.Probe.Arguments, &job.SubmittedAt, source); err != nil {
			return err
		}
	}
	for index, notifier := range configuration.NotifierDefinitions {
		if notifier.Command == nil {
			continue
		}
		id := fmt.Sprintf("ev:notifier:%06d:command", index)
		if err := collector.addCommand(id, diagnostic.CodeNotifierCommand,
			fmt.Sprintf("job.policy.notifier.%d.command", index), notifier.Command.Executable,
			notifier.Command.Arguments, &job.SubmittedAt, source); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) addCommand(
	id, code, field, executable string,
	arguments []string,
	observedAt *modelTime,
	source diagnostic.ItemSource,
) error {
	command := diagnostic.Command{
		Executable: collector.sanitizeText(id, field+".executable", executable),
		Arguments:  make([]string, len(arguments)),
	}
	for index, argument := range arguments {
		command.Arguments[index] = collector.sanitizeText(
			id, fmt.Sprintf("%s.arguments.%d", field, index), argument,
		)
	}
	encoded, err := collector.encodeValue(command)
	if err != nil {
		return fmt.Errorf("encode diagnostic command %s: %w", id, err)
	}
	if len(encoded) > maximumEvidenceCommandBytes || !validDiagnosticCommand(command) {
		collector.omit(diagnostic.OmissionCommandLimitExceeded, strings.TrimPrefix(id, "ev:"))

		return nil
	}

	return collector.addWithDisclosure(id, code, command, observedAt, source,
		diagnostic.QualityObserved, diagnostic.DisclosureCommand)
}

func validDiagnosticCommand(command diagnostic.Command) bool { return command.Validate() == nil }

//nolint:gocognit // Path projection keeps each source kind and disclosure ID explicit and auditable.
func (collector *evidenceCollector) collectPaths() error {
	job := collector.snapshot.Job
	source := diagnostic.ItemSource{Kind: jobSnapshotSourceKind, EntityID: job.ID.String(), Revision: job.Revision}
	if err := collector.addPath("ev:job:target:working_directory", diagnostic.CodeTargetWorkingDirectory,
		"job.spec.working_directory", job.Spec.WorkingDirectory(), &job.SubmittedAt, source); err != nil {
		return err
	}
	configuration := job.Spec.ExecutionPolicy()
	if configuration.StdinPath != "" {
		if err := collector.addPath("ev:job:target:stdin_path", diagnostic.CodeTargetStdinPath,
			"job.policy.stdin_path", configuration.StdinPath, &job.SubmittedAt, source); err != nil {
			return err
		}
	}
	for index, condition := range configuration.WaitConditions {
		path := condition.Path
		field := "path"
		if condition.Kind == model.WaitProbe {
			path = condition.ProbeDirectory
			field = "working_directory"
		}
		if path == "" {
			continue
		}
		id := fmt.Sprintf("ev:wait:%06d:%s", index, field)
		if err := collector.addPath(id, diagnostic.CodeWaitPath,
			fmt.Sprintf("job.policy.wait.%d.%s", index, field), path, &job.SubmittedAt, source); err != nil {
			return err
		}
	}
	for index, notifier := range configuration.NotifierDefinitions {
		if notifier.Command == nil || notifier.Command.WorkingDirectory == "" {
			continue
		}
		id := fmt.Sprintf("ev:notifier:%06d:working_directory", index)
		if err := collector.addPath(id, diagnostic.CodeNotifierWorkingDirectory,
			fmt.Sprintf("job.policy.notifier.%d.working_directory", index),
			notifier.Command.WorkingDirectory, &job.SubmittedAt, source); err != nil {
			return err
		}
	}
	for _, run := range collector.snapshot.Runs {
		if run.ResolvedExecutable == "" {
			continue
		}
		id := fmt.Sprintf("ev:run:%020d:resolved_executable", run.Number)
		runSource := diagnostic.ItemSource{Kind: "run_snapshot", EntityID: run.ID.String(), Revision: run.Revision}
		if err := collector.addPath(id, diagnostic.CodeRunResolvedExecutable,
			fmt.Sprintf("run.%d.resolved_executable", run.Number), run.ResolvedExecutable,
			run.StartedAt, runSource); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) addPath(
	id, code, field, value string,
	observedAt *modelTime,
	source diagnostic.ItemSource,
) error {
	value = collector.sanitizeText(id, field, value)
	if value == "" || len(value) > maximumEvidencePathBytes {
		collector.omit(diagnostic.OmissionPathLimitExceeded, strings.TrimPrefix(id, "ev:"))

		return nil
	}

	return collector.addWithDisclosure(id, code, value, observedAt, source,
		diagnostic.QualityObserved, diagnostic.DisclosurePath)
}

func (collector *evidenceCollector) collectEnvironmentNames() error {
	job := collector.snapshot.Job
	configuration := job.Spec.ExecutionPolicy()
	source := diagnostic.ItemSource{Kind: jobSnapshotSourceKind, EntityID: job.ID.String(), Revision: job.Revision}
	target := diagnostic.EnvironmentNames{
		Inheritance: string(job.Spec.EnvironmentInheritance()),
		Set:         stringMapKeys(job.Spec.Environment()),
		Unset:       job.Spec.UnsetEnvironment(),
		Secret:      secretReferenceKeys(configuration.SecretEnv),
	}
	if err := collector.addEnvironmentNames("ev:job:target:environment_names",
		diagnostic.CodeTargetEnvironmentNames, "job.spec.environment", target, &job.SubmittedAt, source); err != nil {
		return err
	}
	for index, condition := range configuration.WaitConditions {
		if condition.Kind != model.WaitProbe || len(condition.ProbeEnvironment)+len(condition.ProbeUnsetEnvironment)+
			len(condition.ProbeSecretEnv) == 0 {
			continue
		}
		value := diagnostic.EnvironmentNames{
			Set: stringMapKeys(condition.ProbeEnvironment), Unset: slices.Clone(condition.ProbeUnsetEnvironment),
			Secret: secretReferenceKeys(condition.ProbeSecretEnv),
		}
		id := fmt.Sprintf("ev:wait:%06d:environment_names", index)
		if err := collector.addEnvironmentNames(id, diagnostic.CodeWaitEnvironmentNames,
			fmt.Sprintf("job.policy.wait.%d.environment", index), value, &job.SubmittedAt, source); err != nil {
			return err
		}
	}
	for index, notifier := range configuration.NotifierDefinitions {
		if notifier.Command == nil || len(notifier.Command.Environment)+len(notifier.Command.SecretEnvironment) == 0 {
			continue
		}
		value := diagnostic.EnvironmentNames{
			Set: stringMapKeys(notifier.Command.Environment), Secret: secretReferenceKeys(notifier.Command.SecretEnvironment),
		}
		id := fmt.Sprintf("ev:notifier:%06d:environment_names", index)
		if err := collector.addEnvironmentNames(id, diagnostic.CodeNotifierEnvironmentNames,
			fmt.Sprintf("job.policy.notifier.%d.environment", index), value, &job.SubmittedAt, source); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) addEnvironmentNames(
	id, code, field string,
	value diagnostic.EnvironmentNames,
	observedAt *modelTime,
	source diagnostic.ItemSource,
) error {
	value.Inheritance = collector.sanitizeText(id, field+".inheritance", value.Inheritance)
	value.Set = collector.sanitizeNameList(id, field+".set", value.Set)
	value.Unset = collector.sanitizeNameList(id, field+".unset", value.Unset)
	value.Secret = collector.sanitizeNameList(id, field+".secret", value.Secret)
	encoded, err := collector.encodeValue(value)
	if err != nil {
		return fmt.Errorf("encode diagnostic environment names %s: %w", id, err)
	}
	if len(encoded) > maximumEvidenceCommandBytes || !validDiagnosticEnvironmentNames(value) {
		collector.omit(diagnostic.OmissionEnvironmentNamesLimitExceeded, strings.TrimPrefix(id, "ev:"))

		return nil
	}

	return collector.addWithDisclosure(id, code, value, observedAt, source,
		diagnostic.QualityObserved, diagnostic.DisclosureEnvironmentName)
}

func validDiagnosticEnvironmentNames(value diagnostic.EnvironmentNames) bool {
	return value.Validate() == nil
}

func (collector *evidenceCollector) sanitizeNameList(affects, field string, values []string) []string {
	result := make([]string, 0, len(values))
	for index, value := range values {
		value = collector.sanitizeText(affects, fmt.Sprintf("%s.%d", field, index), value)
		if value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)

	return slices.Compact(result)
}

func stringMapKeys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	sort.Strings(result)

	return result
}

func secretReferenceKeys(values map[string]model.SecretReference) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	sort.Strings(result)

	return result
}

//nolint:cyclop // The stable summary explicitly maps every policy union without serializing the internal specification.
func (collector *evidenceCollector) collectExecutionPolicy() error {
	job := collector.snapshot.Job
	// Store snapshots always contain a validated immutable specification. Some
	// collector-focused tests construct intentionally partial snapshots to
	// exercise unrelated evidence domains.
	if job.Spec.Executable() == "" {
		return nil
	}
	configuration := job.Spec.ExecutionPolicy()
	source := diagnostic.ItemSource{Kind: jobSnapshotSourceKind, EntityID: job.ID.String(), Revision: job.Revision}
	successExitCodes := slices.Clone(configuration.Classification.SuccessExitCodes)
	if successExitCodes == nil {
		successExitCodes = []int{0}
	}
	retryableRanges := make([]diagnostic.ExitCodeRange, 0, len(configuration.Classification.RetryableExitCodes))
	for _, configured := range configuration.Classification.RetryableExitCodes {
		retryableRanges = append(retryableRanges, diagnostic.ExitCodeRange{First: configured.First, Last: configured.Last})
	}
	waitKinds := make([]string, 0, len(configuration.WaitConditions))
	for _, condition := range configuration.WaitConditions {
		waitKinds = append(waitKinds, string(condition.Kind))
	}
	notifierKinds := make([]string, 0, len(configuration.NotifierDefinitions))
	for _, notifier := range configuration.NotifierDefinitions {
		notifierKinds = append(notifierKinds, string(notifier.Kind))
	}
	sort.Strings(notifierKinds)
	summary := diagnostic.ExecutionPolicy{
		StdinPolicy: string(job.Spec.StdinPolicy()), Foreground: configuration.Foreground,
		StopGracePeriod: job.Spec.StopPolicy().GracePeriod.String(),
		ForceAfterGrace: job.Spec.StopPolicy().ForceAfterGrace,
		RunTimeout:      configuration.RunTimeout.String(), JobTimeout: configuration.JobTimeout.String(),
		Completion: diagnostic.CompletionPolicy{
			MaximumRuns:   countLimit(configuration.Completion.MaxRuns.Value, configuration.Completion.MaxRuns.Unlimited),
			SuccessTarget: countLimit(configuration.Completion.SuccessTarget.Value, configuration.Completion.SuccessTarget.Unlimited),
			FailureLimit:  countLimit(configuration.Completion.FailureLimit.Value, configuration.Completion.FailureLimit.Unlimited),
		},
		Classification: diagnostic.ClassificationPolicy{
			SuccessExitCodes: successExitCodes, RetryableExitCodes: retryableRanges,
			UnlistedNonzeroExitCodesRetryable: configuration.Classification.RetryableExitCodes == nil,
			RetryableSignals: collector.sanitizeNameList("ev:job:policy", "job.policy.retryable_signals",
				configuration.Classification.RetryableSignals),
			RetryablePlatformReasons: collector.sanitizeNameList("ev:job:policy", "job.policy.retryable_platform_reasons",
				configuration.Classification.RetryablePlatformReasons),
			RetryTimeout:      configuration.Classification.RetryTimeout,
			RetryStartFailure: configuration.Classification.RetryStartFailure,
			RetryCancellation: configuration.Classification.RetryCancellation,
		},
		FailureDelay: diagnosticDelayPolicy(configuration.FailureDelay),
		SuccessDelay: diagnosticDelayPolicy(configuration.SuccessDelay),
		WaitMode:     string(configuration.WaitMode), WaitConditionKinds: waitKinds,
		DependencyCount:               uint64(len(configuration.Dependencies)),
		ConcurrencyPool:               collector.sanitizeText("ev:job:policy", "job.policy.concurrency_pool", configuration.Concurrency.Pool),
		ConcurrencySlots:              configuration.Concurrency.Slots,
		NotificationSubscriptionCount: uint64(len(configuration.Notifications)), NotifierKinds: notifierKinds,
		Tags:       collector.sanitizeNameList("ev:job:policy", "job.policy.tags", configuration.Tags),
		Groups:     collector.sanitizeNameList("ev:job:policy", "job.policy.groups", configuration.Groups),
		LogCapture: configuration.LogCapture, LogRotateBytes: configuration.LogRotateSize,
		LogMaximumSegmentsPerStream: configuration.LogMaxSegmentsPerStream,
		LogRetention:                diagnosticLogRetention(configuration),
	}
	if configuration.Completion.HasRetryAbortAt {
		value := configuration.Completion.RetryAbortAt.UTC().Round(0)
		summary.Completion.RetryAbortAt = &value
	}
	if err := collector.add("ev:job:policy", diagnostic.CodeExecutionPolicy, summary,
		&job.SubmittedAt, source, diagnostic.QualityObserved); err != nil {
		return err
	}
	for index, condition := range configuration.WaitConditions {
		value := map[string]any{
			"index": index, "kind": string(condition.Kind), "poll_interval": condition.PollInterval.String(),
		}
		if !condition.AbortAt.IsZero() {
			value["abort_at"] = condition.AbortAt.UTC().Round(0)
		}
		switch condition.Kind {
		case model.WaitUntil:
			value["until"] = condition.Until.UTC().Round(0)
		case model.WaitDelay:
			value["delay"] = condition.Delay.String()
		case model.WaitFileExists:
			value["file_kind"] = string(condition.FileKind)
		case model.WaitProbe:
			value["probe_timeout"] = condition.Probe.Timeout.String()
			value["probe_output_limit"] = condition.Probe.OutputLimit
			value["probe_fatal_on_error"] = condition.Probe.FatalOnError
		}
		if err := collector.add(fmt.Sprintf("ev:wait:%06d:configuration", index),
			diagnostic.CodeWaitConfiguration, value, &job.SubmittedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	subscriptions := make(map[string][]string, len(configuration.Notifications))
	for _, subscription := range configuration.Notifications {
		subscriptions[subscription.Notifier] = slices.Clone(subscription.Events)
	}
	for index, notifier := range configuration.NotifierDefinitions {
		id := fmt.Sprintf("ev:notifier:%06d:configuration", index)
		value := map[string]any{
			"name": collector.sanitizeText(id, fmt.Sprintf("job.policy.notifier.%d.name", index), notifier.Name),
			"kind": string(notifier.Kind), "timeout": notifier.Timeout.String(),
			"maximum_attempts": notifier.Retry.MaxAttempts, "retry_delay": notifier.Retry.Delay.String(),
			"maximum_retry_delay": notifier.Retry.MaxDelay.String(),
			"subscribed_events":   subscriptions[notifier.Name],
		}
		switch {
		case notifier.Command != nil:
			value["output_limit"] = notifier.Command.OutputLimit
		case notifier.Webhook != nil:
			value["response_limit"] = notifier.Webhook.ResponseLimit
			value["allow_insecure_http"] = notifier.Webhook.AllowInsecureHTTP
			value["allow_private_network"] = notifier.Webhook.AllowPrivateNetwork
			value["follow_redirects"] = notifier.Webhook.FollowRedirects
		case notifier.SMTP != nil:
			value["mode"] = notifier.SMTP.Mode
			value["message_limit"] = notifier.SMTP.MessageLimit
		}
		if err := collector.add(id, diagnostic.CodeNotifierConfiguration, value,
			&job.SubmittedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}

	return nil
}

func countLimit(value uint64, unlimited bool) diagnostic.CountLimit {
	return diagnostic.CountLimit{Value: value, Unlimited: unlimited}
}

func diagnosticDelayPolicy(configuration policy.DelayPolicy) diagnostic.DelayPolicy {
	result := diagnostic.DelayPolicy{
		Base: configuration.Base.String(), Backoff: string(configuration.Backoff),
		ExponentialBase: configuration.ExponentialBase, Jitter: configuration.Jitter.String(),
	}
	if configuration.HasMaxDelay {
		result.Maximum = configuration.MaxDelay.String()
	}

	return result
}

func diagnosticLogRetention(configuration model.ExecutionPolicy) string {
	if configuration.LogRetentionUnlimited {
		return "unlimited"
	}

	return configuration.LogRetentionMaxAge.String()
}

//nolint:gocognit,cyclop // Job facts are projected in stable order with explicit optional-field handling.
func (collector *evidenceCollector) collectJob() error {
	job := collector.snapshot.Job
	source := diagnostic.ItemSource{Kind: jobSnapshotSourceKind, EntityID: job.ID.String(), Revision: job.Revision}
	if err := collector.collectJobName(job, source); err != nil {
		return err
	}
	if err := collector.add("ev:source:context", diagnostic.CodeSourceContext, map[string]any{
		"jobman_version": buildinfo.Version, "collector_version": diagnostic.CollectorVersion,
		"store_schema_version": collector.service.store.SchemaVersion(), "os": runtime.GOOS,
		"architecture": runtime.GOARCH,
	}, nil, diagnostic.ItemSource{Kind: "collector"}, diagnostic.QualityObserved); err != nil {
		return err
	}
	if err := collector.add("ev:job:phase", diagnostic.CodeJobPhase, string(job.Phase), nil, source, diagnostic.QualityObserved); err != nil {
		return err
	}
	if job.Outcome != "" {
		if err := collector.add("ev:job:outcome", diagnostic.CodeJobOutcome, string(job.Outcome), job.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	values := []struct {
		id   string
		code string
		at   *modelTime
	}{
		{"ev:job:submitted_at", diagnostic.CodeJobSubmittedAt, &job.SubmittedAt},
		{"ev:job:claimed_at", diagnostic.CodeJobClaimedAt, job.ClaimedAt},
		{"ev:job:started_at", diagnostic.CodeJobStartedAt, job.StartedAt},
		{"ev:job:completed_at", diagnostic.CodeJobCompletedAt, job.CompletedAt},
	}
	if err := collector.add("ev:job:revision", diagnostic.CodeJobRevision, job.Revision, nil, source, diagnostic.QualityObserved); err != nil {
		return err
	}
	for _, value := range values {
		if value.at != nil {
			if err := collector.add(value.id, value.code, *value.at, value.at, source, diagnostic.QualityObserved); err != nil {
				return err
			}
		}
	}
	if job.Cancellation != nil {
		if err := collector.add("ev:job:cancellation:reason", diagnostic.CodeJobCancellationReason,
			string(job.Cancellation.Reason), &job.Cancellation.RequestedAt, source, diagnostic.QualityConfirmed); err != nil {
			return err
		}
		if err := collector.add("ev:job:cancellation:requested_at", diagnostic.CodeJobCancellationAt,
			job.Cancellation.RequestedAt, &job.Cancellation.RequestedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if job.LastDiagnosticCode != "" {
		if err := collector.addDiagnostic("ev:job:diagnostic", diagnostic.CodeJobDiagnostic,
			job.LastDiagnosticCode, job.CompletedAt, source); err != nil {
			return err
		}
	}
	if class, quality := classifyJobFailure(job, len(collector.snapshot.Runs)); class != "" {
		return collector.add("ev:job:failure_class", diagnostic.CodeFailureClass,
			map[string]string{"class": class, "scope": "job"}, job.CompletedAt, source, quality)
	}

	return nil
}

func (collector *evidenceCollector) collectJobName(job model.JobState, source diagnostic.ItemSource) error {
	if job.Spec.Name() == "" {
		return nil
	}
	name := collector.sanitizeText("ev:job:name", "job.spec.name", job.Spec.Name())
	if name == "" {
		return nil
	}

	return collector.add("ev:job:name", diagnostic.CodeJobName, name,
		&job.SubmittedAt, source, diagnostic.QualityObserved)
}

//nolint:gocognit,cyclop,nestif // One run is projected in stable domain order and each optional fact remains explicit.
func (collector *evidenceCollector) collectRun(run model.RunState, includeLogMetadata bool) error {
	prefix := fmt.Sprintf("ev:run:%020d:", run.Number)
	source := diagnostic.ItemSource{Kind: "run_snapshot", EntityID: run.ID.String(), Revision: run.Revision}
	base := []struct {
		name string
		code string
		data any
	}{
		{"phase", diagnostic.CodeRunPhase, string(run.Phase)},
		{"revision", diagnostic.CodeRunRevision, run.Revision},
		{"reserved_at", diagnostic.CodeRunReservedAt, run.ReservedAt},
	}
	for _, value := range base {
		if err := collector.add(prefix+value.name, value.code, value.data, nil, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if run.Outcome != "" {
		if err := collector.add(prefix+"outcome", diagnostic.CodeRunOutcome, string(run.Outcome), run.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	for _, value := range []struct {
		name string
		code string
		at   *modelTime
	}{
		{"started_at", diagnostic.CodeRunStartedAt, run.StartedAt},
		{"completed_at", diagnostic.CodeRunCompletedAt, run.CompletedAt},
	} {
		if value.at != nil {
			if err := collector.add(prefix+value.name, value.code, *value.at, value.at, source, diagnostic.QualityObserved); err != nil {
				return err
			}
		}
	}
	if duration, observedAt, ok := runDuration(run, collector.capturedAt); ok {
		if err := collector.add(prefix+"duration", diagnostic.CodeRunDuration, duration.String(), observedAt,
			source, diagnostic.QualityDerivedExact); err != nil {
			return err
		}
	}
	if run.Exit != nil {
		if run.Exit.ExitCode != nil {
			if err := collector.add(prefix+"exit:code", diagnostic.CodeRunExitCode, *run.Exit.ExitCode,
				&run.Exit.ObservedAt, source, diagnostic.QualityObserved); err != nil {
				return err
			}
		}
		if run.Exit.Signal != "" {
			value := collector.sanitizeText(prefix+"exit:signal", "run.exit.signal", run.Exit.Signal)
			if err := collector.add(prefix+"exit:signal", diagnostic.CodeRunExitSignal, value,
				&run.Exit.ObservedAt, source, diagnostic.QualityConfirmed); err != nil {
				return err
			}
		}
		if run.Exit.PlatformReason != "" {
			value := collector.sanitizeText(prefix+"exit:platform_reason", "run.exit.platform_reason", run.Exit.PlatformReason)
			if err := collector.add(prefix+"exit:platform_reason", diagnostic.CodeRunExitPlatformReason, value,
				&run.Exit.ObservedAt, source, diagnostic.QualityObserved); err != nil {
				return err
			}
		}
	}
	if run.LastDiagnosticCode != "" {
		if err := collector.addDiagnostic(prefix+"diagnostic", diagnostic.CodeRunDiagnostic,
			run.LastDiagnosticCode, run.CompletedAt, source); err != nil {
			return err
		}
	}
	if run.StopReason != "" {
		if err := collector.add(prefix+"stop_reason", diagnostic.CodeRunStopReason, string(run.StopReason),
			run.StopRequestedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if run.StopReason == model.StopReasonTimeout {
		scope := "run"
		if run.LastDiagnosticCode == string(model.DiagnosticJobTimeout) {
			scope = "job"
		}
		if err := collector.add(prefix+"timeout_scope", diagnostic.CodeRunTimeoutScope, scope,
			run.StopRequestedAt, source, diagnostic.QualityConfirmed); err != nil {
			return err
		}
	}
	if class, quality := classifyRunFailure(run); class != "" {
		if err := collector.add(prefix+"failure_class", diagnostic.CodeFailureClass,
			map[string]string{"class": class, "scope": "run"}, run.CompletedAt, source, quality); err != nil {
			return err
		}
	}

	if includeLogMetadata {
		return collector.collectLogMetadata(run, prefix, source)
	}

	return nil
}

func (collector *evidenceCollector) collectLogMetadata(
	run model.RunState,
	prefix string,
	source diagnostic.ItemSource,
) error {
	values := []struct {
		name string
		code string
		data any
	}{
		{"logs:available", diagnostic.CodeLogAvailable, run.Logs.Available()},
		{"logs:integrity", diagnostic.CodeLogIntegrity, string(run.Logs.Integrity)},
		{"logs:recording_health", diagnostic.CodeLogRecordingHealth, string(run.Logs.RecordingHealth)},
		{"logs:stdout_bytes", diagnostic.CodeLogStdoutBytes, max(run.Logs.StdoutSize, 0)},
		{"logs:stderr_bytes", diagnostic.CodeLogStderrBytes, max(run.Logs.StderrSize, 0)},
	}
	for _, value := range values {
		if err := collector.add(prefix+value.name, value.code, value.data, run.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if run.Logs.DiagnosticCode != "" {
		return collector.addDiagnostic(prefix+"logs:diagnostic", diagnostic.CodeLogDiagnostic,
			run.Logs.DiagnosticCode, run.CompletedAt, source)
	}

	return nil
}

func (collector *evidenceCollector) collectRunFacts(run model.RunState) error {
	facts, found := collector.snapshot.RunFacts[run.ID]
	if !found {
		return nil
	}
	prefix := fmt.Sprintf("ev:run:%020d:", run.Number)
	source := diagnostic.ItemSource{
		Kind: "run_diagnostic_facts", EntityID: run.ID.String(), Revision: 1,
	}
	for _, observation := range facts.Resources {
		if err := collector.add(prefix+"resource:"+observation.Metric, diagnostic.CodeResourceObservation,
			observation, &facts.RecordedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if facts.Fingerprint != nil {
		return collector.addWithDisclosure(
			prefix+"failure:fingerprint",
			diagnostic.CodeFailureFingerprint,
			*facts.Fingerprint,
			&facts.RecordedAt,
			source,
			diagnostic.QualityDerivedExact,
			diagnostic.DisclosureLocalOnly,
		)
	}

	return nil
}

func (collector *evidenceCollector) collectSimilar() error {
	for index, failure := range collector.snapshot.SimilarFailures {
		id := fmt.Sprintf("ev:similar:%06d", index)
		source := diagnostic.ItemSource{Kind: "failure_fingerprint_index", EntityID: failure.RunID}
		if err := collector.addWithDisclosure(
			id,
			diagnostic.CodeSimilarFailure,
			failure,
			&failure.CompletedAt,
			source,
			diagnostic.QualityDerivedExact,
			diagnostic.DisclosureLocalOnly,
		); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) collectRuntime() error {
	runtimeState := collector.snapshot.Runtime
	source := diagnostic.ItemSource{Kind: "runtime_snapshot", EntityID: runtimeState.JobID.String(), Revision: runtimeState.Revision}
	values := []struct {
		name string
		code string
		data any
	}{
		{"run_count", diagnostic.CodeRuntimeRunCount, runtimeState.RunCount},
		{"success_count", diagnostic.CodeRuntimeSuccessCount, runtimeState.SuccessCount},
		{"failure_count", diagnostic.CodeRuntimeFailureCount, runtimeState.FailureCount},
		{"total_paused", diagnostic.CodeRuntimeTotalPaused, runtimeState.TotalPaused.String()},
	}
	for _, value := range values {
		if err := collector.add("ev:runtime:"+value.name, value.code, value.data, nil, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if runtimeState.NextRunAt != nil {
		if err := collector.add("ev:runtime:next_run_at", diagnostic.CodeRuntimeNextRunAt,
			*runtimeState.NextRunAt, runtimeState.NextRunAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if runtimeState.WaitingReason != "" {
		value := collector.sanitizeText("ev:runtime:waiting_reason", "runtime.waiting_reason", runtimeState.WaitingReason)
		if err := collector.add("ev:runtime:waiting_reason", diagnostic.CodeRuntimeWaitingReason,
			value, nil, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	if runtimeState.PausedFrom != "" {
		return collector.add("ev:runtime:paused_from", diagnostic.CodeRuntimePausedFrom,
			string(runtimeState.PausedFrom), runtimeState.PausedAt, source, diagnostic.QualityObserved)
	}

	return nil
}

func (collector *evidenceCollector) collectDependencies() error {
	for index, dependency := range collector.snapshot.Dependencies {
		prefix := fmt.Sprintf("ev:dependency:%06d:", index)
		source := diagnostic.ItemSource{Kind: "dependency_snapshot", EntityID: dependency.DependsOn.String(), Revision: dependency.ObservedRevision}
		if err := collector.add(prefix+"job_id", diagnostic.CodeDependencyJobID,
			dependency.DependsOn.String(), nil, source, diagnostic.QualityObserved); err != nil {
			return err
		}
		if err := collector.add(prefix+"predicate", diagnostic.CodeDependencyPredicate,
			string(dependency.Predicate), nil, source, diagnostic.QualityObserved); err != nil {
			return err
		}
		if dependency.ObservedOutcome != "" {
			if err := collector.add(prefix+"observed_outcome", diagnostic.CodeDependencyObservedOutcome,
				string(dependency.ObservedOutcome), dependency.SatisfiedAt, source, diagnostic.QualityObserved); err != nil {
				return err
			}
		}
		if err := collector.add(prefix+"satisfied", diagnostic.CodeDependencySatisfied,
			dependency.SatisfiedAt != nil, dependency.SatisfiedAt, source, diagnostic.QualityDerivedExact); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) collectWaits() error {
	for _, evaluation := range collector.snapshot.WaitEvaluations {
		prefix := fmt.Sprintf("ev:wait:%06d:", evaluation.ConditionIndex)
		source := diagnostic.ItemSource{Kind: "wait_snapshot", EntityID: fmt.Sprintf("condition:%d", evaluation.ConditionIndex)}
		if err := collector.add(prefix+"kind", diagnostic.CodeWaitKind, string(evaluation.ConditionKind),
			evaluation.EvaluatedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
		if err := collector.add(prefix+"attempt_count", diagnostic.CodeWaitAttempts, evaluation.AttemptCount,
			evaluation.EvaluatedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
		if err := collector.add(prefix+"satisfied", diagnostic.CodeWaitSatisfied, evaluation.SatisfiedAt != nil,
			evaluation.EvaluatedAt, source, diagnostic.QualityDerivedExact); err != nil {
			return err
		}
		if evaluation.LastDiagnosticCode != "" {
			if err := collector.addDiagnostic(prefix+"diagnostic", diagnostic.CodeWaitDiagnostic,
				evaluation.LastDiagnosticCode, evaluation.EvaluatedAt, source); err != nil {
				return err
			}
		}
	}

	return nil
}

func (collector *evidenceCollector) collectAdmission() error {
	if collector.snapshot.Admission == nil {
		return nil
	}
	admission := collector.snapshot.Admission
	source := diagnostic.ItemSource{Kind: "admission_snapshot", EntityID: collector.snapshot.Job.ID.String()}
	pool := collector.sanitizeText("ev:admission:pool", "admission.pool", admission.Pool)
	if err := collector.add("ev:admission:pool", diagnostic.CodeAdmissionPool, pool,
		&admission.AcquiredAt, source, diagnostic.QualityObserved); err != nil {
		return err
	}
	if err := collector.add("ev:admission:slots", diagnostic.CodeAdmissionSlots, admission.Slots,
		&admission.AcquiredAt, source, diagnostic.QualityObserved); err != nil {
		return err
	}
	if err := collector.add("ev:admission:lease_expires", diagnostic.CodeAdmissionLeaseExpires,
		admission.LeaseExpires, &admission.AcquiredAt, source, diagnostic.QualityObserved); err != nil {
		return err
	}

	return collector.add("ev:admission:released", diagnostic.CodeAdmissionReleased,
		admission.ReleasedAt != nil, admission.ReleasedAt, source, diagnostic.QualityDerivedExact)
}

//nolint:gocognit // Delivery and attempt unions retain explicit optional transport observations.
func (collector *evidenceCollector) collectNotifications() error {
	for index, delivery := range collector.snapshot.NotificationDeliveries {
		id := fmt.Sprintf("ev:notification:delivery:%06d:status", index)
		source := diagnostic.ItemSource{Kind: "notification_delivery", EntityID: delivery.EventID.String()}
		value := map[string]any{
			"notifier":   collector.sanitizeText(id, "notification.delivery.notifier", delivery.NotifierName),
			"event_type": delivery.EventType, "status": string(delivery.Status),
			"attempt_count": delivery.AttemptCount, "max_attempts": delivery.MaxAttempts,
		}
		if delivery.NextAttemptAt != nil {
			value["next_attempt_at"] = delivery.NextAttemptAt.UTC().Round(0)
		}
		if delivery.ClaimedAt != nil {
			value["claimed_at"] = delivery.ClaimedAt.UTC().Round(0)
		}
		if delivery.ClaimExpiresAt != nil {
			value["claim_expires_at"] = delivery.ClaimExpiresAt.UTC().Round(0)
		}
		if err := collector.add(id, diagnostic.CodeNotificationStatus, value,
			delivery.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}
	for index, attempt := range collector.snapshot.NotificationAttempts {
		prefix := fmt.Sprintf("ev:notification:attempt:%06d:", index)
		source := diagnostic.ItemSource{Kind: "notification_attempt", EntityID: attempt.ID.String()}
		status := map[string]any{
			"notifier":   collector.sanitizeText(prefix+"status", "notification.attempt.notifier", attempt.NotifierName),
			"event_type": attempt.EventType, "status": string(attempt.Status), "attempt": attempt.AttemptNumber,
			"response_status_code": attempt.ResponseStatusCode, "command_exit_code": attempt.CommandExitCode,
			"response_truncated": attempt.ResponseTruncated, "next_attempt_at": attempt.NextAttemptAt,
		}
		if attempt.StartedAt != nil {
			status["started_at"] = attempt.StartedAt.UTC().Round(0)
		}
		if attempt.MessageID != "" {
			status["message_id"] = collector.sanitizeText(prefix+"status", "notification.attempt.message_id", attempt.MessageID)
		}
		if err := collector.add(prefix+"status", diagnostic.CodeNotificationStatus,
			status, attempt.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
		if attempt.DiagnosticCode != "" {
			if err := collector.addDiagnostic(prefix+"diagnostic", diagnostic.CodeNotificationDiagnostic,
				attempt.DiagnosticCode, attempt.CompletedAt, source); err != nil {
				return err
			}
		}
		if err := collector.add(prefix+"retryable", diagnostic.CodeNotificationRetryable,
			attempt.Retryable, attempt.CompletedAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}

	return nil
}

func (collector *evidenceCollector) collectEvents() error {
	for _, event := range collector.snapshot.Events {
		id := "ev:event:" + event.ID.String()
		source := diagnostic.ItemSource{Kind: "state_event", EntityID: event.EntityID, Revision: event.EntityRevision}
		value := map[string]any{
			"type": event.Type, "entity": event.Entity,
			"from_phase": event.FromPhase, "to_phase": event.ToPhase,
			"from_outcome": event.FromOutcome, "to_outcome": event.ToOutcome,
		}
		if event.RunID.Valid() {
			value["run_id"] = event.RunID.String()
		}
		if len(event.Details) != 0 {
			value["details"] = event.Details
		}
		if err := collector.add(id, diagnostic.CodeLifecycleEvent, value,
			&event.OccurredAt, source, diagnostic.QualityObserved); err != nil {
			return err
		}
	}

	return nil
}

//nolint:gocognit,cyclop // Log selection keeps budget, availability, stream, redaction, and consistency decisions together.
func (collector *evidenceCollector) collectLogs(ctx context.Context, request diagnostic.EvidenceRequest) error {
	if request.Logs != diagnostic.LogsTail || len(collector.selected) == 0 {
		for _, run := range collector.selected {
			if request.Logs == diagnostic.LogsMetadata {
				collector.omit(diagnostic.OmissionLogContentNotRequested,
					fmt.Sprintf("run:%d:stderr", run.Number), fmt.Sprintf("run:%d:stdout", run.Number))
			}
		}
		return nil
	}
	remaining := uint64(maximumEvidenceLogBytes)
	qualities := make(map[diagnostic.Quality]struct{})
	selected := slices.Clone(collector.selected)
	slices.Reverse(selected)
	for _, run := range selected {
		if !run.Logs.Available() {
			collector.omit(diagnostic.OmissionLogsPruned,
				fmt.Sprintf("run:%d:stderr", run.Number), fmt.Sprintf("run:%d:stdout", run.Number))
			continue
		}
		reader, err := logstore.OpenRun(collector.service.stateDir, run.JobID.String(), run.Number)
		if err != nil {
			collector.omit(diagnostic.OmissionLogsUnavailable,
				fmt.Sprintf("run:%d:stderr", run.Number), fmt.Sprintf("run:%d:stdout", run.Number))
			continue
		}
		for _, stream := range []logstore.Stream{logstore.Stderr, logstore.Stdout} {
			if remaining == 0 {
				collector.omit(diagnostic.OmissionLogBudgetExceeded, fmt.Sprintf("run:%d:%s", run.Number, stream))
				continue
			}
			maximum := min(request.LogBytes, remaining)
			tail, err := reader.ReadTail(ctx, stream, maximum)
			if err != nil {
				return fmt.Errorf("collect run %d %s log tail: %w", run.Number, stream, err)
			}
			artifactID := fmt.Sprintf("artifact:run:%020d:%s", run.Number, stream)
			data, _ := collector.sanitize(artifactID, "run.log."+stream.String(), tail.Data)
			if uint64(len(data)) > remaining {
				return errors.New("collect diagnostic evidence: sanitizer expanded log content beyond the evidence budget")
			}
			quality := diagnostic.QualityObserved
			if run.Phase != model.RunPhaseCompleted {
				quality = diagnostic.QualityPointInTime
			}
			qualities[quality] = struct{}{}
			collector.evidence.Artifacts = append(collector.evidence.Artifacts, diagnostic.Artifact{
				ID: artifactID, Role: diagnostic.ArtifactRoleLogTail, Run: run.Number,
				Stream: stream.String(), MediaType: "application/octet-stream", Data: data,
				OriginalBytes: tail.OriginalBytes, ByteStart: tail.ByteStart, ByteEnd: tail.ByteEnd,
				Truncated:  tail.ByteStart > 0 || tail.ByteEnd < tail.OriginalBytes,
				CapturedAt: collector.service.now().UTC(), Quality: quality,
				Disclosure: diagnostic.DisclosureLogContent,
			})
			remaining -= uint64(len(data))
		}
	}
	switch {
	case len(qualities) == 0:
		collector.evidence.Consistency.Artifacts = diagnostic.ArtifactsNotCollected
	case len(qualities) > 1:
		collector.evidence.Consistency.Artifacts = diagnostic.ArtifactsMixed
	case hasQuality(qualities, diagnostic.QualityPointInTime):
		collector.evidence.Consistency.Artifacts = diagnostic.ArtifactsPointInTime
	default:
		collector.evidence.Consistency.Artifacts = diagnostic.ArtifactsStable
	}

	return nil
}

func (collector *evidenceCollector) addCollectionOmissions(request diagnostic.EvidenceRequest) {
	if !request.IncludeCommand {
		collector.omit(diagnostic.OmissionCommandNotRequested, "commands")
	}
	if !request.IncludePaths {
		collector.omit(diagnostic.OmissionPathsNotRequested, "paths")
	}
	if !request.IncludeEnvironmentNames {
		collector.omit(diagnostic.OmissionEnvironmentNamesNotRequested, "environment_names")
	}
	if !request.IncludeSystem {
		collector.omit(diagnostic.OmissionSystemContextNotRequested, "system_context")
	}
	if request.Logs == diagnostic.LogsNone {
		collector.omit(diagnostic.OmissionLogContentNotRequested, "logs")
	}
	if collector.snapshot.RunsTruncated {
		collector.omit(diagnostic.OmissionHistoryTruncated, "runs")
	}
	if collector.snapshot.EventsTruncated {
		collector.omit(diagnostic.OmissionEventsTruncated, "lifecycle_events")
	}
	if collector.snapshot.NotificationsTruncated {
		collector.omit(diagnostic.OmissionHistoryTruncated, "notifications")
	}
	switch {
	case request.Similar == 0:
		collector.omit(diagnostic.OmissionSimilarNotRequested, "similar_failures")
	case !collector.snapshot.SimilarityAvailable:
		collector.omit(diagnostic.OmissionSimilarUnavailable, "similar_failures")
	default:
		if collector.snapshot.SimilarityPartiallyIndexed {
			collector.omit(diagnostic.OmissionSimilarPartiallyIndexed, "similar_failures")
		}
		if collector.snapshot.SimilarTruncated {
			collector.omit(diagnostic.OmissionSimilarTruncated, "similar_failures")
		}
	}
	collector.addResourceOmissions()
	if collector.evidence.Consistency.ActiveStateMayHaveAdvanced {
		collector.omit(diagnostic.OmissionActiveStateMayHaveAdvanced, "job_state", "run_state")
	}
}

func (collector *evidenceCollector) addResourceOmissions() {
	if len(collector.selected) == 0 {
		collector.omit(diagnostic.OmissionResourceNotApplicable, "resource_observations")

		return
	}
	for _, run := range collector.selected {
		affects := fmt.Sprintf("run:%d:resource_observations", run.Number)
		facts, found := collector.snapshot.RunFacts[run.ID]
		switch {
		case !found:
			collector.omit(diagnostic.OmissionResourceUnavailable, affects)
		case len(facts.Resources) != 0:
			continue
		case run.StartedAt == nil:
			collector.omit(diagnostic.OmissionResourceNotApplicable, affects)
		default:
			collector.omit(diagnostic.OmissionResourceUnsupported, affects)
		}
	}
}

func (collector *evidenceCollector) add(
	id string,
	code string,
	value any,
	observedAt *modelTime,
	source diagnostic.ItemSource,
	quality diagnostic.Quality,
) error {
	return collector.addWithDisclosure(id, code, value, observedAt, source, quality, diagnostic.DisclosureMetadata)
}

func (collector *evidenceCollector) addWithDisclosure(
	id string,
	code string,
	value any,
	observedAt *modelTime,
	source diagnostic.ItemSource,
	quality diagnostic.Quality,
	disclosure diagnostic.DisclosureClass,
) error {
	encoded, err := collector.encodeValue(value)
	if err != nil {
		return fmt.Errorf("encode diagnostic item %s: %w", id, err)
	}
	collector.evidence.Items = append(collector.evidence.Items, diagnostic.Item{
		ID: id, Code: code, Value: encoded, ObservedAt: cloneEvidenceTime(observedAt),
		Source: source, Quality: quality, Disclosure: disclosure,
	})

	return nil
}

func (collector *evidenceCollector) addDiagnostic(
	id string,
	code string,
	legacy string,
	observedAt *modelTime,
	source diagnostic.ItemSource,
) error {
	legacy = collector.sanitizeText(id, "diagnostic.code", legacy)
	record, err := model.NewDiagnosticRecord(model.DiagnosticCode(legacy), nil)
	if err != nil {
		record, err = model.NewDiagnosticRecord("legacy_unclassified", nil)
		if err != nil {
			return fmt.Errorf("encode legacy diagnostic record %s: %w", id, err)
		}
		collector.omit(diagnostic.OmissionLegacyDiagnosticUnclassified, id)
	}

	return collector.add(id, code, record, observedAt, source, diagnostic.QualityConfirmed)
}

func (collector *evidenceCollector) sanitizeText(affects, field, value string) string {
	sanitized, changed := collector.sanitize(affects, field, []byte(value))
	if !utf8.Valid(sanitized) {
		sanitized = []byte(strings.ToValidUTF8(string(sanitized), "�"))
		if !changed {
			collector.noteRedaction(diagnostic.RedactionConfiguredPattern, affects)
		}
	}

	return string(sanitized)
}

func (collector *evidenceCollector) sanitize(affects, field string, value []byte) ([]byte, bool) {
	if collector.sanitizer == nil {
		return slices.Clone(value), false
	}
	sanitized, changed := collector.sanitizer.Sanitize(field, slices.Clone(value))
	if changed {
		collector.noteRedaction(diagnostic.RedactionConfiguredPattern, affects)
	}

	return slices.Clone(sanitized), changed
}

func (collector *evidenceCollector) omit(code string, affects ...string) {
	set := collector.omissions[code]
	if set == nil {
		set = make(map[string]struct{})
		collector.omissions[code] = set
	}
	for _, affect := range affects {
		set[affect] = struct{}{}
	}
}

func (collector *evidenceCollector) noteRedaction(code, affects string) {
	set := collector.redactions[code]
	if set == nil {
		set = make(map[string]struct{})
		collector.redactions[code] = set
	}
	set[affects] = struct{}{}
	collector.redacted[code]++
}

func (collector *evidenceCollector) finish() diagnostic.Evidence {
	for code, values := range collector.omissions {
		collector.evidence.Omissions = append(collector.evidence.Omissions, diagnostic.Omission{
			Code: code, Affects: mapKeys(values),
		})
	}
	for code, values := range collector.redactions {
		collector.evidence.RedactionNotices = append(collector.evidence.RedactionNotices, diagnostic.RedactionNotice{
			Code: code, Affects: mapKeys(values), Count: collector.redacted[code],
		})
	}

	return collector.evidence
}

func mapKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)

	return result
}

func cloneEvidenceTime(value *modelTime) *modelTime {
	if value == nil {
		return nil
	}
	cloned := value.UTC()

	return &cloned
}

func runDuration(run model.RunState, capturedAt modelTime) (time.Duration, *modelTime, bool) {
	if run.StartedAt == nil {
		return 0, nil, false
	}
	end := capturedAt
	qualityAt := &capturedAt
	if run.CompletedAt != nil {
		end = *run.CompletedAt
		qualityAt = run.CompletedAt
	}
	if end.Before(*run.StartedAt) {
		return 0, nil, false
	}

	return end.Sub(*run.StartedAt), qualityAt, true
}

func classifyJobFailure(job model.JobState, runCount int) (string, diagnostic.Quality) {
	classification := failureclass.Job(job, runCount)
	if classification.Class == "" {
		return "", ""
	}
	if classification.Confirmed {
		return classification.Class, diagnostic.QualityConfirmed
	}

	return classification.Class, diagnostic.QualityObserved
}

func classifyRunFailure(run model.RunState) (string, diagnostic.Quality) {
	classification := failureclass.Run(run)
	if classification.Class == "" {
		return "", ""
	}
	if classification.Confirmed {
		return classification.Class, diagnostic.QualityConfirmed
	}

	return classification.Class, diagnostic.QualityObserved
}

func hasQuality(values map[diagnostic.Quality]struct{}, value diagnostic.Quality) bool {
	_, ok := values[value]

	return ok
}
