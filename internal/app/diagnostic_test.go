package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/logstore"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
	"github.com/ryancswallace/jobman/internal/store"
)

func TestDiagnosticEvidenceDefaultsToSafeMetadata(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, _, _ := completeCapturedRun(t, service, clock)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(),
	}, nil)
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	if verifyErr := diagnostic.Verify(evidence); verifyErr != nil {
		t.Fatalf("Verify() error = %v", verifyErr)
	}
	if len(evidence.Subject.SelectedRuns) != 1 || evidence.Subject.SelectedRuns[0] != 1 {
		t.Fatalf("selected runs = %v, want [1]", evidence.Subject.SelectedRuns)
	}
	if len(evidence.Artifacts) != 0 || evidence.Consistency.Artifacts != diagnostic.ArtifactsNotCollected {
		t.Fatalf("artifacts = %#v, consistency = %q", evidence.Artifacts, evidence.Consistency.Artifacts)
	}
	if !hasEvidenceCode(evidence, diagnostic.CodeFailureClass) ||
		!hasOmission(evidence, diagnostic.OmissionLogContentNotRequested) ||
		!hasOmission(evidence, diagnostic.OmissionSystemContextNotRequested) {
		t.Fatalf("evidence lacks failure class or log omission: %#v", evidence)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("missing-test-executable"), []byte(service.stateDir)} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatalf("evidence contains forbidden value %q", forbidden)
		}
	}
}

func TestEvidenceCollectorContextProjectionErrorAndLimitPaths(t *testing.T) {
	t.Parallel()

	workingDirectory := t.TempDir()
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable:       filepath.Join(workingDirectory, "tool"),
		Arguments:        []string{"--check"},
		WorkingDirectory: workingDirectory,
		Name:             "context-errors",
		ExecutionPolicy:  model.DefaultExecutionPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	snapshot := store.DiagnosticSnapshot{Job: model.JobState{Spec: specification, SubmittedAt: now}}
	encodingFailure := errors.New("encode failure")
	failing := &evidenceCollector{
		snapshot: snapshot,
		encodeValue: func(any) (json.RawMessage, error) {
			return nil, encodingFailure
		},
		omissions: make(map[string]map[string]struct{}), redactions: make(map[string]map[string]struct{}),
		redacted: make(map[string]uint64),
	}
	for name, collect := range map[string]func() error{
		"commands":    failing.collectCommands,
		"paths":       failing.collectPaths,
		"environment": failing.collectEnvironmentNames,
		"policy":      failing.collectExecutionPolicy,
	} {
		t.Run(name, func(t *testing.T) {
			if collectErr := collect(); !errors.Is(collectErr, encodingFailure) {
				t.Fatalf("collection error = %v, want encoding failure", collectErr)
			}
		})
	}

	bounded := &evidenceCollector{
		encodeValue: diagnostic.JSONValue,
		evidence:    diagnostic.Evidence{Items: []diagnostic.Item{}},
		omissions:   make(map[string]map[string]struct{}), redactions: make(map[string]map[string]struct{}),
		redacted: make(map[string]uint64),
	}
	if addErr := bounded.addCommand("ev:command", diagnostic.CodeTargetCommand, "command", "", nil, nil,
		diagnostic.ItemSource{}); addErr != nil {
		t.Fatal(addErr)
	}
	if addErr := bounded.addPath("ev:path", diagnostic.CodeTargetWorkingDirectory, "path", "", nil,
		diagnostic.ItemSource{}); addErr != nil {
		t.Fatal(addErr)
	}
	if addErr := bounded.addEnvironmentNames("ev:environment", diagnostic.CodeTargetEnvironmentNames, "environment",
		diagnostic.EnvironmentNames{Inheritance: "bad\x00policy"}, nil, diagnostic.ItemSource{}); addErr != nil {
		t.Fatal(addErr)
	}
	if bounded.omissions[diagnostic.OmissionCommandLimitExceeded] == nil ||
		bounded.omissions[diagnostic.OmissionPathLimitExceeded] == nil ||
		bounded.omissions[diagnostic.OmissionEnvironmentNamesLimitExceeded] == nil {
		t.Fatalf("context omissions = %#v", bounded.omissions)
	}
	if got := diagnosticLogRetention(model.ExecutionPolicy{LogRetentionUnlimited: true}); got != "unlimited" {
		t.Fatalf("diagnosticLogRetention(unlimited) = %q", got)
	}
}

func TestEvidenceCollectorReportsUnavailableSystemContext(t *testing.T) {
	t.Parallel()

	collector := &evidenceCollector{
		service: &Service{}, encodeValue: diagnostic.JSONValue,
		evidence:  diagnostic.Evidence{Items: []diagnostic.Item{}},
		omissions: make(map[string]map[string]struct{}),
	}
	if err := collector.collectSystem(); err != nil {
		t.Fatalf("collectSystem() error = %v", err)
	}
	if collector.omissions[diagnostic.OmissionSystemContextUnavailable] == nil || len(collector.evidence.Items) != 0 {
		t.Fatalf("system context collection = omissions %#v, items %#v", collector.omissions, collector.evidence.Items)
	}
}

func TestDiagnosticEvidenceIncludesOnlyExplicitCommandContext(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, _, _ := completeCapturedRun(t, service, clock)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(), IncludeCommand: true,
	}, nil)
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	item := findEvidenceItem(t, evidence, diagnostic.CodeTargetCommand)
	var command diagnostic.Command
	if decodeErr := json.Unmarshal(item.Value, &command); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if item.Disclosure != diagnostic.DisclosureCommand || command.Executable != "missing-test-executable" ||
		len(command.Arguments) != 0 {
		t.Fatalf("target command item = %#v / %#v", item, command)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(service.stateDir)) {
		t.Fatalf("evidence contains unrelated path %q", service.stateDir)
	}
	if hasOmission(evidence, diagnostic.OmissionCommandNotRequested) {
		t.Fatalf("evidence reports requested command as omitted: %#v", evidence.Omissions)
	}
}

func TestDiagnosticEvidenceProjectsBoundedExecutionContextWithoutValues(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)
	workingDirectory := t.TempDir()
	configuration := model.DefaultExecutionPolicy()
	configuration.RunTimeout = 45 * time.Second
	configuration.Tags = []string{"batch"}
	configuration.SecretEnv = map[string]model.SecretReference{
		"API_TOKEN": {Provider: "env", Name: "UPSTREAM_SECRET_REFERENCE"},
	}
	job, err := service.Submit(t.Context(), SubmitRequest{
		Name: "context-test", Executable: "/usr/bin/false", Arguments: []string{"--mode", "fast path"},
		WorkingDirectory: workingDirectory, Environment: map[string]string{"MODE": "secret-value-canary"},
		UnsetEnvironment: []string{"DEBUG"}, ExecutionPolicy: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(), IncludeCommand: true, IncludePaths: true, IncludeEnvironmentNames: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var command diagnostic.Command
	if decodeErr := json.Unmarshal(findEvidenceItem(t, evidence, diagnostic.CodeTargetCommand).Value, &command); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if command.Executable != "/usr/bin/false" || !slices.Equal(command.Arguments, []string{"--mode", "fast path"}) {
		t.Fatalf("command = %#v", command)
	}
	var environment diagnostic.EnvironmentNames
	if decodeErr := json.Unmarshal(findEvidenceItem(t, evidence, diagnostic.CodeTargetEnvironmentNames).Value, &environment); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !slices.Equal(environment.Set, []string{"MODE"}) || !slices.Equal(environment.Unset, []string{"DEBUG"}) ||
		!slices.Equal(environment.Secret, []string{"API_TOKEN"}) {
		t.Fatalf("environment names = %#v", environment)
	}
	var executionPolicy diagnostic.ExecutionPolicy
	if decodeErr := json.Unmarshal(findEvidenceItem(t, evidence, diagnostic.CodeExecutionPolicy).Value, &executionPolicy); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if executionPolicy.RunTimeout != "45s" || !slices.Equal(executionPolicy.Tags, []string{"batch"}) {
		t.Fatalf("execution policy = %#v", executionPolicy)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-value-canary", "UPSTREAM_SECRET_REFERENCE"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("evidence contains excluded environment data %q", forbidden)
		}
	}
}

func TestDiagnosticEvidenceSanitizesLogArtifactsBeforeSealing(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, _, _ := completeCapturedRun(t, service, clock)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(), Logs: diagnostic.LogsTail, LogBytes: 64,
	}, replacingSanitizer{old: []byte("captured"), replacement: []byte("[redacted]")})
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	if verifyErr := diagnostic.Verify(evidence); verifyErr != nil {
		t.Fatalf("Verify() error = %v", verifyErr)
	}
	artifact := findArtifact(t, evidence, "stdout")
	if bytes.Contains(artifact.Data, []byte("captured")) || string(artifact.Data) != "[redacted]\n" {
		t.Fatalf("stdout artifact = %q", artifact.Data)
	}
	if artifact.SelectedBytes != uint64(len("captured\n")) || artifact.ContentBytes != uint64(len("[redacted]\n")) {
		t.Fatalf("artifact byte accounting = %#v", artifact)
	}
	if len(evidence.RedactionNotices) != 1 || evidence.RedactionNotices[0].Count != 1 {
		t.Fatalf("redaction notices = %#v", evidence.RedactionNotices)
	}
	if evidence.Consistency.Artifacts != diagnostic.ArtifactsStable {
		t.Fatalf("artifact consistency = %q, want stable", evidence.Consistency.Artifacts)
	}
}

func TestDiagnosticEvidenceReportsConfiguredValueRedaction(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, _, _ := completeCapturedRun(t, service, clock)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(), Logs: diagnostic.LogsTail, LogBytes: 64,
	}, reportingSanitizer{replacingSanitizer: replacingSanitizer{
		old: []byte("captured"), replacement: []byte("[redacted]"),
	}})
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	if !slices.Contains(evidence.Source.Capabilities, "configured_value_redaction_v1") {
		t.Fatalf("capabilities = %v", evidence.Source.Capabilities)
	}
}

func TestDiagnosticEvidenceValidatesCollectionOptions(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)
	tests := []diagnostic.EvidenceRequest{
		{},
		{Selector: "job", Run: 1, AllRuns: true},
		{Selector: "job", Logs: "everything"},
		{Selector: "job", Logs: diagnostic.LogsTail, LogBytes: maximumEvidenceLogBytes + 1},
		{Selector: "job", Similar: maximumEvidenceSimilar + 1},
	}
	for _, request := range tests {
		if _, err := service.DiagnosticEvidence(t.Context(), request, nil); err == nil {
			t.Fatalf("DiagnosticEvidence(%#v) error = nil", request)
		}
	}
	if _, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: "missing",
	}, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DiagnosticEvidence(missing) error = %v, want ErrNotFound", err)
	}
}

func TestDiagnosticEvidenceIncludesPersistedResourcesAndLocalFingerprint(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, err := service.Submit(t.Context(), SubmitRequest{
		Executable: "worker", WorkingDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	runID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatalf("NewRunID() error = %v", err)
	}
	capture, err := logstore.CreateRun(service.stateDir, job.ID.String(), 1)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	paths := capture.Paths()
	if closeErr := capture.Close(); closeErr != nil {
		t.Fatalf("close empty capture: %v", closeErr)
	}
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityValid,
		RecordingHealth: model.RecordingHealthy,
	}
	reservedAt := clock.now.Add(2 * time.Millisecond)
	if _, reserveErr := service.store.ReserveRun(t.Context(), job.ID, runID, 1, logs, reservedAt); reserveErr != nil {
		t.Fatalf("ReserveRun() error = %v", reserveErr)
	}
	if _, startErr := service.store.MarkProcessStarted(t.Context(), job.ID, runID, "/test/bin/worker", model.ProcessIdentity{
		PID: 4444, Platform: "test", CreationID: "facts", BootID: "boot", TreeID: "tree",
	}, clock.now.Add(3*time.Millisecond)); startErr != nil {
		t.Fatalf("MarkProcessStarted() error = %v", startErr)
	}
	exitCode := 7
	completedAt := clock.now.Add(4 * time.Millisecond)
	if _, completeErr := service.store.CompleteRunWithDispositionAndFacts(
		t.Context(), job.ID, runID, model.RunOutcomeFailure,
		&model.ExitInfo{ExitCode: &exitCode, ObservedAt: completedAt},
		logs, "", completedAt,
		model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure},
		store.RunDiagnosticFactsInput{
			Resources: []diagnostic.ResourceObservation{{
				Metric: diagnostic.ResourceCPUUserTime, Value: 123, Unit: diagnostic.ResourceUnitNanoseconds,
				Scope: diagnostic.ResourceScopeProcess, Source: diagnostic.ResourceSourceProcessState,
				Completeness: diagnostic.ResourceCompleteAtExit,
			}},
			PolicyDisposition: policy.RunClassificationNonRetryableFailure,
		},
	); completeErr != nil {
		t.Fatalf("CompleteRunWithDispositionAndFacts() error = %v", completeErr)
	}
	clock.now = completedAt.Add(time.Millisecond)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(),
	}, nil)
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	resource := findEvidenceItem(t, evidence, diagnostic.CodeResourceObservation)
	if resource.Disclosure != diagnostic.DisclosureMetadata {
		t.Fatalf("resource disclosure = %q", resource.Disclosure)
	}
	fingerprint := findEvidenceItem(t, evidence, diagnostic.CodeFailureFingerprint)
	if fingerprint.Disclosure != diagnostic.DisclosureLocalOnly {
		t.Fatalf("fingerprint disclosure = %q", fingerprint.Disclosure)
	}
	if hasOmission(evidence, diagnostic.OmissionResourceUnavailable) ||
		hasOmission(evidence, diagnostic.OmissionResourceUnsupported) {
		t.Fatalf("resource evidence has contradictory omissions: %#v", evidence.Omissions)
	}
}

func TestDiagnosticEvidenceReconcilesExpiredOwnershipBeforeCapture(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	job, err := service.Submit(t.Context(), SubmitRequest{
		Executable: "worker", WorkingDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	evidence, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
		Selector: job.ID.String(), Logs: diagnostic.LogsNone,
	}, nil)
	if err != nil {
		t.Fatalf("DiagnosticEvidence() error = %v", err)
	}
	if evidence.Subject.Outcome != string(model.JobOutcomeLost) ||
		!hasEvidenceCode(evidence, diagnostic.CodeJobDiagnostic) {
		t.Fatalf("reconciled evidence subject/items = %#v/%#v", evidence.Subject, evidence.Items)
	}
}

func TestDiagnosticEvidenceReportsReconciliationAndSelectionErrors(t *testing.T) {
	t.Parallel()

	t.Run("reconciliation", func(t *testing.T) {
		service, clock := newTestService(t)
		job, err := service.Submit(t.Context(), SubmitRequest{
			Executable: "worker", WorkingDirectory: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		clock.now = clock.now.Add(time.Minute)
		raw := openAppRawDatabase(t, service)
		if _, err := raw.ExecContext(t.Context(), `
			CREATE TRIGGER reject_diagnostic_reconciliation
			BEFORE UPDATE ON jobs
			BEGIN SELECT RAISE(ABORT, 'injected reconciliation failure'); END`); err != nil {
			t.Fatal(err)
		}
		if _, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
			Selector: job.ID.String(),
		}, nil); err == nil {
			t.Fatal("DiagnosticEvidence() reconciliation error = nil")
		}
	})

	t.Run("selection", func(t *testing.T) {
		service, _ := newTestService(t)
		job, err := service.Submit(t.Context(), SubmitRequest{
			Executable: "worker", WorkingDirectory: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.DiagnosticEvidence(t.Context(), diagnostic.EvidenceRequest{
			Selector: job.ID.String(), Run: 99,
		}, nil); !errors.Is(err, ErrNotFound) {
			t.Fatalf("DiagnosticEvidence(missing run) error = %v, want ErrNotFound", err)
		}
	})

	request, err := normalizeEvidenceRequest(diagnostic.EvidenceRequest{
		Selector: "job", Logs: diagnostic.LogsTail,
	})
	if err != nil || request.LogBytes != defaultEvidenceLogBytes {
		t.Fatalf("normalizeEvidenceRequest(default tail) = (%#v, %v)", request, err)
	}
	if _, err := normalizeEvidenceRequest(diagnostic.EvidenceRequest{
		Selector: "job", Logs: diagnostic.LogsMetadata, LogBytes: 1,
	}); err == nil {
		t.Fatal("normalizeEvidenceRequest(metadata bytes) error = nil")
	}
}

func TestEvidenceCollectorProjectsRichSnapshot(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	jobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	runID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	dependencyID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := service.ids.NewEventID()
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := service.ids.NewEventID()
	if err != nil {
		t.Fatal(err)
	}
	similarJobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	similarRunID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	now := clock.now.UTC()
	claimedAt := now.Add(time.Second)
	startedAt := now.Add(2 * time.Second)
	stoppedAt := now.Add(3 * time.Second)
	completedAt := now.Add(4 * time.Second)
	nextRunAt := now.Add(time.Minute)
	pausedAt := now.Add(-time.Minute)
	satisfiedAt := now.Add(-time.Second)
	exitCode := 124
	responseStatus := 503
	configuration := model.DefaultExecutionPolicy()
	configuration.Completion.RetryAbortAt = nextRunAt
	configuration.Completion.HasRetryAbortAt = true
	configuration.Classification = policy.ClassificationPolicy{
		SuccessExitCodes: []int{0}, RetryableExitCodes: []policy.ExitCodeRange{{First: 1, Last: 10}},
		RetryableSignals: []string{"SIGTERM"}, RetryablePlatformReasons: []string{"temporary"},
		RetryTimeout: true, RetryStartFailure: true, RetryCancellation: true,
	}
	configuration.FailureDelay = policy.DelayPolicy{
		Base: time.Second, Backoff: policy.BackoffExponential, ExponentialBase: 2,
		MaxDelay: time.Minute, HasMaxDelay: true, Jitter: time.Second,
	}
	configuration.SuccessDelay = policy.DelayPolicy{Base: time.Second, Backoff: policy.BackoffLinear}
	configuration.RunTimeout = 30 * time.Second
	configuration.JobTimeout = 2 * time.Minute
	configuration.WaitConditions = []model.WaitCondition{
		{Kind: model.WaitUntil, Until: nextRunAt, PollInterval: time.Second},
		{Kind: model.WaitDelay, Delay: time.Second, PollInterval: time.Second, AbortAt: nextRunAt},
		{Kind: model.WaitFileExists, Path: filepath.Join(t.TempDir(), "ready"), FileKind: policy.FileKindRegular, PollInterval: time.Second},
		{
			Kind: model.WaitProbe, PollInterval: time.Second,
			Probe:          policy.ProbeSpec{Executable: "probe", Arguments: []string{"--ready"}, Timeout: time.Second, OutputLimit: 1024, FatalOnError: true},
			ProbeDirectory: t.TempDir(), ProbeEnvironment: map[string]string{"MODE": "ready"},
			ProbeUnsetEnvironment: []string{"DEBUG"},
			ProbeSecretEnv:        map[string]model.SecretReference{"TOKEN": {Provider: "env", Name: "PROBE_TOKEN"}},
		},
	}
	configuration.Dependencies = []model.DependencyRequirement{{JobID: dependencyID, Predicate: string(store.DependencySuccess)}}
	configuration.Concurrency = model.ConcurrencyPolicy{Pool: "workers", Slots: 2}
	configuration.Notifications = []model.NotificationSubscription{{
		Notifier: "hook", Events: []string{"job_failed"},
	}}
	configuration.NotifierDefinitions = []model.NotifierDefinition{{
		Name: "hook", Kind: model.NotifierCommand, Timeout: 3 * time.Second,
		Retry: model.NotifierRetryPolicy{MaxAttempts: 2, Delay: time.Second, MaxDelay: 4 * time.Second},
		Command: &model.CommandNotifierDefinition{
			Executable: filepath.Join(t.TempDir(), "job-event"), Arguments: []string{"--json"},
			WorkingDirectory: t.TempDir(), Environment: map[string]string{"MODE": "production"},
			SecretEnvironment: map[string]model.SecretReference{"TOKEN": {Provider: "env", Name: "HOOK_TOKEN"}},
			OutputLimit:       4096,
		},
	}, {
		Name: "webhook", Kind: model.NotifierWebhook, Timeout: 5 * time.Second,
		Retry: model.NotifierRetryPolicy{MaxAttempts: 3, Delay: time.Second, MaxDelay: 8 * time.Second},
		Webhook: &model.WebhookNotifierDefinition{
			URL: "https://events.example.test/jobman", ResponseLimit: 8192,
			AllowPrivateNetwork: true, FollowRedirects: true,
		},
	}, {
		Name: "mail", Kind: model.NotifierSMTP, Timeout: 10 * time.Second,
		Retry: model.NotifierRetryPolicy{MaxAttempts: 1},
		SMTP: &model.SMTPNotifierDefinition{
			Address: "smtp.example.test:587", Username: "jobman",
			PasswordSecret: &model.SecretReference{Provider: "env", Name: "SMTP_PASSWORD"},
			From:           "Jobman <jobman@example.test>", To: []string{"ops@example.test"},
			SubjectPrefix: "Jobman", Mode: "starttls", MessageLimit: 65536,
		},
	}}
	configuration.Tags = []string{"batch"}
	configuration.Groups = []string{"operations"}
	configuration.SecretEnv = map[string]model.SecretReference{"API_TOKEN": {Provider: "env", Name: "TARGET_TOKEN"}}
	configuration.LogRotateSize = 4096
	configuration.LogMaxSegmentsPerStream = 3
	configuration.LogRetentionMaxAge = 24 * time.Hour
	configuration.StdinPath = filepath.Join(t.TempDir(), "stdin")
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable: "/usr/bin/worker", Arguments: []string{"--serve"}, WorkingDirectory: t.TempDir(),
		Environment: map[string]string{"MODE": "production"}, UnsetEnvironment: []string{"DEBUG"},
		EnvironmentInheritance: model.EnvironmentInheritSubmission, Name: "rich-context",
		StopPolicy:  model.StopPolicy{GracePeriod: 5 * time.Second, ForceAfterGrace: true},
		StdinPolicy: model.StdinFile, ExecutionPolicy: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := diagnostic.FailureFingerprint{
		Algorithm:          diagnostic.FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: diagnostic.FingerprintInputSchemaVersion,
		Value:              strings.Repeat("a", 64), Scope: diagnostic.FingerprintScopeStoreLocal,
	}
	snapshot := store.DiagnosticSnapshot{
		Job: model.JobState{
			ID: jobID, Phase: model.JobPhaseCompleted, Outcome: model.JobOutcomeTimedOut, Revision: 8,
			Spec:        specification,
			SubmittedAt: now, ClaimedAt: &claimedAt, StartedAt: &startedAt, CompletedAt: &completedAt,
			LastDiagnosticCode: string(model.DiagnosticJobTimeout),
		},
		Runs: []model.RunState{{
			ID: runID, JobID: jobID, Number: 1, Phase: model.RunPhaseCompleted,
			Outcome: model.RunOutcomeTimedOut, Revision: 5, ReservedAt: claimedAt, StartedAt: &startedAt,
			ResolvedExecutable: "/usr/bin/worker", StopRequestedAt: &stoppedAt,
			CompletedAt: &completedAt, StopReason: model.StopReasonTimeout,
			Exit: &model.ExitInfo{
				ExitCode: &exitCode, Signal: string([]byte{0xff}),
				PlatformReason: "deadline", ObservedAt: completedAt,
			},
			Logs: model.LogMetadata{
				StdoutPath: "/logs/stdout", StderrPath: "/logs/stderr", IndexPath: "/logs/index",
				IndexVersion: model.LogIndexVersion, StdoutSize: 12, StderrSize: 34,
				Integrity: model.LogIntegrityPartial, RecordingHealth: model.RecordingDegraded,
				DiagnosticCode: string(model.DiagnosticLogCaptureDegraded),
			},
			LastDiagnosticCode: string(model.DiagnosticRunTimeout),
		}},
		Runtime: store.JobRuntime{
			JobID: jobID, Revision: 9, RunCount: 3, SuccessCount: 1, FailureCount: 2,
			NextRunAt: &nextRunAt, WaitingReason: "capacity", PausedFrom: model.JobPhaseBackoff,
			PausedAt: &pausedAt, TotalPaused: 2 * time.Second,
		},
		Dependencies: []store.Dependency{{
			JobID: jobID, DependsOn: dependencyID, Predicate: store.DependencySuccess,
			ObservedRevision: 3, ObservedOutcome: model.JobOutcomeSuccess, SatisfiedAt: &satisfiedAt,
		}},
		WaitEvaluations: []store.WaitEvaluation{{
			JobID: jobID, ConditionIndex: 2, ConditionKind: model.WaitProbe,
			EvaluatedAt: &stoppedAt, SatisfiedAt: &satisfiedAt, AttemptCount: 4,
			LastDiagnosticCode: string(model.DiagnosticWaitEvaluationError),
		}},
		Admission: &store.Admission{
			JobID: jobID, RunID: runID, Pool: "workers", Slots: 2,
			AcquiredAt: startedAt, LeaseExpires: nextRunAt, ReleasedAt: &completedAt,
		},
		NotificationDeliveries: []store.NotificationDelivery{{
			JobID: jobID, EventID: eventID, RunID: runID, NotifierName: "hook", EventType: string(model.EventRunCompleted),
			Status: store.NotificationDeliveryFailed, OccurredAt: completedAt, CreatedAt: completedAt,
			NextAttemptAt: &nextRunAt, ClaimedAt: &startedAt, ClaimExpiresAt: &nextRunAt,
			CompletedAt: &completedAt, MaxAttempts: 3, AttemptCount: 3,
		}},
		NotificationAttempts: []store.NotificationAttempt{{
			ID: attemptID, JobID: jobID, EventID: eventID, NotifierName: "hook", EventType: string(model.EventRunCompleted),
			AttemptNumber: 3, Status: store.NotificationAttemptFailed, CreatedAt: completedAt,
			StartedAt: &stoppedAt, CompletedAt: &completedAt, NextAttemptAt: &nextRunAt,
			ResponseStatusCode: &responseStatus, MessageID: "message-123", ResponseTruncated: true,
			DiagnosticCode: string(model.DiagnosticNotificationTransport), Retryable: true,
		}},
		Events: []store.DiagnosticEvent{{
			ID: eventID, JobID: jobID, RunID: runID, Entity: model.EntityRun,
			EntityID: runID.String(), Type: model.EventRunCompleted,
			FromPhase: string(model.RunPhaseRunning), ToPhase: string(model.RunPhaseCompleted),
			ToOutcome: string(model.RunOutcomeTimedOut), EntityRevision: 5,
			OccurredAt: completedAt, Details: json.RawMessage(`{"schema_version":1}`),
		}},
		RunFacts: map[model.RunID]store.RunDiagnosticFacts{
			runID: {
				SchemaVersion: 1,
				Resources: []diagnostic.ResourceObservation{{
					Metric: diagnostic.ResourceCPUUserTime, Value: 123,
					Unit: diagnostic.ResourceUnitNanoseconds, Scope: diagnostic.ResourceScopeProcess,
					Source: diagnostic.ResourceSourceProcessState, Completeness: diagnostic.ResourceCompleteAtExit,
				}},
				FailureClass: "run_timeout", Fingerprint: &fingerprint, RecordedAt: completedAt,
			},
		},
		SimilarFailures: []diagnostic.SimilarFailure{{
			JobID: similarJobID.String(), RunID: similarRunID.String(), RunNumber: 7,
			CompletedAt: completedAt, Outcome: string(model.RunOutcomeTimedOut),
			FailureClass: "run_timeout", Fingerprint: fingerprint, LaterSucceeded: true,
		}},
		TotalRuns: 101, RunsTruncated: true, EventsTruncated: true, NotificationsTruncated: true,
		SimilarTruncated: true, SimilarityAvailable: true, SimilarityPartiallyIndexed: true,
	}
	collector := newEvidenceCollector(
		service, snapshot, snapshot.Runs, completedAt.Add(time.Second),
		replacingSanitizer{old: []byte("workers"), replacement: []byte("pool")},
	)
	if collectErr := collector.collect(t.Context(), diagnostic.EvidenceRequest{
		Selector: jobID.String(), AllRuns: true, Logs: diagnostic.LogsMetadata, Similar: 1,
		IncludeCommand: true, IncludePaths: true, IncludeEnvironmentNames: true, IncludeSystem: true,
	}); collectErr != nil {
		t.Fatalf("collect() error = %v", collectErr)
	}
	evidence, err := diagnostic.Seal(collector.finish())
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := diagnostic.Verify(evidence); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !hasEvidenceCode(evidence, diagnostic.CodeSystemContext) ||
		hasOmission(evidence, diagnostic.OmissionSystemContextNotRequested) ||
		hasOmission(evidence, diagnostic.OmissionSystemContextUnavailable) {
		t.Fatalf("rich evidence lacks collected system context: %#v", evidence)
	}
	for _, code := range []string{
		diagnostic.CodeSourceContext, diagnostic.CodeTargetCommand, diagnostic.CodeTargetWorkingDirectory,
		diagnostic.CodeTargetEnvironmentNames, diagnostic.CodeExecutionPolicy,
		diagnostic.CodeWaitConfiguration, diagnostic.CodeWaitCommand, diagnostic.CodeWaitPath,
		diagnostic.CodeWaitEnvironmentNames, diagnostic.CodeNotifierConfiguration,
		diagnostic.CodeNotifierCommand, diagnostic.CodeNotifierWorkingDirectory,
		diagnostic.CodeNotifierEnvironmentNames, diagnostic.CodeRunResolvedExecutable,
		diagnostic.CodeRuntimeNextRunAt, diagnostic.CodeDependencyPredicate,
		diagnostic.CodeWaitDiagnostic, diagnostic.CodeAdmissionPool,
		diagnostic.CodeNotificationRetryable, diagnostic.CodeLifecycleEvent,
		diagnostic.CodeSimilarFailure,
	} {
		if !hasEvidenceCode(evidence, code) {
			t.Errorf("rich evidence lacks code %q", code)
		}
	}
	for _, code := range []string{
		diagnostic.OmissionHistoryTruncated, diagnostic.OmissionEventsTruncated,
		diagnostic.OmissionSimilarPartiallyIndexed, diagnostic.OmissionSimilarTruncated,
	} {
		if !hasOmission(evidence, code) {
			t.Errorf("rich evidence lacks omission %q", code)
		}
	}
}

func TestEvidenceCollectorPropagatesValueEncodingFailures(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	jobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	runID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	dependencyID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := service.ids.NewEventID()
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := service.ids.NewEventID()
	if err != nil {
		t.Fatal(err)
	}
	similarJobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	similarRunID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}

	now := clock.now.UTC()
	claimedAt := now.Add(time.Second)
	startedAt := now.Add(2 * time.Second)
	stoppedAt := now.Add(3 * time.Second)
	completedAt := now.Add(4 * time.Second)
	nextRunAt := now.Add(time.Minute)
	pausedAt := now.Add(-time.Minute)
	satisfiedAt := now.Add(-time.Second)
	exitCode := 124
	failureFingerprint := diagnostic.FailureFingerprint{
		Algorithm:          diagnostic.FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: diagnostic.FingerprintInputSchemaVersion,
		Value:              strings.Repeat("b", 64),
		Scope:              diagnostic.FingerprintScopeStoreLocal,
	}
	snapshot := store.DiagnosticSnapshot{
		Job: model.JobState{
			ID: jobID, Phase: model.JobPhaseCompleted, Outcome: model.JobOutcomeCancelled, Revision: 8,
			SubmittedAt: now, ClaimedAt: &claimedAt, StartedAt: &startedAt, CompletedAt: &completedAt,
			Cancellation:       &model.CancellationIntent{RequestedAt: stoppedAt, Reason: model.StopReasonCancellation},
			LastDiagnosticCode: string(model.DiagnosticJobTimeout),
		},
		Runs: []model.RunState{{
			ID: runID, JobID: jobID, Number: 1, Phase: model.RunPhaseCompleted,
			Outcome: model.RunOutcomeTimedOut, Revision: 5, ReservedAt: claimedAt, StartedAt: &startedAt,
			StopRequestedAt: &stoppedAt, CompletedAt: &completedAt, StopReason: model.StopReasonTimeout,
			Exit: &model.ExitInfo{
				ExitCode: &exitCode, Signal: "killed", PlatformReason: "deadline", ObservedAt: completedAt,
			},
			Logs: model.LogMetadata{
				StdoutPath: "/logs/stdout", StderrPath: "/logs/stderr", IndexPath: "/logs/index",
				IndexVersion: model.LogIndexVersion, StdoutSize: 12, StderrSize: 34,
				Integrity: model.LogIntegrityPartial, RecordingHealth: model.RecordingDegraded,
				DiagnosticCode: string(model.DiagnosticLogCaptureDegraded),
			},
			LastDiagnosticCode: string(model.DiagnosticRunTimeout),
		}},
		Runtime: store.JobRuntime{
			JobID: jobID, Revision: 9, RunCount: 3, SuccessCount: 1, FailureCount: 2,
			NextRunAt: &nextRunAt, WaitingReason: "capacity", PausedFrom: model.JobPhaseBackoff,
			PausedAt: &pausedAt, TotalPaused: 2 * time.Second,
		},
		Dependencies: []store.Dependency{{
			JobID: jobID, DependsOn: dependencyID, Predicate: store.DependencySuccess,
			ObservedRevision: 3, ObservedOutcome: model.JobOutcomeSuccess, SatisfiedAt: &satisfiedAt,
		}},
		WaitEvaluations: []store.WaitEvaluation{{
			JobID: jobID, ConditionIndex: 2, ConditionKind: model.WaitProbe,
			EvaluatedAt: &stoppedAt, SatisfiedAt: &satisfiedAt, AttemptCount: 4,
			LastDiagnosticCode: string(model.DiagnosticWaitEvaluationError),
		}},
		Admission: &store.Admission{
			JobID: jobID, RunID: runID, Pool: "workers", Slots: 2,
			AcquiredAt: startedAt, LeaseExpires: nextRunAt, ReleasedAt: &completedAt,
		},
		NotificationDeliveries: []store.NotificationDelivery{{
			JobID: jobID, EventID: eventID, RunID: runID, EventType: string(model.EventRunCompleted),
			Status: store.NotificationDeliveryFailed, OccurredAt: completedAt, CreatedAt: completedAt,
			CompletedAt: &completedAt, MaxAttempts: 3, AttemptCount: 3,
		}},
		NotificationAttempts: []store.NotificationAttempt{{
			ID: attemptID, JobID: jobID, EventID: eventID, EventType: string(model.EventRunCompleted),
			AttemptNumber: 3, Status: store.NotificationAttemptFailed, CreatedAt: completedAt,
			StartedAt: &stoppedAt, CompletedAt: &completedAt,
			DiagnosticCode: string(model.DiagnosticNotificationTransport), Retryable: true,
		}},
		Events: []store.DiagnosticEvent{{
			ID: eventID, JobID: jobID, RunID: runID, Entity: model.EntityRun,
			EntityID: runID.String(), Type: model.EventRunCompleted,
			FromPhase: string(model.RunPhaseRunning), ToPhase: string(model.RunPhaseCompleted),
			ToOutcome: string(model.RunOutcomeTimedOut), EntityRevision: 5,
			OccurredAt: completedAt, Details: json.RawMessage(`{"schema_version":1}`),
		}},
		RunFacts: map[model.RunID]store.RunDiagnosticFacts{
			runID: {
				SchemaVersion: 1,
				Resources: []diagnostic.ResourceObservation{{
					Metric: diagnostic.ResourceCPUUserTime, Value: 123,
					Unit: diagnostic.ResourceUnitNanoseconds, Scope: diagnostic.ResourceScopeProcess,
					Source: diagnostic.ResourceSourceProcessState, Completeness: diagnostic.ResourceCompleteAtExit,
				}},
				FailureClass: "run_timeout", Fingerprint: &failureFingerprint, RecordedAt: completedAt,
			},
		},
		SimilarFailures: []diagnostic.SimilarFailure{{
			JobID: similarJobID.String(), RunID: similarRunID.String(), RunNumber: 7,
			CompletedAt: completedAt, Outcome: string(model.RunOutcomeTimedOut),
			FailureClass: "run_timeout", Fingerprint: failureFingerprint, LaterSucceeded: true,
		}},
		TotalRuns: 1, SimilarityAvailable: true,
	}
	request := diagnostic.EvidenceRequest{
		Selector: jobID.String(), AllRuns: true, Logs: diagnostic.LogsMetadata, Similar: 1,
	}
	runCollection := func(encoder func(any) (json.RawMessage, error)) error {
		collector := newEvidenceCollector(service, snapshot, snapshot.Runs, completedAt.Add(time.Second), nil)
		collector.encodeValue = encoder

		return collector.collect(t.Context(), request)
	}

	encodedItems := 0
	if err := runCollection(func(value any) (json.RawMessage, error) {
		encodedItems++

		return diagnostic.JSONValue(value)
	}); err != nil {
		t.Fatalf("baseline collect() error = %v", err)
	}
	if encodedItems < 2 {
		t.Fatalf("baseline encoded item count = %d", encodedItems)
	}

	injectedErr := errors.New("injected evidence value encoding failure")
	for failAt := 1; failAt <= encodedItems; failAt++ {
		calls := 0
		err := runCollection(func(value any) (json.RawMessage, error) {
			calls++
			if calls == failAt {
				return nil, injectedErr
			}

			return diagnostic.JSONValue(value)
		})
		if !errors.Is(err, injectedErr) {
			t.Fatalf("collect() encoding failure at item %d = %v", failAt, err)
		}
	}
}

func TestSelectEvidenceRunsUsesExplicitActiveAndFailurePolicies(t *testing.T) {
	t.Parallel()

	service, _ := newTestService(t)
	jobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	runIDs := make([]model.RunID, 3)
	for index := range runIDs {
		runIDs[index], err = service.ids.NewRunID()
		if err != nil {
			t.Fatal(err)
		}
	}
	runs := []model.RunState{
		{ID: runIDs[0], JobID: jobID, Number: 1, Outcome: model.RunOutcomeSuccess},
		{ID: runIDs[1], JobID: jobID, Number: 2, Outcome: model.RunOutcomeFailure},
		{ID: runIDs[2], JobID: jobID, Number: 3, Outcome: model.RunOutcomeSuccess},
	}
	job := model.JobState{ID: jobID, ActiveRunID: runIDs[0]}
	tests := []struct {
		name    string
		job     model.JobState
		runs    []model.RunState
		request diagnostic.EvidenceRequest
		want    []uint64
		wantErr bool
	}{
		{name: "all", job: job, runs: runs, request: diagnostic.EvidenceRequest{AllRuns: true}, want: []uint64{1, 2, 3}},
		{name: "positive", job: job, runs: runs, request: diagnostic.EvidenceRequest{Run: 2}, want: []uint64{2}},
		{name: "negative", job: job, runs: runs, request: diagnostic.EvidenceRequest{Run: -1}, want: []uint64{3}},
		{name: "missing", job: job, runs: runs, request: diagnostic.EvidenceRequest{Run: 4}, wantErr: true},
		{name: "active", job: job, runs: runs, want: []uint64{1}},
		{name: "latest failure", job: model.JobState{ID: jobID}, runs: runs, want: []uint64{2}},
		{name: "latest success", job: model.JobState{ID: jobID}, runs: runs[:1], want: []uint64{1}},
		{name: "none", job: model.JobState{ID: jobID}, runs: []model.RunState{}, want: []uint64{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			selected, err := selectEvidenceRuns(test.job, test.runs, uint64(len(test.runs)), test.request)
			if test.wantErr {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("selectEvidenceRuns() error = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectEvidenceRuns() error = %v", err)
			}
			got := make([]uint64, 0, len(selected))
			for _, run := range selected {
				got = append(got, run.Number)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("selected runs = %v, want %v", got, test.want)
			}
		})
	}

	for _, test := range []struct {
		selected int64
		total    uint64
		want     uint64
	}{
		{selected: 2, total: 3, want: 2},
		{selected: 0, total: 3, want: 0},
		{selected: -1, total: math.MaxUint64, want: 0},
		{selected: -4, total: 3, want: 0},
		{selected: -2, total: 3, want: 2},
	} {
		if got := requestedRunNumber(test.selected, test.total); got != test.want {
			t.Errorf("requestedRunNumber(%d, %d) = %d, want %d", test.selected, test.total, got, test.want)
		}
	}
}

func TestEvidenceCollectorHandlesFallbacksAndOmissionPolicies(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	jobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	runIDs := make([]model.RunID, 3)
	for index := range runIDs {
		runIDs[index], err = service.ids.NewRunID()
		if err != nil {
			t.Fatal(err)
		}
	}
	startedAt := clock.now.Add(time.Second)
	prunedAt := clock.now.Add(2 * time.Second)
	runs := []model.RunState{
		{ID: runIDs[0], JobID: jobID, Number: 1, Phase: model.RunPhaseCompleted, Logs: model.LogMetadata{PrunedAt: &prunedAt}},
		{ID: runIDs[1], JobID: jobID, Number: 2, Phase: model.RunPhaseCompleted},
		{ID: runIDs[2], JobID: jobID, Number: 3, Phase: model.RunPhaseCompleted, StartedAt: &startedAt},
	}
	snapshot := store.DiagnosticSnapshot{
		Job:  model.JobState{ID: jobID, Phase: model.JobPhaseRunning, Revision: 1, SubmittedAt: clock.now},
		Runs: runs, RunFacts: map[model.RunID]store.RunDiagnosticFacts{
			runIDs[1]: {}, runIDs[2]: {},
		},
		RunsTruncated: true, EventsTruncated: true, NotificationsTruncated: true,
	}
	collector := newEvidenceCollector(service, snapshot, runs, clock.now, invalidUTF8Sanitizer{})
	if err := collector.collectLogs(t.Context(), diagnostic.EvidenceRequest{
		Logs: diagnostic.LogsTail, LogBytes: 8,
	}); err != nil {
		t.Fatalf("collectLogs() error = %v", err)
	}
	collector.addCollectionOmissions(diagnostic.EvidenceRequest{Logs: diagnostic.LogsNone, Similar: 1})
	source := diagnostic.ItemSource{Kind: "test", EntityID: jobID.String(), Revision: 1}
	if err := collector.addDiagnostic("ev:test:legacy", diagnostic.CodeJobDiagnostic,
		"not a valid diagnostic code", nil, source); err != nil {
		t.Fatalf("addDiagnostic() error = %v", err)
	}
	if got := collector.sanitizeText("ev:test:utf8", "test", "value"); got != "�" {
		t.Fatalf("sanitizeText(invalid UTF-8) = %q", got)
	}
	if err := collector.addWithDisclosure(
		"ev:test:bad", "test.bad", make(chan int), nil, source,
		diagnostic.QualityObserved, diagnostic.DisclosureMetadata,
	); err == nil {
		t.Fatal("addWithDisclosure(unsupported value) error = nil")
	}
	if err := (&evidenceCollector{snapshot: store.DiagnosticSnapshot{}}).collectAdmission(); err != nil {
		t.Fatalf("collectAdmission(nil) error = %v", err)
	}
	if err := collector.collectRunFacts(model.RunState{ID: model.RunID("missing")}); err != nil {
		t.Fatalf("collectRunFacts(missing) error = %v", err)
	}
	evidence := collector.finish()
	for _, code := range []string{
		diagnostic.OmissionLogsPruned, diagnostic.OmissionLogsUnavailable,
		diagnostic.OmissionLogContentNotRequested, diagnostic.OmissionHistoryTruncated,
		diagnostic.OmissionEventsTruncated, diagnostic.OmissionSimilarUnavailable,
		diagnostic.OmissionResourceUnavailable, diagnostic.OmissionResourceNotApplicable,
		diagnostic.OmissionResourceUnsupported, diagnostic.OmissionActiveStateMayHaveAdvanced,
		diagnostic.OmissionLegacyDiagnosticUnclassified,
	} {
		if !hasOmission(evidence, code) {
			t.Errorf("evidence lacks omission %q", code)
		}
	}
	if evidence.Consistency.Artifacts != diagnostic.ArtifactsNotCollected || len(evidence.RedactionNotices) == 0 {
		t.Fatalf("collector consistency/redactions = %q/%#v", evidence.Consistency.Artifacts, evidence.RedactionNotices)
	}

	empty := newEvidenceCollector(service, store.DiagnosticSnapshot{
		Job: model.JobState{ID: jobID, Revision: 1, Phase: model.JobPhaseCompleted},
	}, nil, clock.now, nil)
	empty.addResourceOmissions()
	if !hasOmission(empty.finish(), diagnostic.OmissionResourceNotApplicable) {
		t.Fatal("empty selection lacks resource-not-applicable omission")
	}

	canceledAt := clock.now.Add(3 * time.Second)
	jobCollector := newEvidenceCollector(service, store.DiagnosticSnapshot{
		Job: model.JobState{
			ID: jobID, Revision: 2, Phase: model.JobPhaseCompleted, Outcome: model.JobOutcomeCancelled,
			SubmittedAt: clock.now, CompletedAt: &canceledAt,
			Cancellation: &model.CancellationIntent{RequestedAt: canceledAt, Reason: model.StopReasonCancellation},
		},
	}, nil, canceledAt, nil)
	if err := jobCollector.collectJob(); err != nil {
		t.Fatalf("collectJob(canceled) error = %v", err)
	}
	timedOutRun := model.RunState{
		ID: runIDs[0], JobID: jobID, Number: 4, Revision: 1,
		Phase: model.RunPhaseCompleted, Outcome: model.RunOutcomeTimedOut,
		ReservedAt: clock.now, StartedAt: &startedAt, CompletedAt: &canceledAt,
		StopRequestedAt: &canceledAt, StopReason: model.StopReasonTimeout,
		LastDiagnosticCode: string(model.DiagnosticJobTimeout),
	}
	if err := jobCollector.collectRun(timedOutRun, false); err != nil {
		t.Fatalf("collectRun(job timeout) error = %v", err)
	}
	if item := findEvidenceItem(t, jobCollector.finish(), diagnostic.CodeRunTimeoutScope); string(item.Value) != `"job"` {
		t.Fatalf("job timeout scope = %s", item.Value)
	}
}

func TestEvidenceCollectorReportsMixedLogSnapshotsAndExpansion(t *testing.T) {
	t.Parallel()

	service, clock := newTestService(t)
	jobID, err := service.ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	runs := make([]model.RunState, 0, 2)
	for number := 1; number <= 2; number++ {
		runID, idErr := service.ids.NewRunID()
		if idErr != nil {
			t.Fatal(idErr)
		}
		capture, createErr := logstore.CreateRun(service.stateDir, jobID.String(), uint64(number))
		if createErr != nil {
			t.Fatal(createErr)
		}
		for _, stream := range []logstore.Stream{logstore.Stdout, logstore.Stderr} {
			if _, appendErr := capture.Append(stream, []byte("diagnostic log\n"), clock.now); appendErr != nil {
				t.Fatal(appendErr)
			}
		}
		if closeErr := capture.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		startedAt := clock.now.Add(time.Duration(number) * time.Second)
		run := model.RunState{
			ID: runID, JobID: jobID, Number: uint64(number), Phase: model.RunPhaseCompleted,
			StartedAt: &startedAt,
		}
		if number == 1 {
			completedAt := startedAt.Add(time.Second)
			run.CompletedAt = &completedAt
		} else {
			run.Phase = model.RunPhaseRunning
		}
		runs = append(runs, run)
	}
	snapshot := store.DiagnosticSnapshot{Job: model.JobState{ID: jobID, Revision: 1, Phase: model.JobPhaseRunning}}
	collector := newEvidenceCollector(service, snapshot, runs, clock.now, nil)
	if collectErr := collector.collectLogs(t.Context(), diagnostic.EvidenceRequest{
		Logs: diagnostic.LogsTail, LogBytes: 8,
	}); collectErr != nil {
		t.Fatalf("collectLogs(mixed) error = %v", collectErr)
	}
	if collector.evidence.Consistency.Artifacts != diagnostic.ArtifactsMixed || len(collector.evidence.Artifacts) != 4 {
		t.Fatalf("mixed artifacts = %q/%d", collector.evidence.Consistency.Artifacts, len(collector.evidence.Artifacts))
	}

	expanding := newEvidenceCollector(service, snapshot, runs[:1], clock.now, expandingSanitizer{})
	if collectErr := expanding.collectLogs(t.Context(), diagnostic.EvidenceRequest{
		Logs: diagnostic.LogsTail, LogBytes: 8,
	}); collectErr == nil {
		t.Fatal("collectLogs(expanding sanitizer) error = nil")
	}

	budgetRunID, err := service.ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	budgetCapture, err := logstore.CreateRun(service.stateDir, jobID.String(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budgetCapture.Append(logstore.Stderr, bytes.Repeat([]byte("x"), maximumEvidenceLogBytes), clock.now); err != nil {
		t.Fatal(err)
	}
	if _, err := budgetCapture.Append(logstore.Stdout, []byte("not selected"), clock.now); err != nil {
		t.Fatal(err)
	}
	if err := budgetCapture.Close(); err != nil {
		t.Fatal(err)
	}
	budgetRun := model.RunState{ID: budgetRunID, JobID: jobID, Number: 3, Phase: model.RunPhaseCompleted}
	budget := newEvidenceCollector(service, snapshot, []model.RunState{budgetRun}, clock.now, nil)
	if err := budget.collectLogs(t.Context(), diagnostic.EvidenceRequest{
		Logs: diagnostic.LogsTail, LogBytes: maximumEvidenceLogBytes,
	}); err != nil {
		t.Fatalf("collectLogs(budget) error = %v", err)
	}
	if !hasOmission(budget.finish(), diagnostic.OmissionLogBudgetExceeded) {
		t.Fatal("budgeted log collection lacks budget-exceeded omission")
	}

	canceledContext, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := newEvidenceCollector(service, snapshot, runs[:1], clock.now, nil)
	if collectErr := canceled.collectLogs(canceledContext, diagnostic.EvidenceRequest{
		Logs: diagnostic.LogsTail, LogBytes: 8,
	}); !errors.Is(collectErr, context.Canceled) {
		t.Fatalf("collectLogs(canceled) error = %v, want context cancellation", collectErr)
	}
}

func TestRunDurationAndFailureQualityHelpers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	started := now.Add(time.Second)
	completed := now.Add(2 * time.Second)
	if _, _, ok := runDuration(model.RunState{}, now); ok {
		t.Error("runDuration(without start) reported a duration")
	}
	if _, _, ok := runDuration(model.RunState{StartedAt: &started}, now); ok {
		t.Error("runDuration(negative interval) reported a duration")
	}
	if duration, observedAt, ok := runDuration(model.RunState{StartedAt: &started, CompletedAt: &completed}, now); !ok || duration != time.Second || observedAt != &completed {
		t.Fatalf("runDuration(completed) = (%s, %v, %t)", duration, observedAt, ok)
	}

	if class, quality := classifyJobFailure(model.JobState{}, 0); class != "" || quality != "" {
		t.Fatalf("classifyJobFailure(empty) = %q/%q", class, quality)
	}
	if class, quality := classifyJobFailure(model.JobState{Outcome: model.JobOutcomeFailure}, 0); class != "job_failure_without_run" || quality != diagnostic.QualityObserved {
		t.Fatalf("classifyJobFailure(generic) = %q/%q", class, quality)
	}
	if class, quality := classifyRunFailure(model.RunState{}); class != "" || quality != "" {
		t.Fatalf("classifyRunFailure(empty) = %q/%q", class, quality)
	}
}

type replacingSanitizer struct {
	old         []byte
	replacement []byte
}

type reportingSanitizer struct{ replacingSanitizer }

type invalidUTF8Sanitizer struct{}

func (invalidUTF8Sanitizer) Sanitize(string, []byte) ([]byte, bool) { return []byte{0xff}, false }

type expandingSanitizer struct{}

func (expandingSanitizer) Sanitize(string, []byte) ([]byte, bool) {
	return bytes.Repeat([]byte("x"), maximumEvidenceLogBytes+1), true
}

func (reportingSanitizer) ValueRedactionConfigured() bool { return true }

func (sanitizer replacingSanitizer) Sanitize(_ string, value []byte) ([]byte, bool) {
	replaced := bytes.ReplaceAll(value, sanitizer.old, sanitizer.replacement)

	return replaced, !bytes.Equal(replaced, value)
}

func hasEvidenceCode(evidence diagnostic.Evidence, code string) bool {
	for _, item := range evidence.Items {
		if item.Code == code {
			return true
		}
	}

	return false
}

func hasOmission(evidence diagnostic.Evidence, code string) bool {
	for _, omission := range evidence.Omissions {
		if omission.Code == code {
			return true
		}
	}

	return false
}

func findArtifact(t *testing.T, evidence diagnostic.Evidence, stream string) diagnostic.Artifact {
	t.Helper()
	for _, artifact := range evidence.Artifacts {
		if artifact.Stream == stream {
			return artifact
		}
	}
	t.Fatalf("artifact stream %q not found in %#v", stream, evidence.Artifacts)

	return diagnostic.Artifact{}
}

func findEvidenceItem(t *testing.T, evidence diagnostic.Evidence, code string) diagnostic.Item {
	t.Helper()
	for _, item := range evidence.Items {
		if item.Code == code {
			return item
		}
	}
	t.Fatalf("evidence code %q not found in %#v", code, evidence.Items)

	return diagnostic.Item{}
}
