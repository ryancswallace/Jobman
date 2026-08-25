package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/protocol"
)

type fakeSlurmScheduler struct {
	submitResponses  []slurmbackend.Submission
	submitErrors     []error
	findResponses    []slurmbackend.Submission
	findErrors       []error
	observations     []slurmbackend.Observation
	submitCalls      int
	findCalls        int
	cancelCalls      int
	cancelErr        error
	lastSubmit       slurmbackend.SubmitRequest
	arraySubmitCalls int
	lastArraySubmit  slurmbackend.ArraySubmitRequest
}

type fakeSlurmCommandRunner struct {
	calls int
	err   error
}

func (runner *fakeSlurmCommandRunner) Run(
	context.Context,
	string,
	...string,
) ([]byte, error) {
	runner.calls++

	return []byte("slurm 25.05\n"), runner.err
}

func (scheduler *fakeSlurmScheduler) Submit(
	_ context.Context,
	request slurmbackend.SubmitRequest,
) (slurmbackend.Submission, error) {
	scheduler.submitCalls++
	scheduler.lastSubmit = request
	if len(scheduler.submitErrors) != 0 {
		err := scheduler.submitErrors[0]
		scheduler.submitErrors = scheduler.submitErrors[1:]
		if err != nil {
			return slurmbackend.Submission{}, err
		}
	}
	if len(scheduler.submitResponses) == 0 {
		return slurmbackend.Submission{}, errors.New("missing fake submission")
	}
	response := scheduler.submitResponses[0]
	scheduler.submitResponses = scheduler.submitResponses[1:]

	return response, nil
}

func (scheduler *fakeSlurmScheduler) SubmitArray(
	_ context.Context,
	request slurmbackend.ArraySubmitRequest,
) (slurmbackend.Submission, error) {
	scheduler.arraySubmitCalls++
	scheduler.lastArraySubmit = request
	if len(scheduler.submitErrors) != 0 {
		err := scheduler.submitErrors[0]
		scheduler.submitErrors = scheduler.submitErrors[1:]
		if err != nil {
			return slurmbackend.Submission{}, err
		}
	}
	if len(scheduler.submitResponses) == 0 {
		return slurmbackend.Submission{}, errors.New("missing fake array submission")
	}
	response := scheduler.submitResponses[0]
	scheduler.submitResponses = scheduler.submitResponses[1:]

	return response, nil
}

func (scheduler *fakeSlurmScheduler) FindByName(
	_ context.Context,
	_, _ string,
	_ time.Time,
) (slurmbackend.Submission, error) {
	scheduler.findCalls++
	if len(scheduler.findErrors) != 0 {
		err := scheduler.findErrors[0]
		scheduler.findErrors = scheduler.findErrors[1:]
		if err != nil {
			return slurmbackend.Submission{}, err
		}
	}
	if len(scheduler.findResponses) == 0 {
		return slurmbackend.Submission{}, errors.New("missing fake find response")
	}
	response := scheduler.findResponses[0]
	scheduler.findResponses = scheduler.findResponses[1:]

	return response, nil
}

func (scheduler *fakeSlurmScheduler) Observe(
	_ context.Context,
	_ string,
) (slurmbackend.Observation, error) {
	if len(scheduler.observations) == 0 {
		return slurmbackend.Observation{}, errors.New("missing fake observation")
	}
	response := scheduler.observations[0]
	scheduler.observations = scheduler.observations[1:]

	return response, nil
}

func (scheduler *fakeSlurmScheduler) Cancel(context.Context, string) error {
	scheduler.cancelCalls++

	return scheduler.cancelErr
}

func TestServiceReconcilesSlurmLifecycle(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	exitCode := 0
	scheduler := &fakeSlurmScheduler{
		submitResponses: []slurmbackend.Submission{{JobID: "12345", Cluster: "alpha"}},
		observations: []slurmbackend.Observation{
			{JobID: "12345", State: "queued", Reason: "Resources"},
			{JobID: "12345", State: "running"},
			{
				JobID: "12345", Cluster: "alpha", State: "completed", Terminal: true,
				Result: &protocol.ProcessResult{Outcome: "success", ExitCode: &exitCode},
			},
		},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	for range 3 {
		executions, listErr := spool.ListExecutions(t.Context())
		if listErr != nil || len(executions) != 1 {
			t.Fatalf("ListExecutions() = %#v, %v", executions, listErr)
		}
		if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
			t.Fatalf("reconcileExecution() error = %v", err)
		}
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || executions[0].State != "terminal" {
		t.Fatalf("execution = %#v, %v", executions, err)
	}
	events, err := spool.PendingEvents(t.Context())
	if err != nil {
		t.Fatalf("PendingEvents() error = %v", err)
	}
	wantTypes := []string{
		"scheduler.submitted", "scheduler.observed", "scheduler.observed", "scheduler.completed",
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("events = %#v", events)
	}
	for index, event := range events {
		if event.Event.Document.Spec.Type != wantTypes[index] ||
			event.Event.Document.Metadata.Sequence != int64(index+1) {
			t.Fatalf("event %d = %#v", index, event.Event.Document)
		}
	}
	if scheduler.submitCalls != 1 || scheduler.lastSubmit.Partition != "gpu" ||
		scheduler.lastSubmit.Resources == nil || scheduler.lastSubmit.Resources.GPU != 1 {
		t.Fatalf("submission = %#v, calls = %d", scheduler.lastSubmit, scheduler.submitCalls)
	}
	sharedDirectory, directoryErr := executionDirectory(sharedRoot, testExecutionID)
	if directoryErr != nil {
		t.Fatalf("executionDirectory() error = %v", directoryErr)
	}
	if contents, readErr := os.ReadFile(filepath.Join(sharedDirectory, slurmScriptFilename)); readErr != nil || string(contents) != slurmBatchScript {
		t.Fatalf("Slurm script = %q, %v", contents, readErr)
	}
}

func TestServiceDoesNotResubmitAmbiguousSlurmAttempt(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	scheduler := &fakeSlurmScheduler{
		submitErrors:  []error{errors.New("connection lost")},
		findResponses: []slurmbackend.Submission{{JobID: "9876", Cluster: "alpha"}},
		observations:  []slurmbackend.Observation{{JobID: "9876", State: "queued"}},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution(ambiguous) error = %v", err)
	}
	executions, err = spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution(find) error = %v", err)
	}
	if scheduler.submitCalls != 1 || scheduler.findCalls != 1 {
		t.Fatalf("submit calls = %d, find calls = %d", scheduler.submitCalls, scheduler.findCalls)
	}
	events, err := spool.PendingEvents(t.Context())
	if err != nil || len(events) != 3 ||
		events[0].Event.Document.Spec.Type != "scheduler.uncertain" ||
		events[1].Event.Document.Spec.Type != "scheduler.submitted" {
		t.Fatalf("events = %#v, %v", events, err)
	}
}

func TestServiceCancelsSlurmJobAndStagesMarker(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	localDirectory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	if err = writePrivateFile(filepath.Join(localDirectory, cancelFilename), []byte("cancel\n")); err != nil {
		t.Fatalf("write cancel marker: %v", err)
	}
	scheduler := &fakeSlurmScheduler{
		submitResponses: []slurmbackend.Submission{{JobID: "12345"}},
		observations: []slurmbackend.Observation{
			{JobID: "12345", State: "queued"}, {JobID: "12345", State: "queued"},
		},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution() error = %v", err)
	}
	sharedDirectory, directoryErr := executionDirectory(sharedRoot, testExecutionID)
	if directoryErr != nil {
		t.Fatalf("executionDirectory() error = %v", directoryErr)
	}
	if scheduler.cancelCalls != 1 {
		t.Fatalf("cancel calls = %d", scheduler.cancelCalls)
	}
	executions, err = spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution(replay) error = %v", err)
	}
	if scheduler.cancelCalls != 1 {
		t.Fatalf("cancel replay calls = %d", scheduler.cancelCalls)
	}
	if _, err = os.Stat(filepath.Join(sharedDirectory, cancelFilename)); err != nil {
		t.Fatalf("shared cancel marker: %v", err)
	}
}

func TestSlurmFilesystemConfiguration(t *testing.T) {
	t.Parallel()
	for _, root := range []string{"relative", t.TempDir() + "/../shared"} {
		if _, err := prepareSharedExecutionRoot(root); err == nil {
			t.Fatalf("prepareSharedExecutionRoot(%q) unexpectedly succeeded", root)
		}
	}
	root := filepath.Join(t.TempDir(), "shared")
	prepared, err := prepareSharedExecutionRoot(root)
	if err != nil || prepared != root {
		t.Fatalf("prepareSharedExecutionRoot() = %q, %v", prepared, err)
	}
	fileRoot := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(fileRoot, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write root file: %v", err)
	}
	if _, err = prepareSharedExecutionRoot(fileRoot); err == nil {
		t.Fatal("prepareSharedExecutionRoot() accepted a file")
	}
	symlinkRoot := filepath.Join(t.TempDir(), "link")
	requireSymlink(t, root, symlinkRoot)
	if _, err = prepareSharedExecutionRoot(symlinkRoot); err == nil {
		t.Fatal("prepareSharedExecutionRoot() accepted a symlink")
	}

	if err = validateSlurmRunner("relative"); err == nil {
		t.Fatal("validateSlurmRunner() accepted a relative path")
	}
	if err = validateSlurmRunner(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("validateSlurmRunner() accepted a missing path")
	}
	runner := filepath.Join(t.TempDir(), "jobman-agent")
	if err = os.WriteFile(runner, []byte("binary"), 0o600); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	if err = validateSlurmRunner(runner); err == nil {
		t.Fatal("validateSlurmRunner() accepted a non-executable file")
	}
	if err = os.Chmod(runner, 0o700); err != nil { // #nosec G302 -- this fixture must be owner-executable.
		t.Fatalf("chmod runner: %v", err)
	}
	if err = validateSlurmRunner(runner); err != nil {
		t.Fatalf("validateSlurmRunner() error = %v", err)
	}
}

func TestPrepareSlurmBundleIsImmutableAndReplaySafe(t *testing.T) {
	t.Parallel()
	assignment, authorization := testSlurmAssignment(t)
	root := t.TempDir()
	directory, err := prepareSlurmBundle(root, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareSlurmBundle() error = %v", err)
	}
	if replayed, replayErr := prepareSlurmBundle(root, assignment, authorization); replayErr != nil || replayed != directory {
		t.Fatalf("prepareSlurmBundle(replay) = %q, %v", replayed, replayErr)
	}
	document := assignment.Document
	document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Command.Args = []string{"changed"}
	changed := resealAssignment(t, document)
	authorization.Spec.EffectiveExecutionDigest = changed.EffectiveExecutionDigest
	if _, err = prepareSlurmBundle(root, changed, authorization); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("prepareSlurmBundle(conflict) error = %v", err)
	}
	missingParent := filepath.Join(t.TempDir(), "missing", "value")
	if err = writeImmutablePrivateFile(missingParent, []byte("value")); err == nil {
		t.Fatal("writeImmutablePrivateFile() accepted a missing parent")
	}
	symlinkRoot := t.TempDir()
	symlinkParent := filepath.Join(symlinkRoot, executionsDirectoryName)
	if err = os.MkdirAll(symlinkParent, 0o700); err != nil {
		t.Fatalf("create execution parent: %v", err)
	}
	outside := t.TempDir()
	requireSymlink(t, outside, filepath.Join(symlinkParent, testExecutionID))
	if _, err = prepareSlurmBundle(symlinkRoot, assignment, authorization); err == nil {
		t.Fatal("prepareSlurmBundle() accepted a symlinked execution directory")
	}
	invalid := assignment
	invalid.Document.Spec.EffectiveExecution.Metadata.ExecutionID = "invalid"
	if _, err = prepareSlurmBundle(t.TempDir(), invalid, authorization); err == nil {
		t.Fatal("prepareSlurmBundle() accepted an invalid execution ID")
	}
}

func TestSlurmManifestsRejectInvalidIdentity(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	if _, err := readSlurmAttempt(directory, testExecutionID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readSlurmAttempt(missing) error = %v", err)
	}
	if err := writeManifest(filepath.Join(directory, slurmAttemptFilename), slurmAttemptManifest{}); err != nil {
		t.Fatalf("write attempt: %v", err)
	}
	if _, err := readSlurmAttempt(directory, testExecutionID); err == nil {
		t.Fatal("readSlurmAttempt() accepted an invalid manifest")
	}
	if err := writeManifest(filepath.Join(directory, slurmSubmissionFilename), slurmSubmissionManifest{}); err != nil {
		t.Fatalf("write submission: %v", err)
	}
	if _, err := readSlurmSubmission(directory, testExecutionID); err == nil {
		t.Fatal("readSlurmSubmission() accepted an invalid manifest")
	}
	if _, err := persistSlurmSubmission(
		filepath.Join(directory, "missing"), testExecutionID,
		slurmbackend.Submission{JobID: "123"}, time.Now().UTC(),
	); err == nil {
		t.Fatal("persistSlurmSubmission() accepted a missing directory")
	}
}

func TestServiceKeepsUnreconciledSubmissionUncertain(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	scheduler := &fakeSlurmScheduler{
		submitErrors: []error{errors.New("connection lost")},
		findErrors:   []error{errors.New("accounting unavailable")},
	}
	service := testSlurmService(stateDirectory, t.TempDir(), spool, scheduler)
	service.logger = slog.New(slog.DiscardHandler)
	for range 2 {
		executions, listErr := spool.ListExecutions(t.Context())
		if listErr != nil || len(executions) != 1 {
			t.Fatalf("ListExecutions() = %#v, %v", executions, listErr)
		}
		if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
			t.Fatalf("reconcileExecution() error = %v", err)
		}
	}
	if scheduler.submitCalls != 1 || scheduler.findCalls != 1 {
		t.Fatalf("submit calls = %d, find calls = %d", scheduler.submitCalls, scheduler.findCalls)
	}
	events, err := spool.PendingEvents(t.Context())
	if err != nil || len(events) != 1 || events[0].Event.Document.Spec.Type != "scheduler.uncertain" {
		t.Fatalf("events = %#v, %v", events, err)
	}
}

func TestServiceRejectsInvalidTerminalSlurmEvidence(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	scheduler := &fakeSlurmScheduler{
		submitResponses: []slurmbackend.Submission{{JobID: "123"}},
		observations: []slurmbackend.Observation{{
			JobID: "123", State: "failed", Terminal: true,
		}},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err == nil {
		t.Fatal("reconcileExecution() accepted terminal evidence without a result")
	}
}

func TestCancelSlurmExecutionReturnsSchedulerFailure(t *testing.T) {
	t.Parallel()
	stateDirectory := t.TempDir()
	sharedDirectory := t.TempDir()
	scheduler := &fakeSlurmScheduler{cancelErr: errors.New("denied")}
	service := &service{slurm: scheduler}
	if err := service.cancelSlurmExecution(
		t.Context(), stateDirectory, sharedDirectory, "123",
	); err == nil {
		t.Fatal("cancelSlurmExecution() ignored scheduler failure")
	}
	if scheduler.cancelCalls != 1 {
		t.Fatalf("cancel calls = %d", scheduler.cancelCalls)
	}
}

func TestRunServiceWithSlurmConfiguration(t *testing.T) {
	stateDirectory := t.TempDir()
	pki := newTestPKI(t)
	saveTestCredentials(t, pki, stateDirectory, "https://127.0.0.1:1")
	commandRunner := &fakeSlurmCommandRunner{}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunService(ctx, ServiceOptions{
		StateDirectory: stateDirectory, Logger: slog.New(slog.DiscardHandler),
		PollInterval: time.Second, SlurmRoot: filepath.Join(t.TempDir(), "shared"),
		SlurmRunner: os.Args[0], slurmCommandRunner: commandRunner,
	}); err != nil {
		t.Fatalf("RunService() error = %v", err)
	}
	if commandRunner.calls != 4 {
		t.Fatalf("Slurm probe calls = %d", commandRunner.calls)
	}
}

func TestRunServiceRejectsSlurmProbeFailure(t *testing.T) {
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: slog.New(slog.DiscardHandler),
		SlurmRoot: filepath.Join(t.TempDir(), "shared"), SlurmRunner: os.Args[0],
		slurmCommandRunner: &fakeSlurmCommandRunner{err: errors.New("missing Slurm CLI")},
	}); err == nil {
		t.Fatal("RunService() accepted failed Slurm probe")
	}
}

func TestValidateSlurmAssignmentRejectsUnsupportedFeatures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*protocol.AgentAssignment)
	}{
		{name: "wrong backend", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend = "subprocess"
		}},
		{name: "shell command", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Command = protocol.Command{
				Shell: &protocol.ShellCommand{Capability: "posix-shell", Script: "true"},
			}
		}},
		{name: "extensions", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Extensions = map[string]json.RawMessage{"slurm": json.RawMessage(`{"constraint":"a100"}`)}
		}},
		{name: "temporary storage", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Resources.TemporaryStorage = "1GiB"
		}},
		{name: "environment profile", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Environment = &protocol.Environment{Profile: "research"}
		}},
		{name: "secret binding", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Environment = &protocol.Environment{
				Secrets: []protocol.SecretBinding{{
					Name: "token", Source: "secret://research/token",
					ExposeAs: protocol.SecretExposure{Environment: "TOKEN"},
				}},
			}
		}},
		{name: "retry", mutate: func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Policy.Retry.MaxRuns = 2
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assignment, authorization := testSlurmAssignment(t)
			document := assignment.Document
			test.mutate(&document)
			assignment = resealAssignment(t, document)
			authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
			if err := validateSlurmAssignment(assignment, authorization, nil); err == nil {
				t.Fatal("validateSlurmAssignment() accepted an unsupported feature")
			}
		})
	}
	assignment, authorization := testSlurmAssignment(t)
	if err := validateExecutableAssignment(assignment, authorization, nil); err != nil {
		t.Fatalf("validateExecutableAssignment(Slurm) error = %v", err)
	}
	assignment.Document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend = "future"
	if err := validateExecutableAssignment(assignment, authorization, nil); err == nil {
		t.Fatal("validateExecutableAssignment() accepted an unknown backend")
	}
	assignment, authorization = testSlurmAssignment(t)
	authorization.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := validateSlurmAssignment(assignment, authorization, nil); err == nil {
		t.Fatal("validateSlurmAssignment() accepted mismatched authorization")
	}
}

func TestSlurmReconciliationFailurePaths(t *testing.T) {
	t.Run("unusable shared root", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
		sharedRoot := filepath.Join(t.TempDir(), "root-file")
		if err := os.WriteFile(sharedRoot, []byte("not a directory"), 0o600); err != nil {
			t.Fatalf("write shared root: %v", err)
		}
		service := testSlurmService(
			stateDirectory, sharedRoot, spool, &fakeSlurmScheduler{},
		)
		localDirectory, err := prepareExecutionFiles(
			stateDirectory, execution.Assignment, *execution.Authorization,
		)
		if err != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", err)
		}
		if err = service.reconcileSlurmExecution(
			t.Context(), execution, localDirectory,
		); err == nil {
			t.Fatal("reconcileSlurmExecution() accepted an unusable root")
		}
	})

	t.Run("corrupt submission manifest", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
		service := testSlurmService(
			stateDirectory, t.TempDir(), spool, &fakeSlurmScheduler{},
		)
		localDirectory, err := prepareExecutionFiles(
			stateDirectory, execution.Assignment, *execution.Authorization,
		)
		if err != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", err)
		}
		if err = writePrivateFile(
			filepath.Join(localDirectory, slurmSubmissionFilename), []byte(`{}`),
		); err != nil {
			t.Fatalf("write submission: %v", err)
		}
		if err = service.reconcileSlurmExecution(
			t.Context(), execution, localDirectory,
		); err == nil {
			t.Fatal("reconcileSlurmExecution() accepted a corrupt submission")
		}
	})

	t.Run("scheduler observation failure", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
		sharedRoot := t.TempDir()
		service := testSlurmService(
			stateDirectory, sharedRoot, spool, &fakeSlurmScheduler{},
		)
		localDirectory, err := prepareExecutionFiles(
			stateDirectory, execution.Assignment, *execution.Authorization,
		)
		if err != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", err)
		}
		if _, err = persistSlurmSubmission(
			localDirectory, testExecutionID,
			slurmbackend.Submission{JobID: "123"}, time.Now().UTC(),
		); err != nil {
			t.Fatalf("persistSlurmSubmission() error = %v", err)
		}
		if err = service.reconcileSlurmExecution(
			t.Context(), execution, localDirectory,
		); err == nil {
			t.Fatal("reconcileSlurmExecution() ignored observation failure")
		}
	})

	t.Run("scheduler event journal failure", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
		service := testSlurmService(
			stateDirectory, t.TempDir(), spool, &fakeSlurmScheduler{},
		)
		localDirectory, err := prepareExecutionFiles(
			stateDirectory, execution.Assignment, *execution.Authorization,
		)
		if err != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", err)
		}
		if _, err = persistSlurmSubmission(
			localDirectory, testExecutionID,
			slurmbackend.Submission{JobID: "123"}, time.Now().UTC(),
		); err != nil {
			t.Fatalf("persistSlurmSubmission() error = %v", err)
		}
		if err = spool.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err = service.reconcileSlurmExecution(
			t.Context(), execution, localDirectory,
		); err == nil {
			t.Fatal("reconcileSlurmExecution() ignored journal failure")
		}
	})

	t.Run("cancellation marker inspection failure", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
		scheduler := &fakeSlurmScheduler{
			observations: []slurmbackend.Observation{{JobID: "123", State: "queued"}},
		}
		service := testSlurmService(stateDirectory, t.TempDir(), spool, scheduler)
		localDirectory, err := prepareExecutionFiles(
			stateDirectory, execution.Assignment, *execution.Authorization,
		)
		if err != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", err)
		}
		if _, err = persistSlurmSubmission(
			localDirectory, testExecutionID,
			slurmbackend.Submission{JobID: "123"}, time.Now().UTC(),
		); err != nil {
			t.Fatalf("persistSlurmSubmission() error = %v", err)
		}
		cancelPath := filepath.Join(localDirectory, cancelFilename)
		requireSymlink(t, cancelPath, cancelPath)
		if err = service.reconcileSlurmExecution(
			t.Context(), execution, localDirectory,
		); err == nil {
			t.Fatal("reconcileSlurmExecution() ignored cancellation inspection failure")
		}
	})
}

func TestEnsureSlurmSubmissionFailurePaths(t *testing.T) {
	stateDirectory := t.TempDir()
	spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
	service := testSlurmService(
		stateDirectory, t.TempDir(), spool, &fakeSlurmScheduler{},
	)
	if _, err := service.ensureSlurmSubmission(
		t.Context(), execution, filepath.Join(t.TempDir(), "missing"), t.TempDir(),
	); err == nil {
		t.Fatal("ensureSlurmSubmission() accepted a missing local directory")
	}

	localDirectory := t.TempDir()
	if err := writePrivateFile(
		filepath.Join(localDirectory, slurmAttemptFilename), []byte(`{}`),
	); err != nil {
		t.Fatalf("write attempt: %v", err)
	}
	if _, err := service.ensureSlurmSubmission(
		t.Context(), execution, localDirectory, t.TempDir(),
	); err == nil {
		t.Fatal("ensureSlurmSubmission() accepted a corrupt attempt")
	}

	closedSpool, closedExecution := newAcceptedSlurmExecution(t, t.TempDir())
	closedService := testSlurmService(
		t.TempDir(), t.TempDir(), closedSpool,
		&fakeSlurmScheduler{submitErrors: []error{errors.New("connection lost")}},
	)
	closedLocal := t.TempDir()
	if err := closedSpool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := closedService.ensureSlurmSubmission(
		t.Context(), closedExecution, closedLocal, t.TempDir(),
	); err == nil {
		t.Fatal("ensureSlurmSubmission() ignored journal failure")
	}

	findSpool, findExecution := newAcceptedSlurmExecution(t, t.TempDir())
	findService := testSlurmService(
		t.TempDir(), t.TempDir(), findSpool,
		&fakeSlurmScheduler{findErrors: []error{errors.New("accounting unavailable")}},
	)
	findLocal := t.TempDir()
	if err := writeManifest(filepath.Join(findLocal, slurmAttemptFilename), slurmAttemptManifest{
		ExecutionID: testExecutionID, JobName: "jobman-" + testExecutionID,
		ExecutionUser: "researcher", AttemptedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("write attempt: %v", err)
	}
	if err := findSpool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := findService.ensureSlurmSubmission(
		t.Context(), findExecution, findLocal, t.TempDir(),
	); err == nil {
		t.Fatal("ensureSlurmSubmission() ignored reconciled journal failure")
	}
}

func TestCompleteSlurmExecutionFailurePaths(t *testing.T) {
	stateDirectory := t.TempDir()
	spool, execution := newAcceptedSlurmExecution(t, stateDirectory)
	service := testSlurmService(stateDirectory, t.TempDir(), spool, &fakeSlurmScheduler{})
	exitCode := 1
	observation := slurmbackend.Observation{
		JobID: "123", State: "failed", Terminal: true,
		Result: &protocol.ProcessResult{Outcome: "failure", ExitCode: &exitCode, FailureCode: "slurm_failed"},
	}
	sharedDirectory := t.TempDir()
	if err := writeManifest(filepath.Join(sharedDirectory, completionFilename), CompletionManifest{
		ExecutionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		ObservedAt:  time.Now().UTC(), Result: *observation.Result,
	}); err != nil {
		t.Fatalf("write completion: %v", err)
	}
	if err := service.completeSlurmExecution(
		t.Context(), execution, sharedDirectory, observation, "alpha",
	); err == nil {
		t.Fatal("completeSlurmExecution() accepted mismatched completion identity")
	}

	if err := writePrivateFile(filepath.Join(sharedDirectory, completionFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write invalid completion: %v", err)
	}
	if err := service.completeSlurmExecution(
		t.Context(), execution, sharedDirectory, observation, "alpha",
	); err == nil {
		t.Fatal("completeSlurmExecution() accepted an invalid completion")
	}

	if err := service.queueSchedulerEvent(
		t.Context(), testExecutionID, "scheduler.submitted", time.Time{},
		"123", "queued", "", "",
	); err == nil {
		t.Fatal("queueSchedulerEvent() accepted invalid ordering metadata")
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := service.queueSchedulerEvent(
		t.Context(), testExecutionID, "scheduler.submitted", time.Now().UTC(),
		"123", "queued", "", "",
	); err == nil {
		t.Fatal("queueSchedulerEvent() ignored journal failure")
	}
	if err := writeManifest(filepath.Join(sharedDirectory, completionFilename), CompletionManifest{
		ExecutionID: testExecutionID, ObservedAt: time.Now().UTC(), Result: *observation.Result,
	}); err != nil {
		t.Fatalf("write completion: %v", err)
	}
	artifactStore, err := artifact.NewFilesystemStore("logs", 1, t.TempDir())
	if err != nil {
		t.Fatalf("NewFilesystemStore() error = %v", err)
	}
	serviceWithArtifacts := *service
	serviceWithArtifacts.artifactStore = artifactStore
	if err = serviceWithArtifacts.completeSlurmExecution(
		t.Context(), execution, sharedDirectory, observation, "alpha",
	); err == nil {
		t.Fatal("completeSlurmExecution() ignored terminal log publication failure")
	}
	if err := service.completeSlurmExecution(
		t.Context(), execution, sharedDirectory, observation, "alpha",
	); err == nil {
		t.Fatal("completeSlurmExecution() ignored terminal event journal failure")
	}
}

func TestCancelSlurmExecutionRejectsMissingSharedDirectory(t *testing.T) {
	t.Parallel()
	service := &service{slurm: &fakeSlurmScheduler{}}
	if err := service.cancelSlurmExecution(
		t.Context(), t.TempDir(), filepath.Join(t.TempDir(), "missing"), "123",
	); err == nil {
		t.Fatal("cancelSlurmExecution() accepted a missing shared directory")
	}
}

func TestCancelSlurmExecutionRejectsUnreadableReceipt(t *testing.T) {
	t.Parallel()
	localDirectory := t.TempDir()
	receipt := filepath.Join(localDirectory, slurmCancelReceiptFilename)
	requireSymlink(t, receipt, receipt)
	service := &service{slurm: &fakeSlurmScheduler{}}
	if err := service.cancelSlurmExecution(
		t.Context(), localDirectory, t.TempDir(), "123",
	); err == nil {
		t.Fatal("cancelSlurmExecution() ignored receipt inspection failure")
	}
}

func TestTerminateAndWaitRecordsIdentityFailure(t *testing.T) {
	t.Parallel()
	waited := make(chan error, 1)
	waited <- nil
	result := terminateAndWait(
		&exec.Cmd{}, platform.ProcessIdentity{PID: -1}, waited, "aborted", "",
	)
	if result.Outcome != "aborted" || result.FailureCode != "graceful_termination_failed" {
		t.Fatalf("terminateAndWait() = %#v", result)
	}
}

func newAcceptedSlurmExecution(t *testing.T, stateDirectory string) (*Spool, SpoolExecution) {
	t.Helper()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, authorization := testSlurmAssignment(t)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}

	return spool, executions[0]
}

func requireSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
}

func TestSlurmAssignmentProtectsSchedulerEnvironment(t *testing.T) {
	assignment, authorization := testSlurmAssignment(t)
	document := assignment.Document
	document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Environment = &protocol.Environment{
		Values: map[string]string{"SLURM_JOB_ID": "forged"},
	}
	assignment = resealAssignment(t, document)
	authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
	if err := validateSlurmAssignment(assignment, authorization, nil); err == nil {
		t.Fatal("validateSlurmAssignment() accepted a scheduler environment override")
	}
	t.Setenv("SLURM_JOB_ID", "12345")
	values := slurmExecutionEnvironment()
	found := false
	for _, value := range values {
		found = found || value == "SLURM_JOB_ID=12345"
	}
	if !found {
		t.Fatalf("slurmExecutionEnvironment() = %#v", values)
	}
}

func testSlurmAssignment(
	t *testing.T,
) (protocol.SealedAgentAssignment, protocol.LaunchAuthorization) {
	t.Helper()
	assignment, authorization := testAssignment(t, "true", nil, nil)
	document := assignment.Document
	document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend = "slurm"
	document.Spec.EffectiveExecution.Spec.Placement.Partition = "gpu"
	document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Resources = &protocol.Resources{
		CPU: 2, GPU: 1, Memory: "4GiB", WallTime: "1h",
	}
	assignment = resealAssignment(t, document)
	authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest

	return assignment, authorization
}

func testSlurmService(
	stateDirectory, sharedRoot string,
	spool *Spool,
	scheduler slurmScheduler,
) *service {
	return &service{
		stateDirectory: stateDirectory, slurmRoot: sharedRoot,
		slurmRunner: "/nfs/jobman-agent", executionUser: "researcher",
		maximumLogBytes: defaultMaximumLogBytes, slurm: scheduler, spool: spool,
		client: &Client{credentials: credentialFiles{Metadata: metadata{AgentID: testAgentID}}},
	}
}
