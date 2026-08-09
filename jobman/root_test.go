package jobman

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/liveinput"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/store"
)

const testJobID = "01980f4c-7b2a-7a6f-8c10-0123456789ab"

type fakeBackend struct {
	submitRequest   *app.SubmitRequest
	jobs            []model.JobState
	details         app.JobDetails
	logs            []byte
	canceled        bool
	paused          bool
	resumed         bool
	rerunRequest    *app.RerunRequest
	input           []byte
	inputEOF        atomic.Bool
	inputEOFOnce    sync.Once
	inputEOFSignal  chan struct{}
	waitForInputEOF bool
	followed        atomic.Int64
	waited          atomic.Int64
	cleanRequest    *app.CleanRequest
	closed          bool
	operationErr    error
	appliedConfig   int
	configured      int
	listRequest     *app.ListRequest
	doctorRequest   *app.DoctorRequest
	evidenceRequest *diagnostic.EvidenceRequest
	evidence        diagnostic.Evidence
}

func (backend *fakeBackend) ConfigureInvocation(config.Config) { backend.configured++ }

func (backend *fakeBackend) ApplyConfig(context.Context, config.Config) error {
	backend.appliedConfig++

	return backend.operationErr
}

func (backend *fakeBackend) Close() error {
	backend.closed = true

	return nil
}

func (backend *fakeBackend) Submit(_ context.Context, request app.SubmitRequest) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	submittedRequest := request
	submittedRequest.Arguments = append([]string(nil), request.Arguments...)
	submittedRequest.Environment = cloneMap(request.Environment)
	backend.submitRequest = &submittedRequest

	return backend.jobs[0], nil
}

func (backend *fakeBackend) List(context.Context) ([]model.JobState, error) {
	if backend.operationErr != nil {
		return nil, backend.operationErr
	}
	return append([]model.JobState(nil), backend.jobs...), nil
}

func (backend *fakeBackend) ListJobs(_ context.Context, request app.ListRequest) ([]app.ListedJob, error) {
	if backend.operationErr != nil {
		return nil, backend.operationErr
	}
	backend.listRequest = &request
	limit := min(request.Limit, len(backend.jobs))
	listed := make([]app.ListedJob, 0, limit)
	for index := range limit {
		item := app.ListedJob{Job: backend.jobs[index]}
		if request.ShowRuns {
			item.Runs = append([]model.RunState(nil), backend.details.Runs...)
		}
		listed = append(listed, item)
	}

	return listed, nil
}

func (backend *fakeBackend) Doctor(_ context.Context, request app.DoctorRequest) (app.DoctorReport, error) {
	if backend.operationErr != nil {
		return app.DoctorReport{}, backend.operationErr
	}
	backend.doctorRequest = &request

	return app.DoctorReport{Store: store.HealthReport{
		Healthy: true, SchemaVersion: 7, SupportedSchema: 7, SQLiteVersion: "3.51.3",
	}}, nil
}

func (backend *fakeBackend) DiagnosticEvidence(
	_ context.Context,
	request diagnostic.EvidenceRequest,
	sanitizer diagnostic.Sanitizer,
) (diagnostic.Evidence, error) {
	if backend.operationErr != nil {
		return diagnostic.Evidence{}, backend.operationErr
	}
	backend.evidenceRequest = &request
	value := backend.evidence
	if len(value.Artifacts) > 0 && sanitizer != nil {
		value.Artifacts = slices.Clone(value.Artifacts)
		for index := range value.Artifacts {
			value.Artifacts[index].Data = slices.Clone(value.Artifacts[index].Data)
			value.Artifacts[index].Data, _ = sanitizer.Sanitize("run.log."+value.Artifacts[index].Stream, value.Artifacts[index].Data)
		}
		var err error
		value, err = diagnostic.Seal(value)
		if err != nil {
			return diagnostic.Evidence{}, err
		}
	}

	return value, nil
}

func (backend *fakeBackend) Inspect(context.Context, string) (app.JobDetails, error) {
	if backend.operationErr != nil {
		return app.JobDetails{}, backend.operationErr
	}
	return backend.details, nil
}

func (backend *fakeBackend) ReadLogs(context.Context, string, app.LogStream) ([]byte, error) {
	if backend.operationErr != nil {
		return nil, backend.operationErr
	}
	return append([]byte(nil), backend.logs...), nil
}

func (backend *fakeBackend) Cancel(context.Context, string) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	backend.canceled = true

	return backend.jobs[0], nil
}

func (backend *fakeBackend) Pause(context.Context, string) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	backend.paused = true

	return backend.jobs[0], nil
}

func (backend *fakeBackend) Resume(context.Context, string) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	backend.resumed = true

	return backend.jobs[0], nil
}

func (backend *fakeBackend) Wait(ctx context.Context, _ string) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	if backend.waitForInputEOF {
		select {
		case <-ctx.Done():
			return model.JobState{}, ctx.Err()
		case <-backend.inputEOFSignal:
		}
	}
	backend.waited.Add(1)
	return backend.jobs[0], nil
}

func (backend *fakeBackend) Rerun(
	_ context.Context,
	_ string,
	request app.RerunRequest,
) (model.JobState, error) {
	if backend.operationErr != nil {
		return model.JobState{}, backend.operationErr
	}
	backend.rerunRequest = &request

	return backend.jobs[0], nil
}

func (backend *fakeBackend) SendInput(
	_ context.Context,
	_ string,
	source io.Reader,
	sendEOF bool,
) (liveinput.Result, error) {
	if backend.operationErr != nil {
		return liveinput.Result{}, backend.operationErr
	}
	content, err := io.ReadAll(source)
	if err != nil {
		return liveinput.Result{}, err
	}
	backend.input = append(backend.input, content...)
	backend.inputEOF.Store(sendEOF)
	if sendEOF {
		backend.inputEOFOnce.Do(func() { close(backend.inputEOFSignal) })
	}

	return liveinput.Result{Delivered: uint64(len(content)), EOF: sendEOF}, nil
}

func (backend *fakeBackend) ReadRunLogs(context.Context, string, app.LogStream, uint64) ([]byte, error) {
	if backend.operationErr != nil {
		return nil, backend.operationErr
	}
	return append([]byte(nil), backend.logs...), nil
}

func (backend *fakeBackend) FollowLogs(
	_ context.Context,
	_ string,
	_ app.LogStream,
	_ uint64,
	destination io.Writer,
) error {
	if backend.operationErr != nil {
		return backend.operationErr
	}
	backend.followed.Add(1)
	_, err := destination.Write(backend.logs)

	return err
}

func (backend *fakeBackend) Clean(_ context.Context, request app.CleanRequest) (app.CleanResult, error) {
	if backend.operationErr != nil {
		return app.CleanResult{}, backend.operationErr
	}
	backend.cleanRequest = &request

	return app.CleanResult{Runs: 1, Files: 3, Bytes: 12}, nil
}

func TestRootShowsHelpWithoutOpeningStore(t *testing.T) {
	t.Parallel()

	stdout, err := executeCommand(t, dependencies{}, nil)
	if err != nil {
		t.Fatalf("ExecuteContext() error = %v", err)
	}
	if !strings.Contains(stdout, "Available Commands:") || !strings.Contains(stdout, "run") {
		t.Fatalf("root output = %q, want command help", stdout)
	}
	if strings.Contains(stdout, "__supervise") || strings.Contains(stdout, "kill") {
		t.Fatalf("root output exposes private or removed command: %q", stdout)
	}
}

func TestNewCommandReturnsIndependentProductionTrees(t *testing.T) {
	first := NewCommand()
	second := NewCommand()

	if first == second {
		t.Fatal("NewCommand returned the same command tree twice")
	}
	if first.Name() != "jobman" || second.Name() != "jobman" {
		t.Fatalf("NewCommand names = %q, %q; want jobman", first.Name(), second.Name())
	}
}

func TestRootReportsBuildVersion(t *testing.T) {
	t.Parallel()

	stdout, err := executeCommand(t, dependencies{}, []string{"--version"})
	if err != nil {
		t.Fatalf("--version error = %v", err)
	}
	if !strings.HasPrefix(stdout, "jobman ") || !strings.HasSuffix(stdout, "\n") {
		t.Fatalf("--version output = %q", stdout)
	}
}

func TestVersionOneJSONContractFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		arguments []string
		fixture   string
	}{
		{name: "list", arguments: []string{"list", "--json"}, fixture: "list-v1.json"},
		{name: "status", arguments: []string{"status", "--json", testJobID}, fixture: "status-v1.json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			output, err := executeCommand(t, dependenciesFor(newFakeBackend(t)), test.arguments)
			if err != nil {
				t.Fatalf("command error = %v", err)
			}
			want, err := os.ReadFile(filepath.Join("testdata", test.fixture))
			if err != nil {
				t.Fatal(err)
			}
			if output != string(want) {
				t.Fatalf("JSON contract output:\n%s\nwant:\n%s", output, want)
			}
		})
	}
}

func TestEmergencyAndInspectionCommandsIgnoreMalformedConfiguration(t *testing.T) {
	t.Parallel()

	configuration := filepath.Join(t.TempDir(), "broken.yml")
	if err := os.WriteFile(configuration, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	commands := [][]string{
		{"list"},
		{"status", testJobID},
		{"show", testJobID},
		{"logs", testJobID},
		{"cancel", testJobID},
		{"pause", testJobID},
		{"resume", testJobID},
		{"clean", testJobID, "--dry-run"},
		{"clean", "--older-than", "1h", "--dry-run"},
	}
	for _, arguments := range commands {
		backend := newFakeBackend(t)
		selected := append([]string{"--config", configuration}, arguments...)
		if _, err := executeCommand(t, dependenciesFor(backend), selected); err != nil {
			t.Errorf("%v with malformed configuration: %v", arguments, err)
		}
		if backend.appliedConfig != 0 {
			t.Errorf("%v applied configuration %d times", arguments, backend.appliedConfig)
		}
	}

	backend := newFakeBackend(t)
	if _, err := executeCommand(
		t,
		dependenciesFor(backend),
		[]string{"--config", configuration, "config", "apply"},
	); err == nil {
		t.Fatal("config apply accepted malformed configuration")
	}
	if backend.appliedConfig != 0 {
		t.Fatalf("malformed config apply reached backend %d times", backend.appliedConfig)
	}
}

func TestConfigurationExitStatusDistinguishesInvalidContentFromIO(t *testing.T) {
	t.Parallel()

	configuration := filepath.Join(t.TempDir(), "broken.yml")
	if err := os.WriteFile(configuration, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := executeCommand(t, dependencies{}, []string{"--config", configuration, "config", "validate"})
	if !errors.Is(err, config.ErrInvalid) || ExitCode(err) != 2 {
		t.Fatalf("malformed configuration error/code = %v/%d, want invalid/2", err, ExitCode(err))
	}

	missing := filepath.Join(t.TempDir(), "missing.yml")
	_, err = executeCommand(t, dependencies{}, []string{"--config", missing, "config", "validate"})
	if err == nil || errors.Is(err, config.ErrInvalid) || !errors.Is(err, os.ErrNotExist) || ExitCode(err) != 1 {
		t.Fatalf("missing configuration error/code = %v/%d, want filesystem/1", err, ExitCode(err))
	}
}

func TestConfiguredRedactionAppliesToOutputAndWrappedErrors(t *testing.T) {
	t.Parallel()

	configuration := filepath.Join(t.TempDir(), "jobman.yml")
	if err := os.WriteFile(configuration, []byte("redaction:\n  patterns: ['private-[0-9]+']\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend(t)
	backend.jobs[0].Spec = mustTestSpec(t, "private-123")
	stdout, err := executeCommand(
		t,
		dependenciesFor(backend),
		[]string{"--config", configuration, "list", "--json"},
	)
	if err != nil {
		t.Fatalf("list error = %v", err)
	}
	if strings.Contains(stdout, "private-123") || !strings.Contains(stdout, config.Redacted) {
		t.Fatalf("redacted list output = %q", stdout)
	}

	backend = newFakeBackend(t)
	backend.operationErr = errors.New("operation exposed private-456")
	_, err = executeCommand(
		t,
		dependenciesFor(backend),
		[]string{"--config", configuration, "status", testJobID},
	)
	if err == nil || strings.Contains(err.Error(), "private-456") || !strings.Contains(err.Error(), config.Redacted) {
		t.Fatalf("redacted error = %v", err)
	}
}

func TestRunPreservesArgumentsAndEnvironment(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"--state-dir", stateDir,
		"run", "--name", "example", "--cwd", t.TempDir(), "--env", "KEY=a b", "--",
		"printf", "%s", "a b", "$HOME", "x;y",
	})
	if err != nil {
		t.Fatalf("ExecuteContext() error = %v", err)
	}
	if stdout != testJobID+"\n" {
		t.Fatalf("run output = %q, want only canonical ID", stdout)
	}
	if backend.submitRequest == nil {
		t.Fatal("Submit() was not called")
	}
	if backend.appliedConfig != 1 || backend.configured != 1 {
		t.Fatalf("configuration calls = applied %d configured %d, want one each", backend.appliedConfig, backend.configured)
	}
	if backend.submitRequest.Executable != "printf" {
		t.Fatalf("executable = %q, want printf", backend.submitRequest.Executable)
	}
	wantArguments := []string{"%s", "a b", "$HOME", "x;y"}
	if !slices.Equal(backend.submitRequest.Arguments, wantArguments) {
		t.Fatalf("arguments = %#v, want %#v", backend.submitRequest.Arguments, wantArguments)
	}
	if backend.submitRequest.Environment["KEY"] != "a b" {
		t.Fatalf("environment = %#v, want KEY preserved", backend.submitRequest.Environment)
	}
	if !backend.closed {
		t.Fatal("backend was not closed")
	}
}

func TestRunOverlaysStopPolicyFlagsIndependently(t *testing.T) {
	tests := []struct {
		name      string
		flags     []string
		wantGrace time.Duration
		wantForce bool
	}{
		{name: "force only", flags: []string{"--force-after-grace=false"}, wantGrace: 10 * time.Second},
		{name: "grace only", flags: []string{"--stop-grace=2s"}, wantGrace: 2 * time.Second, wantForce: true},
		{
			name: "explicit zero no force", flags: []string{"--stop-grace=0s", "--force-after-grace=false"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newFakeBackend(t)
			arguments := []string{"run"}
			arguments = append(arguments, test.flags...)
			arguments = append(arguments, "--", "true")
			if _, err := executeCommand(t, dependenciesFor(backend), arguments); err != nil {
				t.Fatalf("ExecuteContext() error = %v", err)
			}
			if backend.submitRequest == nil {
				t.Fatal("Submit() was not called")
			}
			got := backend.submitRequest.StopPolicy
			if !backend.submitRequest.StopPolicySet || got.GracePeriod != test.wantGrace ||
				got.ForceAfterGrace != test.wantForce {
				t.Fatalf("stop policy = %#v (set=%t), want grace=%s force=%t",
					got, backend.submitRequest.StopPolicySet, test.wantGrace, test.wantForce)
			}
		})
	}
}

func TestRunRequiresArgumentBoundary(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	_, err := executeCommand(t, dependenciesFor(backend), []string{"run", "printf", "hello"})
	if !errors.Is(err, errUsage) || ExitCode(err) != 2 {
		t.Fatalf("run without -- error/code = %v/%d, want usage/2", err, ExitCode(err))
	}
	if backend.submitRequest != nil {
		t.Fatal("invalid run opened the backend")
	}

	backend = newFakeBackend(t)
	_, err = executeCommand(t, dependenciesFor(backend), []string{"run", "--stdin", "invalid", "--", "true"})
	if !errors.Is(err, errUsage) || backend.appliedConfig != 0 || backend.submitRequest != nil {
		t.Fatalf("invalid run policy = error %v applied %d request %+v", err, backend.appliedConfig, backend.submitRequest)
	}
}

func TestRunAppliesDeferredPolicyFlags(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	_, err := executeCommand(t, dependenciesFor(backend), []string{
		"run", "--name", "policy", "--group", "workers", "--tag", "nightly",
		"--stdin", "live", "--retries", "2", "--retry-timeouts",
		"--retry-delay", "1s", "--repeat-delay", "2s", "--retry-backoff", "exponential",
		"--retry-jitter", "100ms", "--retry-max-delay", "5s",
		"--run-timeout", "3s", "--job-timeout", "1m", "--after-success", testJobID,
		"--pool", "workers", "--slots", "2", "--wait-delay", "250ms",
		"--log-segment-bytes", "1024", "--log-segments", "3", "--log-capture", "stdout",
		"--log-retention", "2h", "--", "printf", "done",
	})
	if err != nil {
		t.Fatalf("run policy flags error = %v", err)
	}
	request := backend.submitRequest
	if request == nil {
		t.Fatal("Submit() was not called")
	}
	policy := request.ExecutionPolicy
	if policy.Completion.MaxRuns.Unlimited || policy.Completion.MaxRuns.Value != 3 ||
		!policy.Classification.RetryTimeout {
		t.Fatalf("completion policy = %+v/%+v, want three runs and retryable timeouts", policy.Completion, policy.Classification)
	}
	if policy.FailureDelay.Base != time.Second || policy.SuccessDelay.Base != 2*time.Second ||
		policy.FailureDelay.Backoff != "exponential" || policy.FailureDelay.Jitter != 100*time.Millisecond {
		t.Fatalf("delay policies = %+v/%+v", policy.FailureDelay, policy.SuccessDelay)
	}
	if policy.RunTimeout != 3*time.Second || policy.JobTimeout != time.Minute ||
		policy.Concurrency.Pool != "workers" || policy.Concurrency.Slots != 2 {
		t.Fatalf("timeout/admission policy = %+v", policy)
	}
	if !slices.Equal(policy.Groups, []string{"workers"}) || !slices.Equal(policy.Tags, []string{"nightly"}) ||
		len(request.Dependencies) != 1 || len(policy.WaitConditions) != 1 {
		t.Fatalf("labels/prerequisites = groups %v tags %v dependencies %v waits %v",
			policy.Groups, policy.Tags, request.Dependencies, policy.WaitConditions)
	}
	if request.StdinPolicy != model.StdinLive || policy.LogRotateSize != 1024 ||
		policy.LogMaxSegmentsPerStream != 3 || policy.LogCapture != "stdout" ||
		policy.LogRetentionMaxAge != 2*time.Hour {
		t.Fatalf("input/log policy = stdin %q policy %+v", request.StdinPolicy, policy)
	}
}

func TestRunRerunSourceIsCanonicalAndRejectsPolicyOverrides(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"run", "--rerun", testJobID, "--name", "copied",
	})
	if err != nil {
		t.Fatalf("run --rerun error = %v", err)
	}
	if stdout != testJobID+"\n" || backend.rerunRequest == nil || backend.rerunRequest.Name != "copied" {
		t.Fatalf("run --rerun output/request = %q/%+v", stdout, backend.rerunRequest)
	}

	backend.rerunRequest = nil
	_, err = executeCommand(t, dependenciesFor(backend), []string{
		"run", "--rerun", testJobID, "--slots", "2",
	})
	if !errors.Is(err, errUsage) || backend.rerunRequest != nil {
		t.Fatalf("run --rerun with override error/request = %v/%+v, want usage/no rerun", err, backend.rerunRequest)
	}
	if backend.appliedConfig != 1 {
		t.Fatalf("invalid rerun changed configuration application count to %d", backend.appliedConfig)
	}

	standalone := newFakeBackend(t)
	if _, err := executeCommand(t, dependenciesFor(standalone), []string{"rerun", testJobID}); err != nil {
		t.Fatalf("standalone rerun error = %v", err)
	}
	if standalone.appliedConfig != 1 || standalone.configured != 1 {
		t.Fatalf("standalone rerun configuration calls = applied %d configured %d, want one each",
			standalone.appliedConfig, standalone.configured)
	}

	waiting := newFakeBackend(t)
	waiting.jobs[0].Outcome = model.JobOutcomeSuccess
	if _, err := executeCommand(t, dependenciesFor(waiting), []string{"rerun", "--wait", testJobID}); err != nil {
		t.Fatalf("standalone rerun --wait error = %v", err)
	}
	if waiting.waited.Load() != 1 {
		t.Fatalf("standalone rerun --wait Wait calls = %d, want 1", waiting.waited.Load())
	}
}

func TestLifecycleAndInputCommandsUseTypedBackends(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	if _, err := executeCommand(t, dependenciesFor(backend), []string{"pause", testJobID}); err != nil {
		t.Fatalf("pause error = %v", err)
	}
	if _, err := executeCommand(t, dependenciesFor(backend), []string{"resume", testJobID}); err != nil {
		t.Fatalf("resume error = %v", err)
	}
	if !backend.paused || !backend.resumed {
		t.Fatalf("pause/resume calls = %t/%t", backend.paused, backend.resumed)
	}

	command := newRootCommand(dependenciesFor(backend))
	command.SetArgs([]string{"input", "--eof", testJobID})
	command.SetIn(strings.NewReader("secret bytes\x00"))
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("input error = %v", err)
	}
	if string(backend.input) != "secret bytes\x00" || !backend.inputEOF.Load() {
		t.Fatalf("input/eof = %q/%t", backend.input, backend.inputEOF.Load())
	}
	_, err := executeCommand(t, dependenciesFor(backend), []string{"input", testJobID, "secret-on-argv"})
	if !errors.Is(err, errUsage) {
		t.Fatalf("positional input error = %v, want usage", err)
	}
}

func TestWaitCommandReflectsTerminalOutcome(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		outcome model.JobOutcome
		wantErr bool
	}{
		{outcome: model.JobOutcomeSuccess},
		{outcome: model.JobOutcomeFailure, wantErr: true},
		{outcome: model.JobOutcomeTimedOut, wantErr: true},
		{outcome: model.JobOutcomeCancelled, wantErr: true},
		{outcome: model.JobOutcomeAborted, wantErr: true},
	} {
		t.Run(string(test.outcome), func(t *testing.T) {
			t.Parallel()
			backend := newFakeBackend(t)
			backend.jobs[0].Outcome = test.outcome

			stdout, err := executeCommand(t, dependenciesFor(backend), []string{"wait", testJobID})
			if (err != nil) != test.wantErr {
				t.Fatalf("wait error = %v, want error %t", err, test.wantErr)
			}
			if stdout != testJobID+"\t"+string(test.outcome)+"\n" {
				t.Fatalf("wait output = %q", stdout)
			}
			if test.wantErr && ExitCode(err) != 1 {
				t.Fatalf("wait exit code = %d, want 1", ExitCode(err))
			}
		})
	}
}

func TestInputEOFDoesNotReadInteractiveTerminal(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	terminal := &terminalInput{}
	command := newRootCommand(dependenciesFor(backend))
	command.SetArgs([]string{"input", "--eof", testJobID})
	command.SetIn(terminal)
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)

	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("input --eof error = %v", err)
	}
	if terminal.read {
		t.Fatal("input --eof read from the interactive terminal")
	}
	if len(backend.input) != 0 || !backend.inputEOF.Load() {
		t.Fatalf("input/eof = %q/%t, want empty input and EOF", backend.input, backend.inputEOF.Load())
	}
}

type terminalInput struct {
	read bool
}

func (input *terminalInput) Read([]byte) (int, error) {
	input.read = true
	return 0, errors.New("interactive terminal read would block")
}

func (*terminalInput) Stat() (os.FileInfo, error) {
	return terminalFileInfo{}, nil
}

type terminalFileInfo struct{}

func (terminalFileInfo) Name() string       { return "terminal" }
func (terminalFileInfo) Size() int64        { return 0 }
func (terminalFileInfo) Mode() os.FileMode  { return os.ModeCharDevice }
func (terminalFileInfo) ModTime() time.Time { return time.Time{} }
func (terminalFileInfo) IsDir() bool        { return false }
func (terminalFileInfo) Sys() any           { return nil }

func TestListJSONUsesVersionedEnvelope(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{"list", "--json"})
	if err != nil {
		t.Fatalf("ExecuteContext() error = %v", err)
	}
	if !strings.Contains(stdout, `"schema_version":1`) || !strings.Contains(stdout, testJobID) {
		t.Fatalf("list JSON = %q, want version and job ID", stdout)
	}
}

func TestStatusAndShow(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{"status", testJobID})
	if err != nil {
		t.Fatalf("status error = %v", err)
	}
	if !strings.Contains(stdout, testJobID) || !strings.Contains(stdout, "submitting") {
		t.Fatalf("status output = %q", stdout)
	}

	stdout, err = executeCommand(t, dependenciesFor(backend), []string{"show", "--json", testJobID})
	if err != nil {
		t.Fatalf("show error = %v", err)
	}
	if !strings.Contains(stdout, `"specification"`) || !strings.Contains(stdout, `"runs"`) {
		t.Fatalf("show output = %q", stdout)
	}
}

func TestLogsPreserveBinaryBytes(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	backend.logs = []byte{0, 'a', '\n', 0xff}
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{"logs", testJobID})
	if err != nil {
		t.Fatalf("logs error = %v", err)
	}
	if !bytes.Equal([]byte(stdout), backend.logs) {
		t.Fatalf("logs output = %v, want %v", []byte(stdout), backend.logs)
	}
}

func TestLogsRunSelectionUsesStableErrorCategories(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	backend.details.Runs = []model.RunState{{Number: 1}}
	for _, test := range []struct {
		selection string
		wantCode  int
	}{
		{selection: "invalid", wantCode: 2},
		{selection: "0", wantCode: 2},
		{selection: "2", wantCode: 3},
		{selection: "-2", wantCode: 3},
	} {
		_, err := executeCommand(
			t,
			dependenciesFor(backend),
			[]string{"logs", "--run", test.selection, testJobID},
		)
		if got := ExitCode(err); got != test.wantCode {
			t.Errorf("logs --run %q error/code = %v/%d, want %d", test.selection, err, got, test.wantCode)
		}
		backend.closed = false
	}
}

func TestCancelUsesCanonicalCommand(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	_, err := executeCommand(t, dependenciesFor(backend), []string{"cancel", testJobID})
	if err != nil {
		t.Fatalf("cancel error = %v", err)
	}
	if !backend.canceled {
		t.Fatal("Cancel() was not called")
	}

	_, err = executeCommand(t, dependenciesFor(backend), []string{"kill", testJobID})
	if err == nil {
		t.Fatal("removed kill command unexpectedly succeeded")
	}
}

func TestListShowAndCancelV1Forms(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend(t)
	runID, err := model.ParseRunID("01980f4c-7b2a-7a6f-8c10-0123456789ac")
	if err != nil {
		t.Fatal(err)
	}
	run := model.RunState{
		ID: runID, JobID: backend.jobs[0].ID, Number: 1, Phase: model.RunPhaseRunning,
		Revision: 2, ReservedAt: backend.jobs[0].SubmittedAt.Add(time.Second),
	}
	backend.jobs[0].ActiveRunID = runID
	backend.details.Job = backend.jobs[0]
	backend.details.Runs = []model.RunState{run}

	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"list", "--json", "--active", "--limit", "5", "--phase", "submitting",
		"--name", "example", "--group", "workers", "--submitted-after", "2026-07-01T00:00:00Z",
		"--submitted-before", "2026-08-01T00:00:00Z", "--show-runs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if backend.listRequest == nil || backend.listRequest.Limit != 5 ||
		!backend.listRequest.Active || !backend.listRequest.ShowRuns ||
		backend.listRequest.Phase != model.JobPhaseSubmitting ||
		backend.listRequest.Name != "example" || backend.listRequest.Group != "workers" {
		t.Fatalf("list request = %+v", backend.listRequest)
	}
	if !strings.Contains(stdout, `"runs"`) || !strings.Contains(stdout, runID.String()) {
		t.Fatalf("list --show-runs output = %q", stdout)
	}

	for _, arguments := range [][]string{
		{"show", "job", testJobID},
		{"show", "run", testJobID, "-1"},
		{"show", "run", "--json", testJobID, "1"},
	} {
		if _, commandErr := executeCommand(t, dependenciesFor(backend), arguments); commandErr != nil {
			t.Errorf("%v error = %v", arguments, commandErr)
		}
	}
	stdout, err = executeCommand(
		t, dependenciesFor(backend), []string{"show", "run", testJobID, "-1", "--json"},
	)
	if err != nil || !strings.Contains(stdout, `"number":1`) {
		t.Fatalf("show run with trailing --json = %q/%v", stdout, err)
	}
	if _, commandErr := executeCommand(
		t, dependenciesFor(backend), []string{"cancel", "run", testJobID, "-1", "--state-dir", t.TempDir()},
	); commandErr != nil {
		t.Fatalf("cancel run: %v", commandErr)
	}
	if !backend.canceled {
		t.Fatal("cancel run did not cancel owning job")
	}
	backend.details.Job.ActiveRunID = ""
	if _, err := executeCommand(
		t, dependenciesFor(backend), []string{"cancel", "run", testJobID, "1"},
	); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("cancel inactive run error = %v, want conflict", err)
	}
}

func TestListOptionValidationAndAllLimit(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend(t)
	if _, err := executeCommand(t, dependenciesFor(backend), []string{"list", "--all"}); err != nil {
		t.Fatal(err)
	}
	if backend.listRequest == nil || backend.listRequest.Limit != store.MaximumListLimit {
		t.Fatalf("list --all request = %+v", backend.listRequest)
	}
	for _, arguments := range [][]string{
		{"list", "--all", "--limit", "1"},
		{"list", "--active", "--completed"},
		{"list", "--limit", "0"},
		{"list", "--limit", "1001"},
		{"list", "--phase", "invalid"},
		{"list", "--outcome", "invalid"},
		{"list", "--submitted-after", "bad"},
		{"list", "--submitted-before", "bad"},
	} {
		if _, err := executeCommand(t, dependenciesFor(newFakeBackend(t)), arguments); !errors.Is(err, errUsage) {
			t.Errorf("%v error = %v, want usage", arguments, err)
		}
	}
}

func TestDoctorAndConfigurationApplyCommands(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend(t)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"doctor", "--repair", "--backup", filepath.Join(t.TempDir(), "backup.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if backend.doctorRequest == nil || !backend.doctorRequest.Repair || backend.doctorRequest.BackupPath == "" ||
		!strings.Contains(stdout, "healthy\tschema=7") {
		t.Fatalf("doctor request/output = %+v/%q", backend.doctorRequest, stdout)
	}
	stdout, err = executeCommand(t, dependenciesFor(backend), []string{"doctor", "--json"})
	if err != nil || !strings.Contains(stdout, `"healthy":true`) {
		t.Fatalf("doctor JSON = %q, %v", stdout, err)
	}
	stdout, err = executeCommand(t, dependenciesFor(backend), []string{"config", "apply"})
	if err != nil {
		t.Fatal(err)
	}
	if backend.appliedConfig != 1 || backend.configured == 0 || !strings.Contains(stdout, "applied") {
		t.Fatalf("config apply calls/output = applied %d configured %d/%q", backend.appliedConfig, backend.configured, stdout)
	}
}

func TestSelectRunValidation(t *testing.T) {
	t.Parallel()
	runs := []model.RunState{{Number: 1}, {Number: 2}}
	for _, selector := range []string{"", "0", "bad"} {
		if _, err := selectRun(runs, selector); !errors.Is(err, errUsage) {
			t.Errorf("selectRun(%q) error = %v, want usage", selector, err)
		}
	}
	for _, selector := range []string{"3", "-3"} {
		if _, err := selectRun(runs, selector); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("selectRun(%q) error = %v, want not found", selector, err)
		}
	}
	selected, err := selectRun(runs, "-1")
	if err != nil || selected.Number != 2 {
		t.Fatalf("selectRun(-1) = %+v, %v", selected, err)
	}
}

func TestExitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		want int
	}{
		{err: nil, want: 0},
		{err: errUsage, want: 2},
		{err: config.ErrInvalid, want: 2},
		{err: &model.ValidationError{Field: "job name", Reason: "invalid"}, want: 2},
		{err: app.ErrNotFound, want: 3},
		{err: app.ErrAmbiguous, want: 4},
		{err: app.ErrConflict, want: 5},
		{err: fmt.Errorf("partial live input: %w", io.ErrShortWrite), want: 6},
		{err: errors.New("boom"), want: 1},
	}
	for _, test := range tests {
		if got := ExitCode(test.err); got != test.want {
			t.Errorf("ExitCode(%v) = %d, want %d", test.err, got, test.want)
		}
	}
}

func executeCommand(
	t *testing.T,
	dependencies dependencies,
	arguments []string,
) (stdoutText string, executeErr error) {
	t.Helper()

	command := newRootCommand(dependencies)
	stdin := bytes.NewReader(nil)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	command.SetArgs(arguments)
	command.SetIn(stdin)
	command.SetOut(stdout)
	command.SetErr(stderr)
	executeErr = command.ExecuteContext(t.Context())

	return stdout.String(), executeErr
}

func dependenciesFor(backend app.Backend) dependencies {
	return dependencies{
		OpenBackend: func(context.Context, string) (app.Backend, error) {
			return backend, nil
		},
	}
}

func newFakeBackend(t *testing.T) *fakeBackend {
	t.Helper()

	id, err := model.ParseJobID(testJobID)
	if err != nil {
		t.Fatalf("ParseJobID() error = %v", err)
	}
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable:             "printf",
		Arguments:              []string{"hello"},
		WorkingDirectory:       t.TempDir(),
		EnvironmentInheritance: model.EnvironmentInheritSubmission,
		StopPolicy: model.StopPolicy{
			GracePeriod:     5 * time.Second,
			ForceAfterGrace: true,
		},
		StdinPolicy: model.StdinNull,
	})
	if err != nil {
		t.Fatalf("NewJobSpec() error = %v", err)
	}
	job := model.JobState{
		ID:          id,
		Spec:        specification,
		Phase:       model.JobPhaseSubmitting,
		Revision:    1,
		SubmittedAt: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
	}

	return &fakeBackend{
		jobs:           []model.JobState{job},
		details:        app.JobDetails{Job: job, Runs: []model.RunState{}},
		inputEOFSignal: make(chan struct{}),
	}
}

func mustTestSpec(t *testing.T, name string) model.JobSpec {
	t.Helper()
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable:             "printf",
		Arguments:              []string{"hello"},
		WorkingDirectory:       t.TempDir(),
		EnvironmentInheritance: model.EnvironmentInheritSubmission,
		Name:                   name,
		StopPolicy: model.StopPolicy{
			GracePeriod:     5 * time.Second,
			ForceAfterGrace: true,
		},
		StdinPolicy: model.StdinNull,
	})
	if err != nil {
		t.Fatalf("NewJobSpec() error = %v", err)
	}

	return specification
}

func cloneMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}

var _ io.Closer = (*fakeBackend)(nil)
