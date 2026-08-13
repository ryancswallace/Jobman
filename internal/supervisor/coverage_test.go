package supervisor

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/liveinput"
	"github.com/ryancswallace/jobman/internal/logstore"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/notify"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/policy"
	"github.com/ryancswallace/jobman/internal/store"
)

const (
	supervisorCoverageHelperEnvironment = "JOBMAN_SUPERVISOR_COVERAGE_HELPER"
	supervisorLaunchHelperEnvironment   = "JOBMAN_SUPERVISOR_LAUNCH_HELPER"
)

func TestMain(m *testing.M) {
	if exitCode, handled := runSupervisorLaunchHelper(); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}

func runSupervisorLaunchHelper() (int, bool) {
	mode := os.Getenv(supervisorLaunchHelperEnvironment)
	if mode == "" || len(os.Args) < 3 || os.Args[1] != "__supervise" {
		return 0, false
	}
	credential := make([]byte, credentialSize)
	if _, err := io.ReadFull(os.Stdin, credential); err != nil {
		return 2, true
	}
	const supervisorID = "019c5f8b-7c8a-7000-8000-000000000099"
	switch mode {
	case "success":
		_, _ = fmt.Fprintf(
			os.Stdout,
			"{\"schema_version\":1,\"job_id\":%q,\"supervisor_id\":%q}\n",
			os.Args[2],
			supervisorID,
		)
	case "mismatch":
		_, _ = fmt.Fprintf(
			os.Stdout,
			"{\"schema_version\":1,\"job_id\":%q,\"supervisor_id\":%q}\n",
			"019c5f8b-7c8a-7000-8000-000000000098",
			supervisorID,
		)
	case "malformed":
		_, _ = fmt.Fprintln(os.Stdout, "not-json")
	case "slow":
		time.Sleep(100 * time.Millisecond)
	default:
		return 2, true
	}

	return 0, true
}

func TestSupervisorCoverageHelper(*testing.T) {
	switch os.Getenv(supervisorCoverageHelperEnvironment) {
	case "probe-exit":
		_, _ = fmt.Fprint(os.Stdout, os.Getenv("PROBE_VALUE"))
		os.Exit(7) //nolint:revive // Helper subprocess must expose a configured exit status.
	case "probe-secret":
		_, _ = fmt.Fprint(os.Stdout, os.Getenv("PROBE_SECRET"))
		os.Exit(0) //nolint:revive // Avoid the Go test runner's PASS text in captured output.
	case "exit-9":
		os.Exit(9) //nolint:revive // Helper subprocess must expose a configured exit status.
	case "exit-7":
		os.Exit(7) //nolint:revive // Helper subprocess must expose a configured exit status.
	case "success":
		os.Exit(0) //nolint:revive // Helper subprocess exits without test-runner output.
	case "sleep":
		time.Sleep(time.Hour)
		os.Exit(0) //nolint:revive // Avoid the Go test runner's PASS text in captured output.
	}
}

func supervisorCoverageCommand(t *testing.T, mode string) *exec.Cmd {
	t.Helper()
	executable := supervisorTestExecutable(t)
	command := exec.Command(executable, "-test.run=^TestSupervisorCoverageHelper$") // #nosec G204 -- current test executable.
	command.Env = append(os.Environ(), supervisorCoverageHelperEnvironment+"="+mode)

	return command
}

func supervisorTestExecutable(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	return executable
}

func TestExecProbeRunnerAndBoundedOutput(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := execProbeRunner{directory: directory, environment: map[string]string{
		"PROBE_VALUE": "abcdef", supervisorCoverageHelperEnvironment: "probe-exit",
	}}
	result, err := runner.RunProbe(t.Context(), policy.ProbeSpec{
		Executable: executable, Arguments: []string{"-test.run=^TestSupervisorCoverageHelper$"},
		Timeout: 10 * time.Second, OutputLimit: 3,
	})
	if err != nil || result.ExitCode != 7 || string(result.Output) != "abc" || !result.Truncated {
		t.Fatalf("RunProbe() = (%+v, %v)", result, err)
	}
	if _, err := runner.RunProbe(t.Context(), policy.ProbeSpec{
		Executable: "/definitely/missing", Timeout: time.Second, OutputLimit: 1,
	}); err == nil {
		t.Fatal("RunProbe(missing) error = nil")
	}
	secretPath := filepath.Join(directory, "probe-secret")
	if err := os.WriteFile(secretPath, []byte("resolved"), 0o600); err != nil {
		t.Fatal(err)
	}
	secretRunner := execProbeRunner{
		directory:   directory,
		environment: map[string]string{supervisorCoverageHelperEnvironment: "probe-secret"},
		secretEnvironment: map[string]model.SecretReference{
			"PROBE_SECRET": {Provider: "file", Name: secretPath},
		},
	}
	result, err = secretRunner.RunProbe(t.Context(), policy.ProbeSpec{
		Executable: executable, Arguments: []string{"-test.run=^TestSupervisorCoverageHelper$"},
		Timeout: 10 * time.Second, OutputLimit: 32,
	})
	if err != nil || result.ExitCode != 0 || string(result.Output) != "resolved" {
		t.Fatalf("RunProbe(secret environment) = (%+v, %v)", result, err)
	}
	invalidSecretRunner := execProbeRunner{secretEnvironment: map[string]model.SecretReference{
		"PROBE_SECRET": {Provider: "unsupported", Name: "reference"},
	}}
	if _, err := invalidSecretRunner.RunProbe(t.Context(), policy.ProbeSpec{
		Executable: supervisorTestExecutable(t), Timeout: time.Second,
	}); err == nil {
		t.Fatal("RunProbe(invalid secret reference) error = nil")
	}

	output := &boundedOutput{limit: 2}
	if count, err := output.Write([]byte("abc")); err != nil || count != 3 || string(output.Bytes()) != "ab" || !output.truncated {
		t.Fatalf("boundedOutput.Write() = (%d, %v), bytes %q", count, err, output.Bytes())
	}
	if count, err := output.Write([]byte("z")); err != nil || count != 1 || string(output.Bytes()) != "ab" {
		t.Fatalf("boundedOutput.Write(full) = (%d, %v), bytes %q", count, err, output.Bytes())
	}
}

func TestNotifierInstantiationAndRetryHelpers(t *testing.T) {
	t.Setenv("JOBMAN_NOTIFY_SECRET", "secret-value")
	ctx := t.Context()
	definitions := []model.NotifierDefinition{
		{
			Name: "command", Kind: model.NotifierCommand, Timeout: time.Second,
			Command: &model.CommandNotifierDefinition{
				Executable: supervisorTestExecutable(t), Environment: map[string]string{"A": "B"},
				SecretEnvironment: map[string]model.SecretReference{
					"TOKEN": {Provider: "env", Name: "JOBMAN_NOTIFY_SECRET"},
				}, OutputLimit: 128,
			},
		},
		{
			Name: "webhook", Kind: model.NotifierWebhook, Timeout: time.Second,
			Webhook: &model.WebhookNotifierDefinition{
				URL: "https://example.test", Headers: map[string]string{"X-Test": "yes"}, ResponseLimit: 128,
			},
		},
		{
			Name: "mail", Kind: model.NotifierSMTP, Timeout: time.Second,
			SMTP: &model.SMTPNotifierDefinition{
				Address: "smtp.example.test:25", From: "jobman@example.test", To: []string{"ops@example.test"},
				Mode: "starttls", MessageLimit: 128,
			},
		},
	}
	for _, definition := range definitions {
		notifier, err := instantiateNotifier(ctx, definition)
		if err != nil || notifier.Name() != definition.Name {
			t.Errorf("instantiateNotifier(%s) = (%T, %v)", definition.Name, notifier, err)
		}
	}
	if _, err := instantiateNotifier(ctx, model.NotifierDefinition{Name: "bad", Kind: "unknown"}); err == nil {
		t.Fatal("instantiateNotifier(unknown) error = nil")
	}
	if value, err := resolveNotificationSecret(ctx, model.SecretReference{Provider: "env", Name: "JOBMAN_NOTIFY_SECRET"}); err != nil || value != "secret-value" {
		t.Fatalf("resolveNotificationSecret() = (%q, %v)", value, err)
	}
	if _, err := resolveNotificationSecret(ctx, model.SecretReference{Provider: "unknown", Name: "name"}); err == nil {
		t.Fatal("resolveNotificationSecret(unknown) error = nil")
	}
	cloned := cloneNotificationStrings(map[string]string{"a": "b"})
	cloned["a"] = "changed"
	if notificationRetryDelay(notify.RetryPolicy{Delay: time.Second}, 3) != 4*time.Second ||
		notificationRetryDelay(notify.RetryPolicy{Delay: time.Second, MaxDelay: 2 * time.Second}, 3) != 2*time.Second {
		t.Fatal("notificationRetryDelay() returned an unexpected delay")
	}
	ignoreNotificationError(errors.New("durable failure"))

	secret := model.SecretReference{Provider: "env", Name: "JOBMAN_NOTIFY_SECRET"}
	for _, definition := range []model.NotifierDefinition{
		{
			Name: "signed-webhook", Kind: model.NotifierWebhook, Timeout: time.Second,
			Webhook: &model.WebhookNotifierDefinition{
				URL:           "https://example.test",
				SecretHeaders: map[string]model.SecretReference{"Authorization": secret},
				SigningSecret: &secret,
			},
		},
		{
			Name: "authenticated-mail", Kind: model.NotifierSMTP, Timeout: time.Second,
			SMTP: &model.SMTPNotifierDefinition{
				Address: "smtp.example.test:25", From: "jobman@example.test", To: []string{"ops@example.test"},
				PasswordSecret: &secret,
			},
		},
	} {
		if _, err := instantiateNotifier(ctx, definition); err != nil {
			t.Errorf("instantiateNotifier(%s with secrets) error = %v", definition.Name, err)
		}
	}
}

func TestPipeAndPreparedTargetHelpers(t *testing.T) {
	t.Parallel()

	target := &preparedTarget{}
	if err := target.closeInput(); err != nil {
		t.Fatalf("closeInput(nil) error = %v", err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	target.stdin = writer
	if err := target.closeInput(); err != nil {
		t.Fatalf("closeInput(pipe) error = %v", err)
	}
	_ = reader.Close()

	errorsChannel := make(chan error, 2)
	errorsChannel <- errors.New("first")
	errorsChannel <- errors.New("second")
	close(errorsChannel)
	if err := collectCaptureErrors(errorsChannel); err == nil {
		t.Fatal("collectCaptureErrors() error = nil")
	}
	if err := drainDiscard(&failingReader{err: errors.New("read failed")}); err == nil {
		t.Fatal("drainDiscard() error = nil")
	}
	if err := drainDiscard(bytes.NewReader([]byte("discard"))); err != nil {
		t.Fatalf("drainDiscard(success) error = %v", err)
	}
}

func TestProcessExitInfoAndJitterSource(t *testing.T) {
	t.Parallel()

	command := supervisorCoverageCommand(t, "exit-9")
	err := command.Run()
	info := processExitInfo(command, err, time.Now())
	if info.ExitCode == nil || *info.ExitCode != 9 {
		t.Fatalf("processExitInfo() = %+v", info)
	}
	failed := processExitInfo(&exec.Cmd{}, errors.New("not an exit status"), time.Now())
	if failed.PlatformReason != "process_wait_failed" {
		t.Fatalf("processExitInfo(non-exit) = %+v", failed)
	}
	source, err := newJitterSource()
	if err != nil {
		t.Fatal(err)
	}
	if source.Uint64N(0) != 0 || source.Uint64N(5) >= 5 {
		t.Fatal("Uint64N() returned a value outside its bound")
	}
	if _, err := newJitterSourceFrom(&failingReader{err: errors.New("entropy failed")}); err == nil {
		t.Fatal("newJitterSourceFrom(failing reader) error = nil")
	}
}

func TestExitInfoForOutcome(t *testing.T) {
	t.Parallel()

	code := 1
	observedAt := time.Now()
	for _, outcome := range []model.RunOutcome{model.RunOutcomeTimedOut, model.RunOutcomeCancelled} {
		exit := exitInfoForOutcome(outcome, &model.ExitInfo{ExitCode: &code, ObservedAt: observedAt})
		if exit.ExitCode != nil || exit.PlatformReason != "process_terminated" {
			t.Fatalf("exitInfoForOutcome(%s) = %+v", outcome, exit)
		}
	}
	exit := &model.ExitInfo{ExitCode: &code, ObservedAt: observedAt}
	if got := exitInfoForOutcome(model.RunOutcomeFailure, exit); got != exit || got.ExitCode == nil {
		t.Fatalf("exitInfoForOutcome(failure) = %+v", got)
	}
	if exitInfoForOutcome(model.RunOutcomeCancelled, nil) != nil {
		t.Fatal("exitInfoForOutcome(canceled, nil) != nil")
	}
}

type failingReader struct{ err error }

func (reader *failingReader) Read([]byte) (int, error) { return 0, reader.err }

var _ io.Reader = (*failingReader)(nil)

func TestPolicyResultHelper(t *testing.T) {
	t.Parallel()

	exitCode := 2
	result := policyResult(model.JobState{}, model.RunOutcomeFailure, &model.ExitInfo{ExitCode: &exitCode})
	if result.Termination != policy.RunTerminationExit || result.ExitCode != 2 {
		t.Fatalf("policyResult() = %+v", result)
	}
}

func TestPolicyResultMatrix(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	code := 3
	for _, test := range []struct {
		name    string
		job     model.JobState
		outcome model.RunOutcome
		exit    *model.ExitInfo
		want    policy.RunTermination
	}{
		{
			name: "job timeout", job: model.JobState{Cancellation: &model.CancellationIntent{Reason: model.StopReasonTimeout}},
			want: policy.RunTerminationTimeout,
		},
		{
			name: "job cancellation", job: model.JobState{Cancellation: &model.CancellationIntent{Reason: model.StopReasonCancellation}},
			want: policy.RunTerminationCancellation,
		},
		{name: "run timeout", outcome: model.RunOutcomeTimedOut, want: policy.RunTerminationTimeout},
		{name: "start failure", outcome: model.RunOutcomeStartFailed, want: policy.RunTerminationStartFailure},
		{name: "lost", outcome: model.RunOutcomeLost, want: policy.RunTerminationLost},
		{name: "exit", exit: &model.ExitInfo{ExitCode: &code, ObservedAt: now}, want: policy.RunTerminationExit},
		{name: "signal", exit: &model.ExitInfo{Signal: "TERM", ObservedAt: now}, want: policy.RunTerminationSignal},
		{name: "platform", exit: &model.ExitInfo{PlatformReason: "gone", ObservedAt: now}, want: policy.RunTerminationPlatform},
		{name: "unknown", want: policy.RunTerminationPlatform},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := policyResult(test.job, test.outcome, test.exit); got.Termination != test.want {
				t.Fatalf("policyResult() = %+v, want termination %q", got, test.want)
			}
		})
	}
}

func TestPrepareTargetPoliciesAndFailures(t *testing.T) {
	t.Setenv("JOBMAN_PREPARE_SECRET", "resolved")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runID := model.RunID("019c5f8b-7c8a-7000-8000-000000000088")
	workingDirectory := t.TempDir()
	stdinPath := filepath.Join(workingDirectory, "stdin")
	if err := os.WriteFile(stdinPath, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}

	makeJob := func(stdin model.StdinPolicy, configuration model.ExecutionPolicy) model.JobState {
		t.Helper()
		specification, specErr := model.NewJobSpec(model.JobSpecInput{
			Executable: executable, Arguments: []string{"-test.run=^TestSupervisorTargetHelper$"},
			WorkingDirectory: workingDirectory, StdinPolicy: stdin,
			StopPolicy:      model.StopPolicy{GracePeriod: time.Second, ForceAfterGrace: true},
			ExecutionPolicy: configuration,
		})
		if specErr != nil {
			t.Fatalf("NewJobSpec() error = %v", specErr)
		}
		return model.JobState{Spec: specification}
	}

	for _, test := range []struct {
		name          string
		stdin         model.StdinPolicy
		configuration func() model.ExecutionPolicy
	}{
		{
			name: "null", stdin: model.StdinNull,
			configuration: model.DefaultExecutionPolicy,
		},
		{
			name: "file", stdin: model.StdinFile,
			configuration: func() model.ExecutionPolicy {
				configuration := model.DefaultExecutionPolicy()
				configuration.StdinPath = stdinPath
				return configuration
			},
		},
		{
			name: "secret environment", stdin: model.StdinNull,
			configuration: func() model.ExecutionPolicy {
				configuration := model.DefaultExecutionPolicy()
				configuration.SecretEnv = map[string]model.SecretReference{
					"TOKEN": {Provider: "env", Name: "JOBMAN_PREPARE_SECRET"},
				}
				return configuration
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, prepareErr := prepareTarget(t.Context(), makeJob(test.stdin, test.configuration()), runID, nil)
			if prepareErr != nil {
				t.Fatalf("prepareTarget() error = %v", prepareErr)
			}
			if err := target.closeInput(); err != nil {
				t.Errorf("closeInput() error = %v", err)
			}
			if err := target.closeOutputPipes(); err != nil {
				t.Errorf("closeOutputPipes() error = %v", err)
			}
		})
	}

	missingFile := model.DefaultExecutionPolicy()
	missingFile.StdinPath = filepath.Join(workingDirectory, "missing")
	if _, err := prepareTarget(t.Context(), makeJob(model.StdinFile, missingFile), runID, nil); err == nil {
		t.Fatal("prepareTarget(missing stdin file) error = nil")
	}
	missingSecret := model.DefaultExecutionPolicy()
	missingSecret.SecretEnv = map[string]model.SecretReference{
		"TOKEN": {Provider: "env", Name: "JOBMAN_PREPARE_MISSING"},
	}
	if _, err := prepareTarget(t.Context(), makeJob(model.StdinNull, missingSecret), runID, nil); err == nil {
		t.Fatal("prepareTarget(missing secret) error = nil")
	}
	liveJob := makeJob(model.StdinLive, model.DefaultExecutionPolicy())
	if _, err := prepareTarget(t.Context(), liveJob, runID, nil); err == nil {
		t.Fatal("prepareTarget(live without broker) error = nil")
	}
	inMemoryBroker := new(liveinput.Broker)
	target, err := prepareTarget(t.Context(), liveJob, runID, inMemoryBroker)
	if err != nil {
		t.Fatalf("prepareTarget(in-memory live broker) error = %v", err)
	}
	if err := target.closeInput(); err != nil {
		t.Errorf("closeInput(in-memory live broker) error = %v", err)
	}
	if err := target.closeOutputPipes(); err != nil {
		t.Errorf("closeOutputPipes(in-memory live broker) error = %v", err)
	}
	if _, err := prepareTarget(t.Context(), liveJob, "invalid", new(liveinput.Broker)); err == nil {
		t.Fatal("prepareTarget(live invalid run) error = nil")
	}
	busyBroker := new(liveinput.Broker)
	if err := busyBroker.BeginRun(runID.String()); err != nil {
		t.Fatal(err)
	}
	if err := busyBroker.Attach(nopWriteCloser{Writer: io.Discard}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareTarget(t.Context(), liveJob, runID, busyBroker); err == nil {
		t.Fatal("prepareTarget(live busy broker) error = nil")
	}
	if err := busyBroker.Detach(); err != nil {
		t.Fatal(err)
	}
	endpoint := liveinput.NewEndpoint(t.TempDir(), "prepare-target")
	broker, err := liveinput.Listen(endpoint)
	if err == nil {
		t.Cleanup(func() { _ = broker.Close() })
		target, prepareErr := prepareTarget(t.Context(), liveJob, runID, broker)
		if prepareErr != nil {
			t.Fatalf("prepareTarget(live) error = %v", prepareErr)
		}
		if closeErr := target.closeInput(); closeErr != nil {
			t.Errorf("closeInput(live) error = %v", closeErr)
		}
		if closeErr := target.closeOutputPipes(); closeErr != nil {
			t.Errorf("closeOutputPipes(live) error = %v", closeErr)
		}
	} else {
		t.Logf("live-input success path unavailable in this environment: %v", err)
	}

	foreground := model.DefaultExecutionPolicy()
	foreground.Foreground = true
	if _, err := prepareTarget(t.Context(), makeJob(model.StdinInherit, foreground), runID, nil); err == nil {
		t.Fatal("prepareTarget(inherited stdin) error = nil")
	}

	for _, test := range []struct {
		name   string
		stdin  model.StdinPolicy
		stream string
	}{
		{name: "null stdout rollback", stdin: model.StdinNull, stream: "stdout"},
		{name: "null stderr rollback", stdin: model.StdinNull, stream: "stderr"},
		{name: "live stdout rollback", stdin: model.StdinLive, stream: "stdout"},
		{name: "live stderr rollback", stdin: model.StdinLive, stream: "stderr"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := supervisorCoverageCommand(t, "success")
			if test.stream == "stdout" {
				command.Stdout = io.Discard
			} else {
				command.Stderr = io.Discard
			}
			var broker *liveinput.Broker
			if test.stdin == model.StdinLive {
				broker = new(liveinput.Broker)
			}
			if _, err := configurePreparedTarget(
				command, supervisorTestExecutable(t), makeJob(test.stdin, model.DefaultExecutionPolicy()), runID, broker,
			); err == nil {
				t.Fatal("configurePreparedTarget(preconfigured stream) error = nil")
			}
		})
	}
}

func TestWaitEvaluationMatrix(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.JobTimeout = time.Minute
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, err := database.GetJob(t.Context(), fixture.jobID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ready := filepath.Join(t.TempDir(), "ready")
	if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	conditions := []model.WaitCondition{
		{Kind: model.WaitUntil, Until: now.Add(-time.Second), PollInterval: time.Second},
		{Kind: model.WaitDelay, Delay: 0, PollInterval: 500 * time.Millisecond},
		{Kind: model.WaitFileExists, Path: ready, FileKind: policy.FileKindRegular, PollInterval: time.Second},
		{
			Kind: model.WaitProbe, PollInterval: time.Second,
			Probe: policy.ProbeSpec{
				Executable:  supervisorTestExecutable(t),
				Arguments:   []string{"-test.run=^TestSupervisorCoverageHelper$"},
				Timeout:     10 * time.Second,
				OutputLimit: 16,
			},
		},
	}
	decision, nextPoll, err := evaluateWaitConditions(
		t.Context(), database, job, policy.WaitModeAll, conditions, now,
	)
	if err != nil || !decision.Satisfied || nextPoll != 500*time.Millisecond {
		t.Fatalf("evaluateWaitConditions(all) = (%+v, %s, %v)", decision, nextPoll, err)
	}

	missing := filepath.Join(t.TempDir(), "missing")
	decision, nextPoll, err = evaluateWaitConditions(t.Context(), database, job, policy.WaitModeAny, []model.WaitCondition{
		{Kind: model.WaitFileExists, Path: missing, PollInterval: time.Second},
		{Kind: model.WaitUntil, Until: now.Add(-time.Second)},
	}, now)
	if err != nil || !decision.Satisfied || nextPoll != time.Second {
		t.Fatalf("evaluateWaitConditions(any) = (%+v, %s, %v)", decision, nextPoll, err)
	}

	decision, _, err = evaluateWaitConditions(t.Context(), database, job, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitProbe, PollInterval: time.Second,
		Probe: policy.ProbeSpec{
			Executable: supervisorTestExecutable(t), Timeout: time.Second, OutputLimit: 16, FatalOnError: true,
		},
		ProbeSecretEnv: map[string]model.SecretReference{
			"TOKEN": {Provider: "env", Name: "JOBMAN_WAIT_MISSING"},
		},
	}}, now)
	if err != nil || !decision.Fatal || decision.Err == nil {
		t.Fatalf("evaluateWaitConditions(probe secret failure) = (%+v, %v)", decision, err)
	}

	decision, _, err = evaluateWaitConditions(t.Context(), database, job, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitFileExists, Path: missing, PollInterval: time.Second, AbortAt: now.Add(-time.Second),
	}}, now)
	if err != nil || !decision.Fatal {
		t.Fatalf("evaluateWaitConditions(aborted) = (%+v, %v)", decision, err)
	}

	if _, _, err := evaluateWaitConditions(t.Context(), database, job, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitUntil, PollInterval: time.Second,
	}}, now); err == nil {
		t.Fatal("evaluateWaitConditions(invalid until) error = nil")
	}
	if _, _, err := evaluateWaitConditions(t.Context(), database, job, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitDelay, Delay: -time.Second, PollInterval: time.Second,
	}}, now); err == nil {
		t.Fatal("evaluateWaitConditions(invalid delay) error = nil")
	}
	if _, _, err := evaluateWaitConditions(t.Context(), database, job, policy.WaitMode("invalid"), nil, now); err == nil {
		t.Fatal("evaluateWaitConditions(invalid mode) error = nil")
	}
	claimed := claimCoverageFixture(t, database, fixture, time.Minute)
	claimedNow := time.Now().UTC()
	decision, _, err = evaluateWaitConditions(t.Context(), database, claimed, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitDelay, Delay: 0, PollInterval: time.Second,
	}}, claimedNow)
	if err != nil || !decision.Satisfied {
		t.Fatalf("evaluateWaitConditions(claimed delay) = (%+v, %v)", decision, err)
	}
	if _, _, err := evaluateWaitConditions(t.Context(), database, claimed, policy.WaitModeAll, []model.WaitCondition{{
		Kind: model.WaitConditionKind("unknown"), PollInterval: time.Second,
	}}, claimedNow); err == nil {
		t.Fatal("evaluateWaitConditions(unknown kind) error = nil")
	}
}

func TestSchedulerWaitAndDeadlineHelpers(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.JobTimeout = time.Minute
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, err := database.GetJob(t.Context(), fixture.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if expired, err := jobTimeoutExpired(t.Context(), database, job, time.Now().UTC()); err != nil || expired {
		t.Fatalf("jobTimeoutExpired(unclaimed) = (%t, %v)", expired, err)
	}
	if err := waitForSchedulerTick(t.Context(), t.Context(), database, job.ID, time.Nanosecond); err != nil {
		t.Fatalf("waitForSchedulerTick(timer) error = %v", err)
	}
	operationCtx, cancelOperation := context.WithCancel(t.Context())
	cancelOperation()
	if err := waitForSchedulerTick(t.Context(), operationCtx, database, job.ID, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForSchedulerTick(operation cancellation) error = %v", err)
	}
	if err := waitForSchedulerTick(t.Context(), operationCtx, database, job.ID, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForSchedulerTick(default delay) error = %v", err)
	}
	if _, expired, err := completeSchedulingDeadline(t.Context(), database, job, time.Now().UTC()); err != nil || expired {
		t.Fatalf("completeSchedulingDeadline(no deadline) = (%t, %v)", expired, err)
	}
	claimed := claimCoverageFixture(t, database, fixture, time.Minute)
	pausedAt := time.Now().UTC()
	if _, err := database.MoveJob(t.Context(), claimed.ID, model.JobPhaseQueued, pausedAt, "coverage"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pause(t.Context(), claimed.ID, pausedAt); err != nil {
		t.Fatal(err)
	}
	paused, err := database.GetJob(t.Context(), claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired, err := jobTimeoutExpired(t.Context(), database, paused, pausedAt.Add(time.Second)); err != nil || expired {
		t.Fatalf("jobTimeoutExpired(paused) = (%t, %v)", expired, err)
	}

	closed := openSupervisorStore(t, fixture.stateDir)
	closeSupervisorStore(t, closed)
	if _, err := jobTimeoutExpired(t.Context(), closed, model.JobState{
		ID: job.ID, Spec: job.Spec, ClaimedAt: func() *time.Time { value := time.Now().UTC(); return &value }(),
	}, time.Now().UTC()); err == nil {
		t.Fatal("jobTimeoutExpired(closed store) error = nil")
	}
}

func TestNotificationHelperEdges(t *testing.T) {
	t.Setenv("JOBMAN_NOTIFY_PRESENT", "secret")
	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, err := database.GetJob(t.Context(), fixture.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := deliverNotifications(t.Context(), database, job, "", notify.EventJobStarted, "", time.Now()); err != nil {
		t.Fatalf("deliverNotifications(no subscriptions) error = %v", err)
	}
	if _, _, err := notificationBudgetContext(t.Context(), database, job); err != nil {
		t.Fatalf("notificationBudgetContext(unbounded) error = %v", err)
	}
	if _, bounded, err := notificationDeadline(t.Context(), database, job, time.Now()); err != nil || bounded {
		t.Fatalf("notificationDeadline(unbounded) = (%t, %v)", bounded, err)
	}
	if _, found := notificationDefinition(job, "missing"); found {
		t.Fatal("notificationDefinition(missing) found = true")
	}
	if err := RecoverNotifications(t.Context(), nil); err == nil {
		t.Fatal("RecoverNotifications(nil) error = nil")
	}

	for _, definition := range []model.NotifierDefinition{
		{
			Name: "command", Kind: model.NotifierCommand, Timeout: time.Second,
			Command: &model.CommandNotifierDefinition{
				Executable: supervisorTestExecutable(t),
				SecretEnvironment: map[string]model.SecretReference{
					"TOKEN": {Provider: "env", Name: "JOBMAN_NOTIFY_MISSING"},
				},
			},
		},
		{
			Name: "webhook-header", Kind: model.NotifierWebhook, Timeout: time.Second,
			Webhook: &model.WebhookNotifierDefinition{
				URL: "https://example.test",
				SecretHeaders: map[string]model.SecretReference{
					"Authorization": {Provider: "env", Name: "JOBMAN_NOTIFY_MISSING"},
				},
			},
		},
		{
			Name: "webhook-signature", Kind: model.NotifierWebhook, Timeout: time.Second,
			Webhook: &model.WebhookNotifierDefinition{
				URL:           "https://example.test",
				SigningSecret: &model.SecretReference{Provider: "env", Name: "JOBMAN_NOTIFY_MISSING"},
			},
		},
		{
			Name: "smtp", Kind: model.NotifierSMTP, Timeout: time.Second,
			SMTP: &model.SMTPNotifierDefinition{
				Address: "smtp.example.test:25", From: "a@example.test", To: []string{"b@example.test"},
				PasswordSecret: &model.SecretReference{Provider: "env", Name: "JOBMAN_NOTIFY_MISSING"},
			},
		},
	} {
		if _, err := instantiateNotifier(t.Context(), definition); err == nil {
			t.Errorf("instantiateNotifier(%s missing secret) error = nil", definition.Name)
		}
	}
	if delay := notificationRetryDelay(notify.RetryPolicy{Delay: time.Duration(1 << 62)}, 3); delay != time.Duration(1<<63-1) {
		t.Fatalf("notificationRetryDelay(overflow) = %s", delay)
	}

	for _, outcome := range []model.JobOutcome{
		model.JobOutcomeSuccess,
		model.JobOutcomeTimedOut,
		model.JobOutcomeCancelled,
		model.JobOutcomeAborted,
		model.JobOutcomeLost,
		model.JobOutcomeSubmissionFailed,
		model.JobOutcomeFailure,
		model.JobOutcomeNone,
	} {
		terminalJob := job
		terminalJob.Outcome = outcome
		notifyTerminalJob(t.Context(), database, terminalJob, "", time.Now())
	}
	for _, outcome := range []model.RunOutcome{
		model.RunOutcomeSuccess,
		model.RunOutcomeTimedOut,
		model.RunOutcomeCancelled,
		model.RunOutcomeLost,
		model.RunOutcomeFailure,
		model.RunOutcomeStartFailed,
	} {
		run := model.RunState{ID: model.RunID("019c5f8b-7c8a-7000-8000-000000000077"), Revision: 1}
		notifyCompletedRun(t.Context(), database, model.TransitionResult{Job: job, Run: &run}, outcome, time.Now())
	}
	notifyCompletedRun(t.Context(), database, model.TransitionResult{Job: job}, model.RunOutcomeSuccess, time.Now())
	notifyRunStarted(t.Context(), database, model.TransitionResult{}, time.Now())
	ignoreNotificationError(errors.New("ignored"))
}

func TestDrainAndLogMetadataEdges(t *testing.T) {
	t.Parallel()

	if _, _, err := authoritativeLogSizes(model.LogMetadata{IndexPath: filepath.Join(t.TempDir(), "not-a-run", "index")}); err == nil {
		t.Fatal("authoritativeLogSizes(nonnumeric run) error = nil")
	}
	if _, _, err := authoritativeLogSizes(model.LogMetadata{IndexPath: filepath.Join(t.TempDir(), "0", "index")}); err == nil {
		t.Fatal("authoritativeLogSizes(zero run) error = nil")
	}
	if _, _, err := authoritativeLogSizes(model.LogMetadata{IndexPath: filepath.Join(t.TempDir(), "1", "index")}); err == nil {
		t.Fatal("authoritativeLogSizes(missing run) error = nil")
	}
	closedCaptureRoot := t.TempDir()
	if err := os.Chmod(closedCaptureRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	closedCapture, err := logstore.CreateRun(closedCaptureRoot, "019c5f8b-7c8a-7000-8000-000000000060", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := closedCapture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyPipe(bytes.NewBufferString("unrecorded"), closedCapture, logstore.Stdout); err == nil {
		t.Fatal("copyPipe(closed capture) error = nil")
	}
	if err := copyPipe(bytes.NewBufferString("discarded"), closedCapture, logstore.Stream(255)); err == nil {
		t.Fatal("copyPipe(invalid stream) error = nil")
	}
	metadata := completedLogMetadata(model.LogMetadata{IndexPath: "invalid"}, errors.New("capture"), errors.New("close"))
	if metadata.Integrity != model.LogIntegrityPartial || metadata.RecordingHealth != model.RecordingDegraded {
		t.Fatalf("completedLogMetadata(degraded) = %+v", metadata)
	}

	group := new(sync.WaitGroup)
	group.Add(1)
	errorsChannel := make(chan error, 1)
	drainPipe(group, &errorReadCloser{readErr: errors.New("read"), closeErr: errors.New("close")}, nil, logstore.Stdout, false, errorsChannel)
	group.Wait()
	close(errorsChannel)
	if err := collectCaptureErrors(errorsChannel); err == nil {
		t.Fatal("drainPipe(errors) did not report an error")
	}
}

func TestNotificationSummaryBoundaryCases(t *testing.T) {
	t.Parallel()

	summary := notify.EventSummary{Reason: "unchanged"}
	applyTransitionNotificationDetails(&summary, store.TransitionEvent{Details: []byte("{")})
	if summary.Reason != "unchanged" {
		t.Fatalf("invalid transition details changed summary: %+v", summary)
	}

	now := time.Now().UTC()
	past := now.Add(-time.Second)
	future := now.Add(time.Second)
	applyNotificationRunCounts(&summary, []model.RunState{
		{CompletedAt: &future, Outcome: model.RunOutcomeSuccess},
		{CompletedAt: &past, Outcome: model.RunOutcomeFailure},
	}, now)
	if summary.RunCount == nil || *summary.RunCount != 1 ||
		summary.SuccessCount == nil || *summary.SuccessCount != 0 ||
		summary.FailureCount == nil || *summary.FailureCount != 1 {
		t.Fatalf("filtered run counts = %+v", summary)
	}

	applyNotificationRun(&summary, model.RunState{Number: 2}, store.NotificationDelivery{
		EventType: "job_started", OccurredAt: now,
	})
	if summary.RunOutcome != "" {
		t.Fatalf("nonterminal run outcome = %q", summary.RunOutcome)
	}
	if notificationTime(time.Time{}) != nil || notificationTimeBefore(&future, now) != nil {
		t.Fatal("unset or future notification time was retained")
	}
}

type errorReadCloser struct {
	readErr  error
	closeErr error
}

func (reader *errorReadCloser) Read([]byte) (int, error) { return 0, reader.readErr }
func (reader *errorReadCloser) Close() error             { return reader.closeErr }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestAwaitRunnableTerminalCancellationAndFatalWait(t *testing.T) {
	t.Parallel()

	t.Run("terminal", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		completed, err := database.CompleteWithoutRun(
			t.Context(), job.ID, model.JobOutcomeAborted, "coverage", time.Now().UTC(),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := awaitRunnable(t.Context(), t.Context(), database, job.ID, fixedJitterSource(0))
		if err != nil || !result.terminal || result.job.Revision != completed.Job.Revision {
			t.Fatalf("awaitRunnable(terminal) = (%+v, %v)", result, err)
		}
	})

	t.Run("stopping without run", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.RequestCancellation(t.Context(), job.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		result, err := awaitRunnable(t.Context(), t.Context(), database, job.ID, fixedJitterSource(0))
		if err != nil || !result.terminal || result.job.Outcome != model.JobOutcomeCancelled {
			t.Fatalf("awaitRunnable(stopping) = (%+v, %v)", result, err)
		}
	})

	t.Run("fatal probe", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.WaitConditions = []model.WaitCondition{{
			Kind: model.WaitProbe, PollInterval: time.Millisecond,
			Probe: policy.ProbeSpec{
				Executable: filepath.Join(string(filepath.Separator), "missing", "wait-probe"),
				Timeout:    time.Second, OutputLimit: 32, FatalOnError: true,
			},
		}}
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		if err := Run(t.Context(), fixture.stateDir, fixture.jobID.String(), bytes.NewReader(fixture.credential), new(closingBuffer)); err != nil {
			t.Fatalf("Run(fatal wait probe) error = %v", err)
		}
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job, err := database.GetJob(t.Context(), fixture.jobID)
		if err != nil || job.Outcome != model.JobOutcomeAborted || job.LastDiagnosticCode != "wait_condition_failed" {
			t.Fatalf("fatal wait job = (%+v, %v)", job, err)
		}
	})
}

func TestInvalidAdmissionConfigurationIsDurablyAborted(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.Concurrency.Pool = "undeclared"
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(),
		bytes.NewReader(fixture.credential), new(closingBuffer),
	)
	if err == nil {
		t.Fatal("Run(undeclared pool) error = nil")
	}
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, getErr := database.GetJob(t.Context(), fixture.jobID)
	if getErr != nil || job.Outcome != model.JobOutcomeAborted || job.LastDiagnosticCode != "admission_configuration_invalid" {
		t.Fatalf("invalid admission job = (%+v, %v), run error %v", job, getErr, err)
	}
}

func TestSchedulerAdmissionFinalizationEdges(t *testing.T) {
	t.Parallel()

	t.Run("capacity wait becomes queued", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		installAdmissionBlocker(t, fixture.stateDir)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		result, done, retry, err := tryAdmission(
			t.Context(),
			t.Context(),
			database,
			job,
			job.Spec.ExecutionPolicy().Concurrency,
			time.Now().UTC(),
			fixedJitterSource(0),
		)
		if err != nil || done || !retry || result.terminal {
			t.Fatalf("tryAdmission(capacity) = (%+v, done=%t, retry=%t, %v)", result, done, retry, err)
		}
		queued, err := database.GetJob(t.Context(), job.ID)
		if err != nil || queued.Phase != model.JobPhaseQueued {
			t.Fatalf("capacity-limited job = (%+v, %v), want queued", queued, err)
		}
	})

	t.Run("queued becomes starting", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		moved, err := database.MoveJob(t.Context(), job.ID, model.JobPhaseQueued, time.Now().UTC(), "coverage")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.TryAcquireAdmission(t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute); err != nil {
			t.Fatal(err)
		}
		result, err := finalizeAcquiredAdmission(
			t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
		)
		if err != nil || result.job.Phase != model.JobPhaseStarting || moved.Job.Phase != model.JobPhaseQueued {
			t.Fatalf("finalizeAcquiredAdmission() = (%+v, %v)", result, err)
		}
	})

	t.Run("starting stays starting", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.TryAcquireAdmission(t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute); err != nil {
			t.Fatal(err)
		}
		result, err := finalizeAcquiredAdmission(
			t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
		)
		if err != nil || result.job.Phase != model.JobPhaseStarting {
			t.Fatalf("finalizeAcquiredAdmission(starting) = (%+v, %v)", result, err)
		}
	})

	t.Run("cancellation after acquisition releases admission", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.TryAcquireAdmission(t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err := database.RequestCancellation(t.Context(), job.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		result, err := finalizeAcquiredAdmission(
			t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
		)
		if err != nil || !result.terminal || result.job.Outcome != model.JobOutcomeCancelled {
			t.Fatalf("finalizeAcquiredAdmission(canceled) = (%+v, %v)", result, err)
		}
	})

	t.Run("closed store", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		closeSupervisorStore(t, database)
		if _, err := finalizeAcquiredAdmission(
			t.Context(), t.Context(), database, fixture.jobID, time.Now().UTC(), fixedJitterSource(0),
		); err == nil {
			t.Fatal("finalizeAcquiredAdmission(closed store) error = nil")
		}
	})
}

func TestExpiredOwnershipReconciliationEdges(t *testing.T) {
	t.Parallel()

	t.Run("nothing expired", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		changed, err := reconcileExpiredOwnership(t.Context(), database, time.Now().UTC())
		if err != nil || changed {
			t.Fatalf("reconcileExpiredOwnership(empty) = (%t, %v)", changed, err)
		}
	})

	t.Run("unowned job", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		changed, err := reconcileExpiredJobOwner(t.Context(), database, fixture.jobID, time.Now().UTC())
		if err != nil || changed {
			t.Fatalf("reconcileExpiredJobOwner(unowned) = (%t, %v)", changed, err)
		}
	})

	t.Run("live expired owner", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		claimCoverageFixture(t, database, fixture, time.Nanosecond)
		changed, err := reconcileExpiredJobOwner(t.Context(), database, fixture.jobID, time.Now().UTC().Add(time.Second))
		if err != nil || changed {
			t.Fatalf("reconcileExpiredJobOwner(live) = (%t, %v)", changed, err)
		}
	})

	t.Run("completed admission is released", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		now := time.Now().UTC()
		if _, err := database.TryAcquireAdmission(t.Context(), fixture.jobID, "", 1, now, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
		if _, err := database.MarkSubmissionFailed(t.Context(), fixture.jobID, "coverage", now.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		changed, err := reconcileExpiredOwnership(t.Context(), database, now.Add(time.Second))
		if err != nil || !changed {
			t.Fatalf("reconcileExpiredOwnership(completed) = (%t, %v)", changed, err)
		}
		admission, found, err := database.GetAdmission(t.Context(), fixture.jobID)
		if err != nil || !found || admission.ReleasedAt == nil {
			t.Fatalf("released admission = (%+v, %t, %v)", admission, found, err)
		}
	})

	t.Run("unowned active admission is rejected", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		now := time.Now().UTC()
		if _, err := database.TryAcquireAdmission(t.Context(), fixture.jobID, "", 1, now, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
		if _, err := reconcileExpiredOwnership(t.Context(), database, now.Add(time.Second)); err == nil {
			t.Fatal("reconcileExpiredOwnership(unowned admission) error = nil")
		}
	})
}

func TestFinalizeReservedCancellation(t *testing.T) {
	t.Parallel()

	for _, reason := range []model.StopReason{model.StopReasonCancellation, model.StopReasonTimeout} {
		t.Run(string(reason), func(t *testing.T) {
			fixture := submitSupervisorFixture(t, true)
			database := openSupervisorStore(t, fixture.stateDir)
			defer closeSupervisorStore(t, database)
			job := claimCoverageFixture(t, database, fixture, time.Minute)
			if _, err := database.TryAcquireAdmission(t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute); err != nil {
				t.Fatal(err)
			}
			capture, logs, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
			if reason == model.StopReasonTimeout {
				if _, err := database.RequestTimeout(t.Context(), job.ID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			} else if _, err := database.RequestCancellation(t.Context(), job.ID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			jitter, err := newJitterSource()
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := finalizeReservedCancellation(
				t.Context(), database, capture, job.ID, runID, logs, jitter,
			)
			if err != nil || !terminal {
				t.Fatalf("finalizeReservedCancellation(%s) = (%t, %v)", reason, terminal, err)
			}
			run, err := database.GetRun(t.Context(), runID)
			if err != nil || run.Phase != model.RunPhaseCompleted {
				t.Fatalf("completed reserved run = (%+v, %v)", run, err)
			}
		})
	}

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	database := openSupervisorStore(t, stateDir)
	closeSupervisorStore(t, database)
	if _, err := finalizeReservedCancellation(
		t.Context(), database, nil,
		model.JobID("019c5f8b-7c8a-7000-8000-000000000065"),
		model.RunID("019c5f8b-7c8a-7000-8000-000000000066"),
		model.LogMetadata{}, nil,
	); err == nil {
		t.Fatal("finalizeReservedCancellation(closed store) error = nil")
	}
}

func TestHandlePublishFailureCleansUpTarget(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	jobID := model.JobID("019c5f8b-7c8a-7000-8000-000000000061")
	runID := model.RunID("019c5f8b-7c8a-7000-8000-000000000062")
	capture, err := logstore.CreateRun(stateDir, jobID.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	command := supervisorCoverageCommand(t, "sleep")
	platform.ConfigureTarget(command)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := platform.Inspect(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	target := &preparedTarget{command: command, stdout: stdout, stderr: stderr}
	group := new(sync.WaitGroup)
	errorsChannel := make(chan error, 2)
	group.Add(2)
	go drainPipe(group, stdout, capture, logstore.Stdout, true, errorsChannel)
	go drainPipe(group, stderr, capture, logstore.Stderr, true, errorsChannel)
	database := openSupervisorStore(t, stateDir)
	closeSupervisorStore(t, database)
	err = handlePublishFailure(
		t.Context(), database, capture, jobID, runID, logs, target, identity,
		group, errorsChannel, errors.New("publish failed"),
	)
	if err == nil {
		t.Fatal("handlePublishFailure() error = nil")
	}
}

func TestSuperviseStartedTargetHandlesPublishFailure(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	jobID := model.JobID("019c5f8b-7c8a-7000-8000-000000000063")
	runID := model.RunID("019c5f8b-7c8a-7000-8000-000000000064")
	capture, err := logstore.CreateRun(stateDir, jobID.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	command := supervisorCoverageCommand(t, "sleep")
	platform.ConfigureTarget(command)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := platform.Inspect(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	target := &preparedTarget{
		command:  command,
		stdout:   stdout,
		stderr:   stderr,
		resolved: supervisorTestExecutable(t),
	}
	database := openSupervisorStore(t, stateDir)
	closeSupervisorStore(t, database)

	terminal, err := superviseStartedTarget(
		t.Context(), t.Context(), database, capture, jobID, runID, logs, target, identity,
		time.Now().UTC(), 0, "both", nil,
	)
	if err == nil || !terminal {
		t.Fatalf("superviseStartedTarget() = (%t, %v)", terminal, err)
	}
}

func TestRunAcknowledgementWriterFailures(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	if err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(), bytes.NewReader(fixture.credential),
		failingAcknowledgementWriter{writeErr: errors.New("write failed")},
	); err == nil {
		t.Fatal("Run(failing acknowledgement write) error = nil")
	}

	fixture = submitSupervisorFixture(t, true)
	if err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(), bytes.NewReader(fixture.credential),
		failingAcknowledgementWriter{closeErr: errors.New("close failed")},
	); err == nil {
		t.Fatal("Run(failing acknowledgement close) error = nil")
	}
}

type failingAcknowledgementWriter struct {
	writeErr error
	closeErr error
}

func (writer failingAcknowledgementWriter) Write(data []byte) (int, error) {
	if writer.writeErr != nil {
		return 0, writer.writeErr
	}

	return len(data), nil
}

func (writer failingAcknowledgementWriter) Close() error { return writer.closeErr }

func TestReconcileLaunchClaimAndStoreFailure(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	command := supervisorCoverageCommand(t, "success")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ack, err := reconcileLaunch(t.Context(), LaunchOptions{
		Store: database, JobID: job.ID,
	}, command, errors.New("ack failed"))
	if err != nil || ack.JobID != job.ID || ack.SupervisorID != job.SupervisorID {
		t.Fatalf("reconcileLaunch(claimed) = (%+v, %v)", ack, err)
	}

	fixture = submitSupervisorFixture(t, true)
	closed := openSupervisorStore(t, fixture.stateDir)
	closeSupervisorStore(t, closed)
	command = supervisorCoverageCommand(t, "success")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileLaunch(t.Context(), LaunchOptions{
		Store: closed, JobID: fixture.jobID,
	}, command, errors.New("ack failed")); err == nil {
		t.Fatal("reconcileLaunch(closed store) error = nil")
	}
}

func claimCoverageFixture(
	t *testing.T,
	database *store.Store,
	fixture supervisorFixture,
	lease time.Duration,
) model.JobState {
	t.Helper()

	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	supervisorID, err := ids.NewSupervisorID()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := platform.Inspect(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claim, err := database.Claim(
		t.Context(), fixture.jobID, fixture.credential, supervisorID, modelIdentity(identity), now, now.Add(lease),
	)
	if err != nil {
		t.Fatal(err)
	}

	return claim.Job
}

func reserveCoverageRun(
	t *testing.T,
	database *store.Store,
	stateDir string,
	jobID model.JobID,
) (*logstore.Run, model.LogMetadata, model.RunID) {
	t.Helper()

	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := ids.NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	capture, err := logstore.CreateRun(stateDir, jobID.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, time.Now().UTC()); err != nil {
		_ = capture.Close()
		t.Fatal(err)
	}
	if err := database.BindAdmissionToRun(t.Context(), jobID, runID); err != nil {
		_ = capture.Close()
		t.Fatal(err)
	}

	return capture, logs, runID
}

func TestNotificationQueueFailureRetryAndRenewalEdges(t *testing.T) {
	t.Parallel()

	t.Run("missing definition", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		event, err := database.TransitionEvent(
			t.Context(), model.EntityJob, job.ID.String(), job.Revision,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.QueueNotificationDeliveries(t.Context(), []store.QueueNotificationDeliveryInput{{
			JobID: job.ID, EventID: event.ID, NotifierName: "missing",
			EventType: string(notify.EventJobStarted), MaxAttempts: 1,
		}}); err != nil {
			t.Fatal(err)
		}
		delivery, err := database.ClaimNotificationDelivery(
			t.Context(), event.ID, time.Now().UTC(), time.Now().UTC().Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := processClaimedNotification(t.Context(), database, delivery); err != nil {
			t.Fatalf("processClaimedNotification(missing definition) error = %v", err)
		}
		deliveries, err := database.ListNotificationDeliveries(t.Context(), job.ID)
		if err != nil || len(deliveries) != 1 || deliveries[0].Status != store.NotificationDeliveryFailed {
			t.Fatalf("failed delivery = (%+v, %v)", deliveries, err)
		}
	})

	t.Run("notifier construction failure", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.NotifierDefinitions = []model.NotifierDefinition{{
			Name: "missing-secret", Kind: model.NotifierCommand, Timeout: time.Second,
			Retry: model.NotifierRetryPolicy{MaxAttempts: 1},
			Command: &model.CommandNotifierDefinition{
				Executable: supervisorTestExecutable(t),
				SecretEnvironment: map[string]model.SecretReference{
					"TOKEN": {Provider: "env", Name: "JOBMAN_NOTIFICATION_ABSENT"},
				},
			},
		}}
		configuration.Notifications = []model.NotificationSubscription{{
			Notifier: "missing-secret", Events: []string{string(notify.EventJobStarted)},
		}}
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
		if err != nil {
			t.Fatal(err)
		}
		delivery, err := database.ClaimNotificationDelivery(
			t.Context(), event.ID, time.Now().UTC(), time.Now().UTC().Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := processClaimedNotification(t.Context(), database, delivery); err != nil {
			t.Fatalf("processClaimedNotification(construction failure) error = %v", err)
		}
	})

	t.Run("past job deadline", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.JobTimeout = time.Nanosecond
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.QueueNotificationDeliveries(t.Context(), []store.QueueNotificationDeliveryInput{{
			JobID: job.ID, EventID: event.ID, NotifierName: "missing",
			EventType: string(notify.EventJobStarted), MaxAttempts: 1,
		}}); err != nil {
			t.Fatal(err)
		}
		delivery, err := database.ClaimNotificationDelivery(
			t.Context(), event.ID, time.Now().UTC(), time.Now().UTC().Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := processClaimedNotification(t.Context(), database, delivery); err != nil {
			t.Fatalf("processClaimedNotification(past deadline) error = %v", err)
		}
	})

	t.Run("future retry wait is cancelable", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.QueueNotificationDeliveries(t.Context(), []store.QueueNotificationDeliveryInput{{
			JobID: job.ID, EventID: event.ID, NotifierName: "retry",
			EventType: string(notify.EventJobStarted), MaxAttempts: 2,
		}}); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		delivery, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now, now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		next := now.Add(time.Hour)
		if _, err := database.CompleteNotificationDelivery(t.Context(), store.CompleteNotificationDeliveryInput{
			EventID: delivery.EventID, ClaimToken: delivery.ClaimToken, NotifierName: delivery.NotifierName,
			AttemptNumber: 1, StartedAt: now, CompletedAt: now, NextAttemptAt: &next,
			DiagnosticCode: string(notify.ErrorInternal), Retryable: true,
		}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := processNotificationQueue(ctx, database, event.ID, true); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("processNotificationQueue(canceled wait) error = %v", err)
		}
	})

	t.Run("renewal failure", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.QueueNotificationDeliveries(t.Context(), []store.QueueNotificationDeliveryInput{{
			JobID: job.ID, EventID: event.ID, NotifierName: "renew",
			EventType: string(notify.EventJobStarted), MaxAttempts: 1,
		}}); err != nil {
			t.Fatal(err)
		}
		delivery, err := database.ClaimNotificationDelivery(
			t.Context(), event.ID, time.Now().UTC(), time.Now().UTC().Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		closeSupervisorStore(t, database)
		completed := make(chan error, 1)
		maintainNotificationClaimAtInterval(
			t.Context(), func() {}, database, delivery, completed, time.Millisecond,
		)
		if err := <-completed; err == nil {
			t.Fatal("notification claim renewal error = nil")
		}
	})

	t.Run("already canceled renewal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		completed := make(chan error, 1)
		maintainNotificationClaimAtInterval(
			ctx, func() {}, nil, store.NotificationDelivery{}, completed, time.Hour,
		)
		if err := <-completed; err != nil {
			t.Fatalf("canceled claim renewal error = %v", err)
		}
	})
}

func TestCancellationAndLeaseHelperEdges(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if err := recordContextCancellation(t.Context(), t.Context(), database, job.ID); err != nil {
		t.Fatalf("recordContextCancellation(active) error = %v", err)
	}
	stopCtx, stop := context.WithCancel(t.Context())
	stop()
	if err := recordContextCancellation(stopCtx, t.Context(), database, job.ID); err != nil {
		t.Fatalf("recordContextCancellation(canceled) error = %v", err)
	}

	fixture2 := submitSupervisorFixture(t, true)
	database2 := openSupervisorStore(t, fixture2.stateDir)
	job2 := claimCoverageFixture(t, database2, fixture2, time.Minute)
	stopCtx2, stop2 := context.WithCancel(t.Context())
	stop2()
	if err := waitForSchedulerTick(stopCtx2, t.Context(), database2, job2.ID, time.Hour); err != nil {
		t.Fatalf("waitForSchedulerTick(stop) error = %v", err)
	}
	closeSupervisorStore(t, database2)
	if err := waitForSchedulerTick(stopCtx2, t.Context(), database2, job2.ID, time.Hour); err == nil {
		t.Fatal("waitForSchedulerTick(closed store) error = nil")
	}

	closeSupervisorStore(t, database)
	if err := recordContextCancellation(stopCtx, t.Context(), database, job.ID); err == nil {
		t.Fatal("recordContextCancellation(closed store) error = nil")
	}

	fixture3 := submitSupervisorFixture(t, true)
	database3 := openSupervisorStore(t, fixture3.stateDir)
	defer closeSupervisorStore(t, database3)
	job3 := claimCoverageFixture(t, database3, fixture3, time.Minute)
	owner, err := database3.GetSupervisorForJob(t.Context(), job3.ID)
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx, cancelLease := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		renewLeaseAtInterval(leaseCtx, database3, owner.ID, job3.ID, time.Millisecond)
		close(done)
	}()
	time.AfterFunc(5*time.Millisecond, cancelLease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("renewLeaseAtInterval did not stop")
	}
}

func TestLeaseRenewalCancellationDuringOperations(t *testing.T) {
	t.Parallel()

	for _, failLease := range []bool{true, false} {
		ctx, cancel := context.WithCancel(t.Context())
		renewer := &cancelingLeaseRenewer{cancel: cancel, failLease: failLease}
		renewLeaseAtInterval(
			ctx, renewer,
			model.SupervisorID("019c5f8b-7c8a-7000-8000-000000000097"),
			model.JobID("019c5f8b-7c8a-7000-8000-000000000098"),
			time.Nanosecond,
		)
		if failLease && renewer.admissionCalls != 0 {
			t.Fatalf("admission renewal called after lease failure: %d", renewer.admissionCalls)
		}
		if !failLease && renewer.admissionCalls != 1 {
			t.Fatalf("admission renewal calls = %d, want 1", renewer.admissionCalls)
		}
	}
}

type cancelingLeaseRenewer struct {
	cancel         context.CancelFunc
	failLease      bool
	admissionCalls int
}

func (renewer *cancelingLeaseRenewer) RenewLease(
	context.Context,
	model.SupervisorID,
	time.Time,
	time.Time,
) (model.SupervisorState, error) {
	if renewer.failLease {
		renewer.cancel()
		return model.SupervisorState{}, errors.New("lease failed")
	}

	return model.SupervisorState{}, nil
}

func (renewer *cancelingLeaseRenewer) RenewAdmission(
	context.Context,
	model.JobID,
	time.Time,
	time.Duration,
) error {
	renewer.admissionCalls++
	renewer.cancel()

	return errors.New("admission failed")
}

func TestExecutionSetupFailureEdges(t *testing.T) {
	t.Parallel()

	t.Run("closed runtime store", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		job, err := database.GetJob(t.Context(), fixture.jobID)
		if err != nil {
			t.Fatal(err)
		}
		closeSupervisorStore(t, database)
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		jitter, err := newJitterSource()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executeOneRun(
			t.Context(), t.Context(), database, ids, fixture.stateDir, job, nil, jitter,
		); err == nil {
			t.Fatal("executeOneRun(closed store) error = nil")
		}
	})

	t.Run("unsafe log root", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		jitter, err := newJitterSource()
		if err != nil {
			t.Fatal(err)
		}
		unsafeRoot := t.TempDir()
		if err := os.Chmod(unsafeRoot, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := executeOneRun(
			t.Context(), t.Context(), database, ids, unsafeRoot, job, nil, jitter,
		); err == nil {
			t.Fatal("executeOneRun(unsafe log root) error = nil")
		}
	})

	t.Run("missing admission", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		jitter, err := newJitterSource()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executeOneRun(
			t.Context(), t.Context(), database, ids, fixture.stateDir, job, nil, jitter,
		); err == nil {
			t.Fatal("executeOneRun(missing admission) error = nil")
		}
	})
}

func TestSchedulerDeadlineAndCandidateEdges(t *testing.T) {
	t.Parallel()

	t.Run("expired job timeout", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.JobTimeout = time.Nanosecond
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		result, expired, err := completeSchedulingDeadline(
			t.Context(), database, job, time.Now().UTC().Add(time.Second),
		)
		if err != nil || !expired || !result.terminal || result.job.Outcome != model.JobOutcomeTimedOut {
			t.Fatalf("completeSchedulingDeadline(timeout) = (%+v, %t, %v)", result, expired, err)
		}
	})

	t.Run("expired wait abort", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.WaitConditions = []model.WaitCondition{{
			Kind: model.WaitFileExists, Path: filepath.Join(t.TempDir(), "missing"),
			PollInterval: time.Second, AbortAt: time.Now().UTC().Add(time.Millisecond),
		}}
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		result, expired, err := completeSchedulingDeadline(
			t.Context(), database, job, time.Now().UTC().Add(time.Second),
		)
		if err != nil || !expired || !result.terminal || result.job.Outcome != model.JobOutcomeAborted {
			t.Fatalf("completeSchedulingDeadline(wait abort) = (%+v, %t, %v)", result, expired, err)
		}
	})

	t.Run("candidate errors and interruption", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.CompleteWithoutRun(
			t.Context(), job.ID, model.JobOutcomeAborted, "coverage", time.Now().UTC(),
		); err != nil {
			t.Fatal(err)
		}
		_, result, done, err := loadAdmissionCandidate(
			t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
		)
		if err != nil || !done || !result.terminal {
			t.Fatalf("loadAdmissionCandidate(terminal) = (%+v, %t, %v)", result, done, err)
		}
		closeSupervisorStore(t, database)
		if _, _, _, err := loadAdmissionCandidate(
			t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
		); err == nil {
			t.Fatal("loadAdmissionCandidate(closed store) error = nil")
		}
	})
}

func TestAwaitTargetObservesPauseAndDurableEOF(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute); err != nil {
		t.Fatal(err)
	}
	capture, _, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
	t.Cleanup(func() { _ = capture.Close() })
	identity, err := platform.Inspect(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), job.ID, runID, "/test/target", modelIdentity(identity), time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordInputEOF(t.Context(), job.ID, runID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pause(t.Context(), job.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	completion := make(chan error, 1)
	time.AfterFunc(150*time.Millisecond, func() { completion <- nil })
	closedInput := make(chan struct{}, 1)
	waitErr, _, release, controlErr := awaitTarget(
		t.Context(), t.Context(), database, job.ID, identity, completion,
		func() error { closedInput <- struct{}{}; return nil }, time.Now().UTC(), 0,
	)
	release()
	if waitErr != nil || controlErr != nil {
		t.Fatalf("awaitTarget(paused EOF) = (%v, %v)", waitErr, controlErr)
	}
	select {
	case <-closedInput:
	default:
		t.Fatal("awaitTarget did not observe durable EOF")
	}

	if _, err := database.Resume(t.Context(), job.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	completion = make(chan error, 1)
	time.AfterFunc(150*time.Millisecond, func() { completion <- nil })
	waitErr, _, release, controlErr = awaitTarget(
		t.Context(), t.Context(), database, job.ID, identity, completion,
		func() error { return nil }, time.Now().UTC(), time.Hour,
	)
	release()
	if waitErr != nil || controlErr != nil {
		t.Fatalf("awaitTarget(negative pause delta) = (%v, %v)", waitErr, controlErr)
	}
}

func TestCopyPipeFailurePaths(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	capture, err := logstore.CreateRun(
		stateDir, "019c5f8b-7c8a-7000-8000-000000000071", 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyPipe(&failingReader{err: errors.New("read failed")}, capture, logstore.Stdout); err == nil {
		t.Fatal("copyPipe(read failure) error = nil")
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyPipe(&failingReader{err: errors.New("drain failed")}, capture, logstore.Stdout); err == nil {
		t.Fatal("copyPipe(closed capture) error = nil")
	}
}

func TestLiveInputSetupPath(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixtureWithPolicyOptionsAndStdin(
		t, true, false, model.DefaultExecutionPolicy(), "", model.StdinLive,
	)
	err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(),
		bytes.NewReader(fixture.credential), new(closingBuffer),
	)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, getErr := database.GetJob(t.Context(), fixture.jobID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if err != nil {
		if job.Outcome != model.JobOutcomeAborted || job.LastDiagnosticCode != "live_input_unavailable" {
			t.Fatalf("Run(live input) error = %v; job = %+v", err, job)
		}
		return
	}
	if job.Outcome != model.JobOutcomeSuccess {
		t.Fatalf("Run(live input) job outcome = %q", job.Outcome)
	}
}

func TestAwaitRunnablePausedAndDependencyFailure(t *testing.T) {
	t.Parallel()

	t.Run("paused stop cancellation", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.MoveJob(
			t.Context(), job.ID, model.JobPhaseQueued, time.Now().UTC(), "coverage",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pause(t.Context(), job.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := awaitRunnable(
			stopCtx, t.Context(), database, job.ID, fixedJitterSource(0),
		)
		if err != nil || !result.terminal || result.job.Outcome != model.JobOutcomeCancelled {
			t.Fatalf("awaitRunnable(paused canceled stop) = (%+v, %v)", result, err)
		}
	})

	t.Run("impossible dependency", func(t *testing.T) {
		stateDir := t.TempDir()
		if err := os.Chmod(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		database := openSupervisorStore(t, stateDir)
		defer closeSupervisorStore(t, database)
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		dependencyID, err := ids.NewJobID()
		if err != nil {
			t.Fatal(err)
		}
		dependencyCredential := bytes.Repeat([]byte{0x61}, credentialSize)
		dependencyHash, err := model.NewCredentialHash(dependencyCredential)
		if err != nil {
			t.Fatal(err)
		}
		dependencySpec, err := model.NewJobSpec(model.JobSpecInput{
			Executable: supervisorTestExecutable(t), WorkingDirectory: t.TempDir(),
			ExecutionPolicy: model.DefaultExecutionPolicy(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Submit(
			t.Context(), dependencyID, dependencySpec, dependencyHash, now, now.Add(time.Second),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := database.MarkSubmissionFailed(
			t.Context(), dependencyID, "coverage", now.Add(2*time.Second),
		); err != nil {
			t.Fatal(err)
		}

		jobID, err := ids.NewJobID()
		if err != nil {
			t.Fatal(err)
		}
		credential := bytes.Repeat([]byte{0x62}, credentialSize)
		hash, err := model.NewCredentialHash(credential)
		if err != nil {
			t.Fatal(err)
		}
		configuration := model.DefaultExecutionPolicy()
		configuration.Dependencies = []model.DependencyRequirement{{
			JobID: dependencyID, Predicate: string(store.DependencySuccess),
		}}
		specification, err := model.NewJobSpec(model.JobSpecInput{
			Executable: supervisorTestExecutable(t), WorkingDirectory: t.TempDir(), ExecutionPolicy: configuration,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.SubmitWithDependencies(
			t.Context(), jobID, specification, hash, now, now.Add(time.Minute),
			[]store.Dependency{{JobID: jobID, DependsOn: dependencyID, Predicate: store.DependencySuccess}},
		); err != nil {
			t.Fatal(err)
		}
		fixture := supervisorFixture{stateDir: stateDir, jobID: jobID, credential: credential}
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		result, err := awaitRunnable(
			t.Context(), t.Context(), database, job.ID, fixedJitterSource(0),
		)
		if err != nil || !result.terminal || result.job.Outcome != model.JobOutcomeAborted {
			t.Fatalf("awaitRunnable(impossible dependency) = (%+v, %v)", result, err)
		}
	})
}

func TestBackoffInterruptionAndCancellation(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name            string
		cancelOperation bool
	}{
		{name: "job cancellation"},
		{name: "operation cancellation", cancelOperation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := model.DefaultExecutionPolicy()
			two, err := policy.FiniteLimit(2)
			if err != nil {
				t.Fatal(err)
			}
			configuration.Completion.MaxRuns = two
			configuration.Completion.FailureLimit = two
			fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
			database := openSupervisorStore(t, fixture.stateDir)
			defer closeSupervisorStore(t, database)
			job := claimCoverageFixture(t, database, fixture, time.Minute)
			if _, err := database.TryAcquireAdmission(
				t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
			); err != nil {
				t.Fatal(err)
			}
			capture, logs, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			logs = completedLogMetadata(logs, nil, nil)
			next := time.Now().UTC().Add(time.Hour)
			completed, err := database.CompleteRunWithDisposition(
				t.Context(), job.ID, runID, model.RunOutcomeStartFailed, nil, logs, "coverage",
				time.Now().UTC(), model.RunDisposition{
					NextPhase: model.JobPhaseBackoff, NextRunAt: &next, Reason: "retry",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			operationCtx := t.Context()
			if test.cancelOperation {
				var cancel context.CancelFunc
				operationCtx, cancel = context.WithCancel(t.Context())
				cancel()
			} else if _, err := database.RequestCancellation(t.Context(), job.ID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			_, result, done, err := awaitBackoffEligibility(
				t.Context(), operationCtx, database, completed.Job, fixedJitterSource(0),
			)
			if test.cancelOperation {
				if !errors.Is(err, context.Canceled) || done {
					t.Fatalf("awaitBackoffEligibility(canceled operation) = (%+v, %t, %v)", result, done, err)
				}
			} else if err != nil || !done || !result.terminal {
				t.Fatalf("awaitBackoffEligibility(job cancellation) = (%+v, %t, %v)", result, done, err)
			}
		})
	}
}

func TestFinalizeAcquiredAdmissionAfterCompletion(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(
		t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteWithoutRun(
		t.Context(), job.ID, model.JobOutcomeAborted, "coverage", time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	result, err := finalizeAcquiredAdmission(
		t.Context(), t.Context(), database, job.ID, time.Now().UTC(), fixedJitterSource(0),
	)
	if err != nil || !result.terminal {
		t.Fatalf("finalizeAcquiredAdmission(completed) = (%+v, %v)", result, err)
	}
}

func TestReconcileExpiredOwnerWithActiveRun(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	supervisorID, err := ids.NewSupervisorID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	deadIdentity := model.ProcessIdentity{
		PID: 2_000_000_000, Platform: "linux", CreationID: "missing",
		BootID: "missing", TreeID: "missing",
	}
	claim, err := database.Claim(
		t.Context(), fixture.jobID, fixture.credential, supervisorID, deadIdentity,
		now, now.Add(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.TryAcquireAdmission(
		t.Context(), claim.Job.ID, "", 1, now, time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	capture, logs, runID := reserveCoverageRun(t, database, fixture.stateDir, claim.Job.ID)
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	reserved, err := database.GetRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := reserved.ReservedAt.Add(time.Millisecond)
	if _, err := database.MarkProcessStarted(
		t.Context(), claim.Job.ID, runID, "/missing/target", deadIdentity, startedAt,
	); err != nil {
		t.Fatal(err)
	}
	changed, err := reconcileExpiredJobOwner(
		t.Context(), database, claim.Job.ID, startedAt.Add(time.Second),
	)
	if err != nil || !changed {
		t.Fatalf("reconcileExpiredJobOwner(active run) = (%t, %v)", changed, err)
	}
	run, err := database.GetRun(t.Context(), runID)
	if err != nil || run.Logs.Integrity != model.LogIntegrityPartial ||
		run.Logs.DiagnosticCode != "supervisor_lease_expired" || logs.Integrity != model.LogIntegrityPending {
		t.Fatalf("lost active run = (%+v, %v)", run, err)
	}
}

func TestNotificationClosedStoreAndPausedDeadline(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.JobTimeout = time.Minute
	configuration.NotifierDefinitions = []model.NotifierDefinition{{
		Name: "command", Kind: model.NotifierCommand, Timeout: time.Second,
		Retry:   model.NotifierRetryPolicy{MaxAttempts: 1},
		Command: &model.CommandNotifierDefinition{Executable: supervisorTestExecutable(t)},
	}}
	configuration.Notifications = []model.NotificationSubscription{{
		Notifier: "command", Events: []string{string(notify.EventJobStarted)},
	}}
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.MoveJob(
		t.Context(), job.ID, model.JobPhaseQueued, time.Now().UTC(), "coverage",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pause(t.Context(), job.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	paused, err := database.GetJob(t.Context(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, bounded, err := notificationDeadline(t.Context(), database, paused, time.Now().UTC().Add(time.Second)); err != nil || !bounded {
		t.Fatalf("notificationDeadline(paused) = (%t, %v)", bounded, err)
	}
	event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
	if err != nil {
		t.Fatal(err)
	}
	closeSupervisorStore(t, database)
	if err := deliverNotifications(
		t.Context(), database, job, event.ID, notify.EventJobStarted, "", time.Now().UTC(),
	); err == nil {
		t.Fatal("deliverNotifications(closed store) error = nil")
	}
	if err := processNotificationQueue(t.Context(), database, event.ID, false); err == nil {
		t.Fatal("processNotificationQueue(closed store) error = nil")
	}
	if _, _, err := notificationBudgetContext(t.Context(), database, job); err == nil {
		t.Fatal("notificationBudgetContext(closed store) error = nil")
	}
}

func TestExecuteOneRunCancellationAfterReservation(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(
		t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jitter, err := newJitterSource()
	if err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithCancel(t.Context())
	cancel()
	terminal, err := executeOneRun(
		stopCtx, t.Context(), database, ids, fixture.stateDir, job, nil, jitter,
	)
	if err != nil || !terminal {
		t.Fatalf("executeOneRun(pre-start cancellation) = (%t, %v)", terminal, err)
	}
}

func TestAwaitTargetWithoutForcedEscalation(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Windows process termination has no graceful signal equivalent")
	}

	configuration := model.DefaultExecutionPolicy()
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable: "/bin/sh", Arguments: []string{"-c", "exit 0"}, WorkingDirectory: t.TempDir(),
		StdinPolicy:     model.StdinNull,
		StopPolicy:      model.StopPolicy{GracePeriod: 10 * time.Millisecond, ForceAfterGrace: false},
		ExecutionPolicy: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := submitCoverageSpecification(t, specification)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(
		t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	capture, _, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
	t.Cleanup(func() { _ = capture.Close() })

	command := exec.Command("/bin/sh", "-c", "trap '' TERM; echo ready; while :; do sleep 1; done")
	platform.ConfigureTarget(command)
	ready, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if buffer := make([]byte, len("ready\n")); func() error {
		_, readErr := io.ReadFull(ready, buffer)
		if readErr == nil && string(buffer) != "ready\n" {
			readErr = fmt.Errorf("unexpected readiness output %q", buffer)
		}
		return readErr
	}() != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal("target did not become ready")
	}
	identity, err := platform.Inspect(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), job.ID, runID, "/bin/sh", modelIdentity(identity), time.Now().UTC(),
	); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, release, controlErr := awaitTarget(
		stopCtx, t.Context(), database, job.ID, identity, make(chan error),
		func() error { return nil }, time.Now().UTC(), 0,
	)
	release()
	_ = command.Process.Kill()
	_ = command.Wait()
	if controlErr == nil || !strings.Contains(controlErr.Error(), "did not exit") {
		t.Fatalf("awaitTarget(no forced escalation) error = %v", controlErr)
	}
}

func TestHandlePublishFailureAfterCancellation(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(
		t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	capture, logs, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
	if _, err := database.RequestCancellation(t.Context(), job.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	command := supervisorCoverageCommand(t, "sleep")
	platform.ConfigureTarget(command)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	identity, err := platform.Inspect(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	target := &preparedTarget{command: command, stdout: stdout, stderr: stderr}
	group := new(sync.WaitGroup)
	errorsChannel := make(chan error, 2)
	group.Add(2)
	go drainPipe(group, stdout, capture, logstore.Stdout, true, errorsChannel)
	go drainPipe(group, stderr, capture, logstore.Stderr, true, errorsChannel)
	if err := handlePublishFailure(
		t.Context(), database, capture, job.ID, runID, logs, target, identity,
		group, errorsChannel, errors.New("publish failed"),
	); err != nil {
		t.Fatalf("handlePublishFailure(canceled job) error = %v", err)
	}
	run, err := database.GetRun(t.Context(), runID)
	if err != nil || run.Outcome != model.RunOutcomeCancelled {
		t.Fatalf("canceled unpublished run = (%+v, %v)", run, err)
	}
}

func TestSchedulerJitterDefensiveBranches(t *testing.T) {
	t.Parallel()

	if got := jitteredSchedulerPoll(time.Second, nil); got != time.Second {
		t.Fatalf("jitteredSchedulerPoll(nil) = %s", got)
	}
	if got := jitteredSchedulerPoll(5*time.Nanosecond, fixedJitterSource(0)); got != 5*time.Nanosecond {
		t.Fatalf("jitteredSchedulerPoll(narrow) = %s", got)
	}
}

func TestLaunchAdditionalFailurePaths(t *testing.T) {
	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	options := LaunchOptions{
		Store: database, Executable: filepath.Join(t.TempDir(), "missing"), StateDir: fixture.stateDir,
		JobID: fixture.jobID, Credential: fixture.credential, Timeout: 10 * time.Second,
	}
	defaulted := options
	defaulted.Timeout = 0
	if timeout, err := validateLaunchOptions(defaulted); err != nil || timeout != defaultAckTimeout {
		t.Fatalf("validateLaunchOptions(default timeout) = (%s, %v)", timeout, err)
	}
	if _, err := Launch(t.Context(), options); err == nil {
		t.Fatal("Launch(missing executable) error = nil")
	}

	t.Setenv(supervisorLaunchHelperEnvironment, "slow")
	options.Executable = supervisorTestExecutable(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Launch(ctx, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("Launch(canceled context) error = %v", err)
	}
}

func TestPrepareTargetRejectsUnknownSecretProvider(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.SecretEnv = map[string]model.SecretReference{
		"TOKEN": {Provider: "unknown", Name: "value"},
	}
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable: supervisorTestExecutable(t), WorkingDirectory: t.TempDir(), StdinPolicy: model.StdinNull,
		StopPolicy:      model.StopPolicy{GracePeriod: time.Second, ForceAfterGrace: true},
		ExecutionPolicy: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareTarget(t.Context(), model.JobState{Spec: specification},
		model.RunID("019c5f8b-7c8a-7000-8000-000000000081"), nil); err == nil {
		t.Fatal("prepareTarget(unknown secret provider) error = nil")
	}
}

func TestSchedulerAdditionalErrorEdges(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	if _, err := reconcileExpiredJobOwner(
		t.Context(), database, model.JobID("019c5f8b-7c8a-7000-8000-000000000091"), time.Now().UTC(),
	); err == nil {
		t.Fatal("reconcileExpiredJobOwner(missing job) error = nil")
	}
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	changed, err := reconcileExpiredJobOwner(t.Context(), database, job.ID, time.Now().UTC())
	if err != nil || changed {
		t.Fatalf("reconcileExpiredJobOwner(unexpired) = (%t, %v)", changed, err)
	}
	if _, err := acquireAdmission(
		t.Context(), t.Context(), database,
		model.JobState{ID: job.ID, Phase: model.JobPhaseRunning, Spec: job.Spec}, fixedJitterSource(0),
	); err == nil {
		t.Fatal("acquireAdmission(running phase) error = nil")
	}
	closeSupervisorStore(t, database)
	if _, err := reconcileExpiredOwnership(t.Context(), database, time.Now().UTC()); err == nil {
		t.Fatal("reconcileExpiredOwnership(closed store) error = nil")
	}
	backoff := job
	backoff.Phase = model.JobPhaseBackoff
	next := time.Now().UTC().Add(time.Hour)
	closedRuntime := store.JobRuntime{JobID: job.ID, NextRunAt: &next}
	_ = closedRuntime
	if _, _, _, err := awaitBackoffEligibility(
		t.Context(), t.Context(), database, backoff, fixedJitterSource(0),
	); err == nil {
		t.Fatal("awaitBackoffEligibility(closed store) error = nil")
	}
}

func TestSchedulerEvaluationErrorEdges(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.WaitConditions = []model.WaitCondition{{
		Kind: model.WaitFileExists, Path: filepath.Join(t.TempDir(), "missing"),
		PollInterval: time.Second, AbortAt: time.Now().UTC().Add(-time.Second),
	}}
	configuration.JobTimeout = time.Second
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	closeSupervisorStore(t, database)
	if _, _, err := completeSchedulingDeadline(
		t.Context(), database, job, time.Now().UTC(),
	); err == nil {
		t.Fatal("completeSchedulingDeadline(closed runtime) error = nil")
	}
	if _, _, err := evaluateWaitConditions(
		t.Context(), database, job, policy.WaitModeAll,
		[]model.WaitCondition{{
			Kind: model.WaitFileExists, Path: filepath.Join(t.TempDir(), "missing"), PollInterval: time.Second,
		}}, time.Now().UTC(),
	); err == nil {
		t.Fatal("evaluateWaitConditions(closed store) error = nil")
	}
	if _, err := (execProbeRunner{directory: t.TempDir()}).RunProbe(t.Context(), policy.ProbeSpec{
		Executable: "", Timeout: time.Second, OutputLimit: 1,
	}); err == nil {
		t.Fatal("RunProbe(empty executable) error = nil")
	}
}

func TestAwaitRunnablePersistenceFailureBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("runtime load", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		claimCoverageFixture(t, database, fixture, time.Minute)
		raw := openSupervisorRawDatabase(t, database)
		if _, err := raw.ExecContext(t.Context(), `ALTER TABLE job_runtime RENAME TO broken_runtime`); err != nil {
			t.Fatal(err)
		}
		if _, err := awaitRunnable(t.Context(), t.Context(), database, fixture.jobID, fixedJitterSource(0)); err == nil {
			t.Fatal("awaitRunnable(runtime load failure) error = nil")
		}
	})

	t.Run("initial move", func(t *testing.T) {
		configuration := model.DefaultExecutionPolicy()
		configuration.WaitConditions = []model.WaitCondition{{
			Kind: model.WaitDelay, Delay: time.Hour, PollInterval: time.Second,
		}}
		fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		claimCoverageFixture(t, database, fixture, time.Minute)
		raw := openSupervisorRawDatabase(t, database)
		if _, err := raw.ExecContext(t.Context(), `
			CREATE TRIGGER fail_wait_move BEFORE UPDATE ON jobs
			BEGIN SELECT RAISE(ABORT, 'injected move failure'); END`); err != nil {
			t.Fatal(err)
		}
		if _, err := awaitRunnable(t.Context(), t.Context(), database, fixture.jobID, fixedJitterSource(0)); err == nil {
			t.Fatal("awaitRunnable(initial move failure) error = nil")
		}
	})

	t.Run("prerequisite persistence", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		claimCoverageFixture(t, database, fixture, time.Minute)
		raw := openSupervisorRawDatabase(t, database)
		if _, err := raw.ExecContext(t.Context(), `
			CREATE TRIGGER fail_prerequisite BEFORE UPDATE ON job_runtime
			BEGIN SELECT RAISE(ABORT, 'injected prerequisite failure'); END`); err != nil {
			t.Fatal(err)
		}
		if _, err := awaitRunnable(t.Context(), t.Context(), database, fixture.jobID, fixedJitterSource(0)); err == nil {
			t.Fatal("awaitRunnable(prerequisite failure) error = nil")
		}
	})

	t.Run("paused interruption", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.MoveJob(t.Context(), job.ID, model.JobPhaseQueued, time.Now().UTC(), "queued"); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pause(t.Context(), job.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		raw := openSupervisorRawDatabase(t, database)
		if _, err := raw.ExecContext(t.Context(), `
			CREATE TRIGGER fail_wait_cancel BEFORE UPDATE ON jobs
			BEGIN SELECT RAISE(ABORT, 'injected cancellation failure'); END`); err != nil {
			t.Fatal(err)
		}
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := awaitRunnable(stopCtx, t.Context(), database, fixture.jobID, fixedJitterSource(0)); err == nil {
			t.Fatal("awaitRunnable(paused cancellation persistence) error = nil")
		}
	})
}

func openSupervisorRawDatabase(t *testing.T, database *store.Store) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", database.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	return raw
}

func TestDispositionRejectsMissingJitterSource(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.FailureDelay = policy.DelayPolicy{
		Base: time.Second, Backoff: policy.BackoffConstant, Jitter: time.Second,
	}
	configuration.Completion.MaxRuns = policy.Limit{Value: 2}
	configuration.Completion.FailureLimit = policy.Limit{Value: 2}
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job, err := database.GetJob(t.Context(), fixture.jobID)
	if err != nil {
		t.Fatal(err)
	}
	code := 1
	if _, _, err := dispositionForRun(
		job, store.JobRuntime{}, model.RunOutcomeFailure,
		&model.ExitInfo{ExitCode: &code, ObservedAt: time.Now().UTC()}, time.Now().UTC(), nil,
	); err == nil {
		t.Fatal("dispositionForRun(nil jitter source) error = nil")
	}
	zero := 0
	if _, _, err := dispositionForRun(
		job, store.JobRuntime{SuccessCount: 2}, model.RunOutcomeSuccess,
		&model.ExitInfo{ExitCode: &zero, ObservedAt: time.Now().UTC()},
		time.Now().UTC(), fixedJitterSource(0),
	); err == nil {
		t.Fatal("dispositionForRun(inconsistent counts) error = nil")
	}
}

func TestAwaitTargetStoreAndTerminateErrors(t *testing.T) {
	t.Parallel()

	t.Run("closed store observation", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		closeSupervisorStore(t, database)
		_, _, release, err := awaitTarget(
			t.Context(), t.Context(), database, fixture.jobID,
			platform.ProcessIdentity{}, make(chan error), func() error { return nil },
			time.Now().UTC(), 0,
		)
		release()
		if err == nil {
			t.Fatal("awaitTarget(closed store) error = nil")
		}
	})

	t.Run("invalid target identity", func(t *testing.T) {
		specification, err := model.NewJobSpec(model.JobSpecInput{
			Executable: supervisorTestExecutable(t), WorkingDirectory: t.TempDir(),
			StopPolicy:      model.StopPolicy{GracePeriod: time.Millisecond, ForceAfterGrace: false},
			ExecutionPolicy: model.DefaultExecutionPolicy(),
		})
		if err != nil {
			t.Fatal(err)
		}
		fixture := submitCoverageSpecification(t, specification)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, release, err := awaitTarget(
			stopCtx, t.Context(), database, job.ID,
			platform.ProcessIdentity{PID: 2_000_000_000, Creation: "missing", Boot: "missing"},
			make(chan error), func() error { return nil }, time.Now().UTC(), 0,
		)
		release()
		if err == nil {
			t.Fatal("awaitTarget(invalid identity) error = nil")
		}
	})

	t.Run("cancellation intent persistence failure", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		closeSupervisorStore(t, database)
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, release, err := awaitTarget(
			stopCtx, t.Context(), database, fixture.jobID,
			platform.ProcessIdentity{}, make(chan error), func() error { return nil },
			time.Now().UTC(), 0,
		)
		release()
		if err == nil || !strings.Contains(err.Error(), "record cancellation") {
			t.Fatalf("awaitTarget(closed cancellation store) error = %v", err)
		}
	})

	t.Run("identity mismatch", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		current, err := platform.Inspect(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		current.Creation = "wrong-creation"
		stopCtx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, release, err := awaitTarget(
			stopCtx, t.Context(), database, job.ID, current,
			make(chan error), func() error { return nil }, time.Now().UTC(), 0,
		)
		release()
		if err == nil || !strings.Contains(err.Error(), "forward graceful") {
			t.Fatalf("awaitTarget(identity mismatch) error = %v", err)
		}
	})
}

func TestWaitAndFinalizeRunClosedStore(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	jobID := model.JobID("019c5f8b-7c8a-7000-8000-000000000093")
	runID := model.RunID("019c5f8b-7c8a-7000-8000-000000000094")
	capture, err := logstore.CreateRun(stateDir, jobID.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capture.Close() })
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	command := supervisorCoverageCommand(t, "success")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	target := &preparedTarget{command: command, stdout: stdout, stderr: stderr}
	group := new(sync.WaitGroup)
	errorsChannel := make(chan error, 2)
	group.Add(2)
	go drainPipe(group, stdout, capture, logstore.Stdout, true, errorsChannel)
	go drainPipe(group, stderr, capture, logstore.Stderr, true, errorsChannel)
	database := openSupervisorStore(t, stateDir)
	closeSupervisorStore(t, database)
	jitter, err := newJitterSource()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := waitAndFinalizeRun(
		t.Context(), t.Context(), database, capture, jobID, runID, logs, target,
		platform.ProcessIdentity{}, group, errorsChannel, time.Now().UTC(), 0, jitter,
	); err == nil {
		t.Fatal("waitAndFinalizeRun(closed store) error = nil")
	}
	if _, err := capture.Append(logstore.Stdout, []byte("after close"), time.Now().UTC()); !errors.Is(err, logstore.ErrClosed) {
		t.Fatalf("capture append after waitAndFinalizeRun error = %v, want closed", err)
	}
}

func TestWaitAndFinalizeRunMissingPersistedRun(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	runID := model.RunID("019c5f8b-7c8a-7000-8000-000000000095")
	capture, err := logstore.CreateRun(fixture.stateDir, job.ID.String(), 1)
	if err != nil {
		t.Fatal(err)
	}
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath: paths.Stdout, StderrPath: paths.Stderr, IndexPath: paths.Index,
		IndexVersion: capture.IndexVersion(), Integrity: model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	command := supervisorCoverageCommand(t, "exit-7")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	target := &preparedTarget{command: command, stdout: stdout, stderr: stderr}
	group := new(sync.WaitGroup)
	errorsChannel := make(chan error, 2)
	group.Add(2)
	go drainPipe(group, stdout, capture, logstore.Stdout, true, errorsChannel)
	go drainPipe(group, stderr, capture, logstore.Stderr, true, errorsChannel)
	jitter, err := newJitterSource()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := waitAndFinalizeRun(
		t.Context(), t.Context(), database, capture, job.ID, runID, logs, target,
		platform.ProcessIdentity{}, group, errorsChannel, time.Now().UTC(), 0, jitter,
	); err == nil {
		t.Fatal("waitAndFinalizeRun(missing run) error = nil")
	}
}

func TestProcessClaimedNotificationClosedStore(t *testing.T) {
	t.Parallel()

	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	event, err := database.TransitionEvent(t.Context(), model.EntityJob, job.ID.String(), job.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.QueueNotificationDeliveries(t.Context(), []store.QueueNotificationDeliveryInput{{
		JobID: job.ID, EventID: event.ID, NotifierName: "closed",
		EventType: string(notify.EventJobStarted), MaxAttempts: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	delivery, err := database.ClaimNotificationDelivery(
		t.Context(), event.ID, time.Now().UTC(), time.Now().UTC().Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	closeSupervisorStore(t, database)
	if err := processClaimedNotification(t.Context(), database, delivery); err == nil {
		t.Fatal("processClaimedNotification(closed store) error = nil")
	}
}

func TestExecuteClaimedJobClosedStoreEdges(t *testing.T) {
	t.Parallel()

	for _, canceled := range []bool{false, true} {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		job, err := database.GetJob(t.Context(), fixture.jobID)
		if err != nil {
			t.Fatal(err)
		}
		closeSupervisorStore(t, database)
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		stopCtx := t.Context()
		if canceled {
			var cancel context.CancelFunc
			stopCtx, cancel = context.WithCancel(t.Context())
			cancel()
		}
		if err := executeClaimedJob(
			stopCtx, t.Context(), database, ids, fixture.stateDir, job,
		); err == nil {
			t.Errorf("executeClaimedJob(closed store, canceled=%t) error = nil", canceled)
		}
	}
}

func TestExecuteOneRunLiveInputAndReserveFailure(t *testing.T) {
	t.Parallel()

	t.Run("in-memory live input", func(t *testing.T) {
		fixture := submitSupervisorFixtureWithPolicyOptionsAndStdin(
			t, true, false, model.DefaultExecutionPolicy(), "", model.StdinLive,
		)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		if _, err := database.TryAcquireAdmission(
			t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
		); err != nil {
			t.Fatal(err)
		}
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		jitter, err := newJitterSource()
		if err != nil {
			t.Fatal(err)
		}
		terminal, err := executeOneRun(
			t.Context(), t.Context(), database, ids, fixture.stateDir, job,
			new(liveinput.Broker), jitter,
		)
		if err != nil || !terminal {
			t.Fatalf("executeOneRun(live input) = (%t, %v)", terminal, err)
		}
	})

	t.Run("completed job reserve failure", func(t *testing.T) {
		fixture := submitSupervisorFixture(t, true)
		database := openSupervisorStore(t, fixture.stateDir)
		defer closeSupervisorStore(t, database)
		job := claimCoverageFixture(t, database, fixture, time.Minute)
		completed, err := database.CompleteWithoutRun(
			t.Context(), job.ID, model.JobOutcomeAborted, "coverage", time.Now().UTC(),
		)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		jitter, err := newJitterSource()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executeOneRun(
			t.Context(), t.Context(), database, ids, fixture.stateDir, completed.Job, nil, jitter,
		); err == nil {
			t.Fatal("executeOneRun(completed job) error = nil")
		}
	})
}

func TestRunWithLogRotation(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.LogRotateSize = 8
	configuration.LogMaxSegmentsPerStream = 4
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	if err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(),
		bytes.NewReader(fixture.credential), new(closingBuffer),
	); err != nil {
		t.Fatalf("Run(log rotation) error = %v", err)
	}
}

func TestBackoffWithoutStoredDeadline(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	two, err := policy.FiniteLimit(2)
	if err != nil {
		t.Fatal(err)
	}
	configuration.Completion.MaxRuns = two
	configuration.Completion.FailureLimit = two
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)
	if _, err := database.TryAcquireAdmission(
		t.Context(), job.ID, "", 1, time.Now().UTC(), time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	capture, logs, runID := reserveCoverageRun(t, database, fixture.stateDir, job.ID)
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	logs = completedLogMetadata(logs, nil, nil)
	completed, err := database.CompleteRunWithDisposition(
		t.Context(), job.ID, runID, model.RunOutcomeStartFailed, nil, logs, "coverage",
		time.Now().UTC(), model.RunDisposition{NextPhase: model.JobPhaseBackoff, Reason: "retry"},
	)
	if err != nil {
		t.Fatal(err)
	}
	moved, _, done, err := awaitBackoffEligibility(
		t.Context(), t.Context(), database, completed.Job, fixedJitterSource(0),
	)
	if err != nil || done || moved.Phase != model.JobPhaseQueued {
		t.Fatalf("awaitBackoffEligibility(no deadline) = (%+v, %t, %v)", moved, done, err)
	}
}

func TestNotificationRetryIsProcessedToAttemptLimit(t *testing.T) {
	t.Parallel()

	configuration := model.DefaultExecutionPolicy()
	configuration.NotifierDefinitions = []model.NotifierDefinition{{
		Name: "retry", Kind: model.NotifierCommand, Timeout: time.Second,
		Retry: model.NotifierRetryPolicy{MaxAttempts: 2, Delay: time.Millisecond, MaxDelay: time.Millisecond},
		Command: &model.CommandNotifierDefinition{
			Executable:  supervisorTestExecutable(t),
			Arguments:   []string{"-test.run=^TestSupervisorCoverageHelper$"},
			Environment: map[string]string{supervisorCoverageHelperEnvironment: "exit-9"},
			OutputLimit: 64,
		},
	}}
	configuration.Notifications = []model.NotificationSubscription{{
		Notifier: "retry", Events: []string{string(notify.EventJobStarted)},
	}}
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	if err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(),
		bytes.NewReader(fixture.credential), new(closingBuffer),
	); err != nil {
		t.Fatalf("Run(retrying notifier) error = %v", err)
	}
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	deliveries, err := database.ListNotificationDeliveries(t.Context(), fixture.jobID)
	if err != nil || len(deliveries) != 1 || deliveries[0].AttemptCount != 2 ||
		deliveries[0].Status != store.NotificationDeliveryFailed {
		t.Fatalf("retried notification = (%+v, %v)", deliveries, err)
	}
}

func TestRunRejectsUnsafeStateRoot(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	err := Run(
		t.Context(), stateDir, "019c5f8b-7c8a-7000-8000-000000000096",
		bytes.NewReader(bytes.Repeat([]byte{0x71}, credentialSize)), new(closingBuffer),
	)
	if err == nil {
		t.Fatal("Run(unsafe state root) error = nil")
	}
}

func submitCoverageSpecification(t *testing.T, specification model.JobSpec) supervisorFixture {
	t.Helper()

	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := ids.NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	credential := bytes.Repeat([]byte{0x68}, credentialSize)
	hash, err := model.NewCredentialHash(credential)
	if err != nil {
		t.Fatal(err)
	}
	database := openSupervisorStore(t, stateDir)
	now := time.Now().UTC()
	if _, err := database.Submit(
		t.Context(), jobID, specification, hash, now, now.Add(time.Minute),
	); err != nil {
		closeSupervisorStore(t, database)
		t.Fatal(err)
	}
	closeSupervisorStore(t, database)

	return supervisorFixture{stateDir: stateDir, jobID: jobID, credential: credential}
}

func TestLaunchAcknowledgementAndFailurePaths(t *testing.T) {
	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	supervisorID := "019c5f8b-7c8a-7000-8000-000000000099"

	t.Setenv(supervisorLaunchHelperEnvironment, "success")
	ack, err := Launch(t.Context(), LaunchOptions{
		Store: database, Executable: supervisorTestExecutable(t), StateDir: fixture.stateDir,
		JobID: fixture.jobID, Credential: fixture.credential, Timeout: 10 * time.Second,
	})
	if err != nil || ack.JobID != fixture.jobID || ack.SupervisorID.String() != supervisorID {
		t.Fatalf("Launch(success) = (%+v, %v)", ack, err)
	}

	t.Setenv(supervisorLaunchHelperEnvironment, "mismatch")
	if _, err := Launch(t.Context(), LaunchOptions{
		Store: database, Executable: supervisorTestExecutable(t), StateDir: fixture.stateDir,
		JobID: fixture.jobID, Credential: fixture.credential, Timeout: 10 * time.Second,
	}); err == nil {
		t.Fatal("Launch(identity mismatch) error = nil")
	}

	t.Setenv(supervisorLaunchHelperEnvironment, "malformed")
	if _, err := Launch(t.Context(), LaunchOptions{
		Store: database, Executable: supervisorTestExecutable(t), StateDir: fixture.stateDir,
		JobID: fixture.jobID, Credential: fixture.credential, Timeout: 10 * time.Second,
	}); err == nil {
		t.Fatal("Launch(malformed acknowledgement) error = nil")
	}

	t.Setenv(supervisorLaunchHelperEnvironment, "slow")
	if _, err := Launch(t.Context(), LaunchOptions{
		Store: database, Executable: supervisorTestExecutable(t), StateDir: fixture.stateDir,
		JobID: fixture.jobID, Credential: fixture.credential, Timeout: time.Millisecond,
	}); err == nil {
		t.Fatal("Launch(timeout) error = nil")
	}
}

func TestRunLogCaptureModesAndSuccessfulRepetition(t *testing.T) {
	for _, capture := range []string{"none", "stdout", "stderr"} {
		t.Run("capture_"+capture, func(t *testing.T) {
			configuration := model.DefaultExecutionPolicy()
			configuration.LogCapture = capture
			fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
			if err := Run(
				t.Context(), fixture.stateDir, fixture.jobID.String(),
				bytes.NewReader(fixture.credential), new(closingBuffer),
			); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			database := openSupervisorStore(t, fixture.stateDir)
			defer closeSupervisorStore(t, database)
			runs, err := database.ListRuns(t.Context(), fixture.jobID)
			if err != nil || len(runs) != 1 || runs[0].Outcome != model.RunOutcomeSuccess {
				t.Fatalf("runs = (%+v, %v)", runs, err)
			}
		})
	}

	configuration := model.DefaultExecutionPolicy()
	two, err := policy.FiniteLimit(2)
	if err != nil {
		t.Fatal(err)
	}
	configuration.Completion.MaxRuns = two
	configuration.Completion.SuccessTarget = two
	configuration.SuccessDelay = policy.DelayPolicy{Base: time.Millisecond, Backoff: policy.BackoffConstant, ExponentialBase: 2}
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	if err := Run(
		t.Context(), fixture.stateDir, fixture.jobID.String(),
		bytes.NewReader(fixture.credential), new(closingBuffer),
	); err != nil {
		t.Fatalf("Run(repeated success) error = %v", err)
	}
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	runs, err := database.ListRuns(t.Context(), fixture.jobID)
	if err != nil || len(runs) != 2 {
		t.Fatalf("repeated success runs = (%+v, %v)", runs, err)
	}
}
