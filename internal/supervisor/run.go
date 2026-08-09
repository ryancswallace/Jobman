package supervisor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/executor"
	"github.com/ryancswallace/jobman/internal/faultinject"
	"github.com/ryancswallace/jobman/internal/liveinput"
	"github.com/ryancswallace/jobman/internal/logstore"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/policy"
	"github.com/ryancswallace/jobman/internal/resourceusage"
	"github.com/ryancswallace/jobman/internal/store"
)

const (
	leaseDuration = 15 * time.Second
	leaseInterval = 5 * time.Second
)

// Run claims and owns one job until its persisted completion policy is terminal.
func Run(
	ctx context.Context,
	stateDir string,
	jobIDText string,
	credentialReader io.Reader,
	acknowledgementWriter io.Writer,
) error {
	jobID, err := model.ParseJobID(jobIDText)
	if err != nil {
		return err
	}
	credential := make([]byte, credentialSize)
	if _, readErr := io.ReadFull(credentialReader, credential); readErr != nil {
		return fmt.Errorf("read supervisor credential: %w", readErr)
	}

	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		return fmt.Errorf("construct supervisor ID source: %w", err)
	}
	database, err := store.Open(ctx, store.Options{
		StateDir:      stateDir,
		JobmanVersion: buildinfo.Version,
		Now:           time.Now,
		EventIDs:      ids,
	})
	if err != nil {
		return err
	}
	defer database.Close()

	supervisorID, err := ids.NewSupervisorID()
	if err != nil {
		return err
	}
	identity, err := platform.Inspect(os.Getpid())
	if err != nil {
		return fmt.Errorf("inspect supervisor identity: %w", err)
	}
	now := time.Now().UTC()
	claim, err := database.Claim(
		ctx,
		jobID,
		credential,
		supervisorID,
		modelIdentity(identity),
		now,
		now.Add(leaseDuration),
	)
	if err != nil {
		return fmt.Errorf("claim job: %w", err)
	}
	faultinject.Hit("supervisor-claimed-before-ack")

	acknowledgement := Acknowledgement{
		SchemaVersion: 1,
		JobID:         jobID,
		SupervisorID:  supervisorID,
	}
	if err := json.NewEncoder(acknowledgementWriter).Encode(acknowledgement); err != nil {
		return fmt.Errorf("acknowledge supervisor claim: %w", err)
	}
	if closer, ok := acknowledgementWriter.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("close supervisor acknowledgement: %w", err)
		}
	}
	faultinject.Hit("supervisor-acknowledged")

	ownershipCtx := context.WithoutCancel(ctx)
	leaseCtx, stopLease := context.WithCancel(ownershipCtx)
	defer stopLease()
	go renewLease(leaseCtx, database, supervisorID, jobID)

	executionErr := executeClaimedJob(ctx, ownershipCtx, database, ids, stateDir, claim.Job)
	// Recover prior due work only after this supervisor has safely claimed and
	// finished its own job. Recovery must never consume the bounded claim window
	// or delay the managed target's start.
	ignoreNotificationError(RecoverNotifications(ownershipCtx, database))

	return executionErr
}

//nolint:gocognit // One supervisor owns setup, private input, scheduling, admission release, and terminal delivery.
func executeClaimedJob(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	ids *model.UUIDv7Generator,
	stateDir string,
	job model.JobState,
) error {
	notifyJobStarted(operationCtx, database, job, time.Now().UTC())
	jitter, err := newJitterSource()
	if err != nil {
		return err
	}
	var inputBroker *liveinput.Broker
	if job.Spec.StdinPolicy() == model.StdinLive {
		endpoint := liveinput.NewEndpoint(stateDir, job.ID.String())
		inputBroker, err = liveinput.Listen(endpoint)
		if err != nil {
			completed, completeErr := database.CompleteWithoutRun(
				operationCtx,
				job.ID,
				model.JobOutcomeAborted,
				"live_input_unavailable",
				time.Now().UTC(),
			)
			if completeErr == nil {
				notifyTerminalJob(operationCtx, database, completed.Job, "", time.Now().UTC())
			}

			return errors.Join(err, completeErr)
		}
		if err := database.SetInputEndpoint(operationCtx, job.ID, endpoint, time.Now().UTC()); err != nil {
			return errors.Join(err, inputBroker.Close())
		}
		brokerCtx, stopBroker := context.WithCancel(operationCtx)
		defer stopBroker()
		go func() {
			_ = inputBroker.Serve(brokerCtx) //nolint:errcheck // Client requests surface endpoint failure; shutdown is best effort here.
		}()
		defer func() {
			// Endpoint cleanup is idempotent and cannot change the already durable
			// target outcome, so teardown failures remain diagnostic-only.
			_ = database.SetInputEndpoint(operationCtx, job.ID, "", time.Now().UTC()) //nolint:errcheck // Completion is already durable.
			_ = inputBroker.Close()
		}()
	}

	for {
		if err := recordContextCancellation(stopCtx, operationCtx, database, job.ID); err != nil {
			return err
		}
		runnable, waitErr := awaitRunnable(stopCtx, operationCtx, database, job.ID, jitter)
		if waitErr != nil {
			return waitErr
		}
		if runnable.terminal {
			notifyTerminalJob(operationCtx, database, runnable.job, "", time.Now().UTC())
			return nil
		}
		terminal, runErr := executeOneRun(
			stopCtx,
			operationCtx,
			database,
			ids,
			stateDir,
			runnable.job,
			inputBroker,
			jitter,
		)
		releaseErr := database.ReleaseAdmission(operationCtx, job.ID, time.Now().UTC())
		if runErr != nil || releaseErr != nil {
			return errors.Join(runErr, releaseErr)
		}
		if terminal {
			return nil
		}
	}
}

//nolint:cyclop // Run execution intentionally owns ordered reservation, process, log, and terminal-state cleanup.
func executeOneRun(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	ids *model.UUIDv7Generator,
	stateDir string,
	job model.JobState,
	inputBroker *liveinput.Broker,
	jitter *jitterSource,
) (bool, error) {
	runtimeState, err := database.GetRuntime(operationCtx, job.ID)
	if err != nil {
		return false, err
	}
	runNumber := runtimeState.RunCount + 1
	runID, err := ids.NewRunID()
	if err != nil {
		return false, err
	}
	executionPolicy := job.Spec.ExecutionPolicy()
	logOptions := logstore.RunOptions{}
	if executionPolicy.LogRotateSize > 0 {
		logOptions.Rotation.SegmentBytes = uint64(executionPolicy.LogRotateSize)
		logOptions.Rotation.MaxSegmentsPerStream = uint16(executionPolicy.LogMaxSegmentsPerStream) //nolint:gosec // Model validation bounds this to uint16.
	}
	capture, err := logstore.CreateRunWithOptions(stateDir, job.ID.String(), runNumber, logOptions)
	if err != nil {
		return false, fmt.Errorf("create run logs: %w", err)
	}
	paths := capture.Paths()
	logs := model.LogMetadata{
		StdoutPath:      paths.Stdout,
		StderrPath:      paths.Stderr,
		IndexPath:       paths.Index,
		IndexVersion:    capture.IndexVersion(),
		Integrity:       model.LogIntegrityPending,
		RecordingHealth: model.RecordingHealthy,
	}
	reservedAt := time.Now().UTC()
	if _, reserveErr := database.ReserveRun(
		operationCtx,
		job.ID,
		runID,
		runNumber,
		logs,
		reservedAt,
	); reserveErr != nil {
		return false, errors.Join(fmt.Errorf("reserve run: %w", reserveErr), capture.Close())
	}
	if bindErr := database.BindAdmissionToRun(operationCtx, job.ID, runID); bindErr != nil {
		return false, errors.Join(fmt.Errorf("bind run admission: %w", bindErr), capture.Close())
	}
	if stopErr := recordContextCancellation(stopCtx, operationCtx, database, job.ID); stopErr != nil {
		return false, errors.Join(stopErr, capture.Close())
	}
	if finalized, cancellationErr := finalizeReservedCancellation(
		operationCtx,
		database,
		capture,
		job.ID,
		runID,
		logs,
		jitter,
	); cancellationErr != nil || finalized {
		return finalized, cancellationErr
	}

	if job.Spec.StdinPolicy() == model.StdinLive {
		if resetErr := database.ResetInputEOF(operationCtx, job.ID, time.Now().UTC()); resetErr != nil {
			return false, errors.Join(fmt.Errorf("reset live-input EOF: %w", resetErr), capture.Close())
		}
	}
	target, err := prepareTarget(operationCtx, job, runID, inputBroker)
	if err != nil {
		return finalizeStartFailure(operationCtx, database, capture, job.ID, runID, logs, err, jitter)
	}
	defer target.closeInput() //nolint:errcheck // All result paths close or detach the same pipe; late duplicate-close errors are non-actionable.
	faultinject.Hit("target-before-start")
	if startErr := target.command.Start(); startErr != nil {
		return finalizeStartFailure(
			operationCtx,
			database,
			capture,
			job.ID,
			runID,
			logs,
			errors.Join(startErr, target.closeOutputPipes()),
			jitter,
		)
	}
	faultinject.Hit("target-started-before-identity")
	treeID, err := platform.FinalizeTargetStart(target.command.Process.Pid)
	if err != nil {
		killErr := target.command.Process.Kill()
		waitErr := target.command.Wait()

		return finalizeStartFailure(
			operationCtx,
			database,
			capture,
			job.ID,
			runID,
			logs,
			errors.Join(err, killErr, waitErr),
			jitter,
		)
	}

	targetIdentity, err := platform.Inspect(target.command.Process.Pid)
	if err != nil {
		killErr := target.command.Process.Kill()
		waitErr := target.command.Wait()

		return finalizeStartFailure(
			operationCtx,
			database,
			capture,
			job.ID,
			runID,
			logs,
			errors.Join(err, killErr, waitErr),
			jitter,
		)
	}
	targetIdentity.Tree = treeID

	return superviseStartedTarget(
		stopCtx,
		operationCtx,
		database,
		capture,
		job.ID,
		runID,
		logs,
		target,
		targetIdentity,
		reservedAt,
		runtimeState.TotalPaused,
		executionPolicy.LogCapture,
		jitter,
	)
}

func superviseStartedTarget(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	capture *logstore.Run,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	target *preparedTarget,
	targetIdentity platform.ProcessIdentity,
	runBudgetStarted time.Time,
	pausedBaseline time.Duration,
	logCapture string,
	jitter *jitterSource,
) (bool, error) {
	var captureGroup sync.WaitGroup
	captureErrors := make(chan error, 2)
	captureGroup.Add(2)
	go drainPipe(&captureGroup, target.stdout, capture, logstore.Stdout,
		logCapture == "both" || logCapture == "stdout", captureErrors)
	go drainPipe(&captureGroup, target.stderr, capture, logstore.Stderr,
		logCapture == "both" || logCapture == "stderr", captureErrors)

	startedAt := time.Now().UTC()
	started, err := database.MarkProcessStarted(
		operationCtx,
		jobID,
		runID,
		target.resolved,
		modelIdentity(targetIdentity),
		startedAt,
	)
	if err != nil {
		return true, handlePublishFailure(
			operationCtx,
			database,
			capture,
			jobID,
			runID,
			logs,
			target,
			targetIdentity,
			&captureGroup,
			captureErrors,
			err,
		)
	}
	faultinject.Hit("target-identity-committed")
	notifyRunStarted(operationCtx, database, started, startedAt)

	return waitAndFinalizeRun(
		stopCtx,
		operationCtx,
		database,
		capture,
		jobID,
		runID,
		logs,
		target,
		targetIdentity,
		&captureGroup,
		captureErrors,
		runBudgetStarted,
		pausedBaseline,
		jitter,
	)
}

func handlePublishFailure(
	ctx context.Context,
	database *store.Store,
	capture *logstore.Run,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	target *preparedTarget,
	targetIdentity platform.ProcessIdentity,
	captureGroup *sync.WaitGroup,
	captureErrors chan error,
	publishErr error,
) error {
	terminateErr := platform.Terminate(targetIdentity, true)
	captureGroup.Wait()
	waitErr := target.command.Wait()
	close(captureErrors)
	captureErr := collectCaptureErrors(captureErrors)
	closeErr := capture.Close()
	logs = completedLogMetadata(logs, captureErr, closeErr)
	latest, getErr := database.GetJob(ctx, jobID)
	if getErr == nil && latest.Cancellation != nil {
		exit := exitInfoForOutcome(
			model.RunOutcomeCancelled,
			processExitInfo(target.command, waitErr, time.Now().UTC()),
		)
		_, finalizeErr := database.FinalizeRun(
			ctx,
			jobID,
			runID,
			model.RunOutcomeCancelled,
			exit,
			logs,
			time.Now().UTC(),
		)

		return errors.Join(terminateErr, finalizeErr, captureErr, closeErr)
	}
	_, lostErr := database.MarkOwnershipLost(
		ctx,
		jobID,
		&logs,
		"process_start_publish_failed",
		time.Now().UTC(),
	)

	return errors.Join(
		fmt.Errorf("publish process start: %w", publishErr),
		terminateErr,
		getErr,
		captureErr,
		closeErr,
		lostErr,
	)
}

//nolint:cyclop,nestif // Outcome precedence must distinguish job timeout, cancellation, run timeout, and normal exit.
func waitAndFinalizeRun(
	stopCtx context.Context,
	baseOperationCtx context.Context,
	database *store.Store,
	capture *logstore.Run,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	target *preparedTarget,
	targetIdentity platform.ProcessIdentity,
	captureGroup *sync.WaitGroup,
	captureErrors chan error,
	runBudgetStarted time.Time,
	pausedBaseline time.Duration,
	jitter *jitterSource,
) (bool, error) {
	// StdoutPipe and StderrPipe require every read to finish before Wait. Calling
	// Wait first lets os/exec close a pipe underneath a drain goroutine and can
	// both lose tail bytes and falsely report degraded capture.
	completion := make(chan error, 1)
	go func() {
		captureGroup.Wait()
		completion <- target.command.Wait()
	}()
	waitErr, operationCtx, releaseOperation, controlErr := awaitTarget(
		stopCtx,
		baseOperationCtx,
		database,
		jobID,
		targetIdentity,
		completion,
		target.closeInput,
		runBudgetStarted,
		pausedBaseline,
	)
	defer releaseOperation()
	close(captureErrors)
	captureErr := collectCaptureErrors(captureErrors)
	closeErr := capture.Close()
	logs = completedLogMetadata(logs, captureErr, closeErr)
	resources := resourceusage.Observe(target.command.ProcessState)
	if waitErr == nil && controlErr != nil {
		return false, errors.Join(controlErr, captureErr, closeErr)
	}

	latest, getErr := database.GetJob(operationCtx, jobID)
	if getErr != nil {
		return false, errors.Join(waitErr, captureErr, closeErr, getErr)
	}
	outcome := model.RunOutcomeFailure
	diagnostic := ""
	if latest.Cancellation != nil {
		if latest.Cancellation.Reason == model.StopReasonTimeout {
			outcome = model.RunOutcomeTimedOut
			diagnostic = "job_timeout"
		} else {
			outcome = model.RunOutcomeCancelled
		}
	} else if currentRun, runErr := database.GetRun(operationCtx, runID); runErr != nil {
		return false, errors.Join(waitErr, captureErr, closeErr, runErr)
	} else if currentRun.StopReason == model.StopReasonTimeout {
		outcome = model.RunOutcomeTimedOut
	} else if waitErr == nil {
		outcome = model.RunOutcomeSuccess
	}
	completedAt := time.Now().UTC()
	exit := exitInfoForOutcome(outcome, processExitInfo(target.command, waitErr, completedAt))
	runtimeState, err := database.GetRuntime(operationCtx, jobID)
	if err != nil {
		return false, errors.Join(err, captureErr, closeErr)
	}
	disposition, classification, err := dispositionForRun(
		latest,
		runtimeState,
		outcome,
		exit,
		completedAt,
		jitter,
	)
	if err != nil {
		return false, errors.Join(err, captureErr, closeErr)
	}
	// Run outcomes carry the configured policy meaning, while ExitInfo retains
	// the factual operating-system result. This matters when a nonzero code is
	// configured as successful or code zero is deliberately excluded.
	if exit != nil && exit.ExitCode != nil && latest.Cancellation == nil {
		if classification == policy.RunClassificationSuccess {
			outcome = model.RunOutcomeSuccess
		} else {
			outcome = model.RunOutcomeFailure
		}
	}
	faultinject.Hit("run-completion-before-commit")
	completed, err := database.CompleteRunWithDispositionAndFacts(
		operationCtx,
		jobID,
		runID,
		outcome,
		exit,
		logs,
		diagnostic,
		completedAt,
		disposition,
		store.RunDiagnosticFactsInput{Resources: resources, PolicyDisposition: classification},
	)
	if err != nil {
		return false, errors.Join(fmt.Errorf("finalize run: %w", err), captureErr, closeErr)
	}
	faultinject.Hit("run-completion-committed")
	if disposition.TerminalOutcome != "" {
		faultinject.Hit("job-completion-committed")
	}
	notifyCompletedRun(operationCtx, database, completed, outcome, completedAt)

	return disposition.TerminalOutcome != "", controlErr
}

// exitInfoForOutcome keeps controlled termination distinct from an ordinary
// command failure. Windows reports the job-object termination status as exit
// code 1, but the durable outcome contract permits exit codes only for normal
// success or failure. Preserve the factual observation as a platform reason.
func exitInfoForOutcome(outcome model.RunOutcome, exit *model.ExitInfo) *model.ExitInfo {
	if exit == nil || exit.ExitCode == nil ||
		(outcome != model.RunOutcomeTimedOut && outcome != model.RunOutcomeCancelled) {
		return exit
	}
	exit.ExitCode = nil
	if exit.Signal == "" && exit.PlatformReason == "" {
		exit.PlatformReason = "process_terminated"
	}

	return exit
}

//nolint:gocognit,cyclop // Target control coordinates completion, durable intents, two timeout budgets, and signal escalation.
func awaitTarget(
	stopCtx context.Context,
	baseOperationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
	identity platform.ProcessIdentity,
	completion <-chan error,
	closeInput func() error,
	runBudgetStarted time.Time,
	pausedBaseline time.Duration,
) (waitErr error, operationCtx context.Context, release context.CancelFunc, controlErr error) {
	ticker := time.NewTicker(schedulerPollInterval)
	defer ticker.Stop()
	var result model.TransitionResult
	inputEOFClosed := false
	for {
		select {
		case waitErr = <-completion:
			return waitErr, baseOperationCtx, func() {}, nil
		case <-stopCtx.Done():
			intentCtx, cancelIntent := context.WithTimeout(baseOperationCtx, 2*time.Second)
			var err error
			result, err = database.RequestCancellation(intentCtx, jobID, time.Now().UTC())
			cancelIntent()
			if err != nil {
				fallbackCtx, cancel := context.WithTimeout(baseOperationCtx, 2*time.Second)

				return nil, fallbackCtx, cancel, fmt.Errorf("record cancellation after supervisor signal: %w", err)
			}
		case now := <-ticker.C:
			current, err := database.GetJob(baseOperationCtx, jobID)
			if err != nil {
				return nil, baseOperationCtx, func() {}, err
			}
			if current.Cancellation != nil {
				result.Job = current
				break
			}
			runtimeState, err := database.GetRuntime(baseOperationCtx, jobID)
			if err != nil {
				return nil, baseOperationCtx, func() {}, err
			}
			if runtimeState.InputEOFRequested && !inputEOFClosed {
				// EOF is durable intent. The direct client transport is an eager
				// delivery path; this observation closes the pipe if that client
				// exits after committing intent but before reaching the broker.
				_ = closeInput() //nolint:errcheck // Durable EOF intent must remain authoritative even when the target already closed its pipe.
				inputEOFClosed = true
			}
			if current.Phase == model.JobPhasePaused {
				continue
			}
			pausedDuringRun := runtimeState.TotalPaused - pausedBaseline
			if pausedDuringRun < 0 {
				pausedDuringRun = 0
			}
			runExpired := current.Spec.ExecutionPolicy().RunTimeout > 0 &&
				now.UTC().Sub(runBudgetStarted)-pausedDuringRun >= current.Spec.ExecutionPolicy().RunTimeout
			jobExpired, err := jobTimeoutExpired(baseOperationCtx, database, current, now.UTC())
			if err != nil {
				return nil, baseOperationCtx, func() {}, err
			}
			if !runExpired && !jobExpired {
				continue
			}
			if jobExpired {
				result, err = database.RequestTimeout(baseOperationCtx, jobID, now.UTC())
			} else {
				result, err = database.RequestRunTimeout(baseOperationCtx, jobID, now.UTC())
			}
			if err != nil {
				return nil, baseOperationCtx, func() {}, fmt.Errorf("record target timeout: %w", err)
			}
		}
		break
	}

	grace := result.Job.Spec.StopPolicy().GracePeriod
	operationCtx, release = context.WithTimeout(baseOperationCtx, grace+10*time.Second)
	if err := platform.Terminate(identity, false); err != nil {
		return nil, operationCtx, release, fmt.Errorf("forward graceful supervisor signal: %w", err)
	}

	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case waitErr = <-completion:
		return waitErr, operationCtx, release, nil
	case <-timer.C:
	case <-operationCtx.Done():
		return nil, operationCtx, release, fmt.Errorf("wait after supervisor signal: %w", operationCtx.Err())
	}

	if !result.Job.Spec.StopPolicy().ForceAfterGrace {
		return nil, operationCtx, release, errors.New("target did not exit after forwarded graceful supervisor signal")
	}
	if err := platform.Terminate(identity, true); err != nil {
		return nil, operationCtx, release, fmt.Errorf("forward forced supervisor signal: %w", err)
	}

	select {
	case waitErr = <-completion:
		return waitErr, operationCtx, release, nil
	case <-operationCtx.Done():
		return nil, operationCtx, release, fmt.Errorf("wait after forced supervisor signal: %w", operationCtx.Err())
	}
}

func recordContextCancellation(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
) error {
	if stopCtx.Err() == nil {
		return nil
	}
	if _, err := database.RequestCancellation(operationCtx, jobID, time.Now().UTC()); err != nil {
		return fmt.Errorf("record cancellation after supervisor signal: %w", err)
	}

	return nil
}

type preparedTarget struct {
	command  *exec.Cmd
	resolved string
	stdin    io.Closer
	stdout   io.ReadCloser
	stderr   io.ReadCloser
	broker   *liveinput.Broker
}

func prepareTarget(
	ctx context.Context,
	job model.JobState,
	runID model.RunID,
	broker *liveinput.Broker,
) (*preparedTarget, error) {
	environment := job.Spec.Environment()
	for name, reference := range job.Spec.ExecutionPolicy().SecretEnv {
		parsed, err := config.ParseSecretRef(reference.Provider + ":" + reference.Name)
		if err != nil {
			return nil, fmt.Errorf("parse secret environment reference for %q: %w", name, err)
		}
		value, err := (config.LocalSecretResolver{}).ResolveSecret(ctx, parsed)
		if err != nil {
			return nil, fmt.Errorf("resolve secret environment variable %q with provider %q: %w", name, reference.Provider, err)
		}
		environment[name] = value
	}
	command, resolved, err := executor.Command(executor.Request{
		Executable: job.Spec.Executable(),
		Arguments:  job.Spec.Arguments(),
		Directory:  job.Spec.WorkingDirectory(),
		BaseEnv:    os.Environ(),
		AddEnv:     environment,
		RemoveEnv:  job.Spec.UnsetEnvironment(),
	})
	if err != nil {
		return nil, err
	}
	platform.ConfigureTarget(command)

	return configurePreparedTarget(command, resolved, job, runID, broker)
}

//nolint:cyclop,gocognit // Stdin ownership and pipe rollback differ for each supported policy.
func configurePreparedTarget(
	command *exec.Cmd,
	resolved string,
	job model.JobState,
	runID model.RunID,
	broker *liveinput.Broker,
) (*preparedTarget, error) {
	var input io.Closer
	switch job.Spec.StdinPolicy() {
	case model.StdinNull:
		file, openErr := os.Open(os.DevNull)
		if openErr != nil {
			return nil, openErr
		}
		input = file
		command.Stdin = file
	case model.StdinFile:
		file, openErr := os.Open(job.Spec.ExecutionPolicy().StdinPath)
		if openErr != nil {
			return nil, fmt.Errorf("open target stdin file: %w", openErr)
		}
		input = file
		command.Stdin = file
	case model.StdinLive:
		if broker == nil {
			return nil, errors.New("prepare live input: broker is unavailable")
		}
		if beginErr := broker.BeginRun(runID.String()); beginErr != nil {
			return nil, fmt.Errorf("begin live-input run: %w", beginErr)
		}
		pipe, pipeErr := command.StdinPipe()
		if pipeErr != nil {
			return nil, fmt.Errorf("create target stdin pipe: %w", pipeErr)
		}
		if attachErr := broker.Attach(pipe); attachErr != nil {
			return nil, errors.Join(attachErr, pipe.Close())
		}
		input = pipe
	case model.StdinInherit:
		return nil, errors.New("inherited stdin is unavailable to a detached supervisor")
	default:
		return nil, fmt.Errorf("unsupported stdin policy %q", job.Spec.StdinPolicy())
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		if broker != nil && job.Spec.StdinPolicy() == model.StdinLive {
			return nil, errors.Join(err, broker.Detach())
		}
		return nil, errors.Join(err, input.Close())
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		if broker != nil && job.Spec.StdinPolicy() == model.StdinLive {
			return nil, errors.Join(err, stdout.Close(), broker.Detach())
		}
		return nil, errors.Join(err, stdout.Close(), input.Close())
	}

	return &preparedTarget{
		command:  command,
		resolved: resolved,
		stdin:    input,
		stdout:   stdout,
		stderr:   stderr,
		broker:   broker,
	}, nil
}

func (target *preparedTarget) closeInput() error {
	if target.broker != nil {
		return target.broker.Detach()
	}
	if target.stdin == nil {
		return nil
	}

	return target.stdin.Close()
}

func (target *preparedTarget) closeOutputPipes() error {
	return errors.Join(target.stdout.Close(), target.stderr.Close())
}

func collectCaptureErrors(captureErrors <-chan error) error {
	var result error
	for item := range captureErrors {
		result = errors.Join(result, item)
	}

	return result
}

func finalizeReservedCancellation(
	ctx context.Context,
	database *store.Store,
	capture *logstore.Run,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	jitter *jitterSource,
) (bool, error) {
	current, err := database.GetJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	if current.Cancellation == nil {
		return false, nil
	}

	closeErr := capture.Close()
	logs = completedLogMetadata(logs, nil, closeErr)
	runtimeState, runtimeErr := database.GetRuntime(ctx, jobID)
	outcome := model.RunOutcomeCancelled
	if current.Cancellation.Reason == model.StopReasonTimeout {
		outcome = model.RunOutcomeTimedOut
	}
	completedAt := time.Now().UTC()
	disposition, classification, policyErr := dispositionForRun(
		current,
		runtimeState,
		outcome,
		nil,
		completedAt,
		jitter,
	)
	completed, transitionErr := database.CompleteRunWithDispositionAndFacts(
		ctx,
		jobID,
		runID,
		outcome,
		nil,
		logs,
		"",
		completedAt,
		disposition,
		store.RunDiagnosticFactsInput{PolicyDisposition: classification},
	)
	if transitionErr == nil {
		notifyCompletedRun(ctx, database, completed, outcome, completedAt)
	}

	return disposition.TerminalOutcome != "", errors.Join(closeErr, runtimeErr, policyErr, transitionErr)
}

func finalizeStartFailure(
	ctx context.Context,
	database *store.Store,
	capture *logstore.Run,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	cause error,
	jitter *jitterSource,
) (bool, error) {
	closeErr := capture.Close()
	// A target start failure does not imply that the empty raw streams or their
	// index were recorded incorrectly. Keep execution and recording health
	// independent as required by the persisted model.
	logs = completedLogMetadata(logs, nil, closeErr)
	current, getErr := database.GetJob(ctx, jobID)
	runtimeState, runtimeErr := database.GetRuntime(ctx, jobID)
	completedAt := time.Now().UTC()
	disposition, classification, policyErr := dispositionForRun(
		current,
		runtimeState,
		model.RunOutcomeStartFailed,
		nil,
		completedAt,
		jitter,
	)
	completed, transitionErr := database.CompleteRunWithDispositionAndFacts(
		ctx,
		jobID,
		runID,
		model.RunOutcomeStartFailed,
		nil,
		logs,
		startFailureDiagnostic(cause),
		completedAt,
		disposition,
		store.RunDiagnosticFactsInput{PolicyDisposition: classification},
	)
	if transitionErr == nil {
		notifyCompletedRun(ctx, database, completed, model.RunOutcomeStartFailed, completedAt)
	}

	return disposition.TerminalOutcome != "", errors.Join(closeErr, getErr, runtimeErr, policyErr, transitionErr)
}

func startFailureDiagnostic(cause error) string {
	kind, _ := executor.ClassifyFailure(cause)
	switch kind {
	case executor.FailureExecutableNotFound:
		return string(model.DiagnosticTargetExecutableNotFound)
	case executor.FailureWorkingDirectoryMissing:
		return string(model.DiagnosticTargetWorkingDirectoryMissing)
	case executor.FailurePermissionDenied:
		return string(model.DiagnosticTargetPermissionDenied)
	default:
		return string(model.DiagnosticTargetStartFailed)
	}
}

func drainPipe(
	group *sync.WaitGroup,
	source io.ReadCloser,
	capture *logstore.Run,
	stream logstore.Stream,
	captureEnabled bool,
	errorsChannel chan<- error,
) {
	defer group.Done()
	var captureErr error
	if captureEnabled {
		captureErr = copyPipe(source, capture, stream)
	} else {
		captureErr = drainDiscard(source)
	}
	closeErr := source.Close()
	if err := errors.Join(captureErr, closeErr); err != nil {
		errorsChannel <- err
	}
}

func copyPipe(source io.Reader, capture *logstore.Run, stream logstore.Stream) error {
	writer, err := capture.Writer(stream)
	if err != nil {
		return errors.Join(err, drainDiscard(source))
	}
	if _, err := io.Copy(writer, source); err != nil {
		return errors.Join(err, drainDiscard(source))
	}

	return nil
}

func drainDiscard(source io.Reader) error {
	_, err := io.Copy(io.Discard, source)
	if err != nil {
		return fmt.Errorf("drain target output after capture failure: %w", err)
	}

	return nil
}

func completedLogMetadata(
	metadata model.LogMetadata,
	captureErr error,
	closeErr error,
) model.LogMetadata {
	metadata.Integrity = model.LogIntegrityValid
	metadata.RecordingHealth = model.RecordingHealthy
	stdoutSize, stderrSize, sizeErr := authoritativeLogSizes(metadata)
	if sizeErr == nil {
		metadata.StdoutSize = stdoutSize
		metadata.StderrSize = stderrSize
	}
	if captureErr != nil || closeErr != nil || sizeErr != nil {
		metadata.Integrity = model.LogIntegrityPartial
		metadata.RecordingHealth = model.RecordingDegraded
		metadata.DiagnosticCode = "log_capture_degraded"
	}

	return metadata
}

func authoritativeLogSizes(metadata model.LogMetadata) (stdoutSize, stderrSize int64, returnedErr error) {
	runDirectory := filepath.Dir(metadata.IndexPath)
	runNumber, err := strconv.ParseUint(filepath.Base(runDirectory), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse run log directory: %w", err)
	}
	if runNumber == 0 {
		return 0, 0, errors.New("parse run log directory: run number must be positive")
	}
	jobDirectory := filepath.Dir(runDirectory)
	stateDir := filepath.Dir(filepath.Dir(jobDirectory))
	reader, err := logstore.OpenRun(stateDir, filepath.Base(jobDirectory), runNumber)
	if err != nil {
		return 0, 0, err
	}
	stdout, err := reader.StreamSize(logstore.Stdout)
	if err != nil {
		return 0, 0, err
	}
	stderr, err := reader.StreamSize(logstore.Stderr)
	if err != nil {
		return 0, 0, err
	}
	if stdout > math.MaxInt64 || stderr > math.MaxInt64 {
		return 0, 0, errors.New("run log size exceeds persisted integer range")
	}

	return int64(stdout), int64(stderr), nil
}

func modelIdentity(identity platform.ProcessIdentity) model.ProcessIdentity {
	return model.ProcessIdentity{
		PID:        identity.PID,
		Platform:   runtime.GOOS,
		CreationID: identity.Creation,
		BootID:     identity.Boot,
		TreeID:     identity.Tree,
	}
}

func renewLease(
	ctx context.Context,
	database leaseRenewer,
	supervisorID model.SupervisorID,
	jobID model.JobID,
) {
	renewLeaseAtInterval(ctx, database, supervisorID, jobID, leaseInterval)
}

func renewLeaseAtInterval(
	ctx context.Context,
	database leaseRenewer,
	supervisorID model.SupervisorID,
	jobID model.JobID,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, renewErr := database.RenewLease(
				ctx,
				supervisorID,
				now.UTC(),
				now.UTC().Add(leaseDuration),
			)
			if renewErr != nil && ctx.Err() != nil {
				return
			}
			admissionErr := database.RenewAdmission(ctx, jobID, now.UTC(), admissionLease)
			if admissionErr != nil && ctx.Err() != nil {
				return
			}
		}
	}
}

type leaseRenewer interface {
	RenewLease(context.Context, model.SupervisorID, time.Time, time.Time) (model.SupervisorState, error)
	RenewAdmission(context.Context, model.JobID, time.Time, time.Duration) error
}
