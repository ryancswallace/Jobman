package supervisor

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/executor"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/policy"
	"github.com/ryancswallace/jobman/internal/store"
)

const (
	schedulerPollInterval = 100 * time.Millisecond
	admissionLease        = 15 * time.Second
)

type schedulerResult struct {
	terminal bool
	job      model.JobState
}

//nolint:gocognit,cyclop // The daemonless scheduler must recheck every durable terminal, timeout, and prerequisite state.
func awaitRunnable(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
	jitter policy.JitterSource,
) (schedulerResult, error) {
	job, err := database.GetJob(operationCtx, jobID)
	if err != nil {
		return schedulerResult{}, err
	}
	configuration := job.Spec.ExecutionPolicy()
	runtimeState, err := database.GetRuntime(operationCtx, jobID)
	if err != nil {
		return schedulerResult{}, err
	}
	needsWait := runtimeState.RunCount == 0 &&
		(len(configuration.Dependencies) > 0 || len(configuration.WaitConditions) > 0)
	if needsWait && job.Phase == model.JobPhaseStarting {
		_, moveErr := database.MoveJob(
			operationCtx,
			jobID,
			model.JobPhaseWaiting,
			time.Now().UTC(),
			"prerequisites",
		)
		if moveErr != nil {
			return schedulerResult{}, moveErr
		}
	}

	for {
		now := time.Now().UTC()
		if _, reconcileErr := reconcileExpiredOwnership(operationCtx, database, now); reconcileErr != nil {
			return schedulerResult{}, reconcileErr
		}
		job, err = database.GetJob(operationCtx, jobID)
		if err != nil {
			return schedulerResult{}, err
		}
		if job.Phase == model.JobPhaseCompleted {
			return schedulerResult{terminal: true, job: job}, nil
		}
		if job.Phase == model.JobPhaseStopping {
			if job.ActiveRunID == "" {
				completed, completeErr := database.FinalizeCancellationWithoutRun(operationCtx, jobID, now)
				return schedulerResult{terminal: completeErr == nil, job: completed.Job}, completeErr
			}
		}
		if job.Phase == model.JobPhasePaused {
			if err := waitForSchedulerTick(
				stopCtx, operationCtx, database, jobID,
				jitteredSchedulerPoll(schedulerPollInterval, jitter),
			); err != nil {
				return schedulerResult{}, err
			}
			continue
		}

		if completed, expired, deadlineErr := completeSchedulingDeadline(
			operationCtx,
			database,
			job,
			now,
		); deadlineErr != nil || expired {
			return completed, deadlineErr
		}
		if !needsWait {
			if runtimeState.RunCount == 0 {
				if err := database.MarkPrerequisitesSatisfied(operationCtx, job.ID, now); err != nil {
					return schedulerResult{}, err
				}
			}
			return acquireAdmission(stopCtx, operationCtx, database, job, jitter)
		}

		dependencies, err := database.EvaluateDependencies(operationCtx, jobID, now)
		if err != nil {
			return schedulerResult{}, err
		}
		if dependencies.Impossible {
			completed, completeErr := database.CompleteWithoutRun(
				operationCtx,
				jobID,
				model.JobOutcomeAborted,
				"dependency_unsatisfied",
				now,
			)
			return schedulerResult{terminal: completeErr == nil, job: completed.Job}, completeErr
		}

		waitDecision, nextPoll, err := evaluateWaitConditions(
			operationCtx,
			database,
			job,
			configuration.WaitMode,
			configuration.WaitConditions,
			now,
		)
		if err != nil {
			return schedulerResult{}, err
		}
		if waitDecision.Fatal {
			completed, completeErr := database.CompleteWithoutRun(
				operationCtx,
				jobID,
				model.JobOutcomeAborted,
				"wait_condition_failed",
				now,
			)
			return schedulerResult{terminal: completeErr == nil, job: completed.Job}, completeErr
		}
		if dependencies.Ready && waitDecision.Satisfied {
			if err := database.MarkPrerequisitesSatisfied(operationCtx, job.ID, now); err != nil {
				return schedulerResult{}, err
			}
			if job.Phase == model.JobPhaseWaiting {
				moved, moveErr := database.MoveJob(
					operationCtx,
					jobID,
					model.JobPhaseQueued,
					now,
					"prerequisites_satisfied",
				)
				if moveErr != nil {
					// Pause/resume or cancellation may change the job after the
					// wait condition was evaluated. A stale phase/revision is not
					// a supervisor failure: reload before attempting admission.
					if model.IsConflict(moveErr) || errors.Is(moveErr, store.ErrConflict) {
						current, getErr := database.GetJob(operationCtx, jobID)
						if getErr != nil {
							return schedulerResult{}, errors.Join(moveErr, getErr)
						}
						if current.Revision != job.Revision {
							continue
						}
					}
					return schedulerResult{}, moveErr
				}
				job = moved.Job
			}

			return acquireAdmission(stopCtx, operationCtx, database, job, jitter)
		}
		if nextPoll <= 0 {
			nextPoll = schedulerPollInterval
		}
		if err := waitForSchedulerTick(
			stopCtx, operationCtx, database, jobID, jitteredSchedulerPoll(nextPoll, jitter),
		); err != nil {
			return schedulerResult{}, err
		}
	}
}

func acquireAdmission(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	job model.JobState,
	jitter policy.JitterSource,
) (schedulerResult, error) {
	configuration := job.Spec.ExecutionPolicy().Concurrency
	var done bool
	var result schedulerResult
	var err error
	job, result, done, err = awaitBackoffEligibility(stopCtx, operationCtx, database, job, jitter)
	if err != nil || done {
		return result, err
	}
	if job.Phase != model.JobPhaseStarting && job.Phase != model.JobPhaseQueued {
		return schedulerResult{}, fmt.Errorf("acquire admission from phase %q", job.Phase)
	}

	for {
		now := time.Now().UTC()
		job, result, done, err = loadAdmissionCandidate(
			stopCtx, operationCtx, database, job.ID, now, jitter,
		)
		if err != nil || done {
			return result, err
		}
		result, done, retryImmediately, err := tryAdmission(
			stopCtx, operationCtx, database, job, configuration, now, jitter,
		)
		if err != nil || done {
			return result, err
		}
		if retryImmediately {
			continue
		}
		if waitErr := waitForSchedulerTick(
			stopCtx,
			operationCtx,
			database,
			job.ID,
			jitteredSchedulerPoll(schedulerPollInterval, jitter),
		); waitErr != nil {
			return schedulerResult{}, waitErr
		}
	}
}

func awaitBackoffEligibility(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	job model.JobState,
	jitter policy.JitterSource,
) (model.JobState, schedulerResult, bool, error) {
	if job.Phase != model.JobPhaseBackoff {
		return job, schedulerResult{}, false, nil
	}
	runtimeState, err := database.GetRuntime(operationCtx, job.ID)
	if err != nil {
		return model.JobState{}, schedulerResult{}, false, err
	}
	for runtimeState.NextRunAt != nil {
		now := time.Now().UTC()
		current, getErr := database.GetJob(operationCtx, job.ID)
		if getErr != nil {
			return model.JobState{}, schedulerResult{}, false, getErr
		}
		if isSchedulerInterruption(current.Phase) {
			result, waitErr := awaitRunnable(stopCtx, operationCtx, database, job.ID, jitter)
			return model.JobState{}, result, true, waitErr
		}
		if completed, expired, deadlineErr := completeSchedulingDeadline(
			operationCtx, database, current, now,
		); deadlineErr != nil || expired {
			return model.JobState{}, completed, true, deadlineErr
		}
		if !now.Before(*runtimeState.NextRunAt) {
			job = current
			break
		}
		if waitErr := waitForSchedulerTick(
			stopCtx, operationCtx, database, job.ID,
			min(schedulerPollInterval, time.Until(*runtimeState.NextRunAt)),
		); waitErr != nil {
			return model.JobState{}, schedulerResult{}, false, waitErr
		}
	}
	moved, err := database.MoveJob(
		operationCtx, job.ID, model.JobPhaseQueued, time.Now().UTC(), "backoff_elapsed",
	)
	if err == nil {
		return moved.Job, schedulerResult{}, false, nil
	}
	current, getErr := database.GetJob(operationCtx, job.ID)
	if getErr == nil && isSchedulerInterruption(current.Phase) {
		result, waitErr := awaitRunnable(stopCtx, operationCtx, database, job.ID, jitter)
		return model.JobState{}, result, true, waitErr
	}

	return model.JobState{}, schedulerResult{}, false, err
}

func loadAdmissionCandidate(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
	now time.Time,
	jitter policy.JitterSource,
) (model.JobState, schedulerResult, bool, error) {
	job, err := database.GetJob(operationCtx, jobID)
	if err != nil {
		return model.JobState{}, schedulerResult{}, false, err
	}
	if isSchedulerInterruption(job.Phase) {
		result, waitErr := awaitRunnable(stopCtx, operationCtx, database, jobID, jitter)
		return model.JobState{}, result, true, waitErr
	}
	if completed, expired, deadlineErr := completeSchedulingDeadline(
		operationCtx, database, job, now,
	); deadlineErr != nil || expired {
		return model.JobState{}, completed, true, deadlineErr
	}

	return job, schedulerResult{}, false, nil
}

func tryAdmission(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	job model.JobState,
	configuration model.ConcurrencyPolicy,
	now time.Time,
	jitter policy.JitterSource,
) (result schedulerResult, done, retryImmediately bool, err error) {
	_, err = database.TryAcquireAdmission(
		operationCtx, job.ID, configuration.Pool, configuration.Slots, now, admissionLease,
	)
	if err == nil {
		result, finalizeErr := finalizeAcquiredAdmission(
			stopCtx, operationCtx, database, job.ID, now, jitter,
		)
		return result, true, false, finalizeErr
	}
	if !errors.Is(err, store.ErrCapacity) {
		completed, completeErr := database.CompleteWithoutRun(
			operationCtx, job.ID, model.JobOutcomeAborted, "admission_configuration_invalid", now,
		)
		return schedulerResult{terminal: completeErr == nil, job: completed.Job}, true, false,
			errors.Join(err, completeErr)
	}
	if job.Phase == model.JobPhaseStarting {
		if _, moveErr := database.MoveJob(
			operationCtx,
			job.ID,
			model.JobPhaseQueued,
			now,
			"admission_capacity",
		); moveErr != nil {
			latest, getErr := database.GetJob(operationCtx, job.ID)
			if getErr == nil && isSchedulerInterruption(latest.Phase) {
				result, waitErr := awaitRunnable(stopCtx, operationCtx, database, job.ID, jitter)

				return result, true, false, waitErr
			}

			return schedulerResult{}, true, false, errors.Join(moveErr, getErr)
		}

		return schedulerResult{}, false, true, nil
	}
	reconciled, reconcileErr := reconcileExpiredOwnership(operationCtx, database, now)
	if reconcileErr != nil {
		return schedulerResult{}, true, false, reconcileErr
	}

	return schedulerResult{}, false, reconciled, nil
}

func finalizeAcquiredAdmission(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
	now time.Time,
	jitter policy.JitterSource,
) (schedulerResult, error) {
	job, err := database.GetJob(operationCtx, jobID)
	if err != nil {
		return schedulerResult{}, errors.Join(err, database.ReleaseAdmission(operationCtx, jobID, now))
	}
	if isSchedulerInterruption(job.Phase) {
		if releaseErr := database.ReleaseAdmission(operationCtx, jobID, now); releaseErr != nil {
			return schedulerResult{}, releaseErr
		}

		return awaitRunnable(stopCtx, operationCtx, database, jobID, jitter)
	}
	if job.Phase != model.JobPhaseQueued {
		return schedulerResult{job: job}, nil
	}
	moved, moveErr := database.MoveJob(
		operationCtx, jobID, model.JobPhaseStarting, now, "admitted",
	)
	if moveErr == nil {
		return schedulerResult{job: moved.Job}, nil
	}
	releaseErr := database.ReleaseAdmission(operationCtx, jobID, now)
	latest, getLatestErr := database.GetJob(operationCtx, jobID)
	if getLatestErr == nil && isSchedulerInterruption(latest.Phase) && releaseErr == nil {
		return awaitRunnable(stopCtx, operationCtx, database, jobID, jitter)
	}

	return schedulerResult{}, errors.Join(moveErr, releaseErr)
}

func isSchedulerInterruption(phase model.JobPhase) bool {
	return phase == model.JobPhaseCompleted || phase == model.JobPhaseStopping || phase == model.JobPhasePaused
}

func jitteredSchedulerPoll(delay time.Duration, source policy.JitterSource) time.Duration {
	if delay <= 0 || source == nil {
		return delay
	}
	// A symmetric total width of 20% prevents synchronized local supervisors
	// while retaining at least 90% and at most 110% of the configured poll
	// interval. Retry/backoff deadlines themselves are not jittered here.
	width := delay / 5
	if width < 2*time.Nanosecond {
		return delay
	}
	sample := source.Uint64N(uint64(width) + 1)
	delta := time.Duration(sample) - width/2 //nolint:gosec // sample is bounded by a duration-sized interval.
	jittered := delay + delta
	if jittered <= 0 {
		return time.Nanosecond
	}

	return jittered
}

func reconcileExpiredOwnership(
	ctx context.Context,
	database *store.Store,
	now time.Time,
) (bool, error) {
	expiredJobs, err := database.ListExpiredOwnedJobs(ctx, now)
	if err != nil {
		return false, err
	}
	changed := false
	for _, jobID := range expiredJobs {
		reconciled, reconcileErr := reconcileExpiredJobOwner(ctx, database, jobID, now)
		if reconcileErr != nil {
			return false, reconcileErr
		}
		changed = changed || reconciled
	}
	admissions, err := database.ListExpiredAdmissions(ctx, now)
	if err != nil {
		return false, err
	}
	for _, admission := range admissions {
		job, getErr := database.GetJob(ctx, admission.JobID)
		if getErr != nil {
			return false, getErr
		}
		if job.Phase == model.JobPhaseCompleted {
			if releaseErr := database.ReleaseAdmission(ctx, job.ID, now); releaseErr != nil {
				return false, releaseErr
			}
			changed = true
			continue
		}
		if !job.SupervisorID.Valid() {
			return false, fmt.Errorf("expired admission for job %s has no supervisor owner", job.ID)
		}
		reconciled, reconcileErr := reconcileExpiredJobOwner(ctx, database, job.ID, now)
		if reconcileErr != nil {
			return false, reconcileErr
		}
		changed = changed || reconciled
	}

	return changed, nil
}

func reconcileExpiredJobOwner(
	ctx context.Context,
	database *store.Store,
	jobID model.JobID,
	now time.Time,
) (bool, error) {
	job, err := database.GetJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	if job.Phase == model.JobPhaseCompleted || !job.SupervisorID.Valid() {
		return false, nil
	}
	owner, err := database.GetSupervisor(ctx, job.SupervisorID)
	if err != nil {
		return false, err
	}
	if now.Before(owner.LeaseExpiresAt) {
		return false, nil
	}
	identity := platform.ProcessIdentity{
		PID: owner.Process.PID, Creation: owner.Process.CreationID, Boot: owner.Process.BootID,
		Tree: owner.Process.TreeID,
	}
	alive, aliveErr := platform.Alive(identity)
	if aliveErr != nil && !errors.Is(aliveErr, platform.ErrIdentityMismatch) {
		return false, fmt.Errorf("revalidate expired job owner: %w", aliveErr)
	}
	if alive {
		return false, nil
	}
	var logs *model.LogMetadata
	if job.ActiveRunID != "" {
		run, getRunErr := database.GetRun(ctx, job.ActiveRunID)
		if getRunErr != nil {
			return false, getRunErr
		}
		value := run.Logs
		if value.Integrity == model.LogIntegrityPending {
			value.Integrity = model.LogIntegrityPartial
			value.RecordingHealth = model.RecordingDegraded
			value.DiagnosticCode = "supervisor_lease_expired"
		}
		logs = &value
	}
	if _, lostErr := database.MarkOwnershipLost(
		ctx,
		job.ID,
		logs,
		"supervisor_lease_expired",
		now,
	); lostErr != nil && !model.IsConflict(lostErr) && !errors.Is(lostErr, store.ErrConflict) {
		return false, lostErr
	}

	return true, nil
}

func completeSchedulingDeadline(
	ctx context.Context,
	database *store.Store,
	job model.JobState,
	now time.Time,
) (schedulerResult, bool, error) {
	expired, err := jobTimeoutExpired(ctx, database, job, now)
	if err != nil {
		return schedulerResult{}, false, err
	}
	if expired {
		completed, completeErr := database.CompleteWithoutRun(
			ctx,
			job.ID,
			model.JobOutcomeTimedOut,
			"job_timeout",
			now,
		)

		return schedulerResult{terminal: completeErr == nil, job: completed.Job}, true, completeErr
	}
	hasWaitAbort := false
	for _, condition := range job.Spec.ExecutionPolicy().WaitConditions {
		if !condition.AbortAt.IsZero() && !now.Before(condition.AbortAt) {
			hasWaitAbort = true
			break
		}
	}
	if !hasWaitAbort {
		return schedulerResult{}, false, nil
	}
	runtimeState, err := database.GetRuntime(ctx, job.ID)
	if err != nil {
		return schedulerResult{}, false, err
	}
	if runtimeState.RunCount != 0 {
		return schedulerResult{}, false, nil
	}
	completed, completeErr := database.CompleteWithoutRun(
		ctx,
		job.ID,
		model.JobOutcomeAborted,
		"wait_abort_deadline",
		now,
	)

	return schedulerResult{terminal: completeErr == nil, job: completed.Job}, true, completeErr
}

func waitForSchedulerTick(
	stopCtx context.Context,
	operationCtx context.Context,
	database *store.Store,
	jobID model.JobID,
	delay time.Duration,
) error {
	if delay <= 0 {
		delay = schedulerPollInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-stopCtx.Done():
		_, err := database.RequestCancellation(operationCtx, jobID, time.Now().UTC())
		if err != nil && !model.IsConflict(err) {
			return fmt.Errorf("record cancellation while waiting: %w", err)
		}

		return nil
	case <-operationCtx.Done():
		return operationCtx.Err()
	}
}

func jobTimeoutExpired(
	ctx context.Context,
	database *store.Store,
	job model.JobState,
	now time.Time,
) (bool, error) {
	limit := job.Spec.ExecutionPolicy().JobTimeout
	if limit == 0 || job.ClaimedAt == nil {
		return false, nil
	}
	runtime, err := database.GetRuntime(ctx, job.ID)
	if err != nil {
		return false, err
	}
	paused := runtime.TotalPaused
	if runtime.PausedAt != nil {
		paused += now.Sub(*runtime.PausedAt)
	}
	elapsed := now.Sub(*job.ClaimedAt) - paused

	return elapsed >= limit, nil
}

//nolint:gocognit,cyclop,nestif // Every condition kind records a durable result before aggregate all/any evaluation.
func evaluateWaitConditions(
	ctx context.Context,
	database *store.Store,
	job model.JobState,
	mode policy.WaitMode,
	conditions []model.WaitCondition,
	now time.Time,
) (policy.WaitDecision, time.Duration, error) {
	results := make([]policy.ConditionResult, len(conditions))
	var nextPoll time.Duration
	for index, condition := range conditions {
		if !condition.AbortAt.IsZero() && now.After(condition.AbortAt) {
			results[index] = policy.ConditionResult{
				Fatal: true,
				Err:   errors.New("wait condition abort deadline passed"),
			}
		} else {
			switch condition.Kind {
			case model.WaitUntil:
				evaluated, evaluationErr := policy.EvaluateUntil(now, condition.Until)
				if evaluationErr != nil {
					return policy.WaitDecision{}, 0, evaluationErr
				}
				results[index] = evaluated
			case model.WaitDelay:
				acceptedAt := job.SubmittedAt
				if job.ClaimedAt != nil {
					acceptedAt = *job.ClaimedAt
				}
				evaluated, evaluationErr := policy.EvaluateDelay(now, acceptedAt, condition.Delay)
				if evaluationErr != nil {
					return policy.WaitDecision{}, 0, evaluationErr
				}
				results[index] = evaluated
			case model.WaitFileExists:
				results[index] = policy.EvaluateFileExists(osFileInspector{}, condition.Path, condition.FileKind, false)
			case model.WaitProbe:
				directory := condition.ProbeDirectory
				if directory == "" {
					directory = job.Spec.WorkingDirectory()
				}
				results[index] = policy.EvaluateProbe(ctx, execProbeRunner{
					directory: directory, environment: condition.ProbeEnvironment,
					unsetEnvironment:  condition.ProbeUnsetEnvironment,
					secretEnvironment: condition.ProbeSecretEnv,
				}, condition.Probe)
			default:
				results[index] = policy.ConditionResult{Fatal: true, Err: errors.New("unknown wait condition")}
			}
		}
		diagnostic := ""
		if results[index].Err != nil {
			diagnostic = "wait_evaluation_error"
		}
		if err := database.RecordWaitEvaluation(
			ctx,
			job.ID,
			index,
			condition.Kind,
			results[index].Satisfied,
			diagnostic,
			now,
		); err != nil {
			return policy.WaitDecision{}, 0, err
		}
		if condition.PollInterval > 0 && (nextPoll == 0 || condition.PollInterval < nextPoll) {
			nextPoll = condition.PollInterval
		}
	}
	if nextPoll == 0 {
		nextPoll = schedulerPollInterval
	}
	decision, err := policy.CombineWaitConditions(mode, results)

	return decision, nextPoll, err
}

type osFileInspector struct{}

func (osFileInspector) Lstat(path string) (fs.FileInfo, error) {
	return os.Lstat(path)
}

type execProbeRunner struct {
	directory         string
	environment       map[string]string
	unsetEnvironment  []string
	secretEnvironment map[string]model.SecretReference
}

func (runner execProbeRunner) RunProbe(
	ctx context.Context,
	specification policy.ProbeSpec,
) (policy.ProbeResult, error) {
	probeCtx, cancel := context.WithTimeout(ctx, specification.Timeout)
	defer cancel()
	environment := make(map[string]string, len(runner.environment)+len(runner.secretEnvironment))
	for name, value := range runner.environment {
		environment[name] = value
	}
	for name, reference := range runner.secretEnvironment {
		parsed, err := config.ParseSecretRef(reference.Provider + ":" + reference.Name)
		if err != nil {
			return policy.ProbeResult{}, fmt.Errorf("parse probe secret reference for %q: %w", name, err)
		}
		value, err := (config.LocalSecretResolver{}).ResolveSecret(probeCtx, parsed)
		if err != nil {
			return policy.ProbeResult{}, fmt.Errorf("resolve probe secret environment %q: %w", name, err)
		}
		environment[name] = value
	}
	prepared, resolved, err := executor.Command(executor.Request{
		Executable: specification.Executable, Arguments: specification.Arguments,
		Directory: runner.directory, BaseEnv: os.Environ(), AddEnv: environment,
		RemoveEnv: runner.unsetEnvironment,
	})
	if err != nil {
		return policy.ProbeResult{}, err
	}
	// #nosec G204 -- probes are explicit executable-plus-argument policy, never
	// concatenated into a shell command.
	command := exec.CommandContext(probeCtx, resolved, specification.Arguments...)
	command.Args[0] = specification.Executable
	command.Dir = prepared.Dir
	command.Env = prepared.Env
	output := &boundedOutput{limit: specification.OutputLimit}
	command.Stdout = output
	command.Stderr = output
	err = command.Run()
	exitCode := 0
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		exitCode = exitError.ExitCode()
		err = nil
	}

	return policy.ProbeResult{ExitCode: exitCode, Output: output.Bytes(), Truncated: output.truncated}, err
}

type boundedOutput struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	limit     int64
	truncated bool
}

func (output *boundedOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	original := len(data)
	remaining := output.limit - int64(output.buffer.Len())
	if remaining <= 0 {
		output.truncated = output.truncated || original > 0
		return original, nil
	}
	if int64(len(data)) > remaining {
		data = data[:remaining]
		output.truncated = true
	}
	_, _ = output.buffer.Write(data)

	return original, nil
}

func (output *boundedOutput) Bytes() []byte {
	output.mu.Lock()
	defer output.mu.Unlock()

	return bytes.Clone(output.buffer.Bytes())
}

type jitterSource struct {
	mu     sync.Mutex
	source *rand.Rand
}

func newJitterSource() (*jitterSource, error) {
	return newJitterSourceFrom(cryptorand.Reader)
}

func newJitterSourceFrom(reader io.Reader) (*jitterSource, error) {
	var seed [32]byte
	if _, err := io.ReadFull(reader, seed[:]); err != nil {
		return nil, fmt.Errorf("seed delay jitter: %w", err)
	}

	return &jitterSource{source: rand.New(rand.NewChaCha8(seed))}, nil //nolint:gosec // Jitter is not security-sensitive; the seed is unpredictable to avoid synchronized retries.
}

func (source *jitterSource) Uint64N(upperBound uint64) uint64 {
	source.mu.Lock()
	defer source.mu.Unlock()
	if upperBound == 0 {
		return 0
	}

	return source.source.Uint64N(upperBound)
}

func dispositionForRun(
	job model.JobState,
	runtime store.JobRuntime,
	runOutcome model.RunOutcome,
	exit *model.ExitInfo,
	now time.Time,
	jitter policy.JitterSource,
) (model.RunDisposition, policy.RunClassification, error) {
	configuration := job.Spec.ExecutionPolicy()
	classifier, err := policy.NewClassifier(configuration.Classification)
	if err != nil {
		return model.RunDisposition{}, "", err
	}
	classification, err := classifier.Classify(policyResult(job, runOutcome, exit))
	if err != nil {
		return model.RunDisposition{}, "", err
	}
	counts := policy.RunCounts{
		Completed: runtime.RunCount + 1,
		Successes: runtime.SuccessCount,
		Failures:  runtime.FailureCount,
	}
	delayPolicy := configuration.FailureDelay
	if classification == policy.RunClassificationSuccess {
		counts.Successes++
		delayPolicy = configuration.SuccessDelay
	} else {
		counts.Failures++
	}
	delay, err := delayPolicy.Delay(counts.Completed, jitter)
	if err != nil {
		return model.RunDisposition{}, "", err
	}
	decision, err := configuration.Completion.Evaluate(policy.CompletionEvaluation{
		Counts:         counts,
		Classification: classification,
		Canceled:       job.Cancellation != nil && job.Cancellation.Reason == model.StopReasonCancellation,
		JobTimedOut:    job.Cancellation != nil && job.Cancellation.Reason == model.StopReasonTimeout,
		Now:            now,
		NextDelay:      delay,
	})
	if err != nil {
		return model.RunDisposition{}, "", err
	}
	if decision.Action == policy.CompletionActionComplete {
		return model.RunDisposition{
			TerminalOutcome: model.JobOutcome(decision.Outcome),
			Reason:          string(decision.Reason),
		}, classification, nil
	}
	next := now.Add(delay)
	phase := model.JobPhaseQueued
	var nextPointer *time.Time
	if delay > 0 {
		phase = model.JobPhaseBackoff
		nextPointer = &next
	}

	return model.RunDisposition{
		NextPhase: phase,
		NextRunAt: nextPointer,
		Reason:    string(decision.Reason),
	}, classification, nil
}

func policyResult(
	job model.JobState,
	outcome model.RunOutcome,
	exit *model.ExitInfo,
) policy.RunResult {
	if job.Cancellation != nil {
		if job.Cancellation.Reason == model.StopReasonTimeout {
			return policy.RunResult{Termination: policy.RunTerminationTimeout}
		}
		return policy.RunResult{Termination: policy.RunTerminationCancellation}
	}
	if outcome == model.RunOutcomeTimedOut {
		return policy.RunResult{Termination: policy.RunTerminationTimeout}
	}
	if outcome == model.RunOutcomeStartFailed {
		return policy.RunResult{Termination: policy.RunTerminationStartFailure}
	}
	if outcome == model.RunOutcomeLost {
		return policy.RunResult{Termination: policy.RunTerminationLost}
	}
	if exit != nil {
		if exit.ExitCode != nil {
			return policy.RunResult{Termination: policy.RunTerminationExit, ExitCode: *exit.ExitCode}
		}
		if exit.Signal != "" {
			return policy.RunResult{Termination: policy.RunTerminationSignal, Signal: exit.Signal}
		}
		if exit.PlatformReason != "" {
			return policy.RunResult{Termination: policy.RunTerminationPlatform, PlatformReason: exit.PlatformReason}
		}
	}

	return policy.RunResult{Termination: policy.RunTerminationPlatform, PlatformReason: "unknown_exit"}
}
