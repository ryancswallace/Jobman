package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/internal/model"
)

const maxAdmissionBypasses = 3

var (
	// ErrCapacity means a job remains queued because a configured admission
	// limit or an older admission request currently has priority.
	ErrCapacity = errors.New("concurrency capacity unavailable")
	// ErrAdmissionImpossible means a request can never fit within a configured
	// finite limit. Unlike ErrCapacity, retrying unchanged cannot succeed.
	ErrAdmissionImpossible = errors.New("concurrency request exceeds configured capacity")
)

// JobRuntime contains mutable scheduler state kept separate from the immutable
// job specification and the lifecycle snapshot.
type JobRuntime struct {
	JobID                    model.JobID
	Revision                 uint64
	RunCount                 uint64
	SuccessCount             uint64
	FailureCount             uint64
	NextRunAt                *time.Time
	WaitingReason            string
	PausedFrom               model.JobPhase
	PausedAt                 *time.Time
	TotalPaused              time.Duration
	PrerequisitesSatisfiedAt *time.Time
	InputEndpoint            string
	InputEOFRequested        bool
}

// DependencyPredicate describes the immutable outcome required from another
// job before a dependent job may run.
type DependencyPredicate string

// Supported dependency predicates.
const (
	DependencySuccess          DependencyPredicate = "success"
	DependencyFinish           DependencyPredicate = "finish"
	DependencyFailed           DependencyPredicate = "failed"
	DependencyTimedOut         DependencyPredicate = "timed_out"
	DependencyCancelled        DependencyPredicate = "cancelled" //nolint:misspell // Stable CLI spelling.
	DependencyAborted          DependencyPredicate = "aborted"
	DependencyLost             DependencyPredicate = "lost"
	DependencySubmissionFailed DependencyPredicate = "submission_failed"
	dependencyOutcomeSetPrefix                     = "outcomes:"
)

// OutcomeSetPredicate returns the canonical predicate for one or more
// acceptable terminal outcomes.
func OutcomeSetPredicate(outcomes []model.JobOutcome) (DependencyPredicate, error) {
	if len(outcomes) == 0 {
		return "", errors.New("dependency outcome set must not be empty")
	}
	values := make([]string, 0, len(outcomes))
	seen := make(map[string]struct{}, len(outcomes))
	for _, outcome := range outcomes {
		if !outcome.Valid() || outcome == "" {
			return "", fmt.Errorf("dependency outcome set contains invalid outcome %q", outcome)
		}
		value := string(outcome)
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	slices.Sort(values)

	return DependencyPredicate(dependencyOutcomeSetPrefix + strings.Join(values, ",")), nil
}

// Dependency records one resolved immutable edge.
type Dependency struct {
	JobID            model.JobID
	DependsOn        model.JobID
	Predicate        DependencyPredicate
	ObservedRevision uint64
	ObservedOutcome  model.JobOutcome
	SatisfiedAt      *time.Time
}

// DependencyStatus summarizes whether all dependency predicates can still be
// met.
type DependencyStatus struct {
	Ready      bool
	Impossible bool
	Pending    int
	Failed     []Dependency
}

// Admission is a transactional global and optional pool slot allocation. The
// expiry is liveness metadata; capacity remains occupied until proven release.
type Admission struct {
	JobID        model.JobID
	RunID        model.RunID
	Pool         string
	Slots        uint64
	AcquiredAt   time.Time
	LeaseExpires time.Time
	ReleasedAt   *time.Time
}

// WaitEvaluation is the bounded, non-secret diagnostic history retained for
// one immutable wait condition.
type WaitEvaluation struct {
	JobID              model.JobID
	ConditionIndex     int
	ConditionKind      model.WaitConditionKind
	EvaluatedAt        *time.Time
	SatisfiedAt        *time.Time
	AttemptCount       uint64
	LastDiagnosticCode string
}

type admissionRequest struct {
	sequence    int64
	jobID       string
	enqueuedAt  int64
	pool        string
	slots       uint64
	bypassCount uint64
}

// MarkPrerequisitesSatisfied records when all dependency and initial-wait
// requirements became true. Repeated observations preserve the first instant,
// which is the durable admission-order key for the first run.
func (s *Store) MarkPrerequisitesSatisfied(
	ctx context.Context,
	jobID model.JobID,
	at time.Time,
) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_runtime
		SET prerequisites_satisfied_at_ns = COALESCE(prerequisites_satisfied_at_ns, ?),
		    revision = revision + 1,
		    updated_at_ns = ?
		WHERE job_id = ?`, at.UTC().UnixNano(), at.UTC().UnixNano(), jobID.String())
	if err != nil {
		return classifySQLite("mark prerequisites satisfied", err)
	}

	return requireOneUpdate(result, "job runtime", jobID.String(), 0, "")
}

// RecordWaitEvaluation persists bounded diagnostic state for one prerequisite.
func (s *Store) RecordWaitEvaluation(
	ctx context.Context,
	jobID model.JobID,
	index int,
	kind model.WaitConditionKind,
	satisfied bool,
	diagnosticCode string,
	at time.Time,
) error {
	if index < 0 {
		return errors.New("record wait evaluation: condition index must not be negative")
	}
	var satisfiedAt any
	if satisfied {
		satisfiedAt = at.UTC().UnixNano()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO wait_evaluations(
			job_id, condition_index, condition_kind, evaluated_at_ns,
			satisfied_at_ns, attempt_count, last_diagnostic_code
		) VALUES (?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT(job_id, condition_index) DO UPDATE SET
		condition_kind = excluded.condition_kind,
		evaluated_at_ns = excluded.evaluated_at_ns,
		satisfied_at_ns = COALESCE(wait_evaluations.satisfied_at_ns, excluded.satisfied_at_ns),
		attempt_count = wait_evaluations.attempt_count + 1,
		last_diagnostic_code = excluded.last_diagnostic_code`,
		jobID.String(), index, string(kind), at.UTC().UnixNano(), satisfiedAt, nullableString(diagnosticCode))

	return classifySQLite("record wait evaluation", err)
}

// ListWaitEvaluations loads persisted wait diagnostics in specification
// order. Conditions that have not yet been evaluated do not have a row.
func (s *Store) ListWaitEvaluations(
	ctx context.Context,
	jobID model.JobID,
) ([]WaitEvaluation, error) {
	if !jobID.Valid() {
		return nil, errors.New("list wait evaluations: invalid job ID")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT condition_index, condition_kind, evaluated_at_ns, satisfied_at_ns,
		       attempt_count, last_diagnostic_code
		FROM wait_evaluations WHERE job_id = ? ORDER BY condition_index`, jobID.String())
	if err != nil {
		return nil, fmt.Errorf("list wait evaluations: %w", classifySQLite("list wait evaluations", err))
	}
	defer rows.Close()

	evaluations := make([]WaitEvaluation, 0)
	for rows.Next() {
		var conditionIndex, attemptCount int64
		var conditionKind string
		var evaluatedAt, satisfiedAt sql.NullInt64
		var diagnosticCode sql.NullString
		if scanErr := rows.Scan(
			&conditionIndex,
			&conditionKind,
			&evaluatedAt,
			&satisfiedAt,
			&attemptCount,
			&diagnosticCode,
		); scanErr != nil {
			return nil, fmt.Errorf("list wait evaluations: decode: %w", scanErr)
		}
		index, convertErr := nonnegativeIntFromDatabase("wait condition index", conditionIndex)
		if convertErr != nil {
			return nil, convertErr
		}
		attempts, convertErr := nonnegativeUintFromDatabase("wait attempt count", attemptCount)
		if convertErr != nil {
			return nil, convertErr
		}
		kind := model.WaitConditionKind(conditionKind)
		if !validWaitConditionKind(kind) {
			return nil, fmt.Errorf("list wait evaluations: invalid condition kind %q", conditionKind)
		}
		evaluations = append(evaluations, WaitEvaluation{
			JobID:              jobID,
			ConditionIndex:     index,
			ConditionKind:      kind,
			EvaluatedAt:        optionalTime(evaluatedAt),
			SatisfiedAt:        optionalTime(satisfiedAt),
			AttemptCount:       attempts,
			LastDiagnosticCode: diagnosticCode.String,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list wait evaluations: iterate: %w", err)
	}

	return evaluations, nil
}

func validWaitConditionKind(kind model.WaitConditionKind) bool {
	switch kind {
	case model.WaitUntil, model.WaitDelay, model.WaitFileExists, model.WaitProbe:
		return true
	default:
		return false
	}
}

// SetInputEndpoint publishes or clears the private local IPC address used by a
// live-input job.
func (s *Store) SetInputEndpoint(
	ctx context.Context,
	jobID model.JobID,
	endpoint string,
	at time.Time,
) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_runtime SET input_endpoint = ?, revision = revision + 1, updated_at_ns = ?
		WHERE job_id = ?`, nullableString(endpoint), at.UTC().UnixNano(), jobID.String())
	if err != nil {
		return classifySQLite("set live-input endpoint", err)
	}

	return requireOneUpdate(result, "job runtime", jobID.String(), 0, "")
}

// RecordInputEOF makes an accepted EOF request durable for inspection and
// restart diagnostics.
func (s *Store) RecordInputEOF(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	at time.Time,
) error {
	if !jobID.Valid() || !runID.Valid() {
		return errors.New("record live-input EOF: invalid job or run ID")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_runtime SET input_eof_requested = 1, revision = revision + 1, updated_at_ns = ?
		WHERE job_id = ? AND input_eof_requested = 0
		  AND EXISTS (
		      SELECT 1 FROM jobs
		      WHERE jobs.id = job_runtime.job_id AND jobs.active_run_id = ?
		  )`, at.UTC().UnixNano(), jobID.String(), runID.String())
	if err != nil {
		return classifySQLite("record live-input EOF", err)
	}

	return requireOneUpdate(result, "job runtime", jobID.String(), 0, "")
}

// GetRuntime loads one scheduler snapshot.
func (s *Store) GetRuntime(ctx context.Context, jobID model.JobID) (JobRuntime, error) {
	if !jobID.Valid() {
		return JobRuntime{}, errors.New("get job runtime: invalid job ID")
	}

	return getRuntimeWithQueryer(ctx, s.db, jobID)
}

func getRuntimeWithQueryer(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, jobID model.JobID,
) (JobRuntime, error) {
	var runtime JobRuntime
	var revision, runCount, successes, failures int64
	var nextRun, pausedAt, prerequisites sql.NullInt64
	var pausedFrom, waitingReason, endpoint sql.NullString
	var totalPaused int64
	var eof int
	err := queryer.QueryRowContext(ctx, `
		SELECT revision, run_count, success_count, failure_count,
		       next_run_at_ns, waiting_reason, paused_from_phase, paused_at_ns,
		       total_paused_ns, prerequisites_satisfied_at_ns,
		       input_endpoint, input_eof_requested
		FROM job_runtime WHERE job_id = ?`, jobID.String()).Scan(
		&revision,
		&runCount,
		&successes,
		&failures,
		&nextRun,
		&waitingReason,
		&pausedFrom,
		&pausedAt,
		&totalPaused,
		&prerequisites,
		&endpoint,
		&eof,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return JobRuntime{}, fmt.Errorf("get runtime for job %s: %w", jobID, ErrNotFound)
	}
	if err != nil {
		return JobRuntime{}, fmt.Errorf("get runtime for job %s: %w", jobID, err)
	}
	runtime.JobID = jobID
	if runtime.Revision, err = uintFromDatabase("runtime revision", revision); err != nil {
		return JobRuntime{}, err
	}
	if runtime.RunCount, err = nonnegativeUintFromDatabase("runtime run count", runCount); err != nil {
		return JobRuntime{}, err
	}
	if runtime.SuccessCount, err = nonnegativeUintFromDatabase("runtime success count", successes); err != nil {
		return JobRuntime{}, err
	}
	if runtime.FailureCount, err = nonnegativeUintFromDatabase("runtime failure count", failures); err != nil {
		return JobRuntime{}, err
	}
	runtime.NextRunAt = optionalTime(nextRun)
	runtime.WaitingReason = waitingReason.String
	runtime.PausedFrom = model.JobPhase(pausedFrom.String)
	runtime.PausedAt = optionalTime(pausedAt)
	runtime.TotalPaused = time.Duration(totalPaused)
	runtime.PrerequisitesSatisfiedAt = optionalTime(prerequisites)
	runtime.InputEndpoint = endpoint.String
	runtime.InputEOFRequested = eof != 0

	return runtime, nil
}

// MoveJob advances a scheduler phase and records its human-readable reason.
func (s *Store) MoveJob(
	ctx context.Context,
	jobID model.JobID,
	target model.JobPhase,
	at time.Time,
	reason string,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.MoveJob(job, target, at, reason)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		_, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime
			SET revision = revision + 1, waiting_reason = ?,
			    next_run_at_ns = CASE WHEN ? = 'backoff' THEN next_run_at_ns ELSE NULL END,
			    updated_at_ns = ?
			WHERE job_id = ?`, nullableString(reason), string(target), at.UTC().UnixNano(), jobID.String())
		return updateErr
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// CompleteRunWithDisposition atomically records a run and the policy counters
// that determine whether another invocation is due.
func (s *Store) CompleteRunWithDisposition(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	outcome model.RunOutcome,
	exit *model.ExitInfo,
	logs model.LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	disposition model.RunDisposition,
) (model.TransitionResult, error) {
	return s.commitCompletedRun(
		ctx, jobID, runID, outcome, exit, logs, diagnosticCode, completedAt, disposition, nil,
	)
}

// CompleteRunWithDispositionAndFacts atomically records the completed run,
// scheduler counters, and typed post-wait diagnostic facts.
func (s *Store) CompleteRunWithDispositionAndFacts(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	outcome model.RunOutcome,
	exit *model.ExitInfo,
	logs model.LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	disposition model.RunDisposition,
	factsInput RunDiagnosticFactsInput,
) (model.TransitionResult, error) {
	return s.commitCompletedRun(
		ctx, jobID, runID, outcome, exit, logs, diagnosticCode, completedAt, disposition, &factsInput,
	)
}

//nolint:gocognit // Completion validates policy, facts, ownership, counters, and admission in one transaction.
func (s *Store) commitCompletedRun(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	outcome model.RunOutcome,
	exit *model.ExitInfo,
	logs model.LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	disposition model.RunDisposition,
	factsInput *RunDiagnosticFactsInput,
) (model.TransitionResult, error) {
	if disposition.TerminalOutcome != "" {
		s.supervisorLeaseMu.Lock()
		defer s.supervisorLeaseMu.Unlock()
	}

	job, run, err := s.getJobRun(ctx, jobID, runID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.CompleteRun(
		job,
		run,
		outcome,
		exit,
		logs,
		diagnosticCode,
		completedAt,
		disposition,
	)
	if err != nil {
		return model.TransitionResult{}, err
	}
	var facts *RunDiagnosticFacts
	if factsInput != nil {
		built, buildErr := s.buildRunDiagnosticFacts(job, *result.Run, *factsInput, completedAt)
		if buildErr != nil {
			return model.TransitionResult{}, buildErr
		}
		facts = &built
	}
	if disposition.TerminalOutcome != "" {
		if err := s.attachSupervisorRelease(ctx, &result, completedAt); err != nil {
			return model.TransitionResult{}, err
		}
	}
	successIncrement := 0
	failureIncrement := 1
	if outcome == model.RunOutcomeSuccess {
		successIncrement = 1
		failureIncrement = 0
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		if facts != nil {
			if persistErr := persistRunDiagnosticFacts(ctx, tx, runID, *facts); persistErr != nil {
				return persistErr
			}
		}
		update, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime
			SET revision = revision + 1,
			    run_count = run_count + 1,
			    success_count = success_count + ?,
			    failure_count = failure_count + ?,
			    next_run_at_ns = ?, waiting_reason = ?, updated_at_ns = ?
			WHERE job_id = ?`,
			successIncrement,
			failureIncrement,
			nullableTime(disposition.NextRunAt),
			nullableString(disposition.Reason),
			completedAt.UTC().UnixNano(),
			jobID.String(),
		)
		if updateErr != nil {
			return updateErr
		}
		if _, releaseErr := tx.ExecContext(ctx, `
			UPDATE admissions SET released_at_ns = COALESCE(released_at_ns, ?)
			WHERE job_id = ?`, completedAt.UTC().UnixNano(), jobID.String()); releaseErr != nil {
			return releaseErr
		}
		if _, deleteErr := tx.ExecContext(ctx, `
			DELETE FROM admission_requests WHERE job_id = ?`, jobID.String()); deleteErr != nil {
			return deleteErr
		}
		return requireOneUpdate(update, "job runtime", jobID.String(), 0, "")
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// CompleteWithoutRun atomically records terminal policy failure before another
// target invocation begins.
func (s *Store) CompleteWithoutRun(
	ctx context.Context,
	jobID model.JobID,
	outcome model.JobOutcome,
	diagnosticCode string,
	completedAt time.Time,
) (model.TransitionResult, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.CompleteWithoutRun(job, outcome, diagnosticCode, completedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.attachSupervisorRelease(ctx, &result, completedAt); err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		update, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime SET revision = revision + 1, next_run_at_ns = NULL,
			waiting_reason = ?, updated_at_ns = ? WHERE job_id = ?`,
			nullableString(diagnosticCode), completedAt.UTC().UnixNano(), jobID.String())
		if updateErr != nil {
			return updateErr
		}
		if _, releaseErr := tx.ExecContext(ctx, `
			UPDATE admissions SET released_at_ns = COALESCE(released_at_ns, ?)
			WHERE job_id = ?`, completedAt.UTC().UnixNano(), jobID.String()); releaseErr != nil {
			return releaseErr
		}
		if _, deleteErr := tx.ExecContext(ctx, `
			DELETE FROM admission_requests WHERE job_id = ?`, jobID.String()); deleteErr != nil {
			return deleteErr
		}

		return requireOneUpdate(update, "job runtime", jobID.String(), 0, "")
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// RequestTimeout records a timeout intent before target termination.
func (s *Store) RequestTimeout(
	ctx context.Context,
	jobID model.JobID,
	requestedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	var run *model.RunState
	if job.ActiveRunID != "" {
		value, getErr := s.GetRun(ctx, job.ActiveRunID)
		if getErr != nil {
			return model.TransitionResult{}, getErr
		}
		run = &value
	}
	result, err := model.RequestTimeout(job, run, requestedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if len(result.Events) == 0 {
		return result, nil
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// RequestRunTimeout records a retryable per-run timeout before target
// termination without setting the job-level timeout intent.
func (s *Store) RequestRunTimeout(
	ctx context.Context,
	jobID model.JobID,
	requestedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if job.ActiveRunID == "" {
		return model.TransitionResult{}, fmt.Errorf("request run timeout: %w", ErrConflict)
	}
	run, err := s.GetRun(ctx, job.ActiveRunID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.RequestRunTimeout(job, run, requestedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if len(result.Events) == 0 {
		return result, nil
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// Pause records the original phase and returns a process-suspension effect when
// a target is active.
func (s *Store) Pause(
	ctx context.Context,
	jobID model.JobID,
	pausedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	var run *model.RunState
	if job.ActiveRunID != "" {
		value, getErr := s.GetRun(ctx, job.ActiveRunID)
		if getErr != nil {
			return model.TransitionResult{}, getErr
		}
		run = &value
	}
	result, prior, err := model.PauseJob(job, run, pausedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if len(result.Events) == 0 {
		return result, nil
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		_, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime SET revision = revision + 1,
			paused_from_phase = ?, paused_at_ns = ?, updated_at_ns = ?
			WHERE job_id = ?`, string(prior), pausedAt.UTC().UnixNano(), pausedAt.UTC().UnixNano(), jobID.String())
		return updateErr
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// Resume restores a paused job and accounts for paused monotonic budget time.
func (s *Store) Resume(
	ctx context.Context,
	jobID model.JobID,
	resumedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	runtime, err := s.GetRuntime(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if runtime.PausedAt == nil || runtime.PausedFrom == "" {
		return model.TransitionResult{}, fmt.Errorf("resume job %s: %w", jobID, ErrConflict)
	}
	var run *model.RunState
	if job.ActiveRunID != "" {
		value, getErr := s.GetRun(ctx, job.ActiveRunID)
		if getErr != nil {
			return model.TransitionResult{}, getErr
		}
		if value.Phase == model.RunPhasePaused {
			run = &value
		}
	}
	result, err := model.ResumeJob(job, run, runtime.PausedFrom, resumedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	pausedFor := resumedAt.UTC().Sub(*runtime.PausedAt)
	if pausedFor < 0 {
		return model.TransitionResult{}, errors.New("resume time precedes pause time")
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		_, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime SET revision = revision + 1,
			total_paused_ns = total_paused_ns + ?,
			paused_from_phase = NULL, paused_at_ns = NULL, updated_at_ns = ?
			WHERE job_id = ?`, pausedFor.Nanoseconds(), resumedAt.UTC().UnixNano(), jobID.String())
		return updateErr
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// SetDependencies replaces the edges for a job before it leaves submission.
// Selectors must already have been resolved to immutable canonical IDs.
func (s *Store) SetDependencies(
	ctx context.Context,
	jobID model.JobID,
	edges []Dependency,
) error {
	if !jobID.Valid() {
		return errors.New("set dependencies: invalid job ID")
	}
	return s.writeTransaction(ctx, "set job dependencies", func(tx *sql.Tx) error {
		job, err := getJobWithQueryer(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Phase != model.JobPhaseSubmitting {
			return fmt.Errorf("set dependencies for job %s: %w", jobID, ErrConflict)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM job_dependencies WHERE job_id = ?`, jobID.String()); err != nil {
			return err
		}

		return insertDependencies(ctx, tx, jobID, edges)
	})
}

func insertDependencies(ctx context.Context, tx *sql.Tx, jobID model.JobID, edges []Dependency) error {
	seen := make(map[string]DependencyPredicate, len(edges))
	for _, edge := range edges {
		if edge.JobID != "" && edge.JobID != jobID {
			return errors.New("set dependencies: edge job ID does not match")
		}
		if !edge.DependsOn.Valid() || edge.DependsOn == jobID || !edge.Predicate.Valid() {
			return errors.New("set dependencies: invalid edge")
		}
		key := edge.DependsOn.String()
		if previous, duplicate := seen[key]; duplicate {
			if previous == edge.Predicate {
				continue
			}
			return fmt.Errorf("set dependencies: contradictory predicates for %s", edge.DependsOn)
		}
		seen[key] = edge.Predicate
		if _, err := getJobWithQueryer(ctx, tx, edge.DependsOn); err != nil {
			return fmt.Errorf("load dependency %s: %w", edge.DependsOn, err)
		}
		var cycle int
		err := tx.QueryRowContext(ctx, `
				WITH RECURSIVE descendants(id) AS (
				    SELECT dependency_job_id FROM job_dependencies WHERE job_id = ?
				    UNION
				    SELECT d.dependency_job_id
				    FROM job_dependencies d JOIN descendants x ON d.job_id = x.id
				)
			SELECT EXISTS(SELECT 1 FROM descendants WHERE id = ?)`,
			edge.DependsOn.String(), jobID.String()).Scan(&cycle)
		if err != nil {
			return err
		}
		if cycle != 0 {
			return errors.New("set dependencies: dependency cycle detected")
		}
		if _, err := tx.ExecContext(ctx, `
				INSERT INTO job_dependencies(job_id, dependency_job_id, predicate)
				VALUES (?, ?, ?)`, jobID.String(), edge.DependsOn.String(), string(edge.Predicate)); err != nil {
			return fmt.Errorf("insert dependency: %w", classifySQLite("insert dependency", err))
		}
	}

	return nil
}

// ListDependencies returns immutable edges in canonical order.
func (s *Store) ListDependencies(ctx context.Context, jobID model.JobID) ([]Dependency, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT dependency_job_id, predicate, observed_revision, observed_outcome, satisfied_at_ns
		FROM job_dependencies WHERE job_id = ?
		ORDER BY dependency_job_id, predicate`, jobID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Dependency, 0)
	for rows.Next() {
		var dependencyText, predicate string
		var revision sql.NullInt64
		var outcome sql.NullString
		var satisfied sql.NullInt64
		if err := rows.Scan(&dependencyText, &predicate, &revision, &outcome, &satisfied); err != nil {
			return nil, err
		}
		dependencyID, err := model.ParseJobID(dependencyText)
		if err != nil {
			return nil, err
		}
		edge := Dependency{
			JobID: jobID, DependsOn: dependencyID, Predicate: DependencyPredicate(predicate),
			ObservedOutcome: model.JobOutcome(outcome.String), SatisfiedAt: optionalTime(satisfied),
		}
		if revision.Valid {
			edge.ObservedRevision, err = uintFromDatabase("dependency observed revision", revision.Int64)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, edge)
	}

	return result, rows.Err()
}

// EvaluateDependencies snapshots newly terminal dependencies and reports the
// combined status. Terminal observations are immutable once written.
//
//nolint:gocognit,cyclop,nestif // Dependency evaluation atomically reads, snapshots, and classifies every edge.
func (s *Store) EvaluateDependencies(
	ctx context.Context,
	jobID model.JobID,
	at time.Time,
) (DependencyStatus, error) {
	status := DependencyStatus{Ready: true}
	err := s.writeTransaction(ctx, "evaluate dependencies", func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT d.dependency_job_id, d.predicate, d.observed_revision,
			       d.observed_outcome, d.satisfied_at_ns,
			       j.phase, j.outcome, j.revision
			FROM job_dependencies d
			JOIN jobs j ON j.id = d.dependency_job_id
			WHERE d.job_id = ? ORDER BY d.dependency_job_id, d.predicate`, jobID.String())
		if err != nil {
			return err
		}
		defer rows.Close()
		type rowValue struct {
			id, predicate, phase string
			observedRevision     sql.NullInt64
			observedOutcome      sql.NullString
			satisfiedAt          sql.NullInt64
			currentOutcome       sql.NullString
			currentRevision      int64
		}
		values := make([]rowValue, 0)
		for rows.Next() {
			var value rowValue
			if scanErr := rows.Scan(&value.id, &value.predicate, &value.observedRevision, &value.observedOutcome,
				&value.satisfiedAt, &value.phase, &value.currentOutcome, &value.currentRevision); scanErr != nil {
				return scanErr
			}
			values = append(values, value)
		}
		if iterationErr := rows.Err(); iterationErr != nil {
			return iterationErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return closeErr
		}
		for _, value := range values {
			dependencyID, parseErr := model.ParseJobID(value.id)
			if parseErr != nil {
				return parseErr
			}
			edge := Dependency{JobID: jobID, DependsOn: dependencyID, Predicate: DependencyPredicate(value.predicate)}
			if value.observedRevision.Valid {
				revision, conversionErr := uintFromDatabase("dependency observed revision", value.observedRevision.Int64)
				if conversionErr != nil {
					return conversionErr
				}
				edge.ObservedRevision = revision
				edge.ObservedOutcome = model.JobOutcome(value.observedOutcome.String)
				edge.SatisfiedAt = optionalTime(value.satisfiedAt)
			} else if model.JobPhase(value.phase) == model.JobPhaseCompleted {
				revision, conversionErr := uintFromDatabase("dependency current revision", value.currentRevision)
				if conversionErr != nil {
					return conversionErr
				}
				edge.ObservedRevision = revision
				edge.ObservedOutcome = model.JobOutcome(value.currentOutcome.String)
				matched := edge.Predicate.Matches(edge.ObservedOutcome)
				var satisfied any
				if matched {
					edge.SatisfiedAt = timePointer(at)
					satisfied = at.UTC().UnixNano()
				}
				if _, updateErr := tx.ExecContext(ctx, `
					UPDATE job_dependencies SET observed_revision = ?, observed_outcome = ?, satisfied_at_ns = ?
					WHERE job_id = ? AND dependency_job_id = ? AND predicate = ? AND observed_revision IS NULL`,
					value.currentRevision, value.currentOutcome.String, satisfied,
					jobID.String(), value.id, value.predicate); updateErr != nil {
					return updateErr
				}
			}
			if edge.ObservedRevision == 0 {
				status.Ready = false
				status.Pending++
			} else if !edge.Predicate.Matches(edge.ObservedOutcome) {
				status.Ready = false
				status.Impossible = true
				status.Failed = append(status.Failed, edge)
			}
		}
		return nil
	})

	return status, err
}

// SetConcurrencyLimit configures a store-wide or named-pool capacity. A nil
// capacity means unlimited; named pools must still be declared explicitly.
func (s *Store) SetConcurrencyLimit(
	ctx context.Context,
	pool string,
	capacity *uint64,
	at time.Time,
) error {
	scope := "pool"
	if pool == "" {
		scope = "global"
	} else if strings.TrimSpace(pool) != pool || pool == "" {
		return errors.New("set concurrency limit: invalid pool name")
	}
	var stored any
	if capacity != nil {
		if *capacity == 0 {
			return errors.New("set concurrency limit: capacity must be positive")
		}
		value, err := databaseUint("concurrency capacity", *capacity)
		if err != nil {
			return err
		}
		stored = value
	}
	return s.writeTransaction(ctx, "set concurrency limit", func(tx *sql.Tx) error {
		if err := queuedRequestsFitLimit(ctx, tx, pool, capacity); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO concurrency_limits(scope_kind, scope_name, capacity, updated_at_ns)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(scope_kind, scope_name) DO UPDATE SET
			capacity = excluded.capacity, revision = concurrency_limits.revision + 1,
			updated_at_ns = excluded.updated_at_ns
			WHERE concurrency_limits.capacity IS NOT excluded.capacity`,
			scope, pool, stored, at.UTC().UnixNano())

		return classifySQLite("set concurrency limit", err)
	})
}

// SynchronizeConcurrencyLimits atomically replaces the durable global and
// named-pool configuration. Pools that are no longer declared are removed only
// when no active admission or queued request still references them.
func (s *Store) SynchronizeConcurrencyLimits(
	ctx context.Context,
	global *uint64,
	pools map[string]*uint64,
	at time.Time,
) error {
	if err := validateSynchronizedConcurrencyLimits(global, pools); err != nil {
		return err
	}

	return s.writeTransaction(ctx, "synchronize concurrency limits", func(tx *sql.Tx) error {
		return synchronizeConcurrencyLimits(ctx, tx, global, pools, at)
	})
}

func validateSynchronizedConcurrencyLimits(global *uint64, pools map[string]*uint64) error {
	if global != nil && *global == 0 {
		return errors.New("synchronize concurrency limits: global capacity must be positive")
	}
	for name, capacity := range pools {
		if name == "" || strings.TrimSpace(name) != name {
			return fmt.Errorf("synchronize concurrency limits: invalid pool name %q", name)
		}
		if capacity != nil && *capacity == 0 {
			return fmt.Errorf("synchronize concurrency limits: pool %q capacity must be positive", name)
		}
	}

	return nil
}

func synchronizeConcurrencyLimits(
	ctx context.Context,
	tx *sql.Tx,
	global *uint64,
	pools map[string]*uint64,
	at time.Time,
) error {
	if err := queuedRequestsFitLimit(ctx, tx, "", global); err != nil {
		return err
	}
	for name, capacity := range pools {
		if err := queuedRequestsFitLimit(ctx, tx, name, capacity); err != nil {
			return err
		}
	}
	if err := removeOmittedConcurrencyPools(ctx, tx, pools); err != nil {
		return err
	}
	if err := upsertConcurrencyLimit(ctx, tx, "global", "", global, at); err != nil {
		return err
	}
	for name, capacity := range pools {
		if err := upsertConcurrencyLimit(ctx, tx, "pool", name, capacity, at); err != nil {
			return err
		}
	}

	return nil
}

func removeOmittedConcurrencyPools(ctx context.Context, tx *sql.Tx, retained map[string]*uint64) error {
	existing, err := listConcurrencyPools(ctx, tx)
	if err != nil {
		return err
	}
	for _, name := range existing {
		if _, keep := retained[name]; keep {
			continue
		}
		var references int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM admissions WHERE pool_name = ? AND released_at_ns IS NULL
				UNION ALL
				SELECT 1 FROM admission_requests WHERE pool_name = ?
			)`, name, name).Scan(&references); err != nil {
			return err
		}
		if references != 0 {
			return fmt.Errorf("pool %q is still referenced by active or queued jobs: %w", name, ErrConflict)
		}
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM concurrency_limits WHERE scope_kind = 'pool' AND scope_name = ?`, name); err != nil {
			return err
		}
	}

	return nil
}

func listConcurrencyPools(ctx context.Context, tx *sql.Tx) (pools []string, returnedErr error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT scope_name FROM concurrency_limits WHERE scope_kind = 'pool' ORDER BY scope_name`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			returnedErr = errors.Join(returnedErr, closeErr)
		}
	}()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		pools = append(pools, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pools, nil
}

func upsertConcurrencyLimit(
	ctx context.Context,
	tx *sql.Tx,
	scope,
	name string,
	capacity *uint64,
	at time.Time,
) error {
	var stored any
	if capacity != nil {
		value, err := databaseUint("concurrency capacity", *capacity)
		if err != nil {
			return err
		}
		stored = value
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO concurrency_limits(scope_kind, scope_name, capacity, updated_at_ns)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(scope_kind, scope_name) DO UPDATE SET
		capacity = excluded.capacity,
		revision = concurrency_limits.revision + 1,
		updated_at_ns = excluded.updated_at_ns
		WHERE concurrency_limits.capacity IS NOT excluded.capacity`,
		scope, name, stored, at.UTC().UnixNano())

	return err
}

func queuedRequestsFitLimit(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	pool string,
	capacity *uint64,
) error {
	if capacity == nil {
		return nil
	}
	var largest sql.NullInt64
	query := `SELECT MAX(slots) FROM admission_requests`
	arguments := []any(nil)
	if pool != "" {
		query += ` WHERE pool_name = ?`
		arguments = append(arguments, pool)
	}
	if err := queryer.QueryRowContext(ctx, query, arguments...).Scan(&largest); err != nil {
		return fmt.Errorf("inspect queued admission requests: %w", err)
	}
	if !largest.Valid {
		return nil
	}
	largestSlots, err := nonnegativeUintFromDatabase("queued admission slots", largest.Int64)
	if err != nil {
		return err
	}
	if largestSlots <= *capacity {
		return nil
	}

	return fmt.Errorf(
		"%w: queued request needs %d slots but %s capacity would be %d",
		ErrAdmissionImpossible,
		largest.Int64,
		admissionScopeName(pool),
		*capacity,
	)
}

// TryAcquireAdmission atomically applies both global and optional pool limits.
// A failed capacity attempt keeps a durable FIFO position. A younger request
// may pass a blocked older request only a bounded number of times, and never
// when that older request currently fits.
func (s *Store) TryAcquireAdmission(
	ctx context.Context,
	jobID model.JobID,
	pool string,
	slots uint64,
	at time.Time,
	leaseDuration time.Duration,
) (Admission, error) {
	if slots == 0 || leaseDuration <= 0 {
		return Admission{}, errors.New("acquire admission: slots and lease duration must be positive")
	}
	if !jobID.Valid() {
		return Admission{}, errors.New("acquire admission: invalid job ID")
	}
	if strings.TrimSpace(pool) != pool {
		return Admission{}, errors.New("acquire admission: invalid pool name")
	}
	slotsDB, err := databaseUint("admission slots", slots)
	if err != nil {
		return Admission{}, err
	}
	at = at.UTC()
	desired := Admission{
		JobID: jobID, Pool: pool, Slots: slots, AcquiredAt: at, LeaseExpires: at.Add(leaseDuration),
	}
	var admission Admission
	var decisionErr error
	err = s.writeTransaction(ctx, "acquire admission", func(tx *sql.Tx) error {
		var acquireErr error
		admission, acquireErr = tryAcquireAdmissionTx(ctx, tx, desired, slotsDB)
		if errors.Is(acquireErr, ErrCapacity) {
			decisionErr = acquireErr

			return nil
		}

		return acquireErr
	})
	if err != nil {
		return Admission{}, err
	}
	if decisionErr != nil {
		return Admission{}, decisionErr
	}

	return admission, nil
}

type admissionCapacities struct {
	global *uint64
	pool   *uint64
}

type admissionBypass struct {
	request admissionRequest
	exists  bool
}

// ValidateAdmissionRequest verifies that a slot request refers to a declared
// pool and can fit every finite capacity that applies to it. It performs no
// mutation, so callers can reject impossible jobs before making them visible.
func (s *Store) ValidateAdmissionRequest(ctx context.Context, pool string, slots uint64) error {
	if slots == 0 {
		return errors.New("validate admission: slots must be positive")
	}
	if strings.TrimSpace(pool) != pool {
		return errors.New("validate admission: invalid pool name")
	}
	if _, err := databaseUint("admission slots", slots); err != nil {
		return err
	}
	if _, err := readAdmissionCapacities(ctx, s.db, pool, slots); err != nil {
		return fmt.Errorf("validate admission: %w", err)
	}

	return nil
}

func tryAcquireAdmissionTx(
	ctx context.Context,
	tx *sql.Tx,
	desired Admission,
	slots int64,
) (Admission, error) {
	job, err := getJobWithQueryer(ctx, tx, desired.JobID)
	if err != nil {
		return Admission{}, err
	}
	if job.Phase == model.JobPhaseCompleted {
		return Admission{}, fmt.Errorf(
			"acquire admission for completed job %s: %w", desired.JobID, ErrConflict,
		)
	}
	existing, found, err := activeAdmission(ctx, tx, desired.JobID)
	if err != nil {
		return Admission{}, err
	}
	if found {
		if existing.Pool != desired.Pool || existing.Slots != desired.Slots {
			return Admission{}, fmt.Errorf("active admission parameters changed: %w", ErrConflict)
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM admission_requests WHERE job_id = ?`, desired.JobID.String())

		return existing, err
	}

	capacities, err := readAdmissionCapacities(ctx, tx, desired.Pool, desired.Slots)
	if err != nil {
		return Admission{}, err
	}
	request, err := enqueueAdmissionRequest(ctx, tx, desired.JobID, desired.Pool, slots, desired.AcquiredAt)
	if err != nil {
		return Admission{}, err
	}
	if request.pool != desired.Pool || request.slots != desired.Slots {
		return Admission{}, fmt.Errorf("acquire admission parameters changed while queued: %w", ErrConflict)
	}
	bypass, err := olderRequestBypassedByAdmission(ctx, tx, request, desired, capacities)
	if err != nil {
		return Admission{}, err
	}
	if err := persistAdmission(ctx, tx, desired, slots, bypass); err != nil {
		return Admission{}, err
	}

	return desired, nil
}

func readAdmissionCapacities(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	pool string,
	slots uint64,
) (admissionCapacities, error) {
	global, _, err := readLimit(ctx, queryer, "global", "")
	if err != nil {
		return admissionCapacities{}, err
	}
	if global != nil && slots > *global {
		return admissionCapacities{}, fmt.Errorf(
			"%w: request needs %d slots but global capacity is %d",
			ErrAdmissionImpossible,
			slots,
			*global,
		)
	}
	capacities := admissionCapacities{global: global}
	if pool == "" {
		return capacities, nil
	}
	capacity, exists, err := readLimit(ctx, queryer, "pool", pool)
	if err != nil {
		return admissionCapacities{}, err
	}
	if !exists {
		return admissionCapacities{}, fmt.Errorf("acquire admission: pool %q is not configured", pool)
	}
	if capacity != nil && slots > *capacity {
		return admissionCapacities{}, fmt.Errorf(
			"%w: request needs %d slots but pool %q capacity is %d",
			ErrAdmissionImpossible,
			slots,
			pool,
			*capacity,
		)
	}
	capacities.pool = capacity

	return capacities, nil
}

func olderRequestBypassedByAdmission(
	ctx context.Context,
	queryer schemaQueryer,
	request admissionRequest,
	desired Admission,
	capacities admissionCapacities,
) (admissionBypass, error) {
	older, found, err := oldestCompetingAdmissionRequest(
		ctx,
		queryer,
		request,
		capacities.global != nil,
		desired.Pool,
		capacities.pool != nil,
	)
	if err != nil {
		return admissionBypass{}, err
	}
	if found {
		fits, fitErr := admissionRequestFits(ctx, queryer, older, capacities.global)
		if fitErr != nil {
			return admissionBypass{}, fitErr
		}
		if fits || older.bypassCount >= maxAdmissionBypasses {
			return admissionBypass{}, ErrCapacity
		}
	}
	fits, err := admissionFits(
		ctx, queryer, desired.Pool, desired.Slots, capacities.global, capacities.pool,
	)
	if err != nil {
		return admissionBypass{}, err
	}
	if !fits {
		return admissionBypass{}, ErrCapacity
	}

	return admissionBypass{request: older, exists: found}, nil
}

func persistAdmission(
	ctx context.Context,
	tx *sql.Tx,
	desired Admission,
	slots int64,
	bypass admissionBypass,
) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO admissions(job_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns, released_at_ns)
		VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT(job_id) DO UPDATE SET pool_name = excluded.pool_name,
		slots = excluded.slots, acquired_at_ns = excluded.acquired_at_ns,
		lease_expires_at_ns = excluded.lease_expires_at_ns, released_at_ns = NULL, run_id = NULL`,
		desired.JobID.String(), nullableString(desired.Pool), slots,
		desired.AcquiredAt.UnixNano(), desired.LeaseExpires.UnixNano())
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM admission_requests WHERE job_id = ?`, desired.JobID.String()); err != nil {
		return err
	}
	if !bypass.exists {
		return nil
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE admission_requests SET bypass_count = bypass_count + 1
		WHERE sequence = ?`, bypass.request.sequence)
	if err != nil {
		return err
	}

	return requireOneUpdate(
		result, "admission request", strconv.FormatInt(bypass.request.sequence, 10), 0, "queued",
	)
}

// BindAdmissionToRun records which invocation owns an acquired lease.
func (s *Store) BindAdmissionToRun(ctx context.Context, jobID model.JobID, runID model.RunID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE admissions SET run_id = ? WHERE job_id = ? AND released_at_ns IS NULL`,
		runID.String(), jobID.String())
	if err != nil {
		return err
	}
	return requireOneUpdate(result, "admission", jobID.String(), 0, "active")
}

// RenewAdmission extends an active slot lease.
func (s *Store) RenewAdmission(
	ctx context.Context,
	jobID model.JobID,
	at time.Time,
	leaseDuration time.Duration,
) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE admissions SET lease_expires_at_ns = ?
		WHERE job_id = ? AND released_at_ns IS NULL`,
		at.UTC().Add(leaseDuration).UnixNano(), jobID.String())
	if err != nil {
		return err
	}
	return requireOneUpdate(result, "admission", jobID.String(), 0, "active")
}

// ReleaseAdmission frees global and pool capacity. Repeated release is safe.
func (s *Store) ReleaseAdmission(ctx context.Context, jobID model.JobID, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE admissions SET released_at_ns = COALESCE(released_at_ns, ?)
		WHERE job_id = ?`, at.UTC().UnixNano(), jobID.String())

	return err
}

// GetAdmission loads the latest retained allocation for a job. The boolean is
// false when the job has never acquired capacity; ReleasedAt distinguishes a
// historical allocation from one that remains active.
func (s *Store) GetAdmission(ctx context.Context, jobID model.JobID) (Admission, bool, error) {
	if !jobID.Valid() {
		return Admission{}, false, errors.New("get admission: invalid job ID")
	}
	admission, err := scanAdmission(s.db.QueryRowContext(ctx, `
		SELECT run_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns, released_at_ns
		FROM admissions WHERE job_id = ?`, jobID.String()), jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return Admission{}, false, nil
	}
	if err != nil {
		return Admission{}, false, fmt.Errorf("get admission: %w", err)
	}

	return admission, true, nil
}

// ListExpiredAdmissions returns unreleased leases whose owner must be
// revalidated before they may continue consuming capacity. Lease expiry alone
// never frees slots because a delayed but still-live supervisor may retain
// ownership.
func (s *Store) ListExpiredAdmissions(ctx context.Context, at time.Time) ([]Admission, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, run_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns, released_at_ns
		FROM admissions
		WHERE released_at_ns IS NULL AND lease_expires_at_ns <= ?
		ORDER BY lease_expires_at_ns, job_id`, at.UTC().UnixNano())
	if err != nil {
		return nil, fmt.Errorf("list expired admissions: %w", err)
	}
	defer rows.Close()

	result := make([]Admission, 0)
	for rows.Next() {
		var jobIDText string
		var runIDText, pool sql.NullString
		var slots, acquiredAt, leaseExpires int64
		var releasedAt sql.NullInt64
		if err := rows.Scan(
			&jobIDText,
			&runIDText,
			&pool,
			&slots,
			&acquiredAt,
			&leaseExpires,
			&releasedAt,
		); err != nil {
			return nil, err
		}
		jobID, err := model.ParseJobID(jobIDText)
		if err != nil {
			return nil, fmt.Errorf("parse expired admission job ID: %w", err)
		}
		admission, err := admissionFromColumns(
			jobID,
			runIDText,
			pool,
			slots,
			acquiredAt,
			leaseExpires,
			releasedAt,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, admission)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list expired admissions: %w", err)
	}

	return result, nil
}

// ListExpiredOwnedJobs returns active jobs whose supervisor lease requires
// identity revalidation. It includes waiting, queued, and backoff jobs that do
// not yet hold an admission, because their durable fairness request or a
// dependency edge can otherwise block unrelated progress after owner death.
func (s *Store) ListExpiredOwnedJobs(ctx context.Context, at time.Time) ([]model.JobID, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT j.id
		FROM jobs j
		JOIN supervisors s ON s.id = j.supervisor_id
		WHERE j.phase != 'completed' AND s.released_at_ns IS NULL
		  AND s.lease_expires_at_ns <= ?
		ORDER BY s.lease_expires_at_ns, j.id`, at.UTC().UnixNano())
	if err != nil {
		return nil, fmt.Errorf("list expired job owners: %w", err)
	}
	defer rows.Close()

	result := make([]model.JobID, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		jobID, err := model.ParseJobID(encoded)
		if err != nil {
			return nil, fmt.Errorf("parse expired-owner job ID: %w", err)
		}
		result = append(result, jobID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list expired job owners: %w", err)
	}

	return result, nil
}

func activeAdmission(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	jobID model.JobID,
) (Admission, bool, error) {
	admission, err := scanAdmission(queryer.QueryRowContext(ctx, `
		SELECT run_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns, released_at_ns
		FROM admissions
		WHERE job_id = ? AND released_at_ns IS NULL`, jobID.String()), jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return Admission{}, false, nil
	}
	if err != nil {
		return Admission{}, false, fmt.Errorf("read active admission: %w", err)
	}

	return admission, true, nil
}

func scanAdmission(row rowScanner, jobID model.JobID) (Admission, error) {
	var runIDText, pool sql.NullString
	var slots, acquiredAt, leaseExpires int64
	var releasedAt sql.NullInt64
	if err := row.Scan(
		&runIDText,
		&pool,
		&slots,
		&acquiredAt,
		&leaseExpires,
		&releasedAt,
	); err != nil {
		return Admission{}, err
	}
	return admissionFromColumns(jobID, runIDText, pool, slots, acquiredAt, leaseExpires, releasedAt)
}

func admissionFromColumns(
	jobID model.JobID,
	runIDText sql.NullString,
	pool sql.NullString,
	slots int64,
	acquiredAt int64,
	leaseExpires int64,
	releasedAt sql.NullInt64,
) (Admission, error) {
	convertedSlots, err := uintFromDatabase("admission slots", slots)
	if err != nil {
		return Admission{}, err
	}
	var runID model.RunID
	if runIDText.Valid {
		runID, err = model.ParseRunID(runIDText.String)
		if err != nil {
			return Admission{}, fmt.Errorf("parse admission run ID: %w", err)
		}
	}

	return Admission{
		JobID:        jobID,
		RunID:        runID,
		Pool:         pool.String,
		Slots:        convertedSlots,
		AcquiredAt:   timeFromDatabase(acquiredAt),
		LeaseExpires: timeFromDatabase(leaseExpires),
		ReleasedAt:   optionalTime(releasedAt),
	}, nil
}

func enqueueAdmissionRequest(
	ctx context.Context,
	tx *sql.Tx,
	jobID model.JobID,
	pool string,
	slots int64,
	enqueuedAt time.Time,
) (admissionRequest, error) {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO admission_requests(job_id, pool_name, slots, enqueued_at_ns)
		SELECT ?, ?, ?,
		       CASE WHEN run_count = 0
		            THEN COALESCE(prerequisites_satisfied_at_ns, ?)
		            ELSE ? END
		FROM job_runtime WHERE job_id = ?
		ON CONFLICT(job_id) DO NOTHING`,
		jobID.String(), nullableString(pool), slots, enqueuedAt.UnixNano(), enqueuedAt.UnixNano(),
		jobID.String()); err != nil {
		return admissionRequest{}, fmt.Errorf("enqueue admission request: %w", err)
	}

	var request admissionRequest
	var storedPool sql.NullString
	var storedSlots, bypassCount int64
	err := tx.QueryRowContext(ctx, `
		SELECT sequence, job_id, enqueued_at_ns, pool_name, slots, bypass_count
		FROM admission_requests WHERE job_id = ?`, jobID.String()).Scan(
		&request.sequence,
		&request.jobID,
		&request.enqueuedAt,
		&storedPool,
		&storedSlots,
		&bypassCount,
	)
	if err != nil {
		return admissionRequest{}, fmt.Errorf("read queued admission request: %w", err)
	}
	request.pool = storedPool.String
	if request.slots, err = uintFromDatabase("queued admission slots", storedSlots); err != nil {
		return admissionRequest{}, err
	}
	if request.bypassCount, err = nonnegativeUintFromDatabase("admission bypass count", bypassCount); err != nil {
		return admissionRequest{}, err
	}

	return request, nil
}

func oldestCompetingAdmissionRequest(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	current admissionRequest,
	globalFinite bool,
	pool string,
	poolFinite bool,
) (admissionRequest, bool, error) {
	if !globalFinite && (!poolFinite || pool == "") {
		return admissionRequest{}, false, nil
	}
	globalFlag := 0
	if globalFinite {
		globalFlag = 1
	}
	poolFlag := 0
	if poolFinite && pool != "" {
		poolFlag = 1
	}
	var request admissionRequest
	var storedPool sql.NullString
	var storedSlots, bypassCount int64
	err := queryer.QueryRowContext(ctx, `
		SELECT sequence, job_id, enqueued_at_ns, pool_name, slots, bypass_count
		FROM admission_requests
		WHERE (enqueued_at_ns < ? OR (enqueued_at_ns = ? AND job_id < ?))
		  AND (? = 1 OR (? = 1 AND pool_name = ?))
		ORDER BY enqueued_at_ns, job_id
		LIMIT 1`, current.enqueuedAt, current.enqueuedAt, current.jobID, globalFlag, poolFlag, pool).Scan(
		&request.sequence,
		&request.jobID,
		&request.enqueuedAt,
		&storedPool,
		&storedSlots,
		&bypassCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return admissionRequest{}, false, nil
	}
	if err != nil {
		return admissionRequest{}, false, fmt.Errorf("read older admission request: %w", err)
	}
	request.pool = storedPool.String
	if request.slots, err = uintFromDatabase("queued admission slots", storedSlots); err != nil {
		return admissionRequest{}, false, err
	}
	if request.bypassCount, err = nonnegativeUintFromDatabase("admission bypass count", bypassCount); err != nil {
		return admissionRequest{}, false, err
	}

	return request, true, nil
}

func admissionRequestFits(
	ctx context.Context,
	queryer schemaQueryer,
	request admissionRequest,
	globalCapacity *uint64,
) (bool, error) {
	var poolCapacity *uint64
	if request.pool != "" {
		capacity, declared, err := readLimit(ctx, queryer, "pool", request.pool)
		if err != nil {
			return false, err
		}
		if !declared {
			return false, fmt.Errorf("queued admission references undeclared pool %q", request.pool)
		}
		poolCapacity = capacity
		if poolCapacity != nil && request.slots > *poolCapacity {
			return false, fmt.Errorf(
				"%w: queued request needs %d slots but pool %q capacity is %d",
				ErrAdmissionImpossible,
				request.slots,
				request.pool,
				*poolCapacity,
			)
		}
	}
	if globalCapacity != nil && request.slots > *globalCapacity {
		return false, fmt.Errorf(
			"%w: queued request needs %d slots but global capacity is %d",
			ErrAdmissionImpossible,
			request.slots,
			*globalCapacity,
		)
	}

	return admissionFits(ctx, queryer, request.pool, request.slots, globalCapacity, poolCapacity)
}

func admissionFits(
	ctx context.Context,
	queryer schemaQueryer,
	pool string,
	slots uint64,
	globalCapacity *uint64,
	poolCapacity *uint64,
) (bool, error) {
	if globalCapacity != nil {
		used, err := activeSlots(ctx, queryer, "", false)
		if err != nil {
			return false, err
		}
		if !capacityHasRoom(*globalCapacity, used, slots) {
			return false, nil
		}
	}
	if poolCapacity != nil {
		used, err := activeSlots(ctx, queryer, pool, true)
		if err != nil {
			return false, err
		}
		if !capacityHasRoom(*poolCapacity, used, slots) {
			return false, nil
		}
	}

	return true, nil
}

func capacityHasRoom(capacity, used, requested uint64) bool {
	return used <= capacity && requested <= capacity-used
}

func admissionScopeName(pool string) string {
	if pool == "" {
		return "global"
	}

	return fmt.Sprintf("pool %q", pool)
}

func readLimit(
	ctx context.Context,
	queryer interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	scope,
	name string,
) (capacity *uint64, exists bool, returnedErr error) {
	var value sql.NullInt64
	err := queryer.QueryRowContext(ctx, `
		SELECT capacity FROM concurrency_limits WHERE scope_kind = ? AND scope_name = ?`, scope, name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !value.Valid {
		return nil, true, nil
	}
	converted, err := uintFromDatabase("concurrency capacity", value.Int64)
	return &converted, true, err
}

func activeSlots(ctx context.Context, queryer schemaQueryer, pool string, filterPool bool) (uint64, error) {
	var value int64
	err := queryer.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(slots), 0) FROM admissions
		WHERE released_at_ns IS NULL
		  AND (? = 0 OR pool_name = ?)`, filterPool, pool).Scan(&value)
	if err != nil {
		return 0, err
	}

	return nonnegativeUintFromDatabase("active admission slots", value)
}

// Valid reports whether predicate is one of the canonical dependency predicates.
func (predicate DependencyPredicate) Valid() bool {
	switch predicate {
	case DependencySuccess, DependencyFinish, DependencyFailed, DependencyTimedOut,
		DependencyCancelled, DependencyAborted, DependencyLost, DependencySubmissionFailed:
		return true
	default:
		_, ok := predicate.outcomeSet()
		return ok
	}
}

// Matches reports whether an immutable terminal outcome satisfies a predicate.
func (predicate DependencyPredicate) Matches(outcome model.JobOutcome) bool {
	if outcomes, ok := predicate.outcomeSet(); ok {
		return slices.Contains(outcomes, outcome)
	}
	if predicate == DependencyFinish {
		return outcome.Valid()
	}
	if predicate == DependencyFailed {
		return outcome == model.JobOutcomeFailure
	}

	return string(predicate) == string(outcome)
}

func (predicate DependencyPredicate) outcomeSet() ([]model.JobOutcome, bool) {
	encoded, found := strings.CutPrefix(string(predicate), dependencyOutcomeSetPrefix)
	if !found || encoded == "" {
		return nil, false
	}
	parts := strings.Split(encoded, ",")
	outcomes := make([]model.JobOutcome, 0, len(parts))
	previous := ""
	for _, part := range parts {
		outcome := model.JobOutcome(part)
		if !outcome.Valid() || outcome == "" || part <= previous {
			return nil, false
		}
		previous = part
		outcomes = append(outcomes, outcome)
	}

	return outcomes, true
}

func (s *Store) commitTransitionWithRuntime(
	ctx context.Context,
	result model.TransitionResult,
	updateRuntime func(*sql.Tx) error,
) error {
	if err := validateTransition(result); err != nil {
		return err
	}
	events, err := s.completeEvents(result.Events)
	if err != nil {
		return err
	}

	return s.writeTransaction(ctx, "state and runtime transition", func(tx *sql.Tx) error {
		if err := applyJobTransition(ctx, tx, result, false); err != nil {
			return err
		}
		if err := applyRunTransition(ctx, tx, result); err != nil {
			return err
		}
		if err := applySupervisorTransition(ctx, tx, result); err != nil {
			return err
		}
		if err := updateRuntime(tx); err != nil {
			return fmt.Errorf("update job runtime: %w", classifySQLite("update job runtime", err))
		}

		return insertEvents(ctx, tx, events)
	})
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC().Round(0)
	return &value
}

func nonnegativeUintFromDatabase(field string, value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("persisted %s must not be negative", field)
	}

	return uint64(value), nil
}

func nonnegativeIntFromDatabase(field string, value int64) (int, error) {
	if value < 0 || (strconv.IntSize == 32 && value > 1<<31-1) {
		return 0, fmt.Errorf("persisted %s is outside the supported integer range", field)
	}

	return int(value), nil
}
