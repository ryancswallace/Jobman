package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/model"
)

const (
	defaultDiagnosticRuns          = 100
	defaultDiagnosticEvents        = 500
	defaultDiagnosticNotifications = 200
	maximumDiagnosticSimilar       = 20
)

// DiagnosticSnapshotLimits bounds one transactionally consistent evidence
// query. SelectedRun may be a positive run number or negative index.
type DiagnosticSnapshotLimits struct {
	SelectedRun      int64
	MaxRuns          int
	MaxEvents        int
	MaxNotifications int
	MaxSimilar       int
}

// DiagnosticEvent is the complete safe state-event envelope used by evidence
// collection. Details remains typed JSON and may contain only durable event
// metadata already admitted by the model boundary.
type DiagnosticEvent struct {
	ID             model.EventID
	JobID          model.JobID
	RunID          model.RunID
	SupervisorID   model.SupervisorID
	Entity         model.EntityKind
	EntityID       string
	Type           model.EventType
	FromPhase      string
	ToPhase        string
	FromOutcome    string
	ToOutcome      string
	EntityRevision uint64
	OccurredAt     time.Time
	Details        json.RawMessage
}

// DiagnosticSnapshot contains all core metadata read from one SQLite snapshot.
type DiagnosticSnapshot struct {
	Job                        model.JobState
	Runs                       []model.RunState
	Runtime                    JobRuntime
	Dependencies               []Dependency
	WaitEvaluations            []WaitEvaluation
	Admission                  *Admission
	NotificationDeliveries     []NotificationDelivery
	NotificationAttempts       []NotificationAttempt
	Events                     []DiagnosticEvent
	RunFacts                   map[model.RunID]RunDiagnosticFacts
	SimilarFailures            []diagnostic.SimilarFailure
	TotalRuns                  uint64
	RunsTruncated              bool
	EventsTruncated            bool
	NotificationsTruncated     bool
	SimilarTruncated           bool
	SimilarityAvailable        bool
	SimilarityPartiallyIndexed bool
}

// SchemaVersion returns the persisted schema version understood by this store.
func (*Store) SchemaVersion() int { return currentSchemaVersion }

// GetDiagnosticSnapshot resolves a selector and reads every evidence metadata
// domain from one read-only transaction. It performs no reconciliation or
// filesystem I/O.
//
//nolint:gocognit,cyclop // Snapshot assembly intentionally keeps every read in one transaction.
func (s *Store) GetDiagnosticSnapshot(
	ctx context.Context,
	selector string,
	limits DiagnosticSnapshotLimits,
) (snapshot DiagnosticSnapshot, returnedErr error) {
	limits, err := normalizeDiagnosticLimits(limits)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DiagnosticSnapshot{}, fmt.Errorf("begin diagnostic snapshot: %w", classifySQLite("begin diagnostic snapshot", err))
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("rollback diagnostic snapshot: %w", rollbackErr))
		}
	}()

	snapshot.Job, err = resolveJobWithQueryer(ctx, tx, selector)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	snapshot.Runs, snapshot.TotalRuns, snapshot.RunsTruncated, err = diagnosticRuns(
		ctx, tx, snapshot.Job.ID, limits.SelectedRun, limits.MaxRuns,
	)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	snapshot.RunFacts, err = diagnosticRunFacts(ctx, tx, snapshot.Runs)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	if limits.MaxSimilar > 0 {
		anchor := diagnosticAnchorRun(snapshot.Job, snapshot.Runs, limits.SelectedRun, snapshot.TotalRuns)
		similarity, similarityErr := diagnosticSimilarFailures(ctx, tx, anchor, snapshot.RunFacts, limits.MaxSimilar)
		if similarityErr != nil {
			return DiagnosticSnapshot{}, similarityErr
		}
		snapshot.SimilarFailures = similarity.Failures
		snapshot.SimilarityAvailable = similarity.Available
		snapshot.SimilarTruncated = similarity.Truncated
		snapshot.SimilarityPartiallyIndexed, err = diagnosticSimilarityPartiallyIndexed(ctx, tx)
		if err != nil {
			return DiagnosticSnapshot{}, err
		}
	}
	snapshot.Runtime, err = getRuntimeWithQueryer(ctx, tx, snapshot.Job.ID)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	snapshot.Dependencies, err = diagnosticDependencies(ctx, tx, snapshot.Job.ID)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	snapshot.WaitEvaluations, err = diagnosticWaitEvaluations(ctx, tx, snapshot.Job.ID)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	admission, found, err := diagnosticAdmission(ctx, tx, snapshot.Job.ID)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	if found {
		snapshot.Admission = &admission
	}
	snapshot.NotificationDeliveries, snapshot.NotificationAttempts,
		snapshot.NotificationsTruncated, err = diagnosticNotifications(
		ctx, tx, snapshot.Job.ID, limits.MaxNotifications,
	)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	snapshot.Events, snapshot.EventsTruncated, err = diagnosticEvents(
		ctx, tx, snapshot.Job.ID, limits.MaxEvents,
	)
	if err != nil {
		return DiagnosticSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return DiagnosticSnapshot{}, fmt.Errorf("commit diagnostic snapshot: %w", classifySQLite("commit diagnostic snapshot", err))
	}

	return snapshot, nil
}

func normalizeDiagnosticLimits(limits DiagnosticSnapshotLimits) (DiagnosticSnapshotLimits, error) {
	if limits.MaxRuns == 0 {
		limits.MaxRuns = defaultDiagnosticRuns
	}
	if limits.MaxEvents == 0 {
		limits.MaxEvents = defaultDiagnosticEvents
	}
	if limits.MaxNotifications == 0 {
		limits.MaxNotifications = defaultDiagnosticNotifications
	}
	if limits.MaxRuns < 1 || limits.MaxRuns > 1000 || limits.MaxEvents < 1 || limits.MaxEvents > 5000 ||
		limits.MaxNotifications < 1 || limits.MaxNotifications > 2000 ||
		limits.MaxSimilar < 0 || limits.MaxSimilar > maximumDiagnosticSimilar {
		return DiagnosticSnapshotLimits{}, errors.New("diagnostic snapshot limits are outside supported bounds")
	}

	return limits, nil
}

//nolint:gocritic // Named results would obscure the several independent snapshot values.
func diagnosticRuns(
	ctx context.Context,
	queryer schemaQueryer,
	jobID model.JobID,
	selected int64,
	maximum int,
) ([]model.RunState, uint64, bool, error) {
	var count int64
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs WHERE job_id = ?", jobID.String()).Scan(&count); err != nil {
		return nil, 0, false, err
	}
	total, err := nonnegativeUintFromDatabase("diagnostic run count", count)
	if err != nil {
		return nil, 0, false, err
	}
	selectedNumber, err := selectedRunNumber(selected, total)
	if err != nil {
		return nil, 0, false, err
	}
	rows, err := queryer.QueryContext(ctx,
		"SELECT "+runColumns+" FROM runs WHERE job_id = ? ORDER BY run_number DESC LIMIT ?",
		jobID.String(), maximum,
	)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()
	runs := make([]model.RunState, 0, maximum+1)
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, 0, false, scanErr
		}
		runs = append(runs, run)
	}
	iterationErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(iterationErr, closeErr); err != nil {
		return nil, 0, false, err
	}
	slices.Reverse(runs)
	if selectedNumber != 0 && !containsRunNumber(runs, selectedNumber) {
		run, err := scanRun(queryer.QueryRowContext(ctx,
			"SELECT "+runColumns+" FROM runs WHERE job_id = ? AND run_number = ?",
			jobID.String(), selectedNumber,
		))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, false, fmt.Errorf("select run %d: %w", selectedNumber, ErrNotFound)
		}
		if err != nil {
			return nil, 0, false, err
		}
		runs = append(runs, run)
		slices.SortFunc(runs, func(left, right model.RunState) int {
			if left.Number < right.Number {
				return -1
			}
			if left.Number > right.Number {
				return 1
			}
			return 0
		})
	}

	maximumRuns := uint64(maximum) // #nosec G115 -- normalizeDiagnosticLimits bounds maximum to a positive small integer.

	return runs, total, total > maximumRuns, nil
}

func selectedRunNumber(selected int64, total uint64) (uint64, error) {
	if selected == 0 {
		return 0, nil
	}
	if selected > 0 {
		return uint64(selected), nil
	}
	if total > math.MaxInt64 {
		return 0, errors.New("select run: run count exceeds supported range")
	}
	index := int64(total) + selected + 1
	if index < 1 {
		return 0, fmt.Errorf("select run %d: %w", selected, ErrNotFound)
	}

	return uint64(index), nil
}

func containsRunNumber(runs []model.RunState, number uint64) bool {
	for _, run := range runs {
		if run.Number == number {
			return true
		}
	}

	return false
}

func diagnosticDependencies(ctx context.Context, queryer schemaQueryer, jobID model.JobID) ([]Dependency, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT dependency_job_id, predicate, observed_revision, observed_outcome, satisfied_at_ns
		FROM job_dependencies WHERE job_id = ? ORDER BY dependency_job_id, predicate`, jobID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Dependency, 0)
	for rows.Next() {
		var dependencyText, predicate string
		var revision, satisfied sql.NullInt64
		var outcome sql.NullString
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

func diagnosticWaitEvaluations(ctx context.Context, queryer schemaQueryer, jobID model.JobID) ([]WaitEvaluation, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT condition_index, condition_kind, evaluated_at_ns, satisfied_at_ns,
		       attempt_count, last_diagnostic_code
		FROM wait_evaluations WHERE job_id = ? ORDER BY condition_index`, jobID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]WaitEvaluation, 0)
	for rows.Next() {
		var index, attempts int64
		var kind string
		var evaluatedAt, satisfiedAt sql.NullInt64
		var code sql.NullString
		if err := rows.Scan(&index, &kind, &evaluatedAt, &satisfiedAt, &attempts, &code); err != nil {
			return nil, err
		}
		convertedIndex, err := nonnegativeIntFromDatabase("wait condition index", index)
		if err != nil {
			return nil, err
		}
		convertedAttempts, err := nonnegativeUintFromDatabase("wait attempt count", attempts)
		if err != nil {
			return nil, err
		}
		conditionKind := model.WaitConditionKind(kind)
		if !validWaitConditionKind(conditionKind) {
			return nil, fmt.Errorf("invalid wait condition kind %q", kind)
		}
		result = append(result, WaitEvaluation{
			JobID: jobID, ConditionIndex: convertedIndex,
			ConditionKind: conditionKind, EvaluatedAt: optionalTime(evaluatedAt), SatisfiedAt: optionalTime(satisfiedAt),
			AttemptCount: convertedAttempts, LastDiagnosticCode: code.String,
		})
	}

	return result, rows.Err()
}

func diagnosticAdmission(ctx context.Context, queryer schemaQueryer, jobID model.JobID) (Admission, bool, error) {
	admission, err := scanAdmission(queryer.QueryRowContext(ctx, `
		SELECT run_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns, released_at_ns
		FROM admissions WHERE job_id = ?`, jobID.String()), jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return Admission{}, false, nil
	}

	return admission, err == nil, err
}

func diagnosticNotifications(
	ctx context.Context,
	queryer schemaQueryer,
	jobID model.JobID,
	maximum int,
) ([]NotificationDelivery, []NotificationAttempt, bool, error) {
	deliveries, moreDeliveries, err := diagnosticDeliveries(ctx, queryer, jobID, maximum)
	if err != nil {
		return nil, nil, false, err
	}
	attempts, moreAttempts, err := diagnosticAttempts(ctx, queryer, jobID, maximum)

	return deliveries, attempts, moreDeliveries || moreAttempts, err
}

func diagnosticDeliveries(ctx context.Context, queryer schemaQueryer, jobID model.JobID, maximum int) ([]NotificationDelivery, bool, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT "+notificationDeliveryColumns+`
		FROM notification_deliveries WHERE job_id = ?
		ORDER BY created_at_ns, event_id, notifier_name LIMIT ?`, jobID.String(), maximum+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	values := make([]NotificationDelivery, 0, maximum)
	more := false
	for rows.Next() {
		value, err := scanNotificationDelivery(rows)
		if err != nil {
			return nil, false, err
		}
		if len(values) < maximum {
			values = append(values, value)
		} else {
			more = true
		}
	}

	return values, more, rows.Err()
}

func diagnosticAttempts(ctx context.Context, queryer schemaQueryer, jobID model.JobID, maximum int) ([]NotificationAttempt, bool, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT "+notificationAttemptColumns+`
		FROM notification_attempts WHERE job_id = ?
		ORDER BY created_at_ns, event_id, notifier_name, attempt_number LIMIT ?`, jobID.String(), maximum+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	values := make([]NotificationAttempt, 0, maximum)
	more := false
	for rows.Next() {
		value, err := scanNotificationAttempt(rows)
		if err != nil {
			return nil, false, err
		}
		if len(values) < maximum {
			values = append(values, value)
		} else {
			more = true
		}
	}
	return values, more, rows.Err()
}

func diagnosticEvents(ctx context.Context, queryer schemaQueryer, jobID model.JobID, maximum int) ([]DiagnosticEvent, bool, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT id, job_id, run_id, supervisor_id, entity_kind, entity_id,
		       event_type, from_phase, to_phase, from_outcome, to_outcome,
		       entity_revision, occurred_at_ns, details_json
		FROM state_events WHERE job_id = ?
		ORDER BY occurred_at_ns DESC, id DESC LIMIT ?`, jobID.String(), maximum+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	values := make([]DiagnosticEvent, 0, maximum)
	more := false
	for rows.Next() {
		value, err := scanDiagnosticEvent(rows)
		if err != nil {
			return nil, false, err
		}
		if len(values) < maximum {
			values = append(values, value)
		} else {
			more = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	slices.Reverse(values)

	return values, more, nil
}

func scanDiagnosticEvent(row rowScanner) (DiagnosticEvent, error) {
	var idText, jobIDText, entityKind, entityID, eventType, toPhase, details string
	var runIDText, supervisorIDText, fromPhase, fromOutcome, toOutcome sql.NullString
	var revision, occurredAt int64
	if err := row.Scan(&idText, &jobIDText, &runIDText, &supervisorIDText, &entityKind, &entityID,
		&eventType, &fromPhase, &toPhase, &fromOutcome, &toOutcome, &revision, &occurredAt, &details); err != nil {
		return DiagnosticEvent{}, err
	}
	id, err := model.ParseEventID(idText)
	if err != nil {
		return DiagnosticEvent{}, err
	}
	jobID, err := model.ParseJobID(jobIDText)
	if err != nil {
		return DiagnosticEvent{}, err
	}
	value := DiagnosticEvent{
		ID: id, JobID: jobID, Entity: model.EntityKind(entityKind), EntityID: entityID,
		Type: model.EventType(eventType), FromPhase: fromPhase.String, ToPhase: toPhase,
		FromOutcome: fromOutcome.String, ToOutcome: toOutcome.String, OccurredAt: timeFromDatabase(occurredAt),
		Details: json.RawMessage(details),
	}
	value.EntityRevision, err = uintFromDatabase("event revision", revision)
	if err != nil {
		return DiagnosticEvent{}, err
	}
	if runIDText.Valid {
		value.RunID, err = model.ParseRunID(runIDText.String)
		if err != nil {
			return DiagnosticEvent{}, err
		}
	}
	if supervisorIDText.Valid {
		value.SupervisorID, err = model.ParseSupervisorID(supervisorIDText.String)
		if err != nil {
			return DiagnosticEvent{}, err
		}
	}
	if !json.Valid(value.Details) {
		return DiagnosticEvent{}, errors.New("event details are invalid JSON")
	}

	return value, nil
}
