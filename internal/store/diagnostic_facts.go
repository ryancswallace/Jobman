package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/failureclass"
	"github.com/ryancswallace/jobman/internal/fingerprint"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
)

const (
	runDiagnosticFactsSchemaVersion = 1
	maximumResourceObservations     = 16
	maximumResourceJSONBytes        = 16 * 1024
)

// RunDiagnosticFactsInput carries post-wait observations and the policy
// meaning assigned to one invocation. It contains no raw command or log data.
type RunDiagnosticFactsInput struct {
	Resources         []diagnostic.ResourceObservation
	PolicyDisposition policy.RunClassification
}

// RunDiagnosticFacts is the typed per-run row exposed to the evidence
// snapshot reader. Fingerprints are store-local and never include their key.
type RunDiagnosticFacts struct {
	SchemaVersion int
	Resources     []diagnostic.ResourceObservation
	FailureClass  string
	Fingerprint   *diagnostic.FailureFingerprint
	RecordedAt    time.Time
}

// Validate checks the safe persisted facts boundary.
func (facts RunDiagnosticFacts) Validate() error {
	if facts.SchemaVersion != runDiagnosticFactsSchemaVersion {
		return errors.New("validate run diagnostic facts: unsupported schema version")
	}
	if facts.RecordedAt.IsZero() || facts.RecordedAt.Location() != time.UTC {
		return errors.New("validate run diagnostic facts: recorded time must be nonzero UTC")
	}
	if facts.Resources == nil {
		return errors.New("validate run diagnostic facts: resources must be present")
	}
	if len(facts.Resources) > maximumResourceObservations {
		return errors.New("validate run diagnostic facts: too many resource observations")
	}
	priorMetric := ""
	for _, observation := range facts.Resources {
		if err := observation.Validate(); err != nil {
			return err
		}
		if observation.Metric <= priorMetric {
			return errors.New("validate run diagnostic facts: resource metrics must be sorted and unique")
		}
		priorMetric = observation.Metric
	}
	if facts.FailureClass != "" && !validFactCode(facts.FailureClass) {
		return errors.New("validate run diagnostic facts: invalid failure class")
	}
	if facts.Fingerprint != nil {
		if facts.FailureClass == "" {
			return errors.New("validate run diagnostic facts: fingerprint requires a failure class")
		}
		if err := facts.Fingerprint.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) buildRunDiagnosticFacts(
	job model.JobState,
	run model.RunState,
	input RunDiagnosticFactsInput,
	completedAt time.Time,
) (RunDiagnosticFacts, error) {
	if !validPolicyDisposition(input.PolicyDisposition) {
		return RunDiagnosticFacts{}, fmt.Errorf("build run diagnostic facts: invalid policy disposition %q", input.PolicyDisposition)
	}
	resources := slices.Clone(input.Resources)
	if resources == nil {
		resources = []diagnostic.ResourceObservation{}
	}
	slices.SortFunc(resources, func(left, right diagnostic.ResourceObservation) int {
		return strings.Compare(left.Metric, right.Metric)
	})
	classification := failureclass.Run(run)
	facts := RunDiagnosticFacts{
		SchemaVersion: runDiagnosticFactsSchemaVersion,
		Resources:     resources,
		FailureClass:  classification.Class,
		RecordedAt:    completedAt.UTC().Round(0),
	}
	executable := run.ResolvedExecutable
	if executable == "" {
		executable = job.Spec.Executable()
	}
	var exitCode *int
	signal := ""
	platformReason := ""
	if run.Exit != nil {
		exitCode = run.Exit.ExitCode
		signal = run.Exit.Signal
		platformReason = run.Exit.PlatformReason
	}
	timeoutScope := ""
	if run.StopReason == model.StopReasonTimeout || run.Outcome == model.RunOutcomeTimedOut {
		timeoutScope = "run"
		if run.LastDiagnosticCode == string(model.DiagnosticJobTimeout) {
			timeoutScope = "job"
		}
	}
	value, available, err := fingerprint.Build(s.fingerprintKey[:], fingerprint.Input{
		ExecutableIdentity: executable,
		Outcome:            string(run.Outcome),
		FailureClass:       facts.FailureClass,
		DiagnosticCode:     run.LastDiagnosticCode,
		ExitCode:           exitCode,
		Signal:             signal,
		PlatformReason:     platformReason,
		TimeoutScope:       timeoutScope,
		PolicyDisposition:  string(input.PolicyDisposition),
	})
	if err != nil {
		return RunDiagnosticFacts{}, err
	}
	if available {
		facts.Fingerprint = &value
	}
	if err := facts.Validate(); err != nil {
		return RunDiagnosticFacts{}, err
	}

	return facts, nil
}

func validPolicyDisposition(value policy.RunClassification) bool {
	return value == policy.RunClassificationSuccess ||
		value == policy.RunClassificationRetryableFailure ||
		value == policy.RunClassificationNonRetryableFailure
}

func validFactCode(value string) bool {
	if value == "" || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' {
			continue
		}

		return false
	}

	return true
}

func persistRunDiagnosticFacts(
	ctx context.Context,
	tx *sql.Tx,
	runID model.RunID,
	facts RunDiagnosticFacts,
) error {
	if err := facts.Validate(); err != nil {
		return err
	}
	resources, err := json.Marshal(facts.Resources)
	if err != nil {
		return fmt.Errorf("encode run diagnostic resources: %w", err)
	}
	if len(resources) > maximumResourceJSONBytes {
		return errors.New("encode run diagnostic resources: encoded value exceeds storage limit")
	}
	var algorithm any
	var inputVersion any
	var value any
	if facts.Fingerprint != nil {
		decoded, decodeErr := hex.DecodeString(facts.Fingerprint.Value)
		if decodeErr != nil {
			return fmt.Errorf("decode run failure fingerprint: %w", decodeErr)
		}
		algorithm = facts.Fingerprint.Algorithm
		inputVersion = facts.Fingerprint.InputSchemaVersion
		value = decoded
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_diagnostic_facts(
			run_id, schema_version, resources_json, failure_class,
			fingerprint_algorithm, fingerprint_input_version, fingerprint_value, recorded_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		runID.String(), facts.SchemaVersion, string(resources), nullableString(facts.FailureClass),
		algorithm, inputVersion, value, facts.RecordedAt.UnixNano(),
	); err != nil {
		return fmt.Errorf("persist run diagnostic facts: %w", classifySQLite("persist run diagnostic facts", err))
	}

	return nil
}

func diagnosticRunFacts(
	ctx context.Context,
	queryer schemaQueryer,
	runs []model.RunState,
) (map[model.RunID]RunDiagnosticFacts, error) {
	result := make(map[model.RunID]RunDiagnosticFacts)
	if len(runs) == 0 {
		return result, nil
	}
	arguments := make([]any, 0, len(runs))
	placeholders := make([]string, 0, len(runs))
	for _, run := range runs {
		arguments = append(arguments, run.ID.String())
		placeholders = append(placeholders, "?")
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT run_id, schema_version, resources_json, failure_class,
		       fingerprint_algorithm, fingerprint_input_version, fingerprint_value, recorded_at_ns
		FROM run_diagnostic_facts
		WHERE run_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY run_id`, arguments...)
	if err != nil {
		return nil, fmt.Errorf("read run diagnostic facts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		runID, facts, scanErr := scanRunDiagnosticFacts(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result[runID] = facts
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run diagnostic facts: %w", err)
	}

	return result, nil
}

func scanRunDiagnosticFacts(row rowScanner) (model.RunID, RunDiagnosticFacts, error) {
	var runIDText, resourcesJSON string
	var schemaVersion, recordedAt int64
	var failureClass, algorithm sql.NullString
	var inputVersion sql.NullInt64
	var fingerprintValue []byte
	if err := row.Scan(
		&runIDText, &schemaVersion, &resourcesJSON, &failureClass,
		&algorithm, &inputVersion, &fingerprintValue, &recordedAt,
	); err != nil {
		return "", RunDiagnosticFacts{}, err
	}
	runID, err := model.ParseRunID(runIDText)
	if err != nil {
		return "", RunDiagnosticFacts{}, err
	}
	if len(resourcesJSON) > maximumResourceJSONBytes {
		return "", RunDiagnosticFacts{}, errors.New("decode run diagnostic resources: encoded value exceeds storage limit")
	}
	resources, err := decodeResourceObservations(resourcesJSON)
	if err != nil {
		return "", RunDiagnosticFacts{}, err
	}
	facts := RunDiagnosticFacts{
		SchemaVersion: int(schemaVersion),
		Resources:     resources,
		FailureClass:  failureClass.String,
		RecordedAt:    timeFromDatabase(recordedAt),
	}
	fingerprintPresent := algorithm.Valid || inputVersion.Valid || len(fingerprintValue) != 0
	if fingerprintPresent {
		if !algorithm.Valid || !inputVersion.Valid || len(fingerprintValue) != fingerprintKeyBytes {
			return "", RunDiagnosticFacts{}, errors.New("decode run diagnostic facts: incomplete fingerprint")
		}
		input, err := nonnegativeIntFromDatabase("fingerprint input version", inputVersion.Int64)
		if err != nil {
			return "", RunDiagnosticFacts{}, err
		}
		facts.Fingerprint = &diagnostic.FailureFingerprint{
			Algorithm:          algorithm.String,
			InputSchemaVersion: input,
			Value:              hex.EncodeToString(fingerprintValue),
			Scope:              diagnostic.FingerprintScopeStoreLocal,
		}
	}
	if err := facts.Validate(); err != nil {
		return "", RunDiagnosticFacts{}, err
	}

	return runID, facts, nil
}

func decodeResourceObservations(encoded string) ([]diagnostic.ResourceObservation, error) {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var resources []diagnostic.ResourceObservation
	if err := decoder.Decode(&resources); err != nil {
		return nil, fmt.Errorf("decode run diagnostic resources: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode run diagnostic resources: trailing JSON value")
	}
	if resources == nil {
		return nil, errors.New("decode run diagnostic resources: expected an array")
	}

	return resources, nil
}

//nolint:gocognit // Anchor precedence mirrors the public evidence run-selection contract.
func diagnosticAnchorRun(
	job model.JobState,
	runs []model.RunState,
	selected int64,
	total uint64,
) model.RunState {
	if selected != 0 {
		number, err := selectedRunNumber(selected, total)
		if err == nil {
			for _, run := range runs {
				if run.Number == number {
					return run
				}
			}
		}

		return model.RunState{}
	}
	if job.ActiveRunID.Valid() {
		for _, run := range runs {
			if run.ID == job.ActiveRunID {
				return run
			}
		}
	}
	for index := len(runs) - 1; index >= 0; index-- {
		if runs[index].Outcome != "" && runs[index].Outcome != model.RunOutcomeSuccess {
			return runs[index]
		}
	}
	if len(runs) != 0 {
		return runs[len(runs)-1]
	}

	return model.RunState{}
}

type diagnosticSimilarityResult struct {
	Failures  []diagnostic.SimilarFailure
	Available bool
	Truncated bool
}

func diagnosticSimilarFailures(
	ctx context.Context,
	queryer schemaQueryer,
	anchor model.RunState,
	facts map[model.RunID]RunDiagnosticFacts,
	maximum int,
) (diagnosticSimilarityResult, error) {
	anchorFacts, found := facts[anchor.ID]
	if !found || anchorFacts.Fingerprint == nil {
		return diagnosticSimilarityResult{Failures: []diagnostic.SimilarFailure{}}, nil
	}
	value, err := hex.DecodeString(anchorFacts.Fingerprint.Value)
	if err != nil {
		return diagnosticSimilarityResult{}, fmt.Errorf("query similar failures: decode anchor fingerprint: %w", err)
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT r.job_id, r.id, r.run_number, r.completed_at_ns, r.outcome,
		       f.failure_class, f.fingerprint_algorithm, f.fingerprint_input_version,
		       f.fingerprint_value,
		       EXISTS(
		           SELECT 1 FROM runs later
		           WHERE later.job_id = r.job_id AND later.run_number > r.run_number
		             AND later.phase = 'completed' AND later.outcome = 'success'
		       )
		FROM run_diagnostic_facts f
		JOIN runs r ON r.id = f.run_id
		WHERE f.fingerprint_algorithm = ? AND f.fingerprint_input_version = ?
		  AND f.fingerprint_value = ? AND f.run_id != ?
		ORDER BY f.recorded_at_ns DESC, f.run_id DESC
		LIMIT ?`,
		anchorFacts.Fingerprint.Algorithm,
		anchorFacts.Fingerprint.InputSchemaVersion,
		value,
		anchor.ID.String(),
		maximum+1,
	)
	if err != nil {
		return diagnosticSimilarityResult{}, fmt.Errorf("query similar failures: %w", err)
	}
	defer rows.Close()
	result := make([]diagnostic.SimilarFailure, 0, maximum)
	truncated := false
	for rows.Next() {
		failure, scanErr := scanSimilarFailure(rows)
		if scanErr != nil {
			return diagnosticSimilarityResult{}, scanErr
		}
		if len(result) < maximum {
			result = append(result, failure)
		} else {
			truncated = true
		}
	}
	if err := rows.Err(); err != nil {
		return diagnosticSimilarityResult{}, fmt.Errorf("iterate similar failures: %w", err)
	}

	return diagnosticSimilarityResult{Failures: result, Available: true, Truncated: truncated}, nil
}

func scanSimilarFailure(row rowScanner) (diagnostic.SimilarFailure, error) {
	var jobIDText, runIDText, outcome, class, algorithm string
	var runNumber, completedAt, inputVersion, laterSucceeded int64
	var fingerprintValue []byte
	if err := row.Scan(
		&jobIDText, &runIDText, &runNumber, &completedAt, &outcome,
		&class, &algorithm, &inputVersion, &fingerprintValue, &laterSucceeded,
	); err != nil {
		return diagnostic.SimilarFailure{}, err
	}
	jobID, err := model.ParseJobID(jobIDText)
	if err != nil {
		return diagnostic.SimilarFailure{}, err
	}
	runID, err := model.ParseRunID(runIDText)
	if err != nil {
		return diagnostic.SimilarFailure{}, err
	}
	number, err := nonnegativeUintFromDatabase("similar run number", runNumber)
	if err != nil || number == 0 {
		return diagnostic.SimilarFailure{}, errors.Join(err, errors.New("similar run number must be positive"))
	}
	input, err := nonnegativeIntFromDatabase("similar fingerprint input version", inputVersion)
	if err != nil {
		return diagnostic.SimilarFailure{}, err
	}
	if laterSucceeded != 0 && laterSucceeded != 1 {
		return diagnostic.SimilarFailure{}, fmt.Errorf("similar later-succeeded value is %s", strconv.FormatInt(laterSucceeded, 10))
	}
	failure := diagnostic.SimilarFailure{
		JobID: jobID.String(), RunID: runID.String(), RunNumber: number,
		CompletedAt: timeFromDatabase(completedAt), Outcome: outcome, FailureClass: class,
		Fingerprint: diagnostic.FailureFingerprint{
			Algorithm: algorithm, InputSchemaVersion: input,
			Value: hex.EncodeToString(fingerprintValue), Scope: diagnostic.FingerprintScopeStoreLocal,
		},
		LaterSucceeded: laterSucceeded == 1,
	}
	if err := failure.Validate(); err != nil {
		return diagnostic.SimilarFailure{}, err
	}

	return failure, nil
}

func diagnosticSimilarityPartiallyIndexed(
	ctx context.Context,
	queryer schemaQueryer,
) (bool, error) {
	var value int
	if err := queryer.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM runs r
			LEFT JOIN run_diagnostic_facts f ON f.run_id = r.id
			WHERE r.phase = 'completed' AND r.outcome != 'success'
			  AND (f.run_id IS NULL OR f.fingerprint_value IS NULL)
		)`).Scan(&value); err != nil {
		return false, fmt.Errorf("inspect diagnostic fingerprint coverage: %w", err)
	}

	return value != 0, nil
}
