package store

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
)

func TestCompleteRunPersistsFactsAtomically(t *testing.T) {
	t.Parallel()

	database, jobID, runID, logs, now := runningRuntimeFixture(t, 0xd100)
	if _, err := database.db.ExecContext(t.Context(), `
		CREATE TRIGGER reject_run_diagnostic_facts
		BEFORE INSERT ON run_diagnostic_facts
		BEGIN SELECT RAISE(ABORT, 'injected facts failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	exitCode := 2
	_, err := database.CompleteRunWithDispositionAndFacts(
		t.Context(),
		jobID,
		runID,
		model.RunOutcomeFailure,
		&model.ExitInfo{ExitCode: &exitCode, ObservedAt: now.Add(4 * time.Second)},
		logs,
		"",
		now.Add(4*time.Second),
		model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure},
		RunDiagnosticFactsInput{
			Resources: []diagnostic.ResourceObservation{{
				Metric: diagnostic.ResourceCPUUserTime, Value: 10, Unit: diagnostic.ResourceUnitNanoseconds,
				Scope: diagnostic.ResourceScopeProcess, Source: diagnostic.ResourceSourceProcessState,
				Completeness: diagnostic.ResourceCompleteAtExit,
			}},
			PolicyDisposition: policy.RunClassificationNonRetryableFailure,
		},
	)
	if err == nil {
		t.Fatal("CompleteRunWithDispositionAndFacts() error = nil")
	}
	run, getErr := database.GetRun(t.Context(), runID)
	if getErr != nil {
		t.Fatalf("GetRun() error = %v", getErr)
	}
	if run.Phase != model.RunPhaseRunning || run.Outcome != "" {
		t.Fatalf("run after rolled-back fact insert = %#v", run)
	}
	runtimeState, runtimeErr := database.GetRuntime(t.Context(), jobID)
	if runtimeErr != nil {
		t.Fatalf("GetRuntime() error = %v", runtimeErr)
	}
	if runtimeState.RunCount != 0 {
		t.Fatalf("runtime count after rollback = %d", runtimeState.RunCount)
	}
}

func TestDiagnosticSnapshotReturnsBoundedStoreLocalSimilarity(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-similarity", newSequentialEventIDs(0xd200))
	base := storeTestTime()
	anchorJob, anchorRun := completeFactFailure(t, database, 0xd201, "/test/bin/worker", base)
	firstJob, _ := completeFactFailure(t, database, 0xd211, "/test/bin/worker", base.Add(time.Second))
	secondJob, _ := completeFactFailure(t, database, 0xd221, "/test/bin/worker", base.Add(2*time.Second))
	canaryJob, _ := completeFactFailure(t, database, 0xd231, "/secret/canary-worker", base.Add(3*time.Second))

	snapshot, err := database.GetDiagnosticSnapshot(t.Context(), anchorJob.ID.String(), DiagnosticSnapshotLimits{
		MaxRuns: 100, MaxEvents: 500, MaxNotifications: 200, MaxSimilar: 10,
	})
	if err != nil {
		t.Fatalf("GetDiagnosticSnapshot() error = %v", err)
	}
	facts, found := snapshot.RunFacts[anchorRun.ID]
	if !found || facts.Fingerprint == nil || len(facts.Resources) != 2 {
		t.Fatalf("anchor facts = %#v, found=%t", facts, found)
	}
	if !snapshot.SimilarityAvailable || snapshot.SimilarTruncated || snapshot.SimilarityPartiallyIndexed {
		t.Fatalf("similarity status = available:%t truncated:%t partial:%t",
			snapshot.SimilarityAvailable, snapshot.SimilarTruncated, snapshot.SimilarityPartiallyIndexed)
	}
	if len(snapshot.SimilarFailures) != 2 {
		t.Fatalf("similar failures = %#v, want two", snapshot.SimilarFailures)
	}
	wanted := map[string]bool{firstJob.ID.String(): false, secondJob.ID.String(): false}
	for _, failure := range snapshot.SimilarFailures {
		if _, ok := wanted[failure.JobID]; !ok {
			t.Fatalf("similarity leaked unrelated job %s (canary %s)", failure.JobID, canaryJob.ID)
		}
		wanted[failure.JobID] = true
		if failure.Fingerprint != *facts.Fingerprint {
			t.Fatalf("similar fingerprint = %#v, want %#v", failure.Fingerprint, *facts.Fingerprint)
		}
	}
	for jobID, found := range wanted {
		if !found {
			t.Fatalf("matching job %s missing from similarity results", jobID)
		}
	}

	bounded, err := database.GetDiagnosticSnapshot(t.Context(), anchorJob.ID.String(), DiagnosticSnapshotLimits{
		MaxRuns: 100, MaxEvents: 500, MaxNotifications: 200, MaxSimilar: 1,
	})
	if err != nil {
		t.Fatalf("GetDiagnosticSnapshot(bounded) error = %v", err)
	}
	if len(bounded.SimilarFailures) != 1 || !bounded.SimilarTruncated {
		t.Fatalf("bounded similarity = %#v, truncated=%t", bounded.SimilarFailures, bounded.SimilarTruncated)
	}
	encoded, err := json.Marshal(bounded.SimilarFailures)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("/secret/")) {
		t.Fatalf("similarity output contains secret-canary path: %s", encoded)
	}
}

func TestDiagnosticSnapshotIncludesOperationalMetadataDomains(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-operational-metadata", newSequentialEventIDs(0xd280))
	now := storeTestTime()
	prerequisite, _ := completeFactFailure(t, database, 0xd281, "/test/bin/prerequisite", now)
	jobID := mustJobID(t, 0xd291, 1)
	submitRuntimeJob(t, database, jobID, now.Add(10*time.Second))

	if err := database.SetDependencies(t.Context(), jobID, []Dependency{{
		JobID: jobID, DependsOn: prerequisite.ID, Predicate: DependencyFailed,
	}}); err != nil {
		t.Fatalf("SetDependencies() error = %v", err)
	}
	dependencyStatus, err := database.EvaluateDependencies(t.Context(), jobID, now.Add(11*time.Second))
	if err != nil || !dependencyStatus.Ready {
		t.Fatalf("EvaluateDependencies() = (%#v, %v)", dependencyStatus, err)
	}
	if waitErr := database.RecordWaitEvaluation(
		t.Context(), jobID, 2, model.WaitProbe, false,
		string(model.DiagnosticWaitEvaluationError), now.Add(12*time.Second),
	); waitErr != nil {
		t.Fatalf("RecordWaitEvaluation(first) error = %v", waitErr)
	}
	if waitErr := database.RecordWaitEvaluation(
		t.Context(), jobID, 2, model.WaitProbe, true, "", now.Add(13*time.Second),
	); waitErr != nil {
		t.Fatalf("RecordWaitEvaluation(second) error = %v", waitErr)
	}
	if _, admissionErr := database.TryAcquireAdmission(
		t.Context(), jobID, "", 2, now.Add(14*time.Second), time.Minute,
	); admissionErr != nil {
		t.Fatalf("TryAcquireAdmission() error = %v", admissionErr)
	}

	event := notificationJobEvent(t, database, jobID)
	queued, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{
		{JobID: jobID, EventID: event.ID, NotifierName: "mail", EventType: "job_started", MaxAttempts: 3},
		{JobID: jobID, EventID: event.ID, NotifierName: "webhook", EventType: "job_started", MaxAttempts: 3},
	})
	if err != nil || len(queued) != 2 {
		t.Fatalf("QueueNotificationDeliveries() = (%#v, %v)", queued, err)
	}
	for index, notifier := range []string{"mail", "webhook"} {
		status := 500 + index
		if _, notificationErr := database.RecordNotificationAttempt(t.Context(), RecordNotificationAttemptInput{
			JobID: jobID, EventID: event.ID, NotifierName: notifier,
			EventType: "job_started", AttemptNumber: 1,
			StartedAt: now.Add(15 * time.Second), CompletedAt: now.Add(16 * time.Second),
			DiagnosticCode: "transport", Retryable: true,
			ResponseStatusCode: &status, ResponseTruncated: true,
		}); notificationErr != nil {
			t.Fatalf("RecordNotificationAttempt(%s) error = %v", notifier, notificationErr)
		}
	}

	snapshot, err := database.GetDiagnosticSnapshot(t.Context(), jobID.String(), DiagnosticSnapshotLimits{
		MaxRuns: 1, MaxEvents: 1, MaxNotifications: 1,
	})
	if err != nil {
		t.Fatalf("GetDiagnosticSnapshot() error = %v", err)
	}
	if len(snapshot.Dependencies) != 1 || snapshot.Dependencies[0].ObservedRevision == 0 ||
		snapshot.Dependencies[0].SatisfiedAt == nil {
		t.Fatalf("dependencies = %#v", snapshot.Dependencies)
	}
	if len(snapshot.WaitEvaluations) != 1 || snapshot.WaitEvaluations[0].AttemptCount != 2 ||
		snapshot.WaitEvaluations[0].SatisfiedAt == nil {
		t.Fatalf("wait evaluations = %#v", snapshot.WaitEvaluations)
	}
	if snapshot.Admission == nil || snapshot.Admission.Slots != 2 {
		t.Fatalf("admission = %#v", snapshot.Admission)
	}
	if len(snapshot.NotificationDeliveries) != 1 || len(snapshot.NotificationAttempts) != 1 ||
		!snapshot.NotificationsTruncated {
		t.Fatalf("notifications = deliveries:%#v attempts:%#v truncated:%t",
			snapshot.NotificationDeliveries, snapshot.NotificationAttempts, snapshot.NotificationsTruncated)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Type != model.EventJobSubmitted {
		t.Fatalf("events = %#v", snapshot.Events)
	}
}

func TestDiagnosticSnapshotLimitAndRunSelectionHelpers(t *testing.T) {
	t.Parallel()

	defaults, err := normalizeDiagnosticLimits(DiagnosticSnapshotLimits{})
	if err != nil {
		t.Fatalf("normalizeDiagnosticLimits(defaults) error = %v", err)
	}
	if defaults.MaxRuns != defaultDiagnosticRuns || defaults.MaxEvents != defaultDiagnosticEvents ||
		defaults.MaxNotifications != defaultDiagnosticNotifications {
		t.Fatalf("normalized defaults = %#v", defaults)
	}
	for _, limits := range []DiagnosticSnapshotLimits{
		{MaxRuns: -1},
		{MaxRuns: 1001},
		{MaxEvents: -1},
		{MaxEvents: 5001},
		{MaxNotifications: -1},
		{MaxNotifications: 2001},
		{MaxSimilar: -1},
		{MaxSimilar: maximumDiagnosticSimilar + 1},
	} {
		if _, err := normalizeDiagnosticLimits(limits); err == nil {
			t.Errorf("normalizeDiagnosticLimits(%#v) error = nil", limits)
		}
	}

	selections := []struct {
		selected int64
		total    uint64
		want     uint64
	}{
		{selected: 0, total: 3, want: 0},
		{selected: 2, total: 3, want: 2},
		{selected: -1, total: 3, want: 3},
		{selected: -3, total: 3, want: 1},
	}
	for _, test := range selections {
		got, err := selectedRunNumber(test.selected, test.total)
		if err != nil || got != test.want {
			t.Errorf("selectedRunNumber(%d, %d) = (%d, %v), want %d", test.selected, test.total, got, err, test.want)
		}
	}
	if _, err := selectedRunNumber(-4, 3); !errors.Is(err, ErrNotFound) {
		t.Errorf("selectedRunNumber(out of range) error = %v, want ErrNotFound", err)
	}
	if _, err := selectedRunNumber(-1, math.MaxUint64); err == nil {
		t.Error("selectedRunNumber(overflow) error = nil")
	}
	runs := []model.RunState{{Number: 1}, {Number: 3}}
	if !containsRunNumber(runs, 3) || containsRunNumber(runs, 2) {
		t.Fatalf("containsRunNumber(%#v) returned an unexpected result", runs)
	}
}

func TestSimilarityLookupUsesFingerprintIndex(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-query-plan", newSequentialEventIDs(0xd300))
	rows, err := database.db.QueryContext(t.Context(), `
		EXPLAIN QUERY PLAN
		SELECT run_id FROM run_diagnostic_facts
		WHERE fingerprint_algorithm = ? AND fingerprint_input_version = ? AND fingerprint_value = ?
		ORDER BY recorded_at_ns DESC, run_id DESC LIMIT 10`,
		diagnostic.FingerprintAlgorithmHMACSHA256,
		diagnostic.FingerprintInputSchemaVersion,
		bytes.Repeat([]byte{0x01}, fingerprintKeyBytes),
	)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN error = %v", err)
	}
	defer rows.Close()
	usedIndex := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		if strings.Contains(detail, "run_diagnostic_facts_fingerprint") {
			usedIndex = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query plan: %v", err)
	}
	if !usedIndex {
		t.Fatal("similarity lookup plan did not use run_diagnostic_facts_fingerprint")
	}
}

func TestSimilarityReportsUnindexedFailuresAcrossStore(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-partial-similarity", newSequentialEventIDs(0xd380))
	base := storeTestTime()
	anchorJob, _ := completeFactFailure(t, database, 0xd381, "/test/bin/worker", base)
	_, legacyRun := completeFactFailure(t, database, 0xd391, "/test/bin/worker", base.Add(time.Second))
	if _, err := database.db.ExecContext(t.Context(),
		`DELETE FROM run_diagnostic_facts WHERE run_id = ?`, legacyRun.ID.String(),
	); err != nil {
		t.Fatalf("remove historical facts: %v", err)
	}

	snapshot, err := database.GetDiagnosticSnapshot(t.Context(), anchorJob.ID.String(), DiagnosticSnapshotLimits{
		MaxRuns: 100, MaxEvents: 500, MaxNotifications: 200, MaxSimilar: 10,
	})
	if err != nil {
		t.Fatalf("GetDiagnosticSnapshot() error = %v", err)
	}
	if !snapshot.SimilarityAvailable || !snapshot.SimilarityPartiallyIndexed || len(snapshot.SimilarFailures) != 0 {
		t.Fatalf("partial similarity = available:%t partial:%t failures:%#v",
			snapshot.SimilarityAvailable, snapshot.SimilarityPartiallyIndexed, snapshot.SimilarFailures)
	}
}

func TestSimilarityReportsWhenMatchingJobLaterSucceeded(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-later-success", newSequentialEventIDs(0xd3c0))
	base := storeTestTime()
	anchorJob, _ := completeRetryableFactFailure(t, database, 0xd3c1, base)
	transientJob, transientRun := completeRetryableFactFailure(t, database, 0xd3d1, base.Add(time.Second))
	secondAt := base.Add(20 * time.Second)
	if _, err := database.MoveJob(
		t.Context(), transientJob.ID, model.JobPhaseStarting, secondAt, "retry_due",
	); err != nil {
		t.Fatalf("MoveJob(starting) error = %v", err)
	}
	secondRunID := mustRunID(t, 0xd3e1)
	logs := testLogs(database, transientJob.ID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(
		t.Context(), transientJob.ID, secondRunID, transientRun.Number+1, logs, secondAt.Add(time.Second),
	); err != nil {
		t.Fatalf("ReserveRun(second) error = %v", err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), transientJob.ID, secondRunID, "/test/bin/worker",
		testProcessIdentity(7331, "later-success"), secondAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("MarkProcessStarted(second) error = %v", err)
	}
	exitCode := 0
	logs.Integrity = model.LogIntegrityValid
	completedAt := secondAt.Add(3 * time.Second)
	if _, err := database.CompleteRunWithDispositionAndFacts(
		t.Context(), transientJob.ID, secondRunID, model.RunOutcomeSuccess,
		&model.ExitInfo{ExitCode: &exitCode, ObservedAt: completedAt}, logs, "", completedAt,
		model.RunDisposition{TerminalOutcome: model.JobOutcomeSuccess},
		RunDiagnosticFactsInput{PolicyDisposition: policy.RunClassificationSuccess},
	); err != nil {
		t.Fatalf("CompleteRunWithDispositionAndFacts(success) error = %v", err)
	}

	snapshot, err := database.GetDiagnosticSnapshot(t.Context(), anchorJob.ID.String(), DiagnosticSnapshotLimits{
		MaxRuns: 100, MaxEvents: 500, MaxNotifications: 200, MaxSimilar: 10,
	})
	if err != nil {
		t.Fatalf("GetDiagnosticSnapshot() error = %v", err)
	}
	if len(snapshot.SimilarFailures) != 1 || !snapshot.SimilarFailures[0].LaterSucceeded ||
		snapshot.SimilarFailures[0].RunID != transientRun.ID.String() {
		t.Fatalf("similar later-success summary = %#v", snapshot.SimilarFailures)
	}
}

func TestRunDiagnosticFactsSchemaRejectsPartialFingerprintAndOversizedResources(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-facts-schema", newSequentialEventIDs(0xd3a0))
	_, run := completeFactFailure(t, database, 0xd3a1, "/test/bin/worker", storeTestTime())
	if _, err := database.db.ExecContext(t.Context(),
		`DELETE FROM run_diagnostic_facts WHERE run_id = ?`, run.ID.String(),
	); err != nil {
		t.Fatal(err)
	}
	_, err := database.db.ExecContext(t.Context(), `
		INSERT INTO run_diagnostic_facts(
			run_id, schema_version, resources_json, failure_class,
			fingerprint_algorithm, recorded_at_ns
		) VALUES (?, 1, '[]', 'nonzero_exit', 'hmac-sha256', 1)`, run.ID.String())
	if err == nil {
		t.Fatal("partial fingerprint insert error = nil")
	}
	oversized := `["` + strings.Repeat("x", maximumResourceJSONBytes) + `"]`
	_, err = database.db.ExecContext(t.Context(), `
		INSERT INTO run_diagnostic_facts(
			run_id, schema_version, resources_json, failure_class, recorded_at_ns
		) VALUES (?, 1, ?, 'nonzero_exit', 1)`, run.ID.String(), oversized)
	if err == nil {
		t.Fatal("oversized resource insert error = nil")
	}
}

func TestRunDiagnosticFactsValidationAndDecodingRejectCorruption(t *testing.T) {
	t.Parallel()

	now := storeTestTime()
	cpu := diagnostic.ResourceObservation{
		Metric: diagnostic.ResourceCPUUserTime, Value: 1, Unit: diagnostic.ResourceUnitNanoseconds,
		Scope: diagnostic.ResourceScopeProcess, Source: diagnostic.ResourceSourceProcessState,
		Completeness: diagnostic.ResourceCompleteAtExit,
	}
	fingerprint := diagnostic.FailureFingerprint{
		Algorithm:          diagnostic.FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: diagnostic.FingerprintInputSchemaVersion,
		Value:              strings.Repeat("a", 64), Scope: diagnostic.FingerprintScopeStoreLocal,
	}
	valid := RunDiagnosticFacts{
		SchemaVersion: runDiagnosticFactsSchemaVersion, Resources: []diagnostic.ResourceObservation{cpu},
		FailureClass: "nonzero_exit", Fingerprint: &fingerprint, RecordedAt: now,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate(valid) error = %v", err)
	}
	for name, mutate := range map[string]func(*RunDiagnosticFacts){
		"schema":        func(value *RunDiagnosticFacts) { value.SchemaVersion++ },
		"time":          func(value *RunDiagnosticFacts) { value.RecordedAt = time.Time{} },
		"time zone":     func(value *RunDiagnosticFacts) { value.RecordedAt = now.In(time.FixedZone("test", 60)) },
		"nil resources": func(value *RunDiagnosticFacts) { value.Resources = nil },
		"too many resources": func(value *RunDiagnosticFacts) {
			value.Resources = make([]diagnostic.ResourceObservation, maximumResourceObservations+1)
		},
		"invalid resource": func(value *RunDiagnosticFacts) { value.Resources[0].Unit = diagnostic.ResourceUnitBytes },
		"duplicate resource": func(value *RunDiagnosticFacts) {
			value.Resources = append(value.Resources, value.Resources[0])
		},
		"failure class": func(value *RunDiagnosticFacts) { value.FailureClass = "bad-class" },
		"fingerprint without class": func(value *RunDiagnosticFacts) {
			value.FailureClass = ""
		},
		"invalid fingerprint": func(value *RunDiagnosticFacts) { value.Fingerprint.Value = "bad" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := valid
			value.Resources = append([]diagnostic.ResourceObservation(nil), valid.Resources...)
			fingerprintCopy := fingerprint
			value.Fingerprint = &fingerprintCopy
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}

	for name, encoded := range map[string]string{
		"malformed": `{`,
		"unknown field": `[{
			"metric":"cpu_user_time","value":1,"unit":"nanoseconds","scope":"process",
			"source":"process_state","completeness":"complete_at_exit","extra":true
		}]`,
		"trailing": `[] {}`,
		"null":     `null`,
	} {
		t.Run("resources "+name, func(t *testing.T) {
			t.Parallel()
			if _, err := decodeResourceObservations(encoded); err == nil {
				t.Fatal("decodeResourceObservations() error = nil")
			}
		})
	}
}

func TestRunDiagnosticFactAndSimilarityRowDecodingMatrices(t *testing.T) {
	t.Parallel()

	now := storeTestTime().UnixNano()
	runID := mustRunID(t, 0xd3f1).String()
	jobID := mustJobID(t, 0xd3f2, 1).String()
	fingerprintBytes := bytes.Repeat([]byte{0xaa}, fingerprintKeyBytes)
	factValues := []any{
		runID, int64(runDiagnosticFactsSchemaVersion), "[]",
		sql.NullString{Valid: true, String: "nonzero_exit"},
		sql.NullString{Valid: true, String: diagnostic.FingerprintAlgorithmHMACSHA256},
		sql.NullInt64{Valid: true, Int64: diagnostic.FingerprintInputSchemaVersion},
		fingerprintBytes, now,
	}
	if _, facts, err := scanRunDiagnosticFacts(&staticRow{values: factValues}); err != nil || facts.Fingerprint == nil {
		t.Fatalf("scanRunDiagnosticFacts(valid) = (%#v, %v)", facts, err)
	}
	for name, mutate := range map[string]func([]any){
		"run ID":              func(values []any) { values[0] = "invalid" },
		"oversized resources": func(values []any) { values[2] = strings.Repeat("x", maximumResourceJSONBytes+1) },
		"resource JSON":       func(values []any) { values[2] = "null" },
		"schema":              func(values []any) { values[1] = int64(2) },
		"partial fingerprint": func(values []any) { values[4] = sql.NullString{} },
		"fingerprint version": func(values []any) { values[5] = sql.NullInt64{Valid: true, Int64: -1} },
		"fingerprint metadata": func(values []any) {
			values[4] = sql.NullString{Valid: true, String: "other"}
		},
	} {
		t.Run("facts "+name, func(t *testing.T) {
			t.Parallel()
			values := append([]any(nil), factValues...)
			mutate(values)
			if _, _, err := scanRunDiagnosticFacts(&staticRow{values: values}); err == nil {
				t.Fatal("scanRunDiagnosticFacts() error = nil")
			}
		})
	}
	if _, _, err := scanRunDiagnosticFacts(&staticRow{err: errors.New("scan failed")}); err == nil {
		t.Fatal("scanRunDiagnosticFacts(scan error) error = nil")
	}

	similarValues := []any{
		jobID, runID, int64(1), now, "failure", "nonzero_exit",
		diagnostic.FingerprintAlgorithmHMACSHA256, int64(diagnostic.FingerprintInputSchemaVersion),
		fingerprintBytes, int64(1),
	}
	if failure, err := scanSimilarFailure(&staticRow{values: similarValues}); err != nil || !failure.LaterSucceeded {
		t.Fatalf("scanSimilarFailure(valid) = (%#v, %v)", failure, err)
	}
	for name, mutate := range map[string]func([]any){
		"job ID":              func(values []any) { values[0] = "invalid" },
		"run ID":              func(values []any) { values[1] = "invalid" },
		"zero run":            func(values []any) { values[2] = int64(0) },
		"negative run":        func(values []any) { values[2] = int64(-1) },
		"fingerprint version": func(values []any) { values[7] = int64(-1) },
		"later succeeded":     func(values []any) { values[9] = int64(2) },
		"outcome":             func(values []any) { values[4] = "success" },
		"fingerprint":         func(values []any) { values[8] = []byte("short") },
	} {
		t.Run("similar "+name, func(t *testing.T) {
			t.Parallel()
			values := append([]any(nil), similarValues...)
			mutate(values)
			if _, err := scanSimilarFailure(&staticRow{values: values}); err == nil {
				t.Fatal("scanSimilarFailure() error = nil")
			}
		})
	}
	if _, err := scanSimilarFailure(&staticRow{err: errors.New("scan failed")}); err == nil {
		t.Fatal("scanSimilarFailure(scan error) error = nil")
	}
}

func TestDiagnosticAnchorRunMatchesPublicSelectionPrecedence(t *testing.T) {
	t.Parallel()

	run1 := model.RunState{ID: mustRunID(t, 0xd401), Number: 1, Outcome: model.RunOutcomeSuccess}
	run2 := model.RunState{ID: mustRunID(t, 0xd402), Number: 2, Outcome: model.RunOutcomeFailure}
	run3 := model.RunState{ID: mustRunID(t, 0xd403), Number: 3, Outcome: model.RunOutcomeSuccess}
	runs := []model.RunState{run1, run2, run3}
	for _, test := range []struct {
		name     string
		job      model.JobState
		selected int64
		want     model.RunID
	}{
		{name: "positive", selected: 1, want: run1.ID},
		{name: "negative", selected: -1, want: run3.ID},
		{name: "missing", selected: 4},
		{name: "active", job: model.JobState{ActiveRunID: run1.ID}, want: run1.ID},
		{name: "failure", want: run2.ID},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := diagnosticAnchorRun(test.job, runs, test.selected, uint64(len(runs))).ID; got != test.want {
				t.Fatalf("diagnosticAnchorRun() ID = %s, want %s", got, test.want)
			}
		})
	}
	if got := diagnosticAnchorRun(model.JobState{}, runs[:1], 0, 1).ID; got != run1.ID {
		t.Fatalf("diagnosticAnchorRun(latest success) ID = %s", got)
	}
	if got := diagnosticAnchorRun(model.JobState{}, nil, 0, 0); got.ID != "" {
		t.Fatalf("diagnosticAnchorRun(empty) = %#v", got)
	}
}

func TestBuildRunDiagnosticFactsCoversTimeoutAndInputValidation(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-fact-builder", newSequentialEventIDs(0xd410))
	now := storeTestTime()
	exitCode := 124
	run := model.RunState{
		Outcome: model.RunOutcomeTimedOut, StopReason: model.StopReasonTimeout,
		LastDiagnosticCode: string(model.DiagnosticJobTimeout),
		Exit:               &model.ExitInfo{ExitCode: &exitCode, Signal: "TERM", PlatformReason: "deadline", ObservedAt: now},
	}
	job := model.JobState{Spec: testJobSpec(t, "diagnostic-builder")}
	facts, err := database.buildRunDiagnosticFacts(job, run, RunDiagnosticFactsInput{
		PolicyDisposition: policy.RunClassificationNonRetryableFailure,
	}, now)
	if err != nil || facts.Fingerprint == nil || facts.FailureClass != "job_timeout" {
		t.Fatalf("buildRunDiagnosticFacts(timeout) = (%#v, %v)", facts, err)
	}
	if _, err := database.buildRunDiagnosticFacts(job, run, RunDiagnosticFactsInput{
		PolicyDisposition: policy.RunClassification("invalid"),
	}, now); err == nil {
		t.Fatal("buildRunDiagnosticFacts(invalid policy) error = nil")
	}
}

func TestDiagnosticEventRowDecodingRejectsCorruption(t *testing.T) {
	t.Parallel()

	eventID := mustEventID(t, 0xd420, 1).String()
	jobID := mustJobID(t, 0xd421, 1).String()
	runID := mustRunID(t, 0xd422).String()
	supervisorID := mustSupervisorID(t, 0xd423, 1).String()
	values := []any{
		eventID, jobID,
		sql.NullString{Valid: true, String: runID},
		sql.NullString{Valid: true, String: supervisorID},
		string(model.EntityRun), runID, string(model.EventRunCompleted),
		sql.NullString{Valid: true, String: string(model.RunPhaseRunning)},
		string(model.RunPhaseCompleted),
		sql.NullString{},
		sql.NullString{Valid: true, String: string(model.RunOutcomeFailure)},
		int64(2), storeTestTime().UnixNano(), `{"schema_version":1}`,
	}
	if event, err := scanDiagnosticEvent(&staticRow{values: values}); err != nil ||
		event.RunID.String() != runID || event.SupervisorID.String() != supervisorID {
		t.Fatalf("scanDiagnosticEvent(valid) = (%#v, %v)", event, err)
	}
	for name, mutate := range map[string]func([]any){
		"event ID":      func(row []any) { row[0] = "invalid" },
		"job ID":        func(row []any) { row[1] = "invalid" },
		"run ID":        func(row []any) { row[2] = sql.NullString{Valid: true, String: "invalid"} },
		"supervisor ID": func(row []any) { row[3] = sql.NullString{Valid: true, String: "invalid"} },
		"revision":      func(row []any) { row[11] = int64(-1) },
		"details":       func(row []any) { row[13] = "{" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := append([]any(nil), values...)
			mutate(row)
			if _, err := scanDiagnosticEvent(&staticRow{values: row}); err == nil {
				t.Fatal("scanDiagnosticEvent() error = nil")
			}
		})
	}
	if _, err := scanDiagnosticEvent(&staticRow{err: errors.New("scan failed")}); err == nil {
		t.Fatal("scanDiagnosticEvent(scan error) error = nil")
	}
}

func TestDiagnosticReadersReturnClosedDatabaseErrors(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-closed-reader", newSequentialEventIDs(0xd430))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	jobID := mustJobID(t, 0xd431, 1)
	run := model.RunState{ID: mustRunID(t, 0xd432), JobID: jobID, Number: 1}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "runs", run: func() error { _, _, _, err := diagnosticRuns(t.Context(), database.db, jobID, 0, 1); return err }},
		{name: "dependencies", run: func() error { _, err := diagnosticDependencies(t.Context(), database.db, jobID); return err }},
		{name: "waits", run: func() error { _, err := diagnosticWaitEvaluations(t.Context(), database.db, jobID); return err }},
		{name: "admission", run: func() error { _, _, err := diagnosticAdmission(t.Context(), database.db, jobID); return err }},
		{name: "deliveries", run: func() error { _, _, err := diagnosticDeliveries(t.Context(), database.db, jobID, 1); return err }},
		{name: "attempts", run: func() error { _, _, err := diagnosticAttempts(t.Context(), database.db, jobID, 1); return err }},
		{name: "events", run: func() error { _, _, err := diagnosticEvents(t.Context(), database.db, jobID, 1); return err }},
		{name: "facts", run: func() error {
			_, err := diagnosticRunFacts(t.Context(), database.db, []model.RunState{run})
			return err
		}},
		{name: "index coverage", run: func() error { _, err := diagnosticSimilarityPartiallyIndexed(t.Context(), database.db); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			t.Parallel()
			if err := check.run(); err == nil {
				t.Fatal("reader error = nil")
			}
		})
	}
}

func TestDiagnosticRunsIncludesExplicitSelectionOutsideHistoryWindow(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-selected-history", newSequentialEventIDs(0xd440))
	now := storeTestTime()
	job, first := completeRetryableFactFailure(t, database, 0xd441, now)
	for number := uint64(2); number <= 3; number++ {
		at := now.Add(time.Duration(number*10) * time.Second)
		if _, err := database.MoveJob(t.Context(), job.ID, model.JobPhaseStarting, at, "retry_due"); err != nil {
			t.Fatalf("MoveJob(run %d) error = %v", number, err)
		}
		runID := mustRunID(t, 0xd450+number)
		logs := testLogs(database, job.ID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), job.ID, runID, number, logs, at.Add(time.Second)); err != nil {
			t.Fatalf("ReserveRun(%d) error = %v", number, err)
		}
		if _, err := database.MarkProcessStarted(
			t.Context(), job.ID, runID, "/test/bin/worker",
			testProcessIdentity(7500+int(number), "selected-history"), at.Add(2*time.Second),
		); err != nil {
			t.Fatalf("MarkProcessStarted(%d) error = %v", number, err)
		}
		logs.Integrity = model.LogIntegrityValid
		exitCode := 2
		completedAt := at.Add(3 * time.Second)
		disposition := model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure}
		classification := policy.RunClassificationNonRetryableFailure
		if number == 2 {
			next := at.Add(10 * time.Second)
			disposition = model.RunDisposition{
				NextPhase: model.JobPhaseBackoff, NextRunAt: &next, Reason: "retryable_failure",
			}
			classification = policy.RunClassificationRetryableFailure
		}
		if _, err := database.CompleteRunWithDispositionAndFacts(
			t.Context(), job.ID, runID, model.RunOutcomeFailure,
			&model.ExitInfo{ExitCode: &exitCode, ObservedAt: completedAt}, logs, "", completedAt,
			disposition, RunDiagnosticFactsInput{PolicyDisposition: classification},
		); err != nil {
			t.Fatalf("CompleteRunWithDispositionAndFacts(%d) error = %v", number, err)
		}
	}
	runs, total, truncated, err := diagnosticRuns(t.Context(), database.db, job.ID, 1, 1)
	if err != nil {
		t.Fatalf("diagnosticRuns() error = %v", err)
	}
	if total != 3 || !truncated || len(runs) != 2 || runs[0].ID != first.ID || runs[1].Number != 3 {
		t.Fatalf("diagnosticRuns() = runs:%#v total:%d truncated:%t", runs, total, truncated)
	}
}

func TestDiagnosticSnapshotPropagatesEachDomainReadFailure(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		table string
		seed  uint64
	}{
		{table: "runs", seed: 0xd500},
		{table: "run_diagnostic_facts", seed: 0xd510},
		{table: "job_runtime", seed: 0xd520},
		{table: "job_dependencies", seed: 0xd530},
		{table: "wait_evaluations", seed: 0xd540},
		{table: "admissions", seed: 0xd550},
		{table: "notification_deliveries", seed: 0xd560},
		{table: "notification_attempts", seed: 0xd570},
		{table: "state_events", seed: 0xd580},
	} {
		t.Run(test.table, func(t *testing.T) {
			t.Parallel()
			database := openTestStore(t, "diagnostic-domain-"+test.table, newSequentialEventIDs(test.seed))
			job, _ := completeFactFailure(
				t, database, test.seed+1, "/test/bin/worker", storeTestTime(),
			)
			if _, err := database.db.ExecContext(
				t.Context(), "ALTER TABLE "+test.table+" RENAME TO unavailable_"+test.table,
			); err != nil {
				t.Fatalf("rename %s: %v", test.table, err)
			}
			if _, err := database.GetDiagnosticSnapshot(t.Context(), job.ID.String(), DiagnosticSnapshotLimits{
				MaxRuns: 10, MaxEvents: 10, MaxNotifications: 10,
			}); err == nil {
				t.Fatalf("GetDiagnosticSnapshot() error = nil after renaming %s", test.table)
			}
		})
	}
}

func TestDiagnosticSimilarityRejectsInvalidAnchorAndQueryFailure(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "diagnostic-similarity-errors", newSequentialEventIDs(0xd600))
	run := model.RunState{ID: mustRunID(t, 0xd601), Number: 1}
	invalid := diagnostic.FailureFingerprint{
		Algorithm:          diagnostic.FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: diagnostic.FingerprintInputSchemaVersion,
		Value:              "not-hex", Scope: diagnostic.FingerprintScopeStoreLocal,
	}
	if _, err := diagnosticSimilarFailures(t.Context(), database.db, run, map[model.RunID]RunDiagnosticFacts{
		run.ID: {Fingerprint: &invalid},
	}, 1); err == nil {
		t.Fatal("diagnosticSimilarFailures(invalid fingerprint) error = nil")
	}
	valid := invalid
	valid.Value = strings.Repeat("a", 64)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := diagnosticSimilarFailures(t.Context(), database.db, run, map[model.RunID]RunDiagnosticFacts{
		run.ID: {Fingerprint: &valid},
	}, 1); err == nil {
		t.Fatal("diagnosticSimilarFailures(closed database) error = nil")
	}
	if _, err := database.GetDiagnosticSnapshot(t.Context(), "missing", DiagnosticSnapshotLimits{}); err == nil {
		t.Fatal("GetDiagnosticSnapshot(closed database) error = nil")
	}
}

func TestDiagnosticSnapshotRejectsCorruptDependencyAndWaitRows(t *testing.T) {
	t.Parallel()

	for name, corrupt := range map[string]func(*testing.T, *Store, model.JobID){
		"dependency ID": func(t *testing.T, database *Store, jobID model.JobID) {
			t.Helper()
			if _, err := database.db.ExecContext(t.Context(),
				`UPDATE job_dependencies SET dependency_job_id = 'invalid' WHERE job_id = ?`, jobID.String(),
			); err != nil {
				t.Fatal(err)
			}
		},
		"dependency revision": func(t *testing.T, database *Store, jobID model.JobID) {
			t.Helper()
			if _, err := database.db.ExecContext(t.Context(),
				`UPDATE job_dependencies SET observed_revision = -1 WHERE job_id = ?`, jobID.String(),
			); err != nil {
				t.Fatal(err)
			}
		},
		"wait index": func(t *testing.T, database *Store, jobID model.JobID) {
			t.Helper()
			if _, err := database.db.ExecContext(t.Context(),
				`UPDATE wait_evaluations SET condition_index = -1 WHERE job_id = ?`, jobID.String(),
			); err != nil {
				t.Fatal(err)
			}
		},
		"wait attempts": func(t *testing.T, database *Store, jobID model.JobID) {
			t.Helper()
			if _, err := database.db.ExecContext(t.Context(),
				`UPDATE wait_evaluations SET attempt_count = -1 WHERE job_id = ?`, jobID.String(),
			); err != nil {
				t.Fatal(err)
			}
		},
		"wait kind": func(t *testing.T, database *Store, jobID model.JobID) {
			t.Helper()
			if _, err := database.db.ExecContext(t.Context(),
				`UPDATE wait_evaluations SET condition_kind = 'invalid' WHERE job_id = ?`, jobID.String(),
			); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			database := openTestStore(t, "diagnostic-corrupt-"+strings.ReplaceAll(name, " ", "-"), newSequentialEventIDs(0xd700))
			database.db.SetMaxOpenConns(1)
			if _, err := database.db.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
				t.Fatal(err)
			}
			if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatal(err)
			}
			now := storeTestTime()
			prerequisite := mustJobID(t, 0xd701, 1)
			jobID := mustJobID(t, 0xd702, 1)
			submitRuntimeJob(t, database, prerequisite, now)
			submitRuntimeJob(t, database, jobID, now)
			if err := database.SetDependencies(t.Context(), jobID, []Dependency{{
				JobID: jobID, DependsOn: prerequisite, Predicate: DependencySuccess,
			}}); err != nil {
				t.Fatal(err)
			}
			if err := database.RecordWaitEvaluation(
				t.Context(), jobID, 0, model.WaitDelay, false,
				string(model.DiagnosticWaitEvaluationError), now,
			); err != nil {
				t.Fatal(err)
			}
			corrupt(t, database, jobID)
			if _, err := database.GetDiagnosticSnapshot(t.Context(), jobID.String(), DiagnosticSnapshotLimits{
				MaxRuns: 10, MaxEvents: 10, MaxNotifications: 10,
			}); err == nil {
				t.Fatal("GetDiagnosticSnapshot(corrupt row) error = nil")
			}
		})
	}
}

func TestVerifyFingerprintKeyRejectsStoreDrift(t *testing.T) {
	t.Parallel()

	t.Run("loaded mismatch", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-mismatch", newSequentialEventIDs(0xd710))
		database.fingerprintKey[0] ^= 1
		if err := database.verifyFingerprintKey(t.Context()); err == nil {
			t.Fatal("verifyFingerprintKey(mismatch) error = nil")
		}
	})
	t.Run("missing", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-missing", newSequentialEventIDs(0xd720))
		if _, err := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); err != nil {
			t.Fatal(err)
		}
		if err := database.verifyFingerprintKey(t.Context()); err == nil {
			t.Fatal("verifyFingerprintKey(missing) error = nil")
		}
	})
	t.Run("closed", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-closed", newSequentialEventIDs(0xd730))
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.verifyFingerprintKey(t.Context()); err == nil {
			t.Fatal("verifyFingerprintKey(closed) error = nil")
		}
	})
}

func TestFingerprintKeyInitializationPropagatesStorageFailures(t *testing.T) {
	t.Parallel()

	t.Run("index query", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-query", newSequentialEventIDs(0xd740))
		if _, err := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.ExecContext(
			t.Context(), `ALTER TABLE run_diagnostic_facts RENAME TO unavailable_run_diagnostic_facts`,
		); err != nil {
			t.Fatal(err)
		}
		if err := database.ensureFingerprintKey(t.Context()); err == nil {
			t.Fatal("ensureFingerprintKey(query failure) error = nil")
		}
	})

	t.Run("entropy", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-entropy", newSequentialEventIDs(0xd750))
		if _, err := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); err != nil {
			t.Fatal(err)
		}
		database.random = failedFingerprintEntropy{}
		if err := database.ensureFingerprintKey(t.Context()); err == nil {
			t.Fatal("ensureFingerprintKey(entropy failure) error = nil")
		}
	})

	t.Run("write", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-write", newSequentialEventIDs(0xd760))
		if _, err := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.ExecContext(t.Context(), `
			CREATE TRIGGER reject_fingerprint_key
			BEFORE INSERT ON store_secrets
			BEGIN SELECT RAISE(ABORT, 'injected key write failure'); END`); err != nil {
			t.Fatal(err)
		}
		database.random = bytes.NewReader(bytes.Repeat([]byte{0x76}, fingerprintKeyBytes))
		if err := database.ensureFingerprintKey(t.Context()); err == nil {
			t.Fatal("ensureFingerprintKey(write failure) error = nil")
		}
	})

	t.Run("read back", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-read-back", newSequentialEventIDs(0xd770))
		if _, err := database.db.ExecContext(t.Context(), `DELETE FROM store_secrets`); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.ExecContext(t.Context(), `
			CREATE TRIGGER erase_fingerprint_key
			AFTER INSERT ON store_secrets
			BEGIN DELETE FROM store_secrets WHERE name = NEW.name; END`); err != nil {
			t.Fatal(err)
		}
		database.random = bytes.NewReader(bytes.Repeat([]byte{0x77}, fingerprintKeyBytes))
		if err := database.ensureFingerprintKey(t.Context()); err == nil {
			t.Fatal("ensureFingerprintKey(read-back failure) error = nil")
		}
	})

	t.Run("verify read", func(t *testing.T) {
		database := openTestStore(t, "diagnostic-key-verify-read", newSequentialEventIDs(0xd780))
		if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.ExecContext(t.Context(), `UPDATE store_secrets SET value = zeroblob(31)`); err != nil {
			t.Fatal(err)
		}
		if err := database.verifyFingerprintKey(t.Context()); err == nil {
			t.Fatal("verifyFingerprintKey(corrupt key) error = nil")
		}
	})
}

type failedFingerprintEntropy struct{}

func (failedFingerprintEntropy) Read([]byte) (int, error) {
	return 0, errors.New("injected entropy failure")
}

func completeFactFailure(
	t *testing.T,
	database *Store,
	prefix uint64,
	resolvedExecutable string,
	now time.Time,
) (model.JobState, model.RunState) {
	t.Helper()
	jobID := mustJobID(t, prefix, 1)
	runID := mustRunID(t, prefix+1)
	supervisorID := mustSupervisorID(t, prefix+2, 1)
	credential := submitRuntimeJob(t, database, jobID, now)
	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
		t.Fatalf("ReserveRun() error = %v", err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, resolvedExecutable,
		testProcessIdentity(7000+int(prefix%1000), "facts"), now.Add(3*time.Second),
	); err != nil {
		t.Fatalf("MarkProcessStarted() error = %v", err)
	}
	logs.Integrity = model.LogIntegrityValid
	exitCode := 2
	completedAt := now.Add(4 * time.Second)
	completed, err := database.CompleteRunWithDispositionAndFacts(
		t.Context(), jobID, runID, model.RunOutcomeFailure,
		&model.ExitInfo{ExitCode: &exitCode, ObservedAt: completedAt},
		logs, "", completedAt,
		model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure},
		RunDiagnosticFactsInput{
			Resources: []diagnostic.ResourceObservation{
				{
					Metric: diagnostic.ResourceCPUUserTime, Value: 5, Unit: diagnostic.ResourceUnitNanoseconds,
					Scope: diagnostic.ResourceScopeProcess, Source: diagnostic.ResourceSourceProcessState,
					Completeness: diagnostic.ResourceCompleteAtExit,
				},
				{
					Metric: diagnostic.ResourceCPUSystemTime, Value: 3, Unit: diagnostic.ResourceUnitNanoseconds,
					Scope: diagnostic.ResourceScopeProcess, Source: diagnostic.ResourceSourceProcessState,
					Completeness: diagnostic.ResourceCompleteAtExit,
				},
			},
			PolicyDisposition: policy.RunClassificationNonRetryableFailure,
		},
	)
	if err != nil {
		t.Fatalf("CompleteRunWithDispositionAndFacts() error = %v", err)
	}

	return completed.Job, *completed.Run
}

func completeRetryableFactFailure(
	t *testing.T,
	database *Store,
	prefix uint64,
	now time.Time,
) (model.JobState, model.RunState) {
	t.Helper()
	jobID := mustJobID(t, prefix, 1)
	runID := mustRunID(t, prefix+1)
	supervisorID := mustSupervisorID(t, prefix+2, 1)
	credential := submitRuntimeJob(t, database, jobID, now)
	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
		t.Fatalf("ReserveRun() error = %v", err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, "/test/bin/worker",
		testProcessIdentity(7100+int(prefix%1000), "retryable-facts"), now.Add(3*time.Second),
	); err != nil {
		t.Fatalf("MarkProcessStarted() error = %v", err)
	}
	logs.Integrity = model.LogIntegrityValid
	exitCode := 2
	completedAt := now.Add(4 * time.Second)
	next := now.Add(10 * time.Second)
	completed, err := database.CompleteRunWithDispositionAndFacts(
		t.Context(), jobID, runID, model.RunOutcomeFailure,
		&model.ExitInfo{ExitCode: &exitCode, ObservedAt: completedAt}, logs, "", completedAt,
		model.RunDisposition{NextPhase: model.JobPhaseBackoff, NextRunAt: &next, Reason: "retryable_failure"},
		RunDiagnosticFactsInput{PolicyDisposition: policy.RunClassificationRetryableFailure},
	)
	if err != nil {
		t.Fatalf("CompleteRunWithDispositionAndFacts() error = %v", err)
	}

	return completed.Job, *completed.Run
}
