package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/model"
)

func TestStoreDeferredRuntimeOperations(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "deferred-runtime-operations", newSequentialEventIDs(0xfa00))
	now := storeTestTime()
	jobID := mustJobID(t, 0xfa01, 1)
	runID := mustRunID(t, 0xfa01)
	supervisorID := mustSupervisorID(t, 0xfa01, 2)
	credential := submitRuntimeJob(t, database, jobID, now)

	if database.StateDir() == "" || database.DatabasePath() != filepath.Join(database.StateDir(), DatabaseFilename) || database.SQLiteVersion() == "" {
		t.Fatalf("store metadata = state %q, path %q, SQLite %q", database.StateDir(), database.DatabasePath(), database.SQLiteVersion())
	}
	job, err := database.GetJob(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	event, err := database.TransitionEvent(t.Context(), model.EntityJob, jobID.String(), job.Revision)
	if err != nil {
		t.Fatalf("TransitionEvent() error = %v", err)
	}
	eventID, err := database.TransitionEventID(t.Context(), model.EntityJob, jobID.String(), job.Revision)
	if err != nil || eventID != event.ID {
		t.Fatalf("TransitionEventID() = (%s, %v), want %s", eventID, err, event.ID)
	}
	byID, err := database.TransitionEventByID(t.Context(), event.ID)
	if err != nil || byID.ID != event.ID || !byID.OccurredAt.Equal(event.OccurredAt) ||
		!bytes.Equal(byID.Details, event.Details) {
		t.Fatalf("TransitionEventByID() = (%+v, %v), want %+v", byID, err, event)
	}
	if _, err := database.TransitionEventByID(t.Context(), "invalid"); err == nil {
		t.Fatal("TransitionEventByID(invalid) error = nil")
	}
	if _, err := database.GetSupervisorForJob(t.Context(), jobID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSupervisorForJob(unclaimed) error = %v", err)
	}

	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	if owner, err := database.GetSupervisorForJob(t.Context(), jobID); err != nil || owner.ID != supervisorID {
		t.Fatalf("GetSupervisorForJob() = (%+v, %v)", owner, err)
	}
	if err := database.MarkPrerequisitesSatisfied(t.Context(), jobID, now.Add(2*time.Second)); err != nil {
		t.Fatalf("MarkPrerequisitesSatisfied() error = %v", err)
	}
	if err := database.SetInputEndpoint(t.Context(), jobID, "/tmp/jobman-input.sock", now.Add(3*time.Second)); err != nil {
		t.Fatalf("SetInputEndpoint() error = %v", err)
	}
	runtimeState, err := database.GetRuntime(t.Context(), jobID)
	if err != nil || runtimeState.InputEndpoint == "" || runtimeState.PrerequisitesSatisfiedAt == nil {
		t.Fatalf("GetRuntime() = (%+v, %v)", runtimeState, err)
	}

	global := uint64(2)
	poolCapacity := uint64(1)
	if err := database.SetConcurrencyLimit(t.Context(), "", &global, now); err != nil {
		t.Fatal(err)
	}
	if err := database.SetConcurrencyLimit(t.Context(), "build", &poolCapacity, now); err != nil {
		t.Fatal(err)
	}
	if err := database.ValidateAdmissionRequest(t.Context(), "build", 1); err != nil {
		t.Fatalf("ValidateAdmissionRequest() error = %v", err)
	}
	if err := database.ValidateAdmissionRequest(t.Context(), "missing", 1); err == nil {
		t.Fatal("ValidateAdmissionRequest(missing pool) error = nil")
	}
	admission, err := database.TryAcquireAdmission(t.Context(), jobID, "build", 1, now.Add(4*time.Second), time.Second)
	if err != nil || admission.JobID != jobID {
		t.Fatalf("TryAcquireAdmission() = (%+v, %v)", admission, err)
	}
	if _, err := database.TryAcquireAdmission(t.Context(), jobID, "build", 1, now.Add(5*time.Second), time.Second); err != nil {
		t.Fatalf("repeated TryAcquireAdmission() error = %v", err)
	}
	if err := database.RenewAdmission(t.Context(), jobID, now.Add(5*time.Second), 2*time.Second); err != nil {
		t.Fatalf("RenewAdmission() error = %v", err)
	}
	if expired, err := database.ListExpiredAdmissions(t.Context(), now.Add(8*time.Second)); err != nil || len(expired) != 1 {
		t.Fatalf("ListExpiredAdmissions() = (%+v, %v)", expired, err)
	}
	if expired, err := database.ListExpiredOwnedJobs(t.Context(), now.Add(2*time.Minute)); err != nil || len(expired) != 1 || expired[0] != jobID {
		t.Fatalf("ListExpiredOwnedJobs() = (%+v, %v)", expired, err)
	}

	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := database.BindAdmissionToRun(t.Context(), jobID, runID); err != nil {
		t.Fatalf("BindAdmissionToRun() error = %v", err)
	}
	if err := database.ResetInputEOF(t.Context(), jobID, now.Add(7*time.Second)); err != nil {
		t.Fatalf("ResetInputEOF() error = %v", err)
	}
	if err := database.RecordInputEOF(t.Context(), jobID, runID, now.Add(8*time.Second)); err != nil {
		t.Fatalf("RecordInputEOF() error = %v", err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, "/test/bin/worker", testProcessIdentity(7001, "coverage"), now.Add(9*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	timedOut, err := database.RequestRunTimeout(t.Context(), jobID, now.Add(10*time.Second))
	if err != nil || timedOut.Run == nil || timedOut.Run.StopRequestedAt == nil || timedOut.Run.StopReason != model.StopReasonTimeout {
		t.Fatalf("RequestRunTimeout() = (%+v, %v)", timedOut, err)
	}
	if repeated, err := database.RequestRunTimeout(t.Context(), jobID, now.Add(11*time.Second)); err != nil || len(repeated.Events) != 0 {
		t.Fatalf("repeated RequestRunTimeout() = (%+v, %v)", repeated, err)
	}
	if err := database.ReleaseAdmission(t.Context(), jobID, now.Add(12*time.Second)); err != nil {
		t.Fatalf("ReleaseAdmission() error = %v", err)
	}
}

func TestStoreTimeoutAndCompletionWithoutRun(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "timeout-without-run", newSequentialEventIDs(0xfb00))
	now := storeTestTime()

	timeoutJob := mustJobID(t, 0xfb01, 1)
	timeoutCredential := submitRuntimeJob(t, database, timeoutJob, now)
	claimRuntimeJob(t, database, timeoutJob, mustSupervisorID(t, 0xfb01, 2), timeoutCredential, now)
	result, err := database.RequestTimeout(t.Context(), timeoutJob, now.Add(2*time.Second))
	if err != nil || result.Job.Cancellation == nil || result.Job.Cancellation.Reason != model.StopReasonTimeout {
		t.Fatalf("RequestTimeout() = (%+v, %v)", result, err)
	}
	if repeated, err := database.RequestTimeout(t.Context(), timeoutJob, now.Add(3*time.Second)); err != nil || len(repeated.Events) != 0 {
		t.Fatalf("repeated RequestTimeout() = (%+v, %v)", repeated, err)
	}

	failedJob := mustJobID(t, 0xfb02, 1)
	failedCredential := submitRuntimeJob(t, database, failedJob, now)
	claimRuntimeJob(t, database, failedJob, mustSupervisorID(t, 0xfb02, 2), failedCredential, now)
	completed, err := database.CompleteWithoutRun(
		t.Context(), failedJob, model.JobOutcomeAborted, "prerequisite_failed", now.Add(4*time.Second),
	)
	if err != nil || completed.Job.Phase != model.JobPhaseCompleted {
		t.Fatalf("CompleteWithoutRun() = (%+v, %v)", completed, err)
	}
}

func TestRequestTimeoutRejectsMissingActiveRun(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "timeout-missing-active-run", newSequentialEventIDs(0xfb80))
	now := storeTestTime()
	jobID := mustJobID(t, 0xfb81, 1)
	runID := mustRunID(t, 0xfb81)
	credential := submitRuntimeJob(t, database, jobID, now)
	claimRuntimeJob(t, database, jobID, mustSupervisorID(t, 0xfb81, 2), credential, now)
	if _, err := database.ReserveRun(
		t.Context(), jobID, runID, 1,
		testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy),
		now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	// Simulate an externally corrupted database in which the job still points
	// at an active run that no longer exists. Timeout handling must surface the
	// lookup failure instead of recording a transition with incomplete state.
	if _, err := database.db.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(t.Context(), `DELETE FROM runs WHERE id = ?`, runID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.RequestTimeout(t.Context(), jobID, now.Add(2*time.Second)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RequestTimeout(missing active run) error = %v, want ErrNotFound", err)
	}
}

func TestNotificationDeliveryLeaseQueriesAndRenewal(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "notification-lease-coverage", newSequentialEventIDs(0xfc00))
	now := storeTestTime()
	jobID := mustJobID(t, 0xfc01, 1)
	submitRuntimeJob(t, database, jobID, now)
	event := notificationJobEvent(t, database, jobID)
	queued, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{{
		JobID: jobID, EventID: event.ID, NotifierName: "webhook", EventType: "job_started", MaxAttempts: 2,
	}})
	if err != nil || len(queued) != 1 {
		t.Fatalf("QueueNotificationDeliveries() = (%+v, %v)", queued, err)
	}
	readyAt, found, err := database.NextNotificationDeliveryAt(t.Context(), event.ID)
	if err != nil || !found || !readyAt.Equal(now) {
		t.Fatalf("NextNotificationDeliveryAt() = (%v, %t, %v), want %v", readyAt, found, err, now)
	}
	claimed, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RenewNotificationDelivery(
		t.Context(), event.ID, claimed.NotifierName, claimed.ClaimToken, now.Add(time.Second), now.Add(2*time.Minute),
	); err != nil {
		t.Fatalf("RenewNotificationDelivery() error = %v", err)
	}
	if _, _, err := database.NextNotificationDeliveryAt(t.Context(), model.EventID("invalid")); err == nil {
		t.Fatal("NextNotificationDeliveryAt(invalid) error = nil")
	}
	if err := database.RenewNotificationDelivery(
		t.Context(), event.ID, claimed.NotifierName, mustEventID(t, 0xfcff, 1), now, now.Add(time.Minute),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("RenewNotificationDelivery(stale) error = %v, want ErrConflict", err)
	}
}

func TestStoreErrorsAndValidationHelpers(t *testing.T) {
	t.Parallel()

	schemaErr := (&SchemaError{Reason: "too new"}).Error()
	if schemaErr != "incompatible Jobman database: too new" {
		t.Fatalf("SchemaError.Error() = %q", schemaErr)
	}
	revision := &RevisionConflictError{Entity: "job", ID: "id", ExpectedRevision: 2, ExpectedPhase: "running"}
	if revision.Error() == "" || !errors.Is(revision, ErrConflict) {
		t.Fatalf("RevisionConflictError = %v", revision)
	}
	driverErr := errors.New("driver locked")
	busy := &BusyError{Operation: "write", Err: driverErr}
	if busy.Error() == "" || !errors.Is(busy, ErrBusy) || !errors.Is(busy, driverErr) {
		t.Fatalf("BusyError = %v", busy)
	}
	if !bytes.Equal([]byte(admissionScopeName("")), []byte("global")) || admissionScopeName("build") != "pool \"build\"" {
		t.Fatal("admissionScopeName() returned an unexpected value")
	}
	if !containsNotificationEvent([]string{"job_started"}, "job_started") || containsNotificationEvent(nil, "job_started") {
		t.Fatal("containsNotificationEvent() returned an unexpected value")
	}
	if _, err := uintFromDatabase("revision", 0); err == nil {
		t.Fatal("uintFromDatabase(0) error = nil")
	}
	if _, err := optionalUintFromDatabase("value", sql.NullInt64{Valid: true, Int64: -1}); err == nil {
		t.Fatal("optionalUintFromDatabase(-1) error = nil")
	}
	if value, err := optionalUintFromDatabase("value", sql.NullInt64{}); err != nil || value != 0 {
		t.Fatalf("optionalUintFromDatabase(null) = (%d, %v)", value, err)
	}
	if _, err := databaseUint("value", 0); err == nil {
		t.Fatal("databaseUint(0) error = nil")
	}
	if _, err := databaseUint("value", math.MaxUint64); err == nil {
		t.Fatal("databaseUint(MaxUint64) error = nil")
	}
	if err := decodeStrictJSON([]byte(`{"known":1} {}`), &struct {
		Known int `json:"known"`
	}{}); err == nil {
		t.Fatal("decodeStrictJSON(multiple values) error = nil")
	}
	if err := decodeStrictJSON([]byte(`{"unknown":1}`), &struct{}{}); err == nil {
		t.Fatal("decodeStrictJSON(unknown field) error = nil")
	}
	if _, err := nonnegativeUintFromDatabase("value", -1); err == nil {
		t.Fatal("nonnegativeUintFromDatabase(-1) error = nil")
	}
	if _, err := nonnegativeIntFromDatabase("value", -1); err == nil {
		t.Fatal("nonnegativeIntFromDatabase(-1) error = nil")
	}
	if runtime.GOOS != "windows" {
		if err := validateOwner(fakeFileInfo{name: "fake"}); err == nil {
			t.Fatal("validateOwner(fake metadata) error = nil")
		}
		if err := validateSingleLink(fakeFileInfo{name: "fake"}); err == nil {
			t.Fatal("validateSingleLink(fake metadata) error = nil")
		}
	}
}

func TestAutomaticNotificationQueueAndEventMapping(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "automatic-notification-queue", newSequentialEventIDs(0xfd00))
	now := storeTestTime()
	jobID := mustJobID(t, 0xfd01, 1)
	credential := bytes.Repeat([]byte{0x77}, 32)
	hash, err := model.NewCredentialHash(credential)
	if err != nil {
		t.Fatal(err)
	}
	absoluteRoot := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable: filepath.Join(absoluteRoot, "test", "bin", "worker"), WorkingDirectory: t.TempDir(), StdinPolicy: model.StdinNull,
		StopPolicy: model.StopPolicy{GracePeriod: time.Second, ForceAfterGrace: true},
		ExecutionPolicy: model.ExecutionPolicy{
			Notifications: []model.NotificationSubscription{{Notifier: "hook", Events: []string{"job_started"}}},
			NotifierDefinitions: []model.NotifierDefinition{{
				Name: "hook", Kind: model.NotifierCommand, Timeout: time.Second,
				Retry:   model.NotifierRetryPolicy{MaxAttempts: 3},
				Command: &model.CommandNotifierDefinition{Executable: filepath.Join(absoluteRoot, "bin", "true"), OutputLimit: 1024},
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewJobSpec() error = %v", err)
	}
	if _, err := database.Submit(t.Context(), jobID, specification, hash, now, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Claim(
		t.Context(), jobID, credential, mustSupervisorID(t, 0xfd01, 2),
		testProcessIdentity(5001, "notify"), now.Add(time.Second), now.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	deliveries, err := database.ListNotificationDeliveries(t.Context(), jobID)
	if err != nil || len(deliveries) != 1 || deliveries[0].EventType != "job_started" || deliveries[0].MaxAttempts != 3 {
		t.Fatalf("automatic deliveries = (%+v, %v)", deliveries, err)
	}

	runOutcomes := map[model.RunOutcome]string{
		model.RunOutcomeSuccess: "run_succeeded", model.RunOutcomeFailure: "run_failed",
		model.RunOutcomeStartFailed: "run_failed", model.RunOutcomeTimedOut: "run_timed_out",
		model.RunOutcomeCancelled: "run_cancelled", //nolint:misspell // Persisted event vocabulary uses British spelling.
		model.RunOutcomeLost:      "run_lost",
	}
	for outcome, want := range runOutcomes {
		if got := notificationTypeForRunOutcome(outcome); got != want {
			t.Errorf("notificationTypeForRunOutcome(%q) = %q, want %q", outcome, got, want)
		}
	}
	jobOutcomes := map[model.JobOutcome]string{
		model.JobOutcomeSuccess: "job_succeeded", model.JobOutcomeFailure: "job_failed",
		model.JobOutcomeTimedOut:  "job_timed_out",
		model.JobOutcomeCancelled: "job_cancelled", //nolint:misspell // Persisted event vocabulary uses British spelling.
		model.JobOutcomeAborted:   "job_aborted", model.JobOutcomeLost: "job_lost",
		model.JobOutcomeSubmissionFailed: "job_submission_failed", model.JobOutcomeNone: "",
	}
	for outcome, want := range jobOutcomes {
		if got := notificationTypeForJobOutcome(outcome); got != want {
			t.Errorf("notificationTypeForJobOutcome(%q) = %q, want %q", outcome, got, want)
		}
	}
}

func TestStorePublicValidationMatrix(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "public-validation-matrix", newSequentialEventIDs(0xfe00))
	ctx := t.Context()
	now := storeTestTime()
	invalidJob := model.JobID("invalid")
	invalidRun := model.RunID("invalid")
	invalidSupervisor := model.SupervisorID("invalid")
	if _, err := database.GetJob(ctx, invalidJob); err == nil {
		t.Error("GetJob(invalid) error = nil")
	}
	if _, err := database.GetRun(ctx, invalidRun); err == nil {
		t.Error("GetRun(invalid) error = nil")
	}
	if _, err := database.GetSupervisor(ctx, invalidSupervisor); err == nil {
		t.Error("GetSupervisor(invalid) error = nil")
	}
	if _, err := database.GetSupervisorForJob(ctx, invalidJob); err == nil {
		t.Error("GetSupervisorForJob(invalid) error = nil")
	}
	if _, err := database.GetRuntime(ctx, invalidJob); err == nil {
		t.Error("GetRuntime(invalid) error = nil")
	}
	if _, _, err := database.GetAdmission(ctx, invalidJob); err == nil {
		t.Error("GetAdmission(invalid) error = nil")
	}
	if _, err := database.ListWaitEvaluations(ctx, invalidJob); err == nil {
		t.Error("ListWaitEvaluations(invalid) error = nil")
	}
	if _, err := database.ListNotificationAttempts(ctx, invalidJob); err == nil {
		t.Error("ListNotificationAttempts(invalid) error = nil")
	}
	if _, err := database.ListNotificationDeliveries(ctx, invalidJob); err == nil {
		t.Error("ListNotificationDeliveries(invalid) error = nil")
	}
	if _, err := database.TransitionEvent(ctx, model.EntityKind("invalid"), "id", 1); err == nil {
		t.Error("TransitionEvent(invalid) error = nil")
	}
	if err := database.RecordInputEOF(ctx, invalidJob, invalidRun, now); err == nil {
		t.Error("RecordInputEOF(invalid) error = nil")
	}
	if err := database.SetDependencies(ctx, invalidJob, nil); err == nil {
		t.Error("SetDependencies(invalid) error = nil")
	}
	if err := database.ValidateAdmissionRequest(ctx, "", 0); err == nil {
		t.Error("ValidateAdmissionRequest(zero) error = nil")
	}
	if err := database.ValidateAdmissionRequest(ctx, " bad ", 1); err == nil {
		t.Error("ValidateAdmissionRequest(untrimmed) error = nil")
	}
	if err := database.SetConcurrencyLimit(ctx, " bad ", nil, now); err == nil {
		t.Error("SetConcurrencyLimit(untrimmed) error = nil")
	}
	zero := uint64(0)
	if err := database.SetConcurrencyLimit(ctx, "", &zero, now); err == nil {
		t.Error("SetConcurrencyLimit(zero) error = nil")
	}
	if _, err := database.TryAcquireAdmission(ctx, invalidJob, "", 1, now, time.Second); err == nil {
		t.Error("TryAcquireAdmission(invalid ID) error = nil")
	}
	if _, err := database.TryAcquireAdmission(ctx, mustJobID(t, 0xfe01, 1), "", 0, now, time.Second); err == nil {
		t.Error("TryAcquireAdmission(zero slots) error = nil")
	}
	if _, err := database.TryAcquireAdmission(ctx, mustJobID(t, 0xfe01, 1), " bad ", 1, now, time.Second); err == nil {
		t.Error("TryAcquireAdmission(untrimmed) error = nil")
	}
	if err := database.RecordWaitEvaluation(ctx, mustJobID(t, 0xfe01, 1), -1, model.WaitDelay, false, "", now); err == nil {
		t.Error("RecordWaitEvaluation(negative) error = nil")
	}
	if _, err := database.ListJobs(ctx, ListJobsOptions{Limit: MaximumListLimit + 1}); err == nil {
		t.Error("ListJobs(large limit) error = nil")
	}
	if _, err := database.ListJobs(ctx, ListJobsOptions{Phase: model.JobPhase("invalid")}); err == nil {
		t.Error("ListJobs(invalid phase) error = nil")
	}
	if _, err := database.ListJobs(ctx, ListJobsOptions{Outcome: model.JobOutcome("invalid")}); err == nil {
		t.Error("ListJobs(invalid outcome) error = nil")
	}
	if _, err := database.ListJobs(ctx, ListJobsOptions{Active: true, Completed: true}); err == nil {
		t.Error("ListJobs(conflicting completion filters) error = nil")
	}
}

func TestHardenExistingEmptyStateDirectoryErrors(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing")
	if err := hardenExistingEmptyStateDirectory(missing, false); err == nil {
		t.Fatal("hardenExistingEmptyStateDirectory(missing) error = nil")
	}
}

func TestPersistedConversionValidation(t *testing.T) {
	t.Parallel()

	jobID := mustJobID(t, 0xff01, 1)
	if _, err := admissionFromColumns(jobID, sql.NullString{}, sql.NullString{}, 0, 1, 2, sql.NullInt64{}); err == nil {
		t.Fatal("admissionFromColumns(zero slots) error = nil")
	}
	if _, err := admissionFromColumns(
		jobID, sql.NullString{Valid: true, String: "invalid"}, sql.NullString{}, 1, 1, 2, sql.NullInt64{},
	); err == nil {
		t.Fatal("admissionFromColumns(invalid run ID) error = nil")
	}
	if capacityHasRoom(1, 2, 1) || capacityHasRoom(2, 1, 2) || !capacityHasRoom(2, 1, 1) {
		t.Fatal("capacityHasRoom() returned an unexpected result")
	}
	if !(DependencyFinish.Matches(model.JobOutcomeSuccess)) || DependencyFailed.Matches(model.JobOutcomeSuccess) ||
		!DependencyFailed.Matches(model.JobOutcomeFailure) || DependencySuccess.Matches(model.JobOutcomeFailure) {
		t.Fatal("DependencyPredicate.Matches() returned an unexpected result")
	}
	if DependencyPredicate("outcomes:success,success").Valid() || DependencyPredicate("outcomes:unknown").Valid() {
		t.Fatal("invalid outcome-set predicate reported valid")
	}
}

func TestClosedStorePropagatesEveryPublicOperation(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "closed-public-operations", newSequentialEventIDs(0x11000))
	ctx := t.Context()
	now := storeTestTime()
	jobID := mustJobID(t, 0x11001, 1)
	runID := mustRunID(t, 0x11001)
	supervisorID := mustSupervisorID(t, 0x11001, 2)
	eventID := mustEventID(t, 0x11001, 3)
	claimToken := mustEventID(t, 0x11001, 4)
	credential := bytes.Repeat([]byte{0x31}, 32)
	hash, err := model.NewCredentialHash(credential)
	if err != nil {
		t.Fatal(err)
	}
	specification := testJobSpec(t, "closed")
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s on closed store error = nil", name)
		}
	}
	_, err = database.GetJob(ctx, jobID)
	check("GetJob", err)
	_, err = database.GetRun(ctx, runID)
	check("GetRun", err)
	_, err = database.GetSupervisor(ctx, supervisorID)
	check("GetSupervisor", err)
	_, err = database.GetSupervisorForJob(ctx, jobID)
	check("GetSupervisorForJob", err)
	_, err = database.ListJobs(ctx, ListJobsOptions{})
	check("ListJobs", err)
	_, err = database.ListRuns(ctx, jobID)
	check("ListRuns", err)
	_, err = database.ResolveJob(ctx, jobID.String())
	check("ResolveJob", err)
	_, _, err = database.GetJobWithRuns(ctx, jobID.String())
	check("GetJobWithRuns", err)
	_, err = database.TransitionEvent(ctx, model.EntityJob, jobID.String(), 1)
	check("TransitionEvent", err)
	_, err = database.TransitionEventID(ctx, model.EntityJob, jobID.String(), 1)
	check("TransitionEventID", err)

	_, err = database.Submit(ctx, jobID, specification, hash, now, now.Add(time.Minute))
	check("Submit", err)
	_, err = database.SubmitWithDependencies(ctx, jobID, specification, hash, now, now.Add(time.Minute), nil)
	check("SubmitWithDependencies", err)
	_, err = database.Claim(
		ctx, jobID, credential, supervisorID, testProcessIdentity(101, "closed"), now, now.Add(time.Minute),
	)
	check("Claim", err)
	_, err = database.ReserveRun(ctx, jobID, runID, 1, logs, now)
	check("ReserveRun", err)
	_, err = database.MarkProcessStarted(ctx, jobID, runID, "/bin/true", testProcessIdentity(102, "closed"), now)
	check("MarkProcessStarted", err)
	_, err = database.MarkStartFailed(ctx, jobID, runID, logs, "start_failed", now)
	check("MarkStartFailed", err)
	exitCode := 1
	exit := &model.ExitInfo{ExitCode: &exitCode, ObservedAt: now}
	_, err = database.FinalizeRun(ctx, jobID, runID, model.RunOutcomeFailure, exit, logs, now)
	check("FinalizeRun", err)
	_, err = database.RequestCancellation(ctx, jobID, now)
	check("RequestCancellation", err)
	_, err = database.FinalizeCancellationWithoutRun(ctx, jobID, now)
	check("FinalizeCancellationWithoutRun", err)
	_, err = database.MarkSubmissionFailed(ctx, jobID, "expired", now)
	check("MarkSubmissionFailed", err)
	_, err = database.MarkOwnershipLost(ctx, jobID, &logs, "lost", now)
	check("MarkOwnershipLost", err)
	_, err = database.RenewLease(ctx, supervisorID, now, now.Add(time.Minute))
	check("RenewLease", err)
	_, err = database.ReleaseSupervisor(ctx, supervisorID, now)
	check("ReleaseSupervisor", err)

	check("MarkPrerequisitesSatisfied", database.MarkPrerequisitesSatisfied(ctx, jobID, now))
	check("ResetInputEOF", database.ResetInputEOF(ctx, jobID, now))
	check("SetInputEndpoint", database.SetInputEndpoint(ctx, jobID, "/tmp/input", now))
	check("RecordInputEOF", database.RecordInputEOF(ctx, jobID, runID, now))
	_, err = database.GetRuntime(ctx, jobID)
	check("GetRuntime", err)
	_, err = database.MoveJob(ctx, jobID, model.JobPhaseQueued, now, "ready")
	check("MoveJob", err)
	_, err = database.CompleteRunWithDisposition(
		ctx, jobID, runID, model.RunOutcomeFailure, exit, logs, "failed", now,
		model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure},
	)
	check("CompleteRunWithDisposition", err)
	_, err = database.CompleteWithoutRun(ctx, jobID, model.JobOutcomeAborted, "aborted", now)
	check("CompleteWithoutRun", err)
	_, err = database.RequestTimeout(ctx, jobID, now)
	check("RequestTimeout", err)
	_, err = database.RequestRunTimeout(ctx, jobID, now)
	check("RequestRunTimeout", err)
	_, err = database.Pause(ctx, jobID, now)
	check("Pause", err)
	_, err = database.Resume(ctx, jobID, now)
	check("Resume", err)
	check("SetDependencies", database.SetDependencies(ctx, jobID, []Dependency{{
		JobID: jobID, DependsOn: mustJobID(t, 0x11002, 1), Predicate: DependencySuccess,
	}}))
	_, err = database.ListDependencies(ctx, jobID)
	check("ListDependencies", err)
	_, err = database.EvaluateDependencies(ctx, jobID, now)
	check("EvaluateDependencies", err)
	check("RecordWaitEvaluation", database.RecordWaitEvaluation(ctx, jobID, 0, model.WaitDelay, false, "waiting", now))
	_, err = database.ListWaitEvaluations(ctx, jobID)
	check("ListWaitEvaluations", err)

	limit := uint64(1)
	check("SetConcurrencyLimit", database.SetConcurrencyLimit(ctx, "", &limit, now))
	check("ValidateAdmissionRequest", database.ValidateAdmissionRequest(ctx, "", 1))
	_, err = database.TryAcquireAdmission(ctx, jobID, "", 1, now, time.Minute)
	check("TryAcquireAdmission", err)
	check("BindAdmissionToRun", database.BindAdmissionToRun(ctx, jobID, runID))
	check("RenewAdmission", database.RenewAdmission(ctx, jobID, now, time.Minute))
	check("ReleaseAdmission", database.ReleaseAdmission(ctx, jobID, now))
	_, _, err = database.GetAdmission(ctx, jobID)
	check("GetAdmission", err)
	_, err = database.ListExpiredAdmissions(ctx, now)
	check("ListExpiredAdmissions", err)
	_, err = database.ListExpiredOwnedJobs(ctx, now)
	check("ListExpiredOwnedJobs", err)

	_, err = database.QueueNotificationDeliveries(ctx, []QueueNotificationDeliveryInput{{
		JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started", MaxAttempts: 2,
	}})
	check("QueueNotificationDeliveries", err)
	_, err = database.ClaimNotificationDelivery(ctx, eventID, now, now.Add(time.Minute))
	check("ClaimNotificationDelivery", err)
	check("RenewNotificationDelivery", database.RenewNotificationDelivery(
		ctx, eventID, "ops", claimToken, now, now.Add(time.Minute),
	))
	_, err = database.CompleteNotificationDelivery(ctx, CompleteNotificationDeliveryInput{
		EventID: eventID, ClaimToken: claimToken, NotifierName: "ops", AttemptNumber: 1,
		StartedAt: now, CompletedAt: now, Succeeded: true,
	})
	check("CompleteNotificationDelivery", err)
	_, _, err = database.NextNotificationDeliveryAt(ctx, eventID)
	check("NextNotificationDeliveryAt", err)
	_, err = database.ListNotificationDeliveries(ctx, jobID)
	check("ListNotificationDeliveries", err)
	_, err = database.RecordNotificationAttempt(ctx, RecordNotificationAttemptInput{
		JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started",
		AttemptNumber: 1, StartedAt: now, CompletedAt: now, Succeeded: true,
	})
	check("RecordNotificationAttempt", err)
	_, err = database.ListNotificationAttempts(ctx, jobID)
	check("ListNotificationAttempts", err)
	check("MarkRunLogsPruned", database.MarkRunLogsPruned(ctx, runID, now, 3, 1))
}

func TestNotificationValidationStateMatrices(t *testing.T) {
	t.Parallel()

	now := storeTestTime()
	jobID := mustJobID(t, 0x12001, 1)
	eventID := mustEventID(t, 0x12001, 2)
	attemptID := mustEventID(t, 0x12001, 3)
	statusCode := 200
	validInput := RecordNotificationAttemptInput{
		JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started",
		AttemptNumber: 1, StartedAt: now, CompletedAt: now, Succeeded: true,
	}
	if err := validateNotificationAttemptInput(validInput); err != nil {
		t.Fatalf("validateNotificationAttemptInput(valid) error = %v", err)
	}
	for _, mutate := range []func(*RecordNotificationAttemptInput){
		func(value *RecordNotificationAttemptInput) { value.JobID = "invalid" },
		func(value *RecordNotificationAttemptInput) { value.EventID = "invalid" },
		func(value *RecordNotificationAttemptInput) { value.NotifierName = " bad " },
		func(value *RecordNotificationAttemptInput) { value.EventType = "invalid" },
		func(value *RecordNotificationAttemptInput) { value.AttemptNumber = 0 },
		func(value *RecordNotificationAttemptInput) { value.StartedAt = time.Time{} },
		func(value *RecordNotificationAttemptInput) {
			before := now.Add(-time.Second)
			value.NextAttemptAt = &before
		},
		func(value *RecordNotificationAttemptInput) { value.DiagnosticCode = "timeout" },
		func(value *RecordNotificationAttemptInput) {
			value.Succeeded = false
			value.DiagnosticCode = ""
		},
		func(value *RecordNotificationAttemptInput) {
			value.ResponseStatusCode = func() *int { v := 99; return &v }()
		},
		func(value *RecordNotificationAttemptInput) { value.MessageID = "bad\nmessage" },
	} {
		input := validInput
		mutate(&input)
		if err := validateNotificationAttemptInput(input); err == nil {
			t.Errorf("validateNotificationAttemptInput(%+v) error = nil", input)
		}
	}

	started := now
	completed := now.Add(time.Second)
	validAttempt := NotificationAttempt{
		ID: attemptID, JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started",
		AttemptNumber: 1, Status: NotificationAttemptSucceeded, CreatedAt: now,
		StartedAt: &started, CompletedAt: &completed, ResponseStatusCode: &statusCode,
	}
	if err := validatePersistedNotificationAttempt(validAttempt); err != nil {
		t.Fatalf("validatePersistedNotificationAttempt(valid) error = %v", err)
	}
	for _, mutate := range []func(*NotificationAttempt){
		func(value *NotificationAttempt) { value.ID = "invalid" },
		func(value *NotificationAttempt) { value.NotifierName = "" },
		func(value *NotificationAttempt) { value.CreatedAt = time.Time{} },
		func(value *NotificationAttempt) {
			value.Status = NotificationAttemptPending
			value.StartedAt = &started
		},
		func(value *NotificationAttempt) {
			value.Status = NotificationAttemptDelivering
			value.CompletedAt = &completed
		},
		func(value *NotificationAttempt) {
			value.Status = NotificationAttemptSucceeded
			value.Retryable = true
		},
		func(value *NotificationAttempt) {
			value.Status = NotificationAttemptFailed
			value.DiagnosticCode = ""
		},
		func(value *NotificationAttempt) { value.Status = "invalid" },
		func(value *NotificationAttempt) { value.ResponseStatusCode = func() *int { v := 1000; return &v }() },
	} {
		attempt := validAttempt
		mutate(&attempt)
		if err := validatePersistedNotificationAttempt(attempt); err == nil {
			t.Errorf("validatePersistedNotificationAttempt(%+v) error = nil", attempt)
		}
	}

	next := now
	validDelivery := NotificationDelivery{
		JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started",
		Status: NotificationDeliveryPending, OccurredAt: now, CreatedAt: now,
		NextAttemptAt: &next, MaxAttempts: 2,
	}
	if err := validatePersistedNotificationDelivery(validDelivery); err != nil {
		t.Fatalf("validatePersistedNotificationDelivery(valid) error = %v", err)
	}
	claim := mustEventID(t, 0x12001, 4)
	lease := now.Add(time.Minute)
	delivering := validDelivery
	delivering.Status = NotificationDeliveryDelivering
	delivering.NextAttemptAt = nil
	delivering.ClaimToken = claim
	delivering.ClaimedAt = &now
	delivering.ClaimExpiresAt = &lease
	if err := validatePersistedNotificationDelivery(delivering); err != nil {
		t.Fatalf("validatePersistedNotificationDelivery(delivering) error = %v", err)
	}
	terminal := validDelivery
	terminal.Status = NotificationDeliverySucceeded
	terminal.NextAttemptAt = nil
	terminal.CompletedAt = &completed
	terminal.AttemptCount = 1
	if err := validatePersistedNotificationDelivery(terminal); err != nil {
		t.Fatalf("validatePersistedNotificationDelivery(terminal) error = %v", err)
	}
	for _, mutate := range []func(*NotificationDelivery){
		func(value *NotificationDelivery) { value.JobID = "invalid" },
		func(value *NotificationDelivery) { value.Status = "invalid" },
		func(value *NotificationDelivery) { value.NextAttemptAt = nil },
		func(value *NotificationDelivery) {
			*value = delivering
			value.ClaimToken = "invalid"
		},
		func(value *NotificationDelivery) {
			*value = terminal
			value.CompletedAt = nil
		},
	} {
		delivery := validDelivery
		mutate(&delivery)
		if err := validatePersistedNotificationDelivery(delivery); err == nil {
			t.Errorf("validatePersistedNotificationDelivery(%+v) error = nil", delivery)
		}
	}
}

func TestNotificationRowDecodingMatrices(t *testing.T) {
	t.Parallel()

	now := storeTestTime().UnixNano()
	jobID := mustJobID(t, 0x12101, 1).String()
	eventID := mustEventID(t, 0x12101, 2).String()
	attemptID := mustEventID(t, 0x12101, 3).String()
	runID := mustRunID(t, 0x12101).String()
	claimToken := mustEventID(t, 0x12101, 4).String()
	attemptValues := []any{
		attemptID, jobID, eventID, "ops", "job_started", int64(1), string(NotificationAttemptSucceeded),
		now,
		sql.NullInt64{Valid: true, Int64: now},
		sql.NullInt64{Valid: true, Int64: now},
		sql.NullInt64{},
		sql.NullString{},
		int64(0),
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullString{},
		int64(0),
	}
	if _, err := scanNotificationAttempt(&staticRow{values: attemptValues}); err != nil {
		t.Fatalf("scanNotificationAttempt(valid) error = %v", err)
	}
	for _, index := range []int{0, 1, 2} {
		values := append([]any(nil), attemptValues...)
		values[index] = "invalid"
		if _, err := scanNotificationAttempt(&staticRow{values: values}); err == nil {
			t.Errorf("scanNotificationAttempt(invalid field %d) error = nil", index)
		}
	}
	for _, change := range []struct {
		index int
		value any
	}{
		{index: 6, value: "invalid"},
		{index: 5, value: int64(0)},
		{index: 12, value: int64(2)},
	} {
		values := append([]any(nil), attemptValues...)
		values[change.index] = change.value
		if _, err := scanNotificationAttempt(&staticRow{values: values}); err == nil {
			t.Errorf("scanNotificationAttempt(invalid metadata %d) error = nil", change.index)
		}
	}

	deliveryValues := []any{
		jobID, eventID, "ops", "job_started",
		sql.NullString{Valid: true, String: runID},
		now, now, int64(2), int64(0), string(NotificationDeliveryDelivering),
		sql.NullInt64{},
		sql.NullString{Valid: true, String: claimToken},
		sql.NullInt64{Valid: true, Int64: now},
		sql.NullInt64{Valid: true, Int64: now + int64(time.Minute)},
		sql.NullInt64{},
	}
	if _, err := scanNotificationDelivery(&staticRow{values: deliveryValues}); err != nil {
		t.Fatalf("scanNotificationDelivery(valid) error = %v", err)
	}
	for _, index := range []int{0, 1} {
		values := append([]any(nil), deliveryValues...)
		values[index] = "invalid"
		if _, err := scanNotificationDelivery(&staticRow{values: values}); err == nil {
			t.Errorf("scanNotificationDelivery(invalid field %d) error = nil", index)
		}
	}
	for _, change := range []struct {
		index int
		value any
	}{
		{index: 4, value: sql.NullString{Valid: true, String: "invalid"}},
		{index: 11, value: sql.NullString{Valid: true, String: "invalid"}},
		{index: 9, value: "invalid"},
	} {
		values := append([]any(nil), deliveryValues...)
		values[change.index] = change.value
		if _, err := scanNotificationDelivery(&staticRow{values: values}); err == nil {
			t.Errorf("scanNotificationDelivery(invalid metadata %d) error = nil", change.index)
		}
	}
	if _, err := scanNotificationAttempt(&staticRow{err: errors.New("scan failed")}); err == nil {
		t.Fatal("scanNotificationAttempt(scan error) error = nil")
	}
	if _, err := scanNotificationDelivery(&staticRow{err: errors.New("scan failed")}); err == nil {
		t.Fatal("scanNotificationDelivery(scan error) error = nil")
	}
}

func TestCoreStateRowDecodingMatrices(t *testing.T) {
	t.Parallel()

	now := storeTestTime().UnixNano()
	jobID := mustJobID(t, 0x12201, 1).String()
	runID := mustRunID(t, 0x12201).String()
	supervisorID := mustSupervisorID(t, 0x12201, 2).String()
	specification := testJobSpec(t, "row")
	specificationJSON, err := specification.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	credential, err := model.NewCredentialHash(bytes.Repeat([]byte{0x51}, 32))
	if err != nil {
		t.Fatal(err)
	}
	jobValues := []any{
		jobID,
		sql.NullString{Valid: true, String: "row"},
		string(specificationJSON),
		string(model.JobPhaseSubmitting),
		sql.NullString{},
		int64(1), now,
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullString{},
		sql.NullString{},
		sql.NullInt64{},
		sql.NullString{},
		sql.NullString{},
		credential.Bytes(),
		sql.NullInt64{Valid: true, Int64: now + int64(time.Minute)},
	}
	if _, err := scanJob(&staticRow{values: jobValues}); err != nil {
		t.Fatalf("scanJob(valid) error = %v", err)
	}
	for _, change := range []struct {
		index int
		value any
	}{
		{index: 0, value: "invalid"},
		{index: 2, value: "{}"},
		{index: 1, value: sql.NullString{Valid: true, String: "wrong"}},
		{index: 5, value: int64(0)},
		{index: 10, value: sql.NullString{Valid: true, String: "invalid"}},
		{index: 11, value: sql.NullString{Valid: true, String: "invalid"}},
		{index: 15, value: []byte{1}},
		{index: 3, value: "invalid"},
	} {
		values := append([]any(nil), jobValues...)
		values[change.index] = change.value
		if _, err := scanJob(&staticRow{values: values}); err == nil {
			t.Errorf("scanJob(invalid field %d) error = nil", change.index)
		}
	}

	absoluteRoot := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	runValues := []any{
		runID, jobID, int64(1), string(model.RunPhaseStarting),
		sql.NullString{},
		int64(1),
		sql.NullString{},
		now,
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullString{},
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullString{},
		sql.NullInt64{},
		sql.NullString{},
		sql.NullString{},
		sql.NullInt64{},
		filepath.Join(absoluteRoot, "stdout"), filepath.Join(absoluteRoot, "stderr"),
		filepath.Join(absoluteRoot, "index"), int64(0), int64(0), int(1),
		string(model.LogIntegrityPending), string(model.RecordingHealthy),
		sql.NullString{},
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullInt64{},
		sql.NullString{},
	}
	if _, err := scanRun(&staticRow{values: runValues}); err != nil {
		t.Fatalf("scanRun(valid) error = %v", err)
	}
	identity := testProcessIdentity(42, "row")
	identityJSON, err := jsonValue(identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		index int
		value any
	}{
		{index: 0, value: "invalid"},
		{index: 1, value: "invalid"},
		{index: 2, value: int64(0)},
		{index: 5, value: int64(0)},
		{index: 28, value: sql.NullInt64{Valid: true, Int64: -1}},
		{index: 29, value: sql.NullInt64{Valid: true, Int64: -1}},
		{index: 13, value: sql.NullString{Valid: true, String: "{"}},
		{index: 13, value: sql.NullString{Valid: true, String: string(identityJSON)}},
		{index: 3, value: "invalid"},
	} {
		values := append([]any(nil), runValues...)
		values[change.index] = change.value
		if change.index == 13 && change.value.(sql.NullString).String == string(identityJSON) {
			values[12] = sql.NullInt64{Valid: true, Int64: 41}
		}
		if _, err := scanRun(&staticRow{values: values}); err == nil {
			t.Errorf("scanRun(invalid field %d) error = nil", change.index)
		}
	}
	supervisorValues := []any{
		supervisorID, jobID, int64(1), int64(identity.PID), string(identityJSON),
		now, now, now + int64(time.Minute),
		sql.NullInt64{},
	}
	if _, err := scanSupervisor(&staticRow{values: supervisorValues}); err != nil {
		t.Fatalf("scanSupervisor(valid) error = %v", err)
	}
	for _, change := range []struct {
		index int
		value any
	}{
		{index: 0, value: "invalid"},
		{index: 1, value: "invalid"},
		{index: 2, value: int64(0)},
		{index: 4, value: "{"},
		{index: 3, value: int64(identity.PID + 1)},
		{index: 7, value: now - 1},
	} {
		values := append([]any(nil), supervisorValues...)
		values[change.index] = change.value
		if _, err := scanSupervisor(&staticRow{values: values}); err == nil {
			t.Errorf("scanSupervisor(invalid field %d) error = nil", change.index)
		}
	}
	for name, scan := range map[string]func(rowScanner) error{
		"job":        func(row rowScanner) error { _, err := scanJob(row); return err },
		"run":        func(row rowScanner) error { _, err := scanRun(row); return err },
		"supervisor": func(row rowScanner) error { _, err := scanSupervisor(row); return err },
	} {
		if err := scan(&staticRow{err: errors.New("scan failed")}); err == nil {
			t.Errorf("scan%s(scan error) error = nil", name)
		}
	}
}

func TestSchemaAndMigrationValidationEdges(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		application int
		version     int
		wantError   bool
	}{
		{name: "empty", application: 0, version: 0},
		{name: "current", application: applicationID, version: currentSchemaVersion},
		{name: "foreign application", application: applicationID + 1, version: 0, wantError: true},
		{name: "negative version", application: 0, version: -1, wantError: true},
		{name: "future version", application: applicationID, version: currentSchemaVersion + 1, wantError: true},
		{name: "version without application", application: 0, version: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateSchemaHeaders(test.application, test.version)
			if (err != nil) != test.wantError {
				t.Fatalf("validateSchemaHeaders(%#x, %d) error = %v, wantError %t", test.application, test.version, err, test.wantError)
			}
		})
	}

	for _, test := range []struct {
		name string
		got  [3]int
		want [3]int
		less bool
	}{
		{name: "major less", got: [3]int{2, 99, 99}, want: [3]int{3, 0, 0}, less: true},
		{name: "major greater", got: [3]int{4, 0, 0}, want: [3]int{3, 99, 99}},
		{name: "minor less", got: [3]int{3, 50, 99}, want: [3]int{3, 51, 0}, less: true},
		{name: "minor greater", got: [3]int{3, 52, 0}, want: [3]int{3, 51, 99}},
		{name: "patch less", got: [3]int{3, 51, 2}, want: [3]int{3, 51, 3}, less: true},
		{name: "equal", got: [3]int{3, 51, 3}, want: [3]int{3, 51, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := versionLessThan(test.got[0], test.got[1], test.got[2], test.want[0], test.want[1], test.want[2])
			if got != test.less {
				t.Fatalf("versionLessThan(%v, %v) = %t, want %t", test.got, test.want, got, test.less)
			}
		})
	}

	database := openTestStore(t, "schema-validation-edges", newSequentialEventIDs(0x12300))
	application, version, err := readSchemaHeaders(t.Context(), database.db)
	if err != nil || application != applicationID || version != currentSchemaVersion {
		t.Fatalf("readSchemaHeaders() = (%#x, %d, %v)", application, version, err)
	}
	if err := verifyAppliedMigrations(t.Context(), database.db, 0); err != nil {
		t.Fatalf("verifyAppliedMigrations(version 0) error = %v", err)
	}
	if err := database.applyMigration(t.Context(), nil, migration{version: 2}, 0); err == nil {
		t.Fatal("applyMigration(skipped sequence) error = nil")
	}

	t.Run("migration SQL failure", func(t *testing.T) {
		tx, err := database.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		if err := database.applyMigration(t.Context(), tx, migration{version: 9, sql: "not valid SQL"}, 8); err == nil {
			t.Fatal("applyMigration(invalid SQL) error = nil")
		}
	})

	t.Run("migration record failure", func(t *testing.T) {
		tx, err := database.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		item := migration{version: 9, sql: "DROP TABLE schema_migrations"}
		if err := database.applyMigration(t.Context(), tx, item, 8); err == nil {
			t.Fatal("applyMigration(missing history table) error = nil")
		}
	})
}

func TestMigrationHistoryAndSchemaCorruptionEdges(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate string
		verify func(*Store) error
	}{
		{
			name:   "unexpected history version",
			mutate: "UPDATE schema_migrations SET version = 99 WHERE version = 1",
			verify: func(database *Store) error {
				return verifyAppliedMigrations(t.Context(), database.db, currentSchemaVersion)
			},
		},
		{
			name:   "history checksum mismatch",
			mutate: "UPDATE schema_migrations SET checksum = '0000000000000000000000000000000000000000000000000000000000000000' WHERE version = 1",
			verify: func(database *Store) error {
				return verifyAppliedMigrations(t.Context(), database.db, currentSchemaVersion)
			},
		},
		{
			name:   "history shorter than header",
			mutate: "DELETE FROM schema_migrations WHERE version = 8",
			verify: func(database *Store) error {
				return verifyAppliedMigrations(t.Context(), database.db, currentSchemaVersion)
			},
		},
		{
			name:   "wrong application header",
			mutate: "PRAGMA application_id = 0",
			verify: func(database *Store) error { return database.verifySchema(t.Context()) },
		},
		{
			name:   "wrong version header",
			mutate: "PRAGMA user_version = 0",
			verify: func(database *Store) error { return database.verifySchema(t.Context()) },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := openTestStore(t, test.name, newSequentialEventIDs(0x12400))
			if _, err := database.db.ExecContext(t.Context(), test.mutate); err != nil {
				t.Fatalf("mutate schema: %v", err)
			}
			if err := test.verify(database); err == nil {
				t.Fatal("schema verification error = nil")
			}
		})
	}

	database := openTestStore(t, "closed-schema", newSequentialEventIDs(0x12410))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readSchemaHeaders(t.Context(), database.db); err == nil {
		t.Fatal("readSchemaHeaders(closed database) error = nil")
	}
	if err := verifyAppliedMigrations(t.Context(), database.db, 1); err == nil {
		t.Fatal("verifyAppliedMigrations(closed database) error = nil")
	}
}

func TestStoreOpenAndPathValidationEdges(t *testing.T) {
	t.Parallel()

	if _, err := Open(nil, Options{StateDir: t.TempDir()}); err == nil { //nolint:staticcheck // Nil is the validation input under test.
		t.Fatal("Open(nil context) error = nil")
	}
	if _, err := Open(t.Context(), Options{StateDir: t.TempDir(), BusyTimeout: -time.Second}); err == nil {
		t.Fatal("Open(negative busy timeout) error = nil")
	}
	if _, err := Open(t.Context(), Options{StateDir: t.TempDir(), BusyTimeout: time.Duration(math.MaxInt64)}); err == nil {
		t.Fatal("Open(oversized busy timeout) error = nil")
	}
	if _, err := prepareStateDir(""); err == nil {
		t.Fatal("prepareStateDir(empty) error = nil")
	}

	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareStateDir(filepath.Join(file, "child")); err == nil {
		t.Fatal("prepareStateDir(beneath file) error = nil")
	}
	if err := prepareDatabaseFile(filepath.Join(root, "missing", DatabaseFilename)); err == nil {
		t.Fatal("prepareDatabaseFile(missing parent) error = nil")
	}
	if err := validateDatabaseFile(filepath.Join(root, "missing.db")); err == nil {
		t.Fatal("validateDatabaseFile(missing) error = nil")
	}
	directory := filepath.Join(root, "directory.db")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateDatabaseFile(directory); err == nil {
		t.Fatal("validateDatabaseFile(directory) error = nil")
	}

	if runtime.GOOS != "windows" {
		link := filepath.Join(root, "link.db")
		if err := os.Symlink(file, link); err != nil {
			t.Fatal(err)
		}
		if err := validateDatabaseFile(link); err == nil {
			t.Fatal("validateDatabaseFile(symlink) error = nil")
		}
		hardLink := filepath.Join(root, "hard-link")
		if err := os.Link(file, hardLink); err != nil {
			t.Fatal(err)
		}
		if err := validateDatabaseFile(file); err == nil {
			t.Fatal("validateDatabaseFile(hard-linked file) error = nil")
		}
	}
}

func TestPureStorePolicyAndValidationMatrices(t *testing.T) {
	t.Parallel()

	jobID := mustJobID(t, 0x12501, 1)
	eventID := mustEventID(t, 0x12501, 2)
	if _, err := OutcomeSetPredicate(nil); err == nil {
		t.Error("OutcomeSetPredicate(nil) error = nil")
	}
	if _, err := OutcomeSetPredicate([]model.JobOutcome{"invalid"}); err == nil {
		t.Error("OutcomeSetPredicate(invalid) error = nil")
	}
	predicate, err := OutcomeSetPredicate([]model.JobOutcome{
		model.JobOutcomeFailure, model.JobOutcomeSuccess, model.JobOutcomeFailure,
	})
	if err != nil || predicate != "outcomes:failure,success" {
		t.Fatalf("OutcomeSetPredicate() = (%q, %v)", predicate, err)
	}
	for _, kind := range []model.WaitConditionKind{
		model.WaitUntil, model.WaitDelay, model.WaitFileExists, model.WaitProbe,
	} {
		if !validWaitConditionKind(kind) {
			t.Errorf("validWaitConditionKind(%q) = false", kind)
		}
	}
	if validWaitConditionKind("unknown") {
		t.Error("validWaitConditionKind(unknown) = true")
	}
	for value, want := range map[string]bool{
		"": true, "019f6455": true, "019f6455-d324": true,
		"019f645X": false, "019f6455xd324": false,
		"019f6455-d324-7c44-97c1-aba2a9c4dca70": false,
	} {
		if got := validIDPrefix(value); got != want {
			t.Errorf("validIDPrefix(%q) = %t, want %t", value, got, want)
		}
	}

	validQueue := QueueNotificationDeliveryInput{
		JobID: jobID, EventID: eventID, NotifierName: "ops", EventType: "job_started", MaxAttempts: 1,
	}
	if err := validateQueueNotificationInput(validQueue); err != nil {
		t.Fatalf("validateQueueNotificationInput(valid) error = %v", err)
	}
	for _, mutate := range []func(*QueueNotificationDeliveryInput){
		func(input *QueueNotificationDeliveryInput) { input.JobID = "invalid" },
		func(input *QueueNotificationDeliveryInput) { input.NotifierName = " bad " },
		func(input *QueueNotificationDeliveryInput) { input.EventType = "invalid" },
		func(input *QueueNotificationDeliveryInput) { input.MaxAttempts = 101 },
	} {
		input := validQueue
		mutate(&input)
		if err := validateQueueNotificationInput(input); err == nil {
			t.Errorf("validateQueueNotificationInput(%+v) error = nil", input)
		}
	}
	if validNotifierName("") || validNotifierName(" bad ") || validNotifierName("bad\nname") ||
		validNotifierName(string(bytes.Repeat([]byte{'a'}, 129))) || !validNotifierName("good-name") {
		t.Error("validNotifierName() matrix returned an unexpected result")
	}

	for _, event := range []model.StateEvent{
		{EventDraft: model.EventDraft{Entity: model.EntityRun, Type: model.EventProcessStarted}},
		{EventDraft: model.EventDraft{Entity: model.EntityRun, Type: model.EventRunCompleted, ToOutcome: string(model.RunOutcomeSuccess)}},
		{EventDraft: model.EventDraft{Entity: model.EntityRun, Type: model.EventJobQueued}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventSupervisorClaimed}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventRetryScheduled}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventSubmissionFailed}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventOwnershipLost}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventJobCompleted, ToOutcome: string(model.JobOutcomeSuccess)}},
		{EventDraft: model.EventDraft{Entity: model.EntityJob, Type: model.EventProcessStarted}},
		{EventDraft: model.EventDraft{Entity: model.EntitySupervisor}},
		{EventDraft: model.EventDraft{Entity: model.EntityKind("unknown")}},
	} {
		_ = notificationTypeForStateEvent(event)
	}
	if notificationTypeForRunOutcome("unknown") != "" || notificationTypeForJobOutcome("unknown") != "" {
		t.Error("unknown notification outcome produced an event type")
	}
	if _, err := jsonValue(make(chan struct{})); err == nil {
		t.Error("jsonValue(channel) error = nil")
	}
}

func TestStoreLifecycleConflictAndValidationPaths(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "lifecycle-conflicts", newSequentialEventIDs(0x12600))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12601, 1)
	runID := mustRunID(t, 0x12601)
	supervisorID := mustSupervisorID(t, 0x12601, 2)
	credential := submitRuntimeJob(t, database, jobID, now)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)

	if _, err := database.Claim(
		t.Context(), jobID, bytes.Repeat([]byte{0xff}, 32), supervisorID,
		testProcessIdentity(6100, "wrong-credential"), now, now.Add(time.Minute),
	); err == nil {
		t.Error("Claim(wrong credential) error = nil")
	}
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now); err == nil {
		t.Error("ReserveRun(submitting) error = nil")
	}
	if _, err := database.MoveJob(t.Context(), jobID, model.JobPhaseQueued, now, "invalid"); err == nil {
		t.Error("MoveJob(submitting) error = nil")
	}
	if _, err := database.Pause(t.Context(), jobID, now); err == nil {
		t.Error("Pause(submitting) error = nil")
	}
	if _, err := database.Resume(t.Context(), jobID, now); !errors.Is(err, ErrConflict) {
		t.Errorf("Resume(submitting) error = %v, want ErrConflict", err)
	}
	if _, err := database.RequestRunTimeout(t.Context(), jobID, now); !errors.Is(err, ErrConflict) {
		t.Errorf("RequestRunTimeout(no run) error = %v, want ErrConflict", err)
	}
	if _, err := database.CompleteWithoutRun(
		t.Context(), jobID, model.JobOutcomeAborted, "invalid_phase", now,
	); err == nil {
		t.Error("CompleteWithoutRun(submitting) error = nil")
	}
	if _, err := database.FinalizeCancellationWithoutRun(t.Context(), jobID, now); err == nil {
		t.Error("FinalizeCancellationWithoutRun(submitting) error = nil")
	}
	if _, err := database.MarkOwnershipLost(t.Context(), jobID, nil, "lost", now); err == nil {
		t.Error("MarkOwnershipLost(submitting) error = nil")
	}

	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	if _, err := database.MarkSubmissionFailed(t.Context(), jobID, "expired", now); err == nil {
		t.Error("MarkSubmissionFailed(claimed) error = nil")
	}
	if _, err := database.RenewLease(t.Context(), supervisorID, now.Add(time.Minute), now); err == nil {
		t.Error("RenewLease(backward expiry) error = nil")
	}
	if err := database.SetDependencies(t.Context(), jobID, nil); !errors.Is(err, ErrConflict) {
		t.Errorf("SetDependencies(claimed) error = %v, want ErrConflict", err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, "", testProcessIdentity(6101, "missing-run"), now,
	); err == nil {
		t.Error("MarkProcessStarted(missing run) error = nil")
	}
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, "", testProcessIdentity(6102, "empty-path"), now.Add(2*time.Second),
	); err == nil {
		t.Error("MarkProcessStarted(empty executable) error = nil")
	}
	if _, err := database.MarkProcessStarted(
		t.Context(), jobID, runID, "/bin/true", testProcessIdentity(6103, "running"), now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.MarkStartFailed(
		t.Context(), jobID, runID, logs, "late_start_failure", now.Add(3*time.Second),
	); err == nil {
		t.Error("MarkStartFailed(running) error = nil")
	}
	if _, err := database.FinalizeRun(
		t.Context(), jobID, runID, model.RunOutcome("invalid"), nil, logs, now.Add(3*time.Second),
	); err == nil {
		t.Error("FinalizeRun(invalid outcome) error = nil")
	}
	if _, err := database.Pause(t.Context(), jobID, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if repeated, err := database.Pause(t.Context(), jobID, now.Add(5*time.Second)); err != nil || len(repeated.Events) != 0 {
		t.Fatalf("Pause(repeated) = (%+v, %v)", repeated, err)
	}
	if _, err := database.Resume(t.Context(), jobID, now.Add(3*time.Second)); err == nil {
		t.Error("Resume(before pause) error = nil")
	}

	if err := database.MarkRunLogsPruned(t.Context(), "invalid", now, 0, 0); err == nil {
		t.Error("MarkRunLogsPruned(invalid ID) error = nil")
	}
	if err := database.MarkRunLogsPruned(t.Context(), runID, time.Time{}, 0, 0); err == nil {
		t.Error("MarkRunLogsPruned(zero time) error = nil")
	}
	if err := database.MarkRunLogsPruned(t.Context(), runID, now, math.MaxUint64, 0); err == nil {
		t.Error("MarkRunLogsPruned(overflow) error = nil")
	}
	if err := database.MarkRunLogsPruned(t.Context(), runID, now.Add(5*time.Second), 0, 0); !errors.Is(err, ErrConflict) {
		t.Errorf("MarkRunLogsPruned(active run) error = %v, want ErrConflict", err)
	}
}

func TestAdmissionCapacityAndQueueBranches(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "admission-capacity-branches", newSequentialEventIDs(0x12700))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12701, 1)
	submitRuntimeJob(t, database, jobID, now)
	one := uint64(1)
	two := uint64(2)
	five := uint64(5)
	if err := database.SetConcurrencyLimit(t.Context(), "build", &two, now); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name    string
		request admissionRequest
		global  *uint64
		wantErr bool
	}{
		{name: "undeclared pool", request: admissionRequest{pool: "missing", slots: 1}, wantErr: true},
		{name: "pool impossible", request: admissionRequest{pool: "build", slots: 3}, wantErr: true},
		{name: "global impossible", request: admissionRequest{pool: "build", slots: 2}, global: &one, wantErr: true},
		{name: "fits", request: admissionRequest{pool: "build", slots: 1}, global: &five},
	}
	for _, check := range checks {
		fits, err := admissionRequestFits(t.Context(), database.db, check.request, check.global)
		if (err != nil) != check.wantErr || (!check.wantErr && !fits) {
			t.Errorf("admissionRequestFits(%s) = (%t, %v)", check.name, fits, err)
		}
	}

	if _, err := database.db.ExecContext(t.Context(), `
		INSERT INTO admission_requests(job_id, pool_name, slots, enqueued_at_ns)
		VALUES (?, ?, ?, ?)`, jobID.String(), "build", 3, now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := queuedRequestsFitLimit(t.Context(), database.db, "", &two); !errors.Is(err, ErrAdmissionImpossible) {
		t.Errorf("queuedRequestsFitLimit(global) error = %v", err)
	}
	if err := queuedRequestsFitLimit(t.Context(), database.db, "build", &two); !errors.Is(err, ErrAdmissionImpossible) {
		t.Errorf("queuedRequestsFitLimit(pool) error = %v", err)
	}
	if err := queuedRequestsFitLimit(t.Context(), database.db, "build", nil); err != nil {
		t.Errorf("queuedRequestsFitLimit(unlimited) error = %v", err)
	}

	activeJob := mustJobID(t, 0x12702, 1)
	submitRuntimeJob(t, database, activeJob, now)
	if _, err := database.db.ExecContext(t.Context(), `
		INSERT INTO admissions(job_id, pool_name, slots, acquired_at_ns, lease_expires_at_ns)
		VALUES (?, ?, 1, ?, ?)`, activeJob.String(), "build", now.UnixNano(), now.Add(time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if fits, err := admissionFits(t.Context(), database.db, "", 1, &one, nil); err != nil || fits {
		t.Errorf("admissionFits(full global) = (%t, %v)", fits, err)
	}
	if fits, err := admissionFits(t.Context(), database.db, "build", 1, nil, &one); err != nil || fits {
		t.Errorf("admissionFits(full pool) = (%t, %v)", fits, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := activeSlots(canceled, database.db, "", false); err == nil {
		t.Error("activeSlots(canceled context) error = nil")
	}
}

func TestDependencyValidationAndCycleBranches(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "dependency-validation-branches", newSequentialEventIDs(0x12800))
	now := storeTestTime()
	jobA := mustJobID(t, 0x12801, 1)
	jobB := mustJobID(t, 0x12802, 1)
	jobC := mustJobID(t, 0x12803, 1)
	for _, jobID := range []model.JobID{jobA, jobB, jobC} {
		submitRuntimeJob(t, database, jobID, now)
	}

	if err := database.SetDependencies(t.Context(), jobA, []Dependency{{
		JobID: jobB, DependsOn: jobB, Predicate: DependencySuccess,
	}}); err == nil {
		t.Error("SetDependencies(mismatched owner) error = nil")
	}
	if err := database.SetDependencies(t.Context(), jobA, []Dependency{{
		JobID: jobA, DependsOn: jobA, Predicate: DependencySuccess,
	}}); err == nil {
		t.Error("SetDependencies(self edge) error = nil")
	}
	missing := mustJobID(t, 0x128ff, 1)
	if err := database.SetDependencies(t.Context(), jobA, []Dependency{{
		JobID: jobA, DependsOn: missing, Predicate: DependencySuccess,
	}}); err == nil {
		t.Error("SetDependencies(missing dependency) error = nil")
	}
	edge := Dependency{JobID: jobA, DependsOn: jobB, Predicate: DependencySuccess}
	if err := database.SetDependencies(t.Context(), jobA, []Dependency{edge, edge}); err != nil {
		t.Fatalf("SetDependencies(duplicate identical) error = %v", err)
	}
	contradiction := edge
	contradiction.Predicate = DependencyFailed
	if err := database.SetDependencies(t.Context(), jobA, []Dependency{edge, contradiction}); err == nil {
		t.Error("SetDependencies(contradictory) error = nil")
	}
	if err := database.SetDependencies(t.Context(), jobB, []Dependency{{
		JobID: jobB, DependsOn: jobA, Predicate: DependencySuccess,
	}}); err == nil {
		t.Error("SetDependencies(cycle) error = nil")
	}
}

func TestNotificationQueueFailureAndReplayBranches(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "notification-queue-branches", newSequentialEventIDs(0x12900))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12901, 1)
	submitRuntimeJob(t, database, jobID, now)
	event := notificationJobEvent(t, database, jobID)
	valid := QueueNotificationDeliveryInput{
		JobID: jobID, EventID: event.ID, NotifierName: "ops", EventType: "job_started", MaxAttempts: 2,
	}
	if queued, err := database.QueueNotificationDeliveries(t.Context(), nil); err != nil || len(queued) != 0 {
		t.Fatalf("QueueNotificationDeliveries(nil) = (%+v, %v)", queued, err)
	}
	invalid := valid
	invalid.NotifierName = " bad "
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{invalid}); err == nil {
		t.Error("QueueNotificationDeliveries(invalid) error = nil")
	}
	missing := valid
	missing.EventID = mustEventID(t, 0x129ff, 1)
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{missing}); !errors.Is(err, ErrNotFound) {
		t.Errorf("QueueNotificationDeliveries(missing event) error = %v", err)
	}
	originalNow := database.now
	database.now = func() time.Time { return time.Time{} }
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{valid}); err == nil {
		t.Error("QueueNotificationDeliveries(invalid clock) error = nil")
	}
	database.now = originalNow
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{valid}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{valid}); err != nil {
		t.Fatalf("QueueNotificationDeliveries(idempotent) error = %v", err)
	}
	changed := valid
	changed.MaxAttempts = 3
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{changed}); !errors.Is(err, ErrConflict) {
		t.Errorf("QueueNotificationDeliveries(changed) error = %v", err)
	}
	if _, err := database.ClaimNotificationDelivery(t.Context(), "invalid", now, now.Add(time.Minute)); err == nil {
		t.Error("ClaimNotificationDelivery(invalid ID) error = nil")
	}
	if _, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now, now); err == nil {
		t.Error("ClaimNotificationDelivery(invalid lease) error = nil")
	}
	claimed, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RenewNotificationDelivery(
		t.Context(), "invalid", "ops", claimed.ClaimToken, now, now.Add(time.Minute),
	); err == nil {
		t.Error("RenewNotificationDelivery(invalid identity) error = nil")
	}
	if err := database.RenewNotificationDelivery(
		t.Context(), event.ID, "ops", claimed.ClaimToken, now, now,
	); err == nil {
		t.Error("RenewNotificationDelivery(invalid lease) error = nil")
	}

	baseCompletion := CompleteNotificationDeliveryInput{
		EventID: event.ID, ClaimToken: claimed.ClaimToken, NotifierName: "ops", AttemptNumber: 1,
		StartedAt: now, CompletedAt: now.Add(time.Second), Succeeded: true,
	}
	invalidCompletion := baseCompletion
	invalidCompletion.AttemptNumber = 0
	if _, err := database.CompleteNotificationDelivery(t.Context(), invalidCompletion); err == nil {
		t.Error("CompleteNotificationDelivery(invalid identity) error = nil")
	}
	missingCompletion := baseCompletion
	missingCompletion.EventID = mustEventID(t, 0x129ff, 2)
	if _, err := database.CompleteNotificationDelivery(t.Context(), missingCompletion); !errors.Is(err, ErrNotFound) {
		t.Errorf("CompleteNotificationDelivery(missing) error = %v", err)
	}
	badAttempt := baseCompletion
	badAttempt.Succeeded = false
	if _, err := database.CompleteNotificationDelivery(t.Context(), badAttempt); err == nil {
		t.Error("CompleteNotificationDelivery(invalid attempt) error = nil")
	}
	wrongOwner := baseCompletion
	wrongOwner.ClaimToken = mustEventID(t, 0x129ff, 3)
	if _, err := database.CompleteNotificationDelivery(t.Context(), wrongOwner); !errors.Is(err, ErrConflict) {
		t.Errorf("CompleteNotificationDelivery(wrong owner) error = %v", err)
	}
	retryMismatch := baseCompletion
	retryMismatch.Succeeded = false
	retryMismatch.Retryable = true
	retryMismatch.DiagnosticCode = "temporary"
	if _, err := database.CompleteNotificationDelivery(t.Context(), retryMismatch); err == nil {
		t.Error("CompleteNotificationDelivery(retry mismatch) error = nil")
	}
	completed, err := database.CompleteNotificationDelivery(t.Context(), baseCompletion)
	if err != nil || completed.Status != NotificationDeliverySucceeded {
		t.Fatalf("CompleteNotificationDelivery(success) = (%+v, %v)", completed, err)
	}
	if _, err := database.CompleteNotificationDelivery(t.Context(), baseCompletion); err != nil {
		t.Fatalf("CompleteNotificationDelivery(replay) error = %v", err)
	}
	if _, found, err := database.NextNotificationDeliveryAt(t.Context(), event.ID); err != nil || found {
		t.Fatalf("NextNotificationDeliveryAt(completed) = (%t, %v)", found, err)
	}
	if _, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now.Add(time.Hour), now.Add(2*time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Errorf("ClaimNotificationDelivery(no ready work) error = %v", err)
	}
}

func TestNotificationClaimTokenGenerationFailure(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "notification-token-failure", newSequentialEventIDs(0x12a00))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12a01, 1)
	submitRuntimeJob(t, database, jobID, now)
	event := notificationJobEvent(t, database, jobID)
	if _, err := database.QueueNotificationDeliveries(t.Context(), []QueueNotificationDeliveryInput{{
		JobID: jobID, EventID: event.ID, NotifierName: "ops", EventType: "job_started", MaxAttempts: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	database.eventIDs = failingEventIDSource{err: errors.New("entropy failed")}
	if _, err := database.ClaimNotificationDelivery(t.Context(), event.ID, now, now.Add(time.Minute)); err == nil {
		t.Error("ClaimNotificationDelivery(ID failure) error = nil")
	}
}

func TestCorruptRuntimeAndWaitEvaluationRows(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "corrupt-runtime-rows", newSequentialEventIDs(0x12b00))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12b01, 1)
	submitRuntimeJob(t, database, jobID, now)
	if _, err := database.GetRuntime(t.Context(), mustJobID(t, 0x12bff, 1)); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRuntime(missing) error = %v", err)
	}
	if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"revision", "run_count", "success_count", "failure_count"} {
		value := -1
		if field == "revision" {
			value = 0
		}
		if _, err := database.db.ExecContext(
			t.Context(), "UPDATE job_runtime SET "+field+" = ? WHERE job_id = ?", value, jobID.String(),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := database.GetRuntime(t.Context(), jobID); err == nil {
			t.Errorf("GetRuntime(corrupt %s) error = nil", field)
		}
		if _, err := database.db.ExecContext(
			t.Context(), "UPDATE job_runtime SET "+field+" = ? WHERE job_id = ?", 1, jobID.String(),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.db.ExecContext(t.Context(), `
		INSERT INTO wait_evaluations(
			job_id, condition_index, condition_kind, evaluated_at_ns, attempt_count
		) VALUES (?, 0, ?, ?, 1)`, jobID.String(), string(model.WaitDelay), now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		field string
		bad   any
		good  any
	}{
		{field: "condition_index", bad: -1, good: 0},
		{field: "attempt_count", bad: -1, good: 1},
		{field: "condition_kind", bad: "invalid", good: string(model.WaitDelay)},
	} {
		if _, err := database.db.ExecContext(
			t.Context(), "UPDATE wait_evaluations SET "+mutation.field+" = ? WHERE job_id = ?", mutation.bad, jobID.String(),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ListWaitEvaluations(t.Context(), jobID); err == nil {
			t.Errorf("ListWaitEvaluations(corrupt %s) error = nil", mutation.field)
		}
		if _, err := database.db.ExecContext(
			t.Context(), "UPDATE wait_evaluations SET "+mutation.field+" = ? WHERE job_id = ?", mutation.good, jobID.String(),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreInternalTransitionDefenses(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "internal-transition-defenses", newSequentialEventIDs(0x12c00))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12c01, 1)
	runID := mustRunID(t, 0x12c01)
	supervisorID := mustSupervisorID(t, 0x12c01, 2)
	credential := submitRuntimeJob(t, database, jobID, now)
	job, err := database.GetJob(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTransition(model.TransitionResult{}); err == nil {
		t.Error("validateTransition(invalid job) error = nil")
	}
	if err := validateTransition(model.TransitionResult{Job: job, Run: &model.RunState{}}); err == nil {
		t.Error("validateTransition(invalid run) error = nil")
	}
	if err := validateTransition(model.TransitionResult{Job: job, Supervisor: &model.SupervisorState{}}); err == nil {
		t.Error("validateTransition(invalid supervisor) error = nil")
	}
	if err := database.commitTransition(t.Context(), model.TransitionResult{}); err == nil {
		t.Error("commitTransition(invalid) error = nil")
	}
	if err := database.commitTransitionWithRuntime(
		t.Context(), model.TransitionResult{}, func(*sql.Tx) error { return nil },
	); err == nil {
		t.Error("commitTransitionWithRuntime(invalid) error = nil")
	}
	job.Revision = 0
	if _, err := jobValues(job); err == nil {
		t.Error("jobValues(zero revision) error = nil")
	}
	if _, found := eventForEntity(nil, model.EntityJob); found {
		t.Error("eventForEntity(nil) found an event")
	}
	originalIDs := database.eventIDs
	database.eventIDs = &constantEventIDSource{id: mustEventID(t, 0x12cff, 1)}
	if _, err := database.completeEvents([]model.EventDraft{{}}); err == nil {
		t.Error("completeEvents(invalid draft) error = nil")
	}
	database.eventIDs = originalIDs

	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	claimed, err := database.GetJob(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	supervisorState, err := database.GetSupervisor(t.Context(), supervisorID)
	if err != nil {
		t.Fatal(err)
	}
	badSupervisor := supervisorState
	badSupervisor.Revision = 0
	if _, err := supervisorValues(badSupervisor); err == nil {
		t.Error("supervisorValues(zero revision) error = nil")
	}
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	run, err := database.GetRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	badRun := run
	badRun.Number = 0
	if _, err := runValues(badRun); err == nil {
		t.Error("runValues(zero number) error = nil")
	}
	badRun = run
	badRun.Revision = 0
	if _, err := runValues(badRun); err == nil {
		t.Error("runValues(zero revision) error = nil")
	}
	if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyJobTransition(t.Context(), tx, model.TransitionResult{Job: claimed}, false); err != nil {
		t.Errorf("applyJobTransition(no event) error = %v", err)
	}
	if err := applyRunTransition(t.Context(), tx, model.TransitionResult{Job: claimed, Run: &run}); err != nil {
		t.Errorf("applyRunTransition(no event) error = %v", err)
	}
	if err := applySupervisorTransition(
		t.Context(), tx, model.TransitionResult{Job: claimed, Supervisor: &supervisorState},
	); err != nil {
		t.Errorf("applySupervisorTransition(no event) error = %v", err)
	}
	if err := insertEvents(t.Context(), tx, []model.StateEvent{{
		ID: mustEventID(t, 0x12cff, 2), EventDraft: model.EventDraft{Revision: 0},
	}}); err == nil {
		t.Error("insertEvents(zero revision) error = nil")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := requireOneUpdate(failingSQLResult{err: errors.New("rows failed")}, "job", jobID.String(), 1, "queued"); err == nil {
		t.Error("requireOneUpdate(rows error) error = nil")
	}

	secondID := mustJobID(t, 0x12c02, 1)
	secondCredential := submitRuntimeJob(t, database, secondID, now)
	claimRuntimeJob(t, database, secondID, mustSupervisorID(t, 0x12c02, 2), secondCredential, now)
	second, err := database.GetJob(t.Context(), secondID)
	if err != nil {
		t.Fatal(err)
	}
	moved, err := model.MoveJob(second, model.JobPhaseQueued, now.Add(time.Second), "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.commitTransitionWithRuntime(t.Context(), moved, func(*sql.Tx) error {
		return errors.New("runtime update failed")
	}); err == nil {
		t.Error("commitTransitionWithRuntime(callback failure) error = nil")
	}
}

func TestTransitionEventRejectsCorruptIdentifiers(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "corrupt-event-identifiers", newSequentialEventIDs(0x12d00))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12d01, 1)
	runID := mustRunID(t, 0x12d01)
	supervisorID := mustSupervisorID(t, 0x12d01, 2)
	credential := submitRuntimeJob(t, database, jobID, now)
	jobEvent := notificationJobEvent(t, database, jobID)
	if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(
		t.Context(), "UPDATE state_events SET id = 'invalid' WHERE id = ?", jobEvent.ID.String(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionEvent(t.Context(), model.EntityJob, jobID.String(), 1); err == nil {
		t.Error("TransitionEvent(invalid event ID) error = nil")
	}
	if _, err := database.db.ExecContext(
		t.Context(), "UPDATE state_events SET id = ? WHERE id = 'invalid'", jobEvent.ID.String(),
	); err != nil {
		t.Fatal(err)
	}
	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(
		t.Context(), "UPDATE state_events SET run_id = 'invalid' WHERE entity_kind = ? AND entity_id = ?",
		string(model.EntityRun), runID.String(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionEvent(t.Context(), model.EntityRun, runID.String(), 1); err == nil {
		t.Error("TransitionEvent(invalid run ID) error = nil")
	}
	if _, err := database.ListRuns(t.Context(), "invalid"); err == nil {
		t.Error("ListRuns(invalid job ID) error = nil")
	}
}

func TestStateOperationsPropagateEventIDFailures(t *testing.T) {
	t.Parallel()

	failEvents := func(database *Store) {
		database.eventIDs = failingEventIDSource{err: errors.New("event IDs unavailable")}
	}

	t.Run("reserve run", func(t *testing.T) {
		database, now, jobID, supervisorID := claimedFailureStore(t, 0x12e01)
		failEvents(database)
		if _, err := database.ReserveRun(
			t.Context(), jobID, mustRunID(t, 0x12e01), 1,
			testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy), now.Add(2*time.Second),
		); err == nil {
			t.Error("ReserveRun(event ID failure) error = nil")
		}
		_ = supervisorID
	})

	t.Run("move job", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e02)
		failEvents(database)
		if _, err := database.MoveJob(t.Context(), jobID, model.JobPhaseQueued, now, "ready"); err == nil {
			t.Error("MoveJob(event ID failure) error = nil")
		}
	})

	t.Run("request cancellation", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e03)
		failEvents(database)
		if _, err := database.RequestCancellation(t.Context(), jobID, now); err == nil {
			t.Error("RequestCancellation(event ID failure) error = nil")
		}
	})

	t.Run("request timeout", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e04)
		failEvents(database)
		if _, err := database.RequestTimeout(t.Context(), jobID, now); err == nil {
			t.Error("RequestTimeout(event ID failure) error = nil")
		}
	})

	t.Run("pause", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e05)
		if _, err := database.MoveJob(t.Context(), jobID, model.JobPhaseQueued, now, "ready"); err != nil {
			t.Fatal(err)
		}
		failEvents(database)
		if _, err := database.Pause(t.Context(), jobID, now.Add(time.Second)); err == nil {
			t.Error("Pause(event ID failure) error = nil")
		}
	})

	t.Run("resume", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e06)
		if _, err := database.MoveJob(t.Context(), jobID, model.JobPhaseQueued, now, "ready"); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Pause(t.Context(), jobID, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		failEvents(database)
		if _, err := database.Resume(t.Context(), jobID, now.Add(2*time.Second)); err == nil {
			t.Error("Resume(event ID failure) error = nil")
		}
	})

	t.Run("complete without run", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e07)
		failEvents(database)
		if _, err := database.CompleteWithoutRun(
			t.Context(), jobID, model.JobOutcomeAborted, "policy_failed", now,
		); err == nil {
			t.Error("CompleteWithoutRun(event ID failure) error = nil")
		}
	})

	t.Run("ownership lost", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e08)
		failEvents(database)
		if _, err := database.MarkOwnershipLost(t.Context(), jobID, nil, "lease_expired", now); err == nil {
			t.Error("MarkOwnershipLost(event ID failure) error = nil")
		}
	})

	t.Run("submission failed", func(t *testing.T) {
		database := openTestStore(t, "event-failure-submission", newSequentialEventIDs(0x12e09))
		now := storeTestTime()
		jobID := mustJobID(t, 0x12e09, 1)
		submitRuntimeJob(t, database, jobID, now)
		failEvents(database)
		if _, err := database.MarkSubmissionFailed(t.Context(), jobID, "claim_expired", now); err == nil {
			t.Error("MarkSubmissionFailed(event ID failure) error = nil")
		}
	})

	t.Run("renew lease", func(t *testing.T) {
		database, now, _, supervisorID := claimedFailureStore(t, 0x12e0a)
		failEvents(database)
		if _, err := database.RenewLease(t.Context(), supervisorID, now, now.Add(time.Minute)); err == nil {
			t.Error("RenewLease(event ID failure) error = nil")
		}
	})

	t.Run("release supervisor", func(t *testing.T) {
		database, now, _, supervisorID := claimedFailureStore(t, 0x12e0b)
		failEvents(database)
		if _, err := database.ReleaseSupervisor(t.Context(), supervisorID, now); err == nil {
			t.Error("ReleaseSupervisor(event ID failure) error = nil")
		}
	})

	t.Run("mark process started", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e0c)
		runID := mustRunID(t, 0x12e0c)
		logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		failEvents(database)
		if _, err := database.MarkProcessStarted(
			t.Context(), jobID, runID, "/bin/true", testProcessIdentity(6200, "event-failure"), now.Add(3*time.Second),
		); err == nil {
			t.Error("MarkProcessStarted(event ID failure) error = nil")
		}
	})

	t.Run("mark start failed", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e0d)
		runID := mustRunID(t, 0x12e0d)
		logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		finalLogs := testLogs(database, jobID, model.LogIntegrityValid, model.RecordingHealthy)
		failEvents(database)
		if _, err := database.MarkStartFailed(
			t.Context(), jobID, runID, finalLogs, "start_failed", now.Add(3*time.Second),
		); err == nil {
			t.Error("MarkStartFailed(event ID failure) error = nil")
		}
	})

	t.Run("finalize cancellation", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e0e)
		if _, err := database.RequestCancellation(t.Context(), jobID, now); err != nil {
			t.Fatal(err)
		}
		failEvents(database)
		if _, err := database.FinalizeCancellationWithoutRun(t.Context(), jobID, now.Add(time.Second)); err == nil {
			t.Error("FinalizeCancellationWithoutRun(event ID failure) error = nil")
		}
	})

	t.Run("finalize run", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e0f)
		runID := mustRunID(t, 0x12e0f)
		logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := database.MarkProcessStarted(
			t.Context(), jobID, runID, "/bin/true", testProcessIdentity(6201, "event-failure"), now.Add(3*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		zero := 0
		finalLogs := testLogs(database, jobID, model.LogIntegrityValid, model.RecordingHealthy)
		failEvents(database)
		if _, err := database.FinalizeRun(
			t.Context(), jobID, runID, model.RunOutcomeSuccess,
			&model.ExitInfo{ExitCode: &zero, ObservedAt: now.Add(4 * time.Second)},
			finalLogs, now.Add(4*time.Second),
		); err == nil {
			t.Error("FinalizeRun(event ID failure) error = nil")
		}
	})

	t.Run("request run timeout", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e10)
		runID := mustRunID(t, 0x12e10)
		logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := database.MarkProcessStarted(
			t.Context(), jobID, runID, "/bin/true", testProcessIdentity(6202, "event-failure"), now.Add(3*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		failEvents(database)
		if _, err := database.RequestRunTimeout(t.Context(), jobID, now.Add(4*time.Second)); err == nil {
			t.Error("RequestRunTimeout(event ID failure) error = nil")
		}
	})

	t.Run("complete run with disposition", func(t *testing.T) {
		database, now, jobID, _ := claimedFailureStore(t, 0x12e11)
		runID := mustRunID(t, 0x12e11)
		logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
		if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := database.MarkProcessStarted(
			t.Context(), jobID, runID, "/bin/true", testProcessIdentity(6203, "event-failure"), now.Add(3*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		one := 1
		finalLogs := testLogs(database, jobID, model.LogIntegrityValid, model.RecordingHealthy)
		failEvents(database)
		if _, err := database.CompleteRunWithDisposition(
			t.Context(), jobID, runID, model.RunOutcomeFailure,
			&model.ExitInfo{ExitCode: &one, ObservedAt: now.Add(4 * time.Second)},
			finalLogs, "failed", now.Add(4*time.Second),
			model.RunDisposition{TerminalOutcome: model.JobOutcomeFailure, Reason: "run_limit"},
		); err == nil {
			t.Error("CompleteRunWithDisposition(event ID failure) error = nil")
		}
	})
}

func TestStoreDatabaseFailureSurfaces(t *testing.T) {
	t.Parallel()

	database := openTestStore(t, "database-failure-surfaces", newSequentialEventIDs(0x12f00))
	now := storeTestTime()
	jobID := mustJobID(t, 0x12f01, 1)
	runID := mustRunID(t, 0x12f01)
	supervisorID := mustSupervisorID(t, 0x12f01, 2)
	credential := submitRuntimeJob(t, database, jobID, now)
	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	job, err := database.GetJob(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.GetRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	supervisorState, err := database.GetSupervisor(t.Context(), supervisorID)
	if err != nil {
		t.Fatal(err)
	}

	for name, insert := range map[string]func(context.Context, *sql.Tx) error{
		"job":        func(ctx context.Context, tx *sql.Tx) error { return insertJob(ctx, tx, job) },
		"run":        func(ctx context.Context, tx *sql.Tx) error { return insertRun(ctx, tx, run) },
		"supervisor": func(ctx context.Context, tx *sql.Tx) error { return insertSupervisor(ctx, tx, supervisorState) },
	} {
		t.Run("duplicate "+name, func(t *testing.T) {
			tx, beginErr := database.db.BeginTx(t.Context(), nil)
			if beginErr != nil {
				t.Fatal(beginErr)
			}
			defer tx.Rollback()
			if err := insert(t.Context(), tx); err == nil {
				t.Errorf("duplicate %s insert error = nil", name)
			}
		})
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for name, update := range map[string]func(context.Context, *sql.Tx) error{
		"job": func(ctx context.Context, tx *sql.Tx) error {
			return updateJob(ctx, tx, job, model.EventDraft{FromPhase: string(job.Phase)})
		},
		"run": func(ctx context.Context, tx *sql.Tx) error {
			return updateRun(ctx, tx, run, model.EventDraft{FromPhase: string(run.Phase)})
		},
		"supervisor": func(ctx context.Context, tx *sql.Tx) error {
			return updateSupervisor(ctx, tx, supervisorState, model.EventDraft{})
		},
	} {
		t.Run("canceled "+name+" update", func(t *testing.T) {
			tx, beginErr := database.db.BeginTx(t.Context(), nil)
			if beginErr != nil {
				t.Fatal(beginErr)
			}
			defer tx.Rollback()
			if err := update(canceled, tx); err == nil {
				t.Errorf("canceled %s update error = nil", name)
			}
		})
	}

	if _, err := database.db.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadNotificationPolicy(t.Context(), tx, mustJobID(t, 0x12fff, 1)); err == nil {
		t.Error("loadNotificationPolicy(missing job) error = nil")
	}
	if _, err := tx.ExecContext(t.Context(), "UPDATE jobs SET spec_json = '{' WHERE id = ?", jobID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := loadNotificationPolicy(t.Context(), tx, jobID); err == nil {
		t.Error("loadNotificationPolicy(corrupt specification) error = nil")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	commitCtx, cancelCommit := context.WithCancel(t.Context())
	if err := database.writeTransaction(commitCtx, "forced commit failure", func(*sql.Tx) error {
		cancelCommit()
		return nil
	}); err == nil {
		t.Error("writeTransaction(canceled commit) error = nil")
	}
}

func TestStoreInitializationFailureSurfaces(t *testing.T) {
	t.Parallel()

	if err := (*Store)(nil).Close(); err != nil {
		t.Fatalf("nil Store.Close() error = %v", err)
	}
	if err := (&Store{}).Close(); err != nil {
		t.Fatalf("empty Store.Close() error = %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	memoryStore := &Store{db: db, busyTimeout: time.Millisecond}
	if err := memoryStore.enableAndVerifyWAL(t.Context()); err == nil {
		t.Error("enableAndVerifyWAL(memory database) error = nil")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := memoryStore.initialize(t.Context()); err == nil {
		t.Error("initialize(closed database) error = nil")
	}
	if err := memoryStore.verifySQLiteVersion(t.Context()); err == nil {
		t.Error("verifySQLiteVersion(closed database) error = nil")
	}
	if err := memoryStore.enableAndVerifyWAL(t.Context()); err == nil {
		t.Error("enableAndVerifyWAL(closed database) error = nil")
	}
	if err := memoryStore.verifyConnectionPragmas(t.Context()); err == nil {
		t.Error("verifyConnectionPragmas(closed database) error = nil")
	}
}

func claimedFailureStore(
	t *testing.T,
	prefix uint64,
) (*Store, time.Time, model.JobID, model.SupervisorID) {
	t.Helper()
	database := openTestStore(t, "event-failure", newSequentialEventIDs(prefix))
	now := storeTestTime()
	jobID := mustJobID(t, prefix, 1)
	supervisorID := mustSupervisorID(t, prefix, 2)
	credential := submitRuntimeJob(t, database, jobID, now)
	claimRuntimeJob(t, database, jobID, supervisorID, credential, now)

	return database, now, jobID, supervisorID
}

type failingSQLResult struct{ err error }

func (result failingSQLResult) LastInsertId() (int64, error) { return 0, result.err }
func (result failingSQLResult) RowsAffected() (int64, error) { return 0, result.err }

type failingEventIDSource struct{ err error }

func (source failingEventIDSource) NewEventID() (model.EventID, error) {
	return "", source.err
}

type staticRow struct {
	values []any
	err    error
}

func (row *staticRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("unexpected destination count")
	}
	for index, destination := range destinations {
		reflect.ValueOf(destination).Elem().Set(reflect.ValueOf(row.values[index]))
	}

	return nil
}

type fakeFileInfo struct{ name string }

func (info fakeFileInfo) Name() string  { return info.name }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() fs.FileMode  { return 0 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeFileInfo) IsDir() bool        { return false }
func (fakeFileInfo) Sys() any           { return nil }

var _ os.FileInfo = fakeFileInfo{}
