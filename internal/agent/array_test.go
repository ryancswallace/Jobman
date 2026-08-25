package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/protocol"
)

func TestSlurmArrayManifestRoundTripAndTaskSelection(t *testing.T) {
	t.Parallel()
	manifest := SlurmArrayManifest{
		APIVersion: controlAPIVersion, Kind: "SlurmArrayManifest", MaxActive: 1,
		Tasks: []SlurmArrayManifestTask{{
			Index: 0, ExecutionID: "11111111-1111-4111-8111-111111111111",
			StateDirectory: t.TempDir(),
		}},
	}
	filename := filepath.Join(t.TempDir(), "array.json")
	if err := WriteSlurmArrayManifest(filename, manifest); err != nil {
		t.Fatalf("WriteSlurmArrayManifest() error = %v", err)
	}
	information, err := os.Stat(filename)
	if err != nil || information.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode = %v, %v", information.Mode().Perm(), err)
	}
	if err = RunSlurmArrayTask(t.Context(), filename, "1", ExecutionOptions{}); err == nil ||
		!strings.Contains(err.Error(), "task index") {
		t.Fatalf("RunSlurmArrayTask(invalid index) error = %v", err)
	}
}

func TestSlurmArrayManifestRejectsUnsafeMappings(t *testing.T) {
	t.Parallel()
	valid := SlurmArrayManifest{
		APIVersion: controlAPIVersion, Kind: "SlurmArrayManifest", MaxActive: 1,
		Tasks: []SlurmArrayManifestTask{{
			Index: 0, ExecutionID: "11111111-1111-4111-8111-111111111111",
			StateDirectory: t.TempDir(),
		}},
	}
	for _, mutate := range []func(*SlurmArrayManifest){
		func(value *SlurmArrayManifest) { value.APIVersion = "future" },
		func(value *SlurmArrayManifest) { value.MaxActive = 0 },
		func(value *SlurmArrayManifest) { value.Tasks[0].Index = 1 },
		func(value *SlurmArrayManifest) { value.Tasks[0].StateDirectory = "relative" },
	} {
		candidate := valid
		candidate.Tasks = append([]SlurmArrayManifestTask(nil), valid.Tasks...)
		mutate(&candidate)
		if err := validateSlurmArrayManifest(candidate); err == nil {
			t.Fatalf("validateSlurmArrayManifest(%#v) unexpectedly succeeded", candidate)
		}
	}
	if err := RunSlurmArrayTask(t.Context(), "relative", "0", ExecutionOptions{}); err == nil {
		t.Fatal("RunSlurmArrayTask(relative) unexpectedly succeeded")
	}
	if err := RunSlurmArrayTask(
		t.Context(), filepath.Join(t.TempDir(), "missing"), "0", ExecutionOptions{},
	); err == nil {
		t.Fatal("RunSlurmArrayTask(missing) unexpectedly succeeded")
	}
	invalid := valid
	invalid.APIVersion = "future"
	if err := WriteSlurmArrayManifest(filepath.Join(t.TempDir(), "invalid.json"), invalid); err == nil {
		t.Fatal("WriteSlurmArrayManifest() accepted an invalid manifest")
	}
	second := valid.Tasks[0]
	second.Index = 1
	second.ExecutionID = "22222222-2222-4222-8222-222222222222"
	unsorted := valid
	unsorted.Tasks = []SlurmArrayManifestTask{second, valid.Tasks[0]}
	unsorted.MaxActive = 1
	if err := validateSlurmArrayManifest(unsorted); err == nil || !strings.Contains(err.Error(), "index ordered") {
		t.Fatalf("validateSlurmArrayManifest(unsorted) error = %v", err)
	}
}

func TestServiceCompilesAcceptedCollectionToOneSlurmArray(t *testing.T) {
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	for index := range 2 {
		assignment, authorization := testSlurmArrayAssignment(t, index)
		if err = spool.PutAssignment(t.Context(), assignment); err != nil {
			t.Fatalf("PutAssignment(%d) error = %v", index, err)
		}
		if err = spool.RecordAcceptance(
			t.Context(), assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID, authorization,
		); err != nil {
			t.Fatalf("RecordAcceptance(%d) error = %v", index, err)
		}
	}
	scheduler := &fakeSlurmScheduler{
		submitResponses: []slurmbackend.Submission{{JobID: "12345", Cluster: "alpha"}},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil {
		t.Fatalf("ListExecutions() error = %v", err)
	}
	if err = service.reconcileSlurmArrays(t.Context(), executions); err != nil {
		t.Fatalf("reconcileSlurmArrays() error = %v", err)
	}
	if scheduler.arraySubmitCalls != 1 || scheduler.submitCalls != 0 ||
		scheduler.lastArraySubmit.TaskCount != 2 || scheduler.lastArraySubmit.MaxParallel != 1 ||
		scheduler.lastArraySubmit.Partition != "gpu" {
		t.Fatalf("array submission = %#v, array calls = %d, single calls = %d",
			scheduler.lastArraySubmit, scheduler.arraySubmitCalls, scheduler.submitCalls)
	}
	for index, execution := range executions {
		executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
		directory, directoryErr := executionDirectory(stateDirectory, executionID)
		if directoryErr != nil {
			t.Fatal(directoryErr)
		}
		submission, readErr := readSlurmSubmission(directory, executionID)
		if readErr != nil || submission.JobID != "12345_"+strconv.Itoa(index) {
			t.Fatalf("task %d submission = %#v, %v", index, submission, readErr)
		}
	}
	arrayDirectory, err := prepareArrayDirectory(sharedRoot, "99999999-9999-4999-8999-999999999999")
	if err != nil {
		t.Fatal(err)
	}
	if information, statErr := os.Stat(filepath.Join(arrayDirectory, slurmArrayManifestFilename)); statErr != nil || information.Mode().Perm() != 0o600 {
		t.Fatalf("array manifest = %#v, %v", information, statErr)
	}
	if err = service.reconcileSlurmArrays(t.Context(), executions); err != nil {
		t.Fatalf("reconcileSlurmArrays(replay) error = %v", err)
	}
	if scheduler.arraySubmitCalls != 1 {
		t.Fatalf("array submit calls after replay = %d", scheduler.arraySubmitCalls)
	}
}

func TestServiceRecoversAmbiguousSlurmArrayWithoutResubmission(t *testing.T) {
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	for index := range 2 {
		assignment, authorization := testSlurmArrayAssignment(t, index)
		if err = spool.PutAssignment(t.Context(), assignment); err != nil {
			t.Fatal(err)
		}
		if err = spool.RecordAcceptance(
			t.Context(), assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID, authorization,
		); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := &fakeSlurmScheduler{
		submitErrors:  []error{errors.New("connection lost")},
		findErrors:    []error{errors.New("not visible yet")},
		findResponses: []slurmbackend.Submission{{JobID: "9876", Cluster: "alpha"}},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 3 {
		if err = service.reconcileSlurmArrays(t.Context(), executions); err != nil {
			t.Fatalf("reconcileSlurmArrays(%d) error = %v", attempt, err)
		}
	}
	if scheduler.arraySubmitCalls != 1 || scheduler.findCalls != 2 {
		t.Fatalf("submit calls = %d, find calls = %d", scheduler.arraySubmitCalls, scheduler.findCalls)
	}
	for index, execution := range executions {
		executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
		directory, directoryErr := executionDirectory(stateDirectory, executionID)
		if directoryErr != nil {
			t.Fatal(directoryErr)
		}
		submission, readErr := readSlurmSubmission(directory, executionID)
		if readErr != nil || submission.JobID != "9876_"+strconv.Itoa(index) {
			t.Fatalf("submission %d = %#v, %v", index, submission, readErr)
		}
	}
	events, err := spool.PendingEvents(t.Context())
	if err != nil || len(events) != 2 {
		t.Fatalf("uncertain events = %#v, %v", events, err)
	}
}

func TestServiceReconcileLoopCompilesAndObservesArrayChildren(t *testing.T) {
	stateDirectory := t.TempDir()
	sharedRoot := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	for index := range 2 {
		assignment, authorization := testSlurmArrayAssignment(t, index)
		if err = spool.PutAssignment(t.Context(), assignment); err != nil {
			t.Fatal(err)
		}
		if err = spool.RecordAcceptance(
			t.Context(), assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID, authorization,
		); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := &fakeSlurmScheduler{
		submitResponses: []slurmbackend.Submission{{JobID: "2468", Cluster: "alpha"}},
		observations: []slurmbackend.Observation{
			{JobID: "2468_0", State: "queued"}, {JobID: "2468_1", State: "queued"},
		},
	}
	service := testSlurmService(stateDirectory, sharedRoot, spool, scheduler)
	if err = service.reconcileExecutions(t.Context()); err != nil {
		t.Fatalf("reconcileExecutions() error = %v", err)
	}
	if scheduler.arraySubmitCalls != 1 || len(scheduler.observations) != 0 {
		t.Fatalf("array calls = %d, remaining observations = %#v", scheduler.arraySubmitCalls, scheduler.observations)
	}
}

func TestServiceReconcileLoopWaitsForIncompleteArray(t *testing.T) {
	t.Parallel()
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, authorization := testSlurmArrayAssignment(t, 0)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatal(err)
	}
	if err = spool.RecordAcceptance(
		t.Context(), assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID, authorization,
	); err != nil {
		t.Fatal(err)
	}
	service := testSlurmService(stateDirectory, t.TempDir(), spool, &fakeSlurmScheduler{})
	if err = service.reconcileExecutions(t.Context()); err != nil {
		t.Fatalf("reconcileExecutions() error = %v", err)
	}
}

func TestSlurmArrayFilesystemAndManifestFailureBoundaries(t *testing.T) {
	t.Parallel()
	collectionID := "99999999-9999-4999-8999-999999999999"
	if _, err := prepareArrayDirectory(t.TempDir(), "invalid"); err == nil {
		t.Fatal("prepareArrayDirectory() accepted an invalid collection ID")
	}
	blockedRoot := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blockedRoot, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareArrayDirectory(blockedRoot, collectionID); err == nil {
		t.Fatal("prepareArrayDirectory() ignored an invalid root")
	}
	if err := writeImmutableArrayManifest(filepath.Join(t.TempDir(), "invalid.json"), SlurmArrayManifest{}); err == nil {
		t.Fatal("writeImmutableArrayManifest() accepted an invalid manifest")
	}
	root := t.TempDir()
	arrayRoot := filepath.Join(root, slurmArraysDirectoryName)
	if err := os.Mkdir(arrayRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(arrayRoot, collectionID)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := prepareArrayDirectory(root, collectionID); err == nil {
		t.Fatal("prepareArrayDirectory() accepted a symlink")
	}
	malformed := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(malformed, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RunSlurmArrayTask(t.Context(), malformed, "0", ExecutionOptions{}); err == nil {
		t.Fatal("RunSlurmArrayTask() accepted an invalid manifest")
	}
	if err := RunSlurmArrayTask(t.Context(), t.TempDir(), "0", ExecutionOptions{}); err == nil {
		t.Fatal("RunSlurmArrayTask() accepted a directory manifest")
	}
	localDirectory := t.TempDir()
	if err := writeManifest(filepath.Join(localDirectory, slurmArrayAttemptFilename), slurmArrayAttemptManifest{
		CollectionID: collectionID, JobName: "job", ExecutionUser: "user", TaskCount: 0,
		AttemptedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlurmArrayAttempt(localDirectory, collectionID, 2); err == nil {
		t.Fatal("readSlurmArrayAttempt() accepted inconsistent metadata")
	}
	if err := writeManifest(filepath.Join(localDirectory, slurmArraySubmitFilename), slurmArraySubmissionManifest{
		CollectionID: collectionID, JobID: "123", TaskCount: 0, SubmittedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := readSlurmArraySubmission(localDirectory, collectionID, 2); err == nil {
		t.Fatal("readSlurmArraySubmission() accepted inconsistent metadata")
	}
	service := &service{}
	if err := service.ensureSlurmArraySubmission(t.Context(), collectionID, nil); err == nil {
		t.Fatal("ensureSlurmArraySubmission() accepted a missing scheduler")
	}
}

func TestSlurmArrayCompilerRejectsInconsistentGroups(t *testing.T) {
	t.Parallel()
	newExecutions := func() []SpoolExecution {
		first, firstAuthorization := testSlurmArrayAssignment(t, 0)
		second, secondAuthorization := testSlurmArrayAssignment(t, 1)

		return []SpoolExecution{
			{Assignment: first, Authorization: &firstAuthorization},
			{Assignment: second, Authorization: &secondAuthorization},
		}
	}
	newService := func() *service {
		return testSlurmService(t.TempDir(), t.TempDir(), nil, &fakeSlurmScheduler{})
	}
	executions := newExecutions()
	executions[0].Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray.TaskIndex = 1
	if err := newService().ensureSlurmArraySubmission(
		t.Context(), "99999999-9999-4999-8999-999999999999", executions,
	); err == nil || !strings.Contains(err.Error(), "binding") {
		t.Fatalf("inconsistent binding error = %v", err)
	}
	executions = newExecutions()
	executions[1].Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray.MaxParallel = 2
	if err := newService().ensureSlurmArraySubmission(
		t.Context(), "99999999-9999-4999-8999-999999999999", executions,
	); err == nil || !strings.Contains(err.Error(), "scheduler policy") {
		t.Fatalf("inconsistent policy error = %v", err)
	}
	executions = newExecutions()
	executions[1].Assignment.Document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Resources.CPU = 8
	if err := newService().ensureSlurmArraySubmission(
		t.Context(), "99999999-9999-4999-8999-999999999999", executions,
	); err == nil || !strings.Contains(err.Error(), "resources") {
		t.Fatalf("inconsistent resources error = %v", err)
	}
	service := newService()
	if err := service.reconcileSlurmArrays(t.Context(), executions[:1]); err != nil {
		t.Fatalf("incomplete group error = %v", err)
	}
	if err := service.reconcileSlurmArrays(t.Context(), append(executions, executions[1])); err == nil {
		t.Fatal("reconcileSlurmArrays() accepted an oversized group")
	}
	if _, err := persistSlurmArraySubmission(
		filepath.Join(t.TempDir(), "missing"), "99999999-9999-4999-8999-999999999999", 2,
		slurmbackend.Submission{JobID: "123"}, time.Now().UTC(),
	); err == nil {
		t.Fatal("persistSlurmArraySubmission() ignored a missing directory")
	}
}

func testSlurmArrayAssignment(
	t *testing.T,
	index int,
) (protocol.SealedAgentAssignment, protocol.LaunchAuthorization) {
	t.Helper()
	assignment, authorization := testSlurmAssignment(t)
	executionIDs := []string{
		"33333333-3333-4333-8333-333333333333",
		"44444444-4444-4444-8444-444444444444",
	}
	runIDs := []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
	jobIDs := []string{
		"cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"dddddddd-dddd-4ddd-8ddd-dddddddddddd",
	}
	deliveryIDs := []string{
		"eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
		"ffffffff-ffff-4fff-8fff-ffffffffffff",
	}
	authorizationIDs := []string{
		"12121212-1212-4212-8212-121212121212",
		"13131313-1313-4313-8313-131313131313",
	}
	document := assignment.Document
	document.Metadata.DeliveryID = deliveryIDs[index]
	document.Spec.EffectiveExecution.Metadata.ExecutionID = executionIDs[index]
	document.Spec.EffectiveExecution.Metadata.RunID = runIDs[index]
	document.Spec.EffectiveExecution.Metadata.JobID = jobIDs[index]
	document.Spec.EffectiveExecution.Metadata.SlurmArray = &protocol.SlurmArrayBinding{
		CollectionID: "99999999-9999-4999-8999-999999999999",
		TaskIndex:    index, TaskCount: 2, MaxParallel: 1,
	}
	assignment = resealAssignment(t, document)
	authorization.Metadata.AuthorizationID = authorizationIDs[index]
	authorization.Metadata.ExecutionID = executionIDs[index]
	authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
	if err := protocol.ValidateLaunchAuthorization(authorization); err != nil {
		t.Fatalf("ValidateLaunchAuthorization() error = %v", err)
	}

	return assignment, authorization
}
