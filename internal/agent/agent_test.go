package agent

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	testAgentID            = "77777777-7777-4777-8777-777777777777"
	testExecutionID        = "33333333-3333-4333-8333-333333333333"
	testTargetGenerationID = "55555555-5555-4555-8555-555555555555"
)

func TestSpoolAssignmentAcceptanceAndEventReplay(t *testing.T) {
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := spool.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})
	assignment, authorization := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment(replay) error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance(replay) error = %v", err)
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 || executions[0].State != "accepted" {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID:     derivedEventID(testExecutionID, "process.completed"),
			ExecutionID: testExecutionID, AgentID: testAgentID, Sequence: 1,
			ObservedAt: time.Now().UTC(),
		},
		Spec: protocol.ExecutionEventSpec{
			Type: "process.completed", Result: &protocol.ProcessResult{Outcome: "lost"},
		},
	})
	if err != nil {
		t.Fatalf("SealExecutionEvent() error = %v", err)
	}
	if err = spool.QueueEvent(t.Context(), event); err != nil {
		t.Fatalf("QueueEvent() error = %v", err)
	}
	if err = spool.QueueEvent(t.Context(), event); err != nil {
		t.Fatalf("QueueEvent(replay) error = %v", err)
	}
	pending, err := spool.PendingEvents(t.Context())
	if err != nil || len(pending) != 1 || pending[0].Event.Digest != event.Digest {
		t.Fatalf("PendingEvents() = %#v, %v", pending, err)
	}
	if err = spool.MarkEventDelivered(t.Context(), event.Document.Metadata.EventID); err != nil {
		t.Fatalf("MarkEventDelivered() error = %v", err)
	}
	pending, err = spool.PendingEvents(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingEvents(delivered) = %#v, %v", pending, err)
	}
}

func TestSpoolRejectsChangedReplays(t *testing.T) {
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, _ := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	changed, _ := testAssignment(t, "false", nil, nil)
	if err = spool.PutAssignment(t.Context(), changed); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("PutAssignment(changed) error = %v, want conflict", err)
	}
}

func TestSpoolLogChunkReplayAndPublication(t *testing.T) {
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, authorization := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatal(err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatal(err)
	}
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, stdoutFilename), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, stderrFilename), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := artifact.NewFilesystemStore("department-nfs", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := &service{artifactStore: store, spool: spool}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	completion := &CompletionManifest{Logs: map[string]LogCapture{
		"stdout": {ByteLength: 6}, "stderr": {},
	}}
	if err = service.publishLogs(t.Context(), executions[0], directory, nil); err != nil {
		t.Fatalf("publishLogs(live) error = %v", err)
	}
	pending, err := spool.pendingLogChunks(t.Context())
	if err != nil || len(pending) != 1 || pending[0].Spec.Complete {
		t.Fatalf("pending live chunks = %#v, %v", pending, err)
	}
	if err = spool.queueLogChunk(t.Context(), pending[0]); err != nil {
		t.Fatalf("queueLogChunk(replay) error = %v", err)
	}
	conflict := pending[0]
	conflict.Spec.Checksum = artifact.Digest([]byte("different"))
	if err = spool.queueLogChunk(t.Context(), conflict); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("queueLogChunk(conflict) error = %v", err)
	}
	if err = service.publishLogs(t.Context(), executions[0], directory, completion); err != nil {
		t.Fatalf("publishLogs() error = %v", err)
	}
	if err = service.publishLogs(t.Context(), executions[0], directory, completion); err != nil {
		t.Fatalf("publishLogs(replay) error = %v", err)
	}
	pending, err = spool.pendingLogChunks(t.Context())
	if err != nil || len(pending) != 3 || pending[0].Spec.Complete ||
		!pending[1].Spec.Complete || !pending[2].Spec.Complete {
		t.Fatalf("PendingLogChunks() = %#v, %v", pending, err)
	}
	stdoutPosition, err := spool.logPosition(t.Context(), testExecutionID, logStreamStdout)
	if err != nil || stdoutPosition.ByteOffset != 6 || !stdoutPosition.Complete {
		t.Fatalf("LogPosition(stdout) = %#v, %v", stdoutPosition, err)
	}
	for _, chunk := range pending {
		if err = spool.markLogChunkDelivered(
			t.Context(), chunk.Metadata.ExecutionID, chunk.Metadata.Stream, chunk.Metadata.Sequence,
		); err != nil {
			t.Fatal(err)
		}
	}
	if pending, err = spool.pendingLogChunks(t.Context()); err != nil || len(pending) != 0 {
		t.Fatalf("PendingLogChunks(delivered) = %#v, %v", pending, err)
	}
	if err = service.flushLogChunks(t.Context()); err != nil {
		t.Fatalf("flushLogChunks(empty) error = %v", err)
	}
}

func TestBoundedLogWriterRecordsTruncation(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "capture"), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writer := &boundedLogWriter{file: file, maximum: 5}
	if written, writeErr := writer.Write([]byte("abcdef")); writeErr != nil || written != 6 {
		t.Fatalf("Write() = %d, %v", written, writeErr)
	}
	if err = writer.close(); err != nil {
		t.Fatal(err)
	}
	if capture := writer.capture(); capture.ByteLength != 5 || !capture.Truncated {
		t.Fatalf("capture = %#v", capture)
	}
	if written, writeErr := writer.Write([]byte("later")); writeErr != nil || written != 5 {
		t.Fatalf("Write(after bound) = %d, %v", written, writeErr)
	}
	if err = writer.close(); err == nil {
		t.Fatal("close(already closed) error = nil")
	}
}

func TestCapturedLogInspection(t *testing.T) {
	directory := t.TempDir()
	live, err := inspectCapturedLog(directory, logStreamStdout, stdoutFilename, nil)
	if err != nil || !live.skip {
		t.Fatalf("inspectCapturedLog(missing live) = %#v, %v", live, err)
	}
	terminal, err := inspectCapturedLog(
		directory, logStreamStdout, stdoutFilename,
		&CompletionManifest{ObservedAt: time.Now(), Logs: map[string]LogCapture{logStreamStdout: {}}},
	)
	if err != nil || terminal.skip || terminal.length != 0 || terminal.capturedAt.IsZero() {
		t.Fatalf("inspectCapturedLog(missing terminal) = %#v, %v", terminal, err)
	}
	if err = os.WriteFile(filepath.Join(directory, stdoutFilename), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = inspectCapturedLog(
		directory, logStreamStdout, stdoutFilename,
		&CompletionManifest{Logs: map[string]LogCapture{logStreamStdout: {ByteLength: 3}}},
	); err == nil || !strings.Contains(err.Error(), "length does not match") {
		t.Fatalf("inspectCapturedLog(mismatch) error = %v", err)
	}
	if _, err = inspectCapturedLog(directory, logStreamStdout, ".", nil); err == nil {
		t.Fatal("inspectCapturedLog(directory) error = nil")
	}
}

func TestLogPublicationFailureSurfaces(t *testing.T) {
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, authorization := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatal(err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatal(err)
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatal(err)
	}
	withoutStore := &service{spool: spool}
	if err = withoutStore.publishLogs(t.Context(), executions[0], directory, nil); err != nil {
		t.Fatalf("publishLogs(no store) error = %v", err)
	}
	if _, err = spool.logPosition(t.Context(), testExecutionID, "invalid"); err == nil {
		t.Fatal("logPosition(invalid stream) error = nil")
	}
	if err = spool.queueLogChunk(t.Context(), agentLogChunk{}); err == nil {
		t.Fatal("queueLogChunk(invalid) error = nil")
	}
	if err = spool.markLogChunkDelivered(t.Context(), testExecutionID, logStreamStdout, 99); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("markLogChunkDelivered(missing) error = %v", err)
	}
	if _, err = readLogChunk(filepath.Join(directory, "missing"), 0, 1); err == nil {
		t.Fatal("readLogChunk(missing) error = nil")
	}
	shortPath := filepath.Join(directory, "short")
	if err = os.WriteFile(shortPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = readLogChunk(shortPath, 0, 2); err == nil {
		t.Fatal("readLogChunk(short) error = nil")
	}
	store, err := artifact.NewFilesystemStore("department-nfs", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	withStore := &service{spool: spool, artifactStore: store}
	metadata := assignment.Document.Spec.EffectiveExecution.Metadata
	owner := logOwner{
		namespace: metadata.Namespace, jobID: metadata.JobID, executionID: testExecutionID,
	}
	if err = withStore.queueCapturedLog(
		t.Context(), owner, logStreamStdout, logPosition{NextSequence: 1},
		capturedLog{filePath: filepath.Join(directory, "missing"), length: 1}, false,
	); err == nil {
		t.Fatal("queueCapturedLog(missing capture) error = nil")
	}
	if err = withStore.publishLog(
		t.Context(), owner, directory, "invalid", stdoutFilename, nil,
	); err == nil {
		t.Fatal("publishLog(invalid stream) error = nil")
	}
	chunk := agentLogChunk{
		APIVersion: controlAPIVersion, Kind: "LogChunk",
		Metadata: logChunkMetadata{ExecutionID: testExecutionID, Stream: logStreamStdout, Sequence: 1},
		Spec: logChunkSpec{
			StoreName: "department-nfs", StoreVersion: 1, ObjectKey: "one",
			ByteLength: 1, Checksum: artifact.Digest([]byte("x")), CapturedAt: time.Now().UTC(),
		},
	}
	if err = spool.queueLogChunk(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	if err = withStore.publishLog(
		t.Context(), owner, directory, logStreamStdout, ".", nil,
	); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("publishLog(invalid capture) error = %v", err)
	}
	if runtime.GOOS != "windows" {
		readOnlyRoot := t.TempDir()
		readOnlyStore, storeErr := artifact.NewFilesystemStore("read-only", 1, readOnlyRoot)
		if storeErr != nil {
			t.Fatal(storeErr)
		}
		if err = os.Chmod(readOnlyRoot, 0o500); err != nil { // #nosec G302 -- directory needs owner traversal.
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if chmodErr := os.Chmod(readOnlyRoot, 0o700); chmodErr != nil { // #nosec G302 -- restore private directory traversal.
				t.Errorf("restore artifact root permissions: %v", chmodErr)
			}
		})
		if err = (&service{spool: spool, artifactStore: readOnlyStore}).queueCapturedLog(
			t.Context(), owner, logStreamStdout, logPosition{NextSequence: 98},
			capturedLog{filePath: shortPath, length: 1, capturedAt: time.Now().UTC()}, false,
		); err == nil {
			t.Fatal("queueCapturedLog(read-only store) error = nil")
		}
	}
	if err = withStore.queueCapturedLog(
		t.Context(), owner, logStreamStdout, logPosition{NextSequence: 99},
		capturedLog{capturedAt: time.Now().UTC()}, true,
	); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("queueCapturedLog(spool conflict) error = %v", err)
	}
	if _, err = spool.database.ExecContext(t.Context(), `
UPDATE pending_log_chunks SET chunk_document = '{' WHERE execution_id = ? AND stream = ? AND sequence = 1`,
		testExecutionID, logStreamStdout); err != nil {
		t.Fatal(err)
	}
	if _, err = spool.pendingLogChunks(t.Context()); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("pendingLogChunks(corrupt JSON) error = %v", err)
	}
	if _, err = spool.database.ExecContext(t.Context(), `
UPDATE pending_log_chunks SET chunk_document = '{}' WHERE execution_id = ? AND stream = ? AND sequence = 1`,
		testExecutionID, logStreamStdout); err != nil {
		t.Fatal(err)
	}
	if _, err = spool.pendingLogChunks(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("pendingLogChunks(invalid chunk) error = %v", err)
	}
	if err = RunExecutionWithOptions(
		t.Context(), stateDirectory, testExecutionID, ExecutionOptions{},
	); err == nil || !strings.Contains(err.Error(), "maximum log bytes") {
		t.Fatalf("RunExecutionWithOptions(unbounded) error = %v", err)
	}

	closed, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = closed.logPosition(t.Context(), testExecutionID, logStreamStdout); err == nil {
		t.Fatal("logPosition(closed) error = nil")
	}
	if err = closed.markLogChunkDelivered(t.Context(), testExecutionID, logStreamStdout, 1); err == nil {
		t.Fatal("markLogChunkDelivered(closed) error = nil")
	}
	if err = closed.queueLogChunk(t.Context(), chunk); err == nil {
		t.Fatal("queueLogChunk(closed) error = nil")
	}
	if err = closed.initialize(t.Context()); err == nil {
		t.Fatal("initialize(closed) error = nil")
	}
	if err = (&service{spool: closed}).flushLogChunks(t.Context()); err == nil {
		t.Fatal("flushLogChunks(closed spool) error = nil")
	}
	if err = (&service{spool: closed, artifactStore: store}).publishLogs(
		t.Context(), executions[0], directory, nil,
	); err == nil {
		t.Fatal("publishLogs(closed spool) error = nil")
	}

	emptySpool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptySpool.Close() })
	if err = emptySpool.queueLogChunk(t.Context(), chunk); err == nil {
		t.Fatal("queueLogChunk(missing execution) error = nil")
	}
	if err = writeRunnerFailure(
		filepath.Join(t.TempDir(), "missing"), testExecutionID, "test_failure", errors.New("failed"),
	); err == nil {
		t.Fatal("writeRunnerFailure(missing directory) error = nil")
	}

	memoryDatabase, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err = (&Spool{database: memoryDatabase}).initialize(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "database returned") {
		t.Fatalf("initialize(memory database) error = %v", err)
	}
	if err = memoryDatabase.Close(); err != nil {
		t.Fatal(err)
	}
	schemaDatabase, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = schemaDatabase.ExecContext(
		t.Context(), "CREATE TABLE pending_events (bad INTEGER)",
	); err != nil {
		t.Fatal(err)
	}
	if err = (&Spool{database: schemaDatabase}).initialize(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "initialize agent spool") {
		t.Fatalf("initialize(conflicting schema) error = %v", err)
	}
	if err = schemaDatabase.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunExecutionWritesPrivateLogsAndManifests(t *testing.T) {
	stateDirectory := t.TempDir()
	assignment, authorization := testAssignment(
		t, os.Args[0], []string{"-test.run=^TestAgentRunnerHelper$"},
		map[string]string{"JOBMAN_AGENT_HELPER": "1"},
	)
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	if err = RunExecution(t.Context(), stateDirectory, testExecutionID); err != nil {
		t.Fatalf("RunExecution() error = %v", err)
	}
	started, err := readStartManifest(directory)
	if err != nil || started.ExecutionID != testExecutionID || started.Process.PID < 1 {
		t.Fatalf("readStartManifest() = %#v, %v", started, err)
	}
	completed, err := readCompletionManifest(directory)
	if err != nil || completed.Result.Outcome != "success" || completed.Result.ExitCode == nil || *completed.Result.ExitCode != 0 {
		t.Fatalf("readCompletionManifest() = %#v, %v", completed, err)
	}
	stdout, err := os.ReadFile(filepath.Join(directory, stdoutFilename))
	if err != nil || !strings.Contains(string(stdout), "agent helper stdout\n") {
		t.Fatalf("stdout = %q, %v", stdout, err)
	}
	stderr, err := os.ReadFile(filepath.Join(directory, stderrFilename))
	if err != nil || !strings.Contains(string(stderr), "agent helper stderr\n") {
		t.Fatalf("stderr = %q, %v", stderr, err)
	}
	if err = RunExecution(t.Context(), stateDirectory, testExecutionID); err == nil || !strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("RunExecution(replay) error = %v", err)
	}
}

func TestAgentRunnerHelper(_ *testing.T) {
	switch os.Getenv("JOBMAN_AGENT_HELPER") {
	case "":
		return
	case "block":
		<-time.After(time.Hour)
	case "exit7":
		os.Exit(7) //nolint:revive // This branch runs only in an isolated helper process.
	}
	if os.Getenv("JOBMAN_AGENT_ARTIFACT") == "1" {
		input, err := os.ReadFile(filepath.Join("inputs", "sample.txt"))
		if err != nil {
			os.Exit(8) //nolint:revive // This branch runs only in an isolated helper process.
		}
		if err = os.MkdirAll("outputs", 0o700); err != nil {
			os.Exit(9) //nolint:revive // This branch runs only in an isolated helper process.
		}
		//nolint:gosec // The destination is a fixed test sandbox path, not tainted input.
		if err = os.WriteFile(filepath.Join("outputs", "result.txt"), append([]byte("result:"), input...), 0o600); err != nil {
			os.Exit(10) //nolint:revive // This branch runs only in an isolated helper process.
		}
	}
	_, _ = os.Stdout.WriteString("agent helper stdout\n")
	_, _ = os.Stderr.WriteString("agent helper stderr\n")
}

func TestRunExecutionStagesAndPublishesFileArtifacts(t *testing.T) {
	artifactRoot := t.TempDir()
	store, err := artifact.NewFilesystemStore("department-nfs", 3, artifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("sample\n")
	digest, err := store.PutImmutable("research/inputs/sample.txt", input)
	if err != nil {
		t.Fatal(err)
	}
	assignment, authorization := testAssignment(
		t, os.Args[0], []string{"-test.run=^TestAgentRunnerHelper$"},
		map[string]string{"JOBMAN_AGENT_HELPER": "1", "JOBMAN_AGENT_ARTIFACT": "1"},
	)
	document := assignment.Document
	document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Artifacts = &protocol.Artifacts{
		Inputs: []protocol.InputArtifact{{
			Name: "sample", Source: "artifact://department-nfs/research/inputs/sample.txt",
			Target: "inputs:/sample.txt", Checksum: digest,
		}},
		Outputs: []protocol.OutputArtifact{{
			Name: "result", Source: "outputs:/result.txt",
			Destination: "artifact://department-nfs/research/results/result.txt", Required: true,
		}},
	}
	document.Spec.EffectiveExecution.Spec.ArtifactStores = []protocol.ArtifactStoreBinding{{
		Name: "department-nfs", Version: 3,
	}}
	assignment = resealAssignment(t, document)
	authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
	stateDirectory := t.TempDir()
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if err = RunExecutionWithOptions(t.Context(), stateDirectory, testExecutionID, ExecutionOptions{
		MaximumLogBytes: 1024, MaximumArtifactBytes: 1024, ArtifactStore: store,
	}); err != nil {
		t.Fatalf("RunExecutionWithOptions() error = %v", err)
	}
	completion, err := readCompletionManifest(directory)
	if err != nil || completion.Result.Outcome != "success" || len(completion.Artifacts) != 1 ||
		completion.Artifacts[0].StoreVersion != 3 {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
	published := completion.Artifacts[0]
	contents, err := store.ReadVerified(published.ObjectKey, published.ByteLength, published.Checksum)
	if err != nil || string(contents) != "result:sample\n" {
		t.Fatalf("published artifact = %q, %v", contents, err)
	}
}

func TestRunExecutionTimeoutCancellationAndPreparationFailure(t *testing.T) {
	tests := []struct {
		name         string
		executable   string
		environment  map[string]string
		runTimeout   string
		cancelBefore bool
		wantOutcome  string
		wantFailure  string
		wantRunError bool
	}{
		{
			name: "timeout", executable: os.Args[0], environment: map[string]string{"JOBMAN_AGENT_HELPER": "block"},
			runTimeout: "100ms", wantOutcome: "timed_out",
		},
		{
			name: "cancellation", executable: os.Args[0], environment: map[string]string{"JOBMAN_AGENT_HELPER": "block"},
			cancelBefore: true, wantOutcome: cancelledOutcome,
		},
		{
			name: "nonzero exit", executable: os.Args[0], environment: map[string]string{"JOBMAN_AGENT_HELPER": "exit7"},
			wantOutcome: "failure",
		},
		{
			name: "missing executable", executable: filepath.Join(t.TempDir(), "missing"),
			wantOutcome: "failure", wantFailure: "executable_not_found", wantRunError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDirectory := t.TempDir()
			arguments := []string(nil)
			if test.executable == os.Args[0] {
				arguments = []string{"-test.run=^TestAgentRunnerHelper$"}
			}
			assignment, authorization := testAssignment(t, test.executable, arguments, test.environment)
			if test.runTimeout != "" {
				document := assignment.Document
				document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Policy.RunTimeout = test.runTimeout
				assignment = resealAssignment(t, document)
				authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
			}
			directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
			if err != nil {
				t.Fatalf("prepareExecutionFiles() error = %v", err)
			}
			if test.cancelBefore {
				if err = writePrivateFile(filepath.Join(directory, cancelFilename), []byte("cancel\n")); err != nil {
					t.Fatalf("write cancel marker: %v", err)
				}
			}
			err = RunExecution(t.Context(), stateDirectory, testExecutionID)
			if (err != nil) != test.wantRunError {
				t.Fatalf("RunExecution() error = %v, want error %t", err, test.wantRunError)
			}
			completion, readErr := readCompletionManifest(directory)
			if readErr != nil || completion.Result.Outcome != test.wantOutcome ||
				(test.wantFailure != "" && completion.Result.FailureCode != test.wantFailure) {
				t.Fatalf("completion = %#v, %v", completion, readErr)
			}
		})
	}
}

func TestRunnerValidationBoundaries(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	mutations := []func(*protocol.AgentAssignment){
		func(value *protocol.AgentAssignment) { value.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
		func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend = "slurm"
		},
		func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Command.Shell = &protocol.ShellCommand{}
		},
		func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Environment = &protocol.Environment{Profile: "module"}
		},
		func(value *protocol.AgentAssignment) {
			value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Policy.Retry.MaxRuns = 2
		},
	}
	for index, mutate := range mutations {
		value := assignment.Document
		mutate(&value)
		if err := validateSubprocessAssignment(
			protocol.SealedAgentAssignment{Document: value, EffectiveExecutionDigest: assignment.EffectiveExecutionDigest},
			authorization, nil,
		); err == nil {
			t.Errorf("mutation %d was accepted", index)
		}
	}
	for _, logical := range []string{"artifact:/bad", "workspace:/a/../b", "workspace:/a\\b"} {
		if _, err := mapWorkspacePath(t.TempDir(), logical); err == nil {
			t.Errorf("mapWorkspacePath(%q) error = nil", logical)
		}
	}
	if mapped, err := mapWorkspacePath(t.TempDir(), "workspace:/nested"); err != nil || !strings.HasSuffix(mapped, "nested") {
		t.Fatalf("mapWorkspacePath(valid) = %q, %v", mapped, err)
	}
	for _, invalid := range []string{"", "33333333_3333-4333-8333-333333333333", "33333333-3333-4333-8333-33333333333g"} {
		if validUUID(invalid) {
			t.Errorf("validUUID(%q) = true", invalid)
		}
	}
}

func TestCredentialRoundTripAndURLValidation(t *testing.T) {
	stateDirectory := t.TempDir()
	key, request, err := generateKeyAndCSR("agent-test")
	if err != nil || len(key) == 0 || !strings.Contains(request, "CERTIFICATE REQUEST") {
		t.Fatalf("generateKeyAndCSR() = %d bytes, %q, %v", len(key), request, err)
	}
	expires := time.Now().UTC().Add(time.Hour)
	values := credentialFiles{
		Metadata: metadata{
			ServerURL: "https://control.example.test", AgentID: testAgentID,
			TargetGenerationID: testTargetGenerationID,
			SessionID:          "88888888-8888-4888-8888-888888888888", SessionToken: "jms_test",
			SessionExpiresAt: expires, CertificateExpiresAt: expires,
		},
		PrivateKeyPEM: key, CertificatePEM: []byte("certificate"), ServerCAPEM: []byte("ca"),
	}
	if err = saveCredentials(stateDirectory, values); err != nil {
		t.Fatalf("saveCredentials() error = %v", err)
	}
	loaded, err := loadCredentials(stateDirectory)
	if err != nil || loaded.Metadata.AgentID != testAgentID || !bytes.Equal(loaded.PrivateKeyPEM, key) {
		t.Fatalf("loadCredentials() = %#v, %v", loaded.Metadata, err)
	}
	for _, invalid := range []string{"http://control.example.test", "https://user@control.example.test", "https://control.example.test/api"} {
		if _, err = parseServerURL(invalid); err == nil {
			t.Errorf("parseServerURL(%q) error = nil", invalid)
		}
	}
}

func TestReadEnrollmentToken(t *testing.T) {
	token, err := readEnrollmentToken(strings.NewReader("secret-token\n"), "-")
	if err != nil || token != "secret-token" {
		t.Fatalf("readEnrollmentToken() = %q, %v", token, err)
	}
	if _, err = readEnrollmentToken(strings.NewReader("bad token"), "-"); err == nil {
		t.Fatal("readEnrollmentToken() accepted whitespace")
	}
}

func TestPendingEnrollmentReusesKeyAndRequest(t *testing.T) {
	stateDirectory := t.TempDir()
	values := Enrollment{
		ServerURL: "https://control.example.test", TargetGenerationID: testTargetGenerationID,
		AgentVersion: "test", OperatingSystem: "linux", Architecture: "amd64",
		Hostname: "worker-a", ExecutionUser: "researcher",
		ExecutionBackends: []string{"subprocess"}, Runtimes: []string{"native"},
	}
	first, err := loadOrCreatePendingEnrollment(stateDirectory, values)
	if err != nil {
		t.Fatalf("loadOrCreatePendingEnrollment() error = %v", err)
	}
	second, err := loadOrCreatePendingEnrollment(stateDirectory, values)
	if err != nil {
		t.Fatalf("loadOrCreatePendingEnrollment(replay) error = %v", err)
	}
	if !bytes.Equal(first.PrivateKeyPEM, second.PrivateKeyPEM) ||
		first.Request.Spec.CertificateSigningRequest != second.Request.Spec.CertificateSigningRequest {
		t.Fatal("pending enrollment replay generated different key material")
	}
	values.TargetGenerationID = "99999999-9999-4999-8999-999999999999"
	if _, err = loadOrCreatePendingEnrollment(stateDirectory, values); err == nil {
		t.Fatal("pending enrollment accepted a different target generation")
	}
}

func testAssignment(
	t *testing.T,
	executable string,
	arguments []string,
	environment map[string]string,
) (protocol.SealedAgentAssignment, protocol.LaunchAuthorization) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("..", "..", "protocol", "testdata", "conformance", "valid", "agent-assignment-minimal.json"))
	if err != nil {
		t.Fatalf("ReadFile(assignment) error = %v", err)
	}
	decoded, err := protocol.DecodeAgentAssignment(bytes.NewReader(contents), protocol.DecodeLimits{})
	if err != nil {
		t.Fatalf("DecodeAgentAssignment() error = %v", err)
	}
	document := decoded.Document
	workload := document.Spec.EffectiveExecution.Spec.Workload.Document
	workload.Spec.Command.Executable = executable
	workload.Spec.Command.Args = arguments
	if environment != nil {
		workload.Spec.Environment = &protocol.Environment{Values: environment}
	}
	sealedWorkload, err := protocol.SealWorkload(workload)
	if err != nil {
		t.Fatalf("SealWorkload() error = %v", err)
	}
	document.Spec.EffectiveExecution.Spec.Workload = protocol.WorkloadBinding{
		Digest: sealedWorkload.Digest, Document: sealedWorkload.Document,
	}
	sealedEffective, err := protocol.SealEffectiveExecution(document.Spec.EffectiveExecution)
	if err != nil {
		t.Fatalf("SealEffectiveExecution() error = %v", err)
	}
	document.Spec.EffectiveExecution = sealedEffective.Document
	document.Spec.EffectiveExecutionDigest = sealedEffective.Digest
	assignment, err := protocol.SealAgentAssignment(document)
	if err != nil {
		t.Fatalf("SealAgentAssignment() error = %v", err)
	}
	authorization := protocol.LaunchAuthorization{
		APIVersion: protocol.V1Alpha1, Kind: protocol.LaunchAuthorizationKind,
		Metadata: protocol.LaunchAuthorizationMetadata{
			AuthorizationID: "88888888-8888-4888-8888-888888888888",
			ExecutionID:     testExecutionID, AgentID: testAgentID, Revision: 1,
			AcceptedAt: time.Now().UTC(),
		},
		Spec: protocol.LaunchAuthorizationSpec{
			TargetGenerationID:       testTargetGenerationID,
			EffectiveExecutionDigest: assignment.EffectiveExecutionDigest,
		},
	}
	if err = protocol.ValidateLaunchAuthorization(authorization); err != nil {
		t.Fatalf("ValidateLaunchAuthorization() error = %v", err)
	}

	return assignment, authorization
}

func resealAssignment(t *testing.T, document protocol.AgentAssignment) protocol.SealedAgentAssignment {
	t.Helper()
	workload, err := protocol.SealWorkload(document.Spec.EffectiveExecution.Spec.Workload.Document)
	if err != nil {
		t.Fatalf("SealWorkload() error = %v", err)
	}
	document.Spec.EffectiveExecution.Spec.Workload = protocol.WorkloadBinding{
		Digest: workload.Digest, Document: workload.Document,
	}
	effective, err := protocol.SealEffectiveExecution(document.Spec.EffectiveExecution)
	if err != nil {
		t.Fatalf("SealEffectiveExecution() error = %v", err)
	}
	document.Spec.EffectiveExecution = effective.Document
	document.Spec.EffectiveExecutionDigest = effective.Digest
	assignment, err := protocol.SealAgentAssignment(document)
	if err != nil {
		t.Fatalf("SealAgentAssignment() error = %v", err)
	}

	return assignment
}
