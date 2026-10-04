package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/protocol"
)

type isolatedObservationScheduler struct {
	fakeSlurmScheduler
	failure error
	cancel  context.CancelFunc
	queried []string
}

func (scheduler *isolatedObservationScheduler) Observe(
	_ context.Context,
	id string,
) (slurmbackend.Observation, error) {
	scheduler.queried = append(scheduler.queried, id)
	if id == "45_0" {
		if scheduler.cancel != nil {
			scheduler.cancel()
		}

		return slurmbackend.Observation{}, scheduler.failure
	}
	exit := 0

	return slurmbackend.Observation{
		JobID: id, State: "completed", Terminal: true,
		Result: &protocol.ProcessResult{Outcome: "success", ExitCode: &exit},
	}, nil
}

func observationIsolationFixture(t *testing.T) (*service, *isolatedObservationScheduler, []string) {
	t.Helper()
	state, shared := t.TempDir(), t.TempDir()
	spool, err := OpenSpool(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	ids := make([]string, 0, 2)
	for index := range 2 {
		assignment, authorization := testSlurmArrayAssignment(t, index)
		id := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
		ids = append(ids, id)
		if err = spool.PutAssignment(t.Context(), assignment); err != nil {
			t.Fatal(err)
		}
		if err = spool.RecordAcceptance(t.Context(), id, authorization); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := &isolatedObservationScheduler{
		fakeSlurmScheduler: fakeSlurmScheduler{
			submitResponses: []slurmbackend.Submission{{JobID: "45"}},
		},
		failure: errors.New("scheduler has no accounting allocation for canceled queued task"),
	}
	svc := testSlurmService(state, shared, spool, scheduler)
	executions, err := spool.ListExecutions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = svc.reconcileSlurmArrays(t.Context(), executions); err != nil {
		t.Fatal(err)
	}
	// A successful cancellation request is not a terminal observation.
	missingDirectory, err := executionDirectory(state, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{cancelFilename, slurmCancelReceiptFilename} {
		if err = writeNewPrivateFile(filepath.Join(missingDirectory, name), []byte("2026-10-04T07:48:20Z\n")); err != nil {
			t.Fatal(err)
		}
	}
	completedDirectory, err := executionDirectory(shared, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	// The runner's workload result differs from the successful batch wrapper.
	exit := 7
	if err = writeManifest(filepath.Join(completedDirectory, completionFilename), CompletionManifest{
		ExecutionID: ids[1], ObservedAt: time.Date(2026, 10, 4, 7, 48, 40, 0, time.UTC),
		Result: protocol.ProcessResult{Outcome: "failure", ExitCode: &exit},
	}); err != nil {
		t.Fatal(err)
	}
	if err = writeNewPrivateFile(filepath.Join(completedDirectory, stderrFilename), []byte("synthetic failure\n")); err != nil {
		t.Fatal(err)
	}
	svc.artifactStore, err = artifact.NewFilesystemStore("logs", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	return svc, scheduler, ids
}

func TestUnavailableSlurmTaskDoesNotBlockSiblingPublicationOrActions(t *testing.T) {
	svc, scheduler, ids := observationIsolationFixture(t)
	pki := newTestPKI(t)
	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID: "99999999-9999-4999-8999-999999999999", ExecutionID: ids[0],
			AgentID: testAgentID, Revision: 1, RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	var mutex sync.Mutex
	var events []protocol.ExecutionEvent
	chunks := 0
	assignmentsPolled, acknowledged := false, false
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/agent/actions":
			writeTestJSON(t, w, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "DesiredActionList", "items": []any{action},
			})
		case strings.HasSuffix(r.URL.Path, "/acknowledge"):
			acknowledged = true
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/v1/agent/assignments":
			assignmentsPolled = true
			writeTestJSON(t, w, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "AgentAssignmentList",
				"requiresAcceptance": true, "items": []any{},
			})
		case strings.Contains(r.URL.Path, "/logs/"):
			var chunk agentLogChunk
			decodeTestRequest(t, r, &chunk)
			if chunk.Metadata.ExecutionID != ids[1] {
				t.Errorf("unobserved task published a log chunk: %q", chunk.Metadata.ExecutionID)
			}
			chunks++
			writeTestJSON(t, w, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "LogChunkReceipt",
				"executionId": chunk.Metadata.ExecutionID, "stream": chunk.Metadata.Stream,
				"sequence": chunk.Metadata.Sequence,
			})
		case strings.HasSuffix(r.URL.Path, "/events"):
			var event protocol.ExecutionEvent
			decodeTestRequest(t, r, &event)
			if event.Spec.Type == "scheduler.completed" && chunks != 2 {
				t.Error("terminal event preceded both immutable log manifests")
			}
			events = append(events, event)
			writeTestJSON(t, w, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "ExecutionEventReceipt",
				"eventId": event.Metadata.EventID, "sequence": event.Metadata.Sequence,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	saveTestCredentials(t, pki, svc.stateDirectory, server.URL)
	client, err := OpenClient(svc.stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	svc.client = client
	if err = svc.step(t.Context()); !errors.Is(err, scheduler.failure) {
		t.Fatalf("unavailable observation must remain reported: %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !assignmentsPolled || !acknowledged || chunks != 2 || len(events) != 3 {
		t.Fatalf("progress: assignments=%t action=%t chunks=%d events=%d", assignmentsPolled, acknowledged, chunks, len(events))
	}
	completed := 0
	for _, event := range events {
		if event.Spec.Type != "scheduler.completed" {
			continue
		}
		completed++
		if event.Metadata.ExecutionID != ids[1] || event.Spec.Result == nil ||
			event.Spec.Result.Outcome != "failure" || event.Spec.Result.ExitCode == nil || *event.Spec.Result.ExitCode != 7 {
			t.Fatalf("completion invented or workload failure lost: %#v", event)
		}
	}
	if completed != 1 || scheduler.cancelCalls != 0 || scheduler.arraySubmitCalls != 1 {
		t.Fatalf("completion=%d cancels=%d submissions=%d", completed, scheduler.cancelCalls, scheduler.arraySubmitCalls)
	}
	missingDirectory, err := executionDirectory(svc.slurmRoot, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readCompletionManifest(missingDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancel receipt plus absence fabricated completion: %v", err)
	}
	pending, err := svc.spool.PendingEvents(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatalf("durable sibling events were not flushed: %d, %v", len(pending), err)
	}
}

func TestReconcileObservationsStopsOnContextCancellation(t *testing.T) {
	svc, scheduler, _ := observationIsolationFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scheduler.cancel = cancel
	if err := svc.reconcileExecutions(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile cancellation lost: %v", err)
	}
	if len(scheduler.queried) != 1 || scheduler.queried[0] != "45_0" {
		t.Fatalf("observed another execution after cancellation: %#v", scheduler.queried)
	}
}
