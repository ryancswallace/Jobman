package agent

import (
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/protocol"
)

func TestServiceReconcilesAssignmentEventsAndAction(t *testing.T) {
	pki := newTestPKI(t)
	assignment, authorization := testAssignment(t, "true", nil, nil)
	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID:    "99999999-9999-4999-8999-999999999999",
			ExecutionID: testExecutionID, AgentID: testAgentID, Revision: 1,
			RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	var mutex sync.Mutex
	accepted := false
	actionAcknowledged := false
	var observed []string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/agent/assignments":
			items := []json.RawMessage(nil)
			if !accepted {
				items = append(items, assignment.CanonicalJSON)
			}
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "AgentAssignmentList",
				"requiresAcceptance": true, "items": items,
			})
		case "/v1/agent/assignments/66666666-6666-4666-8666-666666666666/accept":
			accepted = true
			writeTestJSON(t, writer, authorization)
		case "/v1/agent/actions":
			items := []any(nil)
			if accepted && !actionAcknowledged {
				items = append(items, action)
			}
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "DesiredActionList", "items": items,
			})
		case "/v1/agent/actions/99999999-9999-4999-8999-999999999999/acknowledge":
			actionAcknowledged = true
			writer.WriteHeader(http.StatusNoContent)
		case "/v1/agent/executions/33333333-3333-4333-8333-333333333333/events":
			var event protocol.ExecutionEvent
			decodeTestRequest(t, request, &event)
			observed = append(observed, event.Spec.Type)
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "ExecutionEventReceipt",
				"eventId": event.Metadata.EventID, "sequence": event.Metadata.Sequence,
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	stateDirectory := t.TempDir()
	saveTestCredentials(t, pki, stateDirectory, server.URL)
	client, err := OpenClient(stateDirectory)
	if err != nil {
		t.Fatalf("OpenClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	service := &service{
		stateDirectory: stateDirectory, runnerExecutable: os.Args[0],
		logger: slog.New(slog.DiscardHandler), client: client, spool: spool,
	}
	service.startExecution = func(directory, executionID string) error {
		return writeManifest(filepath.Join(directory, startedFilename), StartManifest{
			ExecutionID: executionID, NativeID: "4242",
			Process:   platform.ProcessIdentity{PID: 4242, Creation: "test", Boot: "test"},
			StartedAt: time.Now().UTC(),
		})
	}
	if err = service.step(t.Context()); err != nil {
		t.Fatalf("step(accept) error = %v", err)
	}
	if err = service.step(t.Context()); err != nil {
		t.Fatalf("step(start) error = %v", err)
	}
	directory, err := executionDirectory(stateDirectory, testExecutionID)
	if err != nil {
		t.Fatalf("executionDirectory() error = %v", err)
	}
	exitCode := 0
	if err = writeManifest(filepath.Join(directory, completionFilename), CompletionManifest{
		ExecutionID: testExecutionID, ObservedAt: time.Now().UTC(),
		Result: protocol.ProcessResult{Outcome: "success", ExitCode: &exitCode},
	}); err != nil {
		t.Fatalf("write completion manifest: %v", err)
	}
	if err = service.step(t.Context()); err != nil {
		t.Fatalf("step(completion) error = %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !accepted || !actionAcknowledged || len(observed) != 2 ||
		observed[0] != "process.started" || observed[1] != "process.completed" {
		t.Fatalf("server state accepted=%t acknowledged=%t observed=%v", accepted, actionAcknowledged, observed)
	}
	if _, err = os.Stat(filepath.Join(directory, cancelFilename)); err != nil {
		t.Fatalf("cancel marker missing: %v", err)
	}
}

func TestServiceMarksStaleUnobservedLaunchLost(t *testing.T) {
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	assignment, authorization := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	claimPath := filepath.Join(directory, claimFilename)
	if err = writeNewPrivateFile(claimPath, []byte("claim")); err != nil {
		t.Fatalf("write launch claim: %v", err)
	}
	old := time.Now().Add(-time.Minute)
	if err = os.Chtimes(claimPath, old, old); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}
	service := &service{
		stateDirectory: stateDirectory,
		client:         &Client{credentials: credentialFiles{Metadata: metadata{AgentID: testAgentID}}},
		spool:          spool,
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution() error = %v", err)
	}
	completion, err := readCompletionManifest(directory)
	if err != nil || completion.Result.Outcome != "lost" {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
}

func saveTestCredentials(t *testing.T, pki testPKI, stateDirectory, serverURL string) {
	t.Helper()
	key, request, err := generateKeyAndCSR("worker-a")
	if err != nil {
		t.Fatalf("generateKeyAndCSR() error = %v", err)
	}
	certificate := pki.issueCSR(t, request)
	if err = saveCredentials(stateDirectory, credentialFiles{
		Metadata: metadata{
			ServerURL: serverURL, AgentID: testAgentID,
			TargetGenerationID:   testTargetGenerationID,
			SessionID:            "88888888-8888-4888-8888-888888888888",
			SessionToken:         "jms_test",
			SessionExpiresAt:     time.Now().Add(time.Hour),
			CertificateExpiresAt: certificate.ExpiresAt,
		},
		PrivateKeyPEM: key, CertificatePEM: []byte(certificate.CertificatePEM),
		ServerCAPEM: pki.caPEM,
	}); err != nil {
		t.Fatalf("saveCredentials() error = %v", err)
	}
}
