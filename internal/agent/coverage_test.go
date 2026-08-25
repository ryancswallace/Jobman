package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/protocol"
)

func TestSpoolFailureAndConflictBoundaries(t *testing.T) {
	if _, err := OpenSpool(nil, t.TempDir()); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("OpenSpool() accepted nil context")
	}
	if _, err := OpenSpool(t.Context(), "relative"); err == nil {
		t.Fatal("OpenSpool() accepted relative directory")
	}
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	assignment, authorization := testAssignment(t, "true", nil, nil)
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	invalidAuthorization := authorization
	invalidAuthorization.Metadata.Revision = 0
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, invalidAuthorization); err == nil {
		t.Fatal("RecordAcceptance() accepted invalid authorization")
	}
	if err = spool.RecordAcceptance(t.Context(), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", authorization); err == nil {
		t.Fatal("RecordAcceptance() accepted mismatched execution")
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	changed := authorization
	changed.Metadata.AuthorizationID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, changed); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("RecordAcceptance(changed) error = %v", err)
	}
	if err = spool.SetExecutionState(t.Context(), testExecutionID, "unknown"); err == nil {
		t.Fatal("SetExecutionState() accepted invalid state")
	}
	if err = spool.SetExecutionState(t.Context(), testExecutionID, "running"); err != nil {
		t.Fatalf("SetExecutionState(running) error = %v", err)
	}
	if err = spool.SetExecutionState(t.Context(), testExecutionID, "accepted"); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("SetExecutionState(regression) error = %v", err)
	}
	if err = spool.SetExecutionState(t.Context(), testExecutionID, "running"); err != nil {
		t.Fatalf("SetExecutionState(replay) error = %v", err)
	}
	if err = spool.MarkEventDelivered(t.Context(), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("MarkEventDelivered(missing) error = %v", err)
	}
	if err = spool.MarkActionAcknowledged(t.Context(), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("MarkActionAcknowledged(missing) error = %v", err)
	}
	invalidAction := protocol.DesiredAction{}
	if err = spool.PutAction(t.Context(), invalidAction); err == nil {
		t.Fatal("PutAction() accepted invalid action")
	}
	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID:    "99999999-9999-4999-8999-999999999999",
			ExecutionID: testExecutionID, AgentID: testAgentID, Revision: 1,
			RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	if err = spool.PutAction(t.Context(), action); err != nil {
		t.Fatalf("PutAction() error = %v", err)
	}
	if err = spool.PutAction(t.Context(), action); err != nil {
		t.Fatalf("PutAction(replay) error = %v", err)
	}
	changedAction := action
	changedAction.Metadata.RequestedAt = action.Metadata.RequestedAt.Add(time.Second)
	if err = spool.PutAction(t.Context(), changedAction); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("PutAction(changed) error = %v", err)
	}
	if err = spool.MarkActionAcknowledged(t.Context(), action.Metadata.ActionID); err != nil {
		t.Fatalf("MarkActionAcknowledged() error = %v", err)
	}
	if err = spool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	var nilSpool *Spool
	if err = nilSpool.Close(); err != nil {
		t.Fatalf("nil Close() error = %v", err)
	}
	closedOperations := []func() error{
		func() error { return spool.PutAssignment(t.Context(), assignment) },
		func() error { return spool.RecordAcceptance(t.Context(), testExecutionID, authorization) },
		func() error { return spool.SetExecutionState(t.Context(), testExecutionID, "terminal") },
		func() error { _, operationErr := spool.ListExecutions(t.Context()); return operationErr },
		func() error {
			event, eventErr := protocol.SealExecutionEvent(protocol.ExecutionEvent{
				APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
				Metadata: protocol.ExecutionEventMetadata{
					EventID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ExecutionID: testExecutionID,
					AgentID: testAgentID, Sequence: 1, ObservedAt: time.Now().UTC(),
				},
				Spec: protocol.ExecutionEventSpec{
					Type: "process.completed", Result: &protocol.ProcessResult{Outcome: "lost"},
				},
			})
			if eventErr != nil {
				return eventErr
			}
			return spool.QueueEvent(t.Context(), event)
		},
		func() error { _, operationErr := spool.PendingEvents(t.Context()); return operationErr },
		func() error { return spool.MarkEventDelivered(t.Context(), "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb") },
		func() error { return spool.PutAction(t.Context(), action) },
		func() error { return spool.MarkActionAcknowledged(t.Context(), action.Metadata.ActionID) },
	}
	for index, operation := range closedOperations {
		if err = operation(); err == nil {
			t.Errorf("closed operation %d error = nil", index)
		}
	}
}

func TestJSONAndCredentialFailureBoundaries(t *testing.T) {
	var target map[string]any
	if err := decodeStrictJSON([]byte(`{} {}`), &target); err == nil {
		t.Fatal("decodeStrictJSON() accepted trailing value")
	}
	if err := decodeStrictJSON([]byte(`{`), &target); err == nil {
		t.Fatal("decodeStrictJSON() accepted malformed JSON")
	}
	stateDirectory := t.TempDir()
	if _, err := loadCredentials(stateDirectory); err == nil {
		t.Fatal("loadCredentials() accepted missing file")
	}
	if err := writePrivateFile(filepath.Join(stateDirectory, agentCredentialsFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	if _, err := loadCredentials(stateDirectory); err == nil {
		t.Fatal("loadCredentials() accepted incomplete credentials")
	}
	if _, err := newTLSHTTPClient([]byte("not a certificate"), nil); err == nil {
		t.Fatal("newTLSHTTPClient() accepted invalid CA")
	}
	if _, err := newMTLSHTTPClient(credentialFiles{}); err == nil {
		t.Fatal("newMTLSHTTPClient() accepted empty key pair")
	}
	if _, err := newHTTPClientForCertificate(nil, []byte("bad"), []byte("bad")); err == nil {
		t.Fatal("newHTTPClientForCertificate() accepted invalid key pair")
	}
}

func TestHTTPClientResponseBoundaries(t *testing.T) {
	var target map[string]any
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	if err = performRequest(nil, http.DefaultClient, baseURL, http.MethodGet, "/", "", nil, nil); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("performRequest() accepted nil context")
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport failed")
	})}
	if err = performRequest(t.Context(), client, baseURL, http.MethodGet, "/", "", nil, nil); err == nil {
		t.Fatal("performRequest() accepted transport failure")
	}
	tests := []struct {
		name   string
		status int
		body   string
		target any
	}{
		{name: "status", status: http.StatusConflict, body: `{}`},
		{name: "unexpected body", status: http.StatusNoContent, body: `{}`},
		{name: "oversized", status: http.StatusOK, body: strings.Repeat("x", maximumResponseSize+1), target: &target},
		{name: "malformed", status: http.StatusOK, body: `{`, target: &target},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return responseWithBody(test.status, test.body), nil
			})}
			if operationErr := performRequest(t.Context(), client, baseURL, http.MethodGet, "/", "", nil, test.target); operationErr == nil {
				t.Fatal("performRequest() error = nil")
			}
		})
	}
	if err = performJSON(t.Context(), http.DefaultClient, baseURL, http.MethodPost, "/", "", make(chan int), nil); err == nil {
		t.Fatal("performJSON() accepted unencodable request")
	}
}

func TestCapabilityAndSessionTransportFailures(t *testing.T) {
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatal(err)
	}
	credentials := credentialFiles{Metadata: metadata{
		AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
		SessionToken: "session",
	}}
	client := &Client{
		baseURL: baseURL, credentials: credentials,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusOK, `{}`), nil
		})},
	}
	report := CapabilityReport{
		ObservedAt: time.Now().UTC(), AgentVersion: "dev", OperatingSystem: "linux",
		Architecture: "amd64", Hostname: "submit", ExecutionUser: "researcher",
		ExecutionBackends: []string{"subprocess"}, Runtimes: []string{"native"},
	}
	if err = client.ReportCapabilities(t.Context(), report); err == nil {
		t.Fatal("ReportCapabilities() accepted an invalid receipt")
	}
	receipt, err := json.Marshal(map[string]any{
		"apiVersion": controlAPIVersion, "kind": "AgentCapabilityReceipt",
		"agentId": testAgentID, "revision": 1, "observedAt": report.ObservedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWithBody(http.StatusOK, string(receipt)), nil
	})}
	if err = client.ReportCapabilities(t.Context(), report); err != nil {
		t.Fatalf("ReportCapabilities() error = %v", err)
	}
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("offline")
	})}
	if err = client.ReportCapabilities(t.Context(), report); err == nil {
		t.Fatal("ReportCapabilities() ignored transport failure")
	}
	if err = client.RenewSession(t.Context(), t.TempDir()); err == nil {
		t.Fatal("RenewSession() ignored transport failure")
	}

	expires := time.Now().UTC().Add(time.Hour)
	response, err := json.Marshal(map[string]any{
		"apiVersion": controlAPIVersion, "kind": "AgentSession",
		"metadata": map[string]any{
			"agentId": testAgentID, "sessionId": "99999999-9999-4999-8999-999999999999",
			"expiresAt": expires,
		},
		"spec": map[string]any{"token": "rotated", "authScheme": "Jobman-Agent", "inertOnly": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWithBody(http.StatusOK, string(response)), nil
	})}
	if err = client.RenewSession(t.Context(), "relative"); err == nil {
		t.Fatal("RenewSession() ignored credential persistence failure")
	}
}

func TestControlClientRejectsInvalidServerDocuments(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	newClient := func(body string) *Client {
		return &Client{
			baseURL: baseURL,
			httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return responseWithBody(http.StatusOK, body), nil
			})},
			credentials: credentialFiles{Metadata: metadata{
				AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
			}},
		}
	}
	if _, err = newClient(`{"apiVersion":"wrong","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[]}`).ListAssignments(t.Context(), 1); err == nil {
		t.Fatal("ListAssignments() accepted invalid list")
	}
	if _, err = newClient(`{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[{}]}`).ListAssignments(t.Context(), 1); err == nil {
		t.Fatal("ListAssignments() accepted invalid item")
	}
	mismatchList := `{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[` + string(assignment.CanonicalJSON) + `]}`
	mismatchClient := newClient(mismatchList)
	mismatchClient.credentials.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err = mismatchClient.ListAssignments(t.Context(), 1); err == nil {
		t.Fatal("ListAssignments() accepted mismatched item")
	}
	if _, err = newClient(`{}`).AcceptAssignment(t.Context(), assignment); err == nil {
		t.Fatal("AcceptAssignment() accepted invalid authorization")
	}
	mismatchedAuthorization := authorization
	mismatchedAuthorization.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	authorizationJSON, err := marshalStrictJSON(mismatchedAuthorization)
	if err != nil {
		t.Fatalf("marshal authorization: %v", err)
	}
	if _, err = newClient(string(authorizationJSON)).AcceptAssignment(t.Context(), assignment); err == nil {
		t.Fatal("AcceptAssignment() accepted mismatched authorization")
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ExecutionID: testExecutionID,
			AgentID: testAgentID, Sequence: 1, ObservedAt: time.Now().UTC(),
		},
		Spec: protocol.ExecutionEventSpec{
			Type: "process.completed", Result: &protocol.ProcessResult{Outcome: "lost"},
		},
	})
	if err != nil {
		t.Fatalf("SealExecutionEvent() error = %v", err)
	}
	if err = newClient(`{"apiVersion":"jobman.control/v1alpha1","kind":"ExecutionEventReceipt","eventId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","sequence":1}`).RecordEvent(t.Context(), event); err == nil {
		t.Fatal("RecordEvent() accepted mismatched receipt")
	}
	if _, err = newClient(`{"apiVersion":"wrong","kind":"DesiredActionList","items":[]}`).ListActions(t.Context(), 1); err == nil {
		t.Fatal("ListActions() accepted invalid list")
	}
	if _, err = newClient(`{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[{}]}`).ListActions(t.Context(), 1); err == nil {
		t.Fatal("ListActions() accepted invalid action")
	}
	if _, err = newClient(`{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[{"unknown":true}]}`).ListActions(t.Context(), 1); err == nil {
		t.Fatal("ListActions() accepted unknown action fields")
	}
	if err = newClient("").AcknowledgeAction(t.Context(), protocol.DesiredAction{}); err == nil {
		t.Fatal("AcknowledgeAction() accepted invalid action")
	}
	if err = newClient(`{}`).RenewCertificate(t.Context(), t.TempDir()); err == nil {
		t.Fatal("RenewCertificate() accepted invalid certificate response")
	}
}

func TestServiceComponentFailureBoundaries(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	client := &Client{credentials: credentialFiles{Metadata: metadata{AgentID: testAgentID}}}
	service := &service{stateDirectory: stateDirectory, client: client, spool: spool}
	badStarted := StartManifest{ExecutionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	if err = service.observeStart(t.Context(), SpoolExecution{Assignment: assignment}, badStarted); err == nil {
		t.Fatal("observeStart() accepted mismatched execution")
	}
	badCompletion := CompletionManifest{ExecutionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	if err = service.observeCompletion(t.Context(), SpoolExecution{Assignment: assignment}, StartManifest{}, os.ErrNotExist, badCompletion); err == nil {
		t.Fatal("observeCompletion() accepted mismatched execution")
	}
	goodCompletion := CompletionManifest{
		ExecutionID: testExecutionID, ObservedAt: time.Now().UTC(),
		Result: protocol.ProcessResult{Outcome: "lost"},
	}
	if err = service.observeCompletion(t.Context(), SpoolExecution{Assignment: assignment}, StartManifest{}, errors.New("corrupt"), goodCompletion); err == nil {
		t.Fatal("observeCompletion() accepted corrupt start manifest")
	}
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	service.startExecution = func(string, string) error { return errors.New("start failed") }
	if err = service.reconcileExecution(t.Context(), executions[0]); err == nil || !strings.Contains(err.Error(), "start failed") {
		t.Fatalf("reconcileExecution(start failure) error = %v", err)
	}
	directory, err := executionDirectory(stateDirectory, testExecutionID)
	if err != nil {
		t.Fatalf("executionDirectory() error = %v", err)
	}
	if err = writePrivateFile(filepath.Join(directory, startedFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}
	if err = service.reconcileExecution(t.Context(), executions[0]); err == nil {
		t.Fatal("reconcileExecution() accepted corrupt start manifest")
	}
}

func TestServiceTransportAndJournalFailures(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	failedClient := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
		})},
		credentials: credentialFiles{Metadata: metadata{
			AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
		}},
	}
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	if err = (&service{client: failedClient, spool: spool}).pollAssignments(t.Context()); err == nil {
		t.Fatal("pollAssignments() accepted server failure")
	}
	if err = (&service{client: failedClient, spool: spool}).pollActions(t.Context()); err == nil {
		t.Fatal("pollActions() accepted server failure")
	}
	if err = spool.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assignmentList := `{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[` + string(assignment.CanonicalJSON) + `]}`
	assignmentClient := staticResponseClient(t, baseURL, assignmentList)
	if err = (&service{client: assignmentClient, spool: spool}).pollAssignments(t.Context()); err == nil {
		t.Fatal("pollAssignments() accepted closed journal")
	}
	if err = (&service{spool: spool}).reconcileExecutions(t.Context()); err == nil {
		t.Fatal("reconcileExecutions() accepted closed journal")
	}
	if err = (&service{spool: spool}).flushEvents(t.Context()); err == nil {
		t.Fatal("flushEvents() accepted closed journal")
	}

	stateDirectory := t.TempDir()
	actionSpool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool(action) error = %v", err)
	}
	t.Cleanup(func() { _ = actionSpool.Close() })
	if err = actionSpool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = actionSpool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID:    "99999999-9999-4999-8999-999999999999",
			ExecutionID: testExecutionID, AgentID: testAgentID, Revision: 1,
			RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	actionDocument, err := marshalStrictJSON(action)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}
	actionList := `{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[` + string(actionDocument) + `]}`
	actionClient := staticResponseClient(t, baseURL, actionList)
	missingState := filepath.Join(t.TempDir(), "missing")
	if err = (&service{stateDirectory: missingState, client: actionClient, spool: actionSpool}).pollActions(t.Context()); err == nil {
		t.Fatal("pollActions() accepted missing execution directory")
	}
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	_ = directory
	ackFailureClient := &Client{
		baseURL: baseURL, credentials: actionClient.credentials,
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/acknowledge") {
				return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
			}
			return responseWithBody(http.StatusOK, actionList), nil
		})},
	}
	if err = (&service{stateDirectory: stateDirectory, client: ackFailureClient, spool: actionSpool}).pollActions(t.Context()); err == nil {
		t.Fatal("pollActions() accepted acknowledgement failure")
	}
}

func staticResponseClient(t *testing.T, baseURL *url.URL, document string) *Client {
	t.Helper()
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusOK, document), nil
		})},
		credentials: credentialFiles{Metadata: metadata{
			AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
		}},
	}
}

func TestRunnerMalformedDocumentsAndManifestValidation(t *testing.T) {
	stateDirectory := t.TempDir()
	if err := RunExecution(nil, stateDirectory, testExecutionID); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("RunExecution() accepted nil context")
	}
	if err := RunExecution(t.Context(), stateDirectory, "invalid"); err == nil {
		t.Fatal("RunExecution() accepted invalid ID")
	}
	directory, err := executionDirectory(stateDirectory, testExecutionID)
	if err != nil {
		t.Fatalf("executionDirectory() error = %v", err)
	}
	if err = os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err = writePrivateFile(filepath.Join(directory, assignmentFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write assignment: %v", err)
	}
	if err = writePrivateFile(filepath.Join(directory, authorizationFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write authorization: %v", err)
	}
	if err = RunExecution(t.Context(), stateDirectory, testExecutionID); err == nil {
		t.Fatal("RunExecution() accepted malformed documents")
	}
	if completion, err := readCompletionManifest(directory); err != nil || completion.Result.FailureCode != "invalid_execution" {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
	invalidDirectory := t.TempDir()
	if err = writeManifest(filepath.Join(invalidDirectory, startedFilename), StartManifest{}); err != nil {
		t.Fatalf("write start manifest: %v", err)
	}
	if _, err = readStartManifest(invalidDirectory); err == nil {
		t.Fatal("readStartManifest() accepted empty manifest")
	}
	if err = writeManifest(filepath.Join(invalidDirectory, completionFilename), CompletionManifest{}); err != nil {
		t.Fatalf("write completion manifest: %v", err)
	}
	if _, err = readCompletionManifest(invalidDirectory); err == nil {
		t.Fatal("readCompletionManifest() accepted empty manifest")
	}
	assignment, authorization := testAssignment(t, "true", nil, nil)
	assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID = "invalid"
	if _, err = prepareExecutionFiles(stateDirectory, assignment, authorization); err == nil {
		t.Fatal("prepareExecutionFiles() accepted invalid assignment identity")
	}
}

func TestRunnerFilesystemAndLifecycleBoundaries(t *testing.T) {
	if _, err := prepareStateDirectory("relative"); err == nil {
		t.Fatal("prepareStateDirectory() accepted relative path")
	}
	stateFile := filepath.Join(t.TempDir(), "state-file")
	if err := os.WriteFile(stateFile, []byte("state"), 0o600); err != nil {
		t.Fatalf("WriteFile(state file) error = %v", err)
	}
	if _, err := prepareStateDirectory(stateFile); err == nil {
		t.Fatal("prepareStateDirectory() accepted a regular file")
	}
	if err := writePrivateFile(filepath.Join(t.TempDir(), "missing", "value"), []byte("value")); err == nil {
		t.Fatal("writePrivateFile() accepted missing parent")
	}
	privateDirectory := t.TempDir()
	privatePath := filepath.Join(privateDirectory, "new")
	if err := writeNewPrivateFile(privatePath, []byte("value")); err != nil {
		t.Fatalf("writeNewPrivateFile() error = %v", err)
	}
	if err := writeNewPrivateFile(privatePath, []byte("value")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("writeNewPrivateFile(replay) error = %v", err)
	}
	if err := removePrivateFile(privatePath); err != nil {
		t.Fatalf("removePrivateFile() error = %v", err)
	}
	if err := removePrivateFile(privatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removePrivateFile(missing) error = %v", err)
	}

	assignment, authorization := testAssignment(t, "true", nil, nil)
	if _, err := prepareExecutionFiles(stateFile, assignment, authorization); err == nil {
		t.Fatal("prepareExecutionFiles() accepted state root file")
	}
	assignmentFailureState := t.TempDir()
	assignmentFailureDirectory, err := executionDirectory(assignmentFailureState, testExecutionID)
	if err != nil {
		t.Fatalf("executionDirectory(assignment failure) error = %v", err)
	}
	if err = os.MkdirAll(filepath.Join(assignmentFailureDirectory, assignmentFilename), 0o700); err != nil {
		t.Fatalf("MkdirAll(assignment filename) error = %v", err)
	}
	if _, err = prepareExecutionFiles(assignmentFailureState, assignment, authorization); err == nil {
		t.Fatal("prepareExecutionFiles() accepted assignment path directory")
	}
	authorizationFailureState := t.TempDir()
	authorizationFailureDirectory, err := executionDirectory(authorizationFailureState, testExecutionID)
	if err != nil {
		t.Fatalf("executionDirectory(authorization failure) error = %v", err)
	}
	if err = os.MkdirAll(filepath.Join(authorizationFailureDirectory, authorizationFilename), 0o700); err != nil {
		t.Fatalf("MkdirAll(authorization filename) error = %v", err)
	}
	if _, err = prepareExecutionFiles(authorizationFailureState, assignment, authorization); err == nil {
		t.Fatal("prepareExecutionFiles() accepted authorization path directory")
	}
	if err := RunExecution(t.Context(), t.TempDir(), testExecutionID); err == nil {
		t.Fatal("RunExecution() claimed a missing execution directory")
	}
	completionFailureState := t.TempDir()
	completionFailureDirectory, err := prepareExecutionFiles(completionFailureState, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles(completion failure) error = %v", err)
	}
	if err = os.Mkdir(filepath.Join(completionFailureDirectory, completionFilename), 0o700); err != nil {
		t.Fatalf("Mkdir(completion filename) error = %v", err)
	}
	if err = RunExecution(t.Context(), completionFailureState, testExecutionID); err == nil {
		t.Fatal("RunExecution() accepted completion manifest write failure")
	}

	t.Run("missing authorization", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "execution")
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := writePrivateFile(filepath.Join(directory, assignmentFilename), assignment.CanonicalJSON); err != nil {
			t.Fatalf("write assignment: %v", err)
		}
		if _, _, err := loadExecutionDocuments(directory); err == nil {
			t.Fatal("loadExecutionDocuments() accepted missing authorization")
		}
	})

	t.Run("malformed authorization", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "execution")
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := writePrivateFile(filepath.Join(directory, assignmentFilename), assignment.CanonicalJSON); err != nil {
			t.Fatalf("write assignment: %v", err)
		}
		if err := writePrivateFile(filepath.Join(directory, authorizationFilename), []byte(`{}`)); err != nil {
			t.Fatalf("write authorization: %v", err)
		}
		if _, _, err := loadExecutionDocuments(directory); err == nil {
			t.Fatal("loadExecutionDocuments() accepted malformed authorization")
		}
	})

	tests := []struct {
		name        string
		prepare     func(*testing.T, string, *protocol.AgentAssignment)
		mutate      func(*protocol.AgentAssignment)
		context     func(*testing.T) context.Context
		wantCode    string
		wantOutcome string
		wantError   bool
	}{
		{
			name: "unsupported assignment",
			mutate: func(value *protocol.AgentAssignment) {
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Runtime.Kind = "container"
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Runtime.Container = &protocol.ContainerRuntime{
					Image: "example.invalid/image:latest", PullPolicy: "if-not-present", Network: "none",
				}
			},
			wantCode: "unsupported_workload", wantOutcome: "failure", wantError: true,
		},
		{
			name: "workspace is file",
			prepare: func(t *testing.T, directory string, _ *protocol.AgentAssignment) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(directory, "workspace"), []byte("file"), 0o600); err != nil {
					t.Fatalf("write workspace file: %v", err)
				}
			},
			wantCode: "workspace_create_failed", wantOutcome: "failure", wantError: true,
		},
		{
			name: "working directory is file",
			mutate: func(value *protocol.AgentAssignment) {
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.WorkingDirectory = "workspace:/nested"
			},
			prepare: func(t *testing.T, directory string, _ *protocol.AgentAssignment) {
				t.Helper()
				workspace := filepath.Join(directory, "workspace")
				if err := os.MkdirAll(workspace, 0o700); err != nil {
					t.Fatalf("MkdirAll(workspace) error = %v", err)
				}
				if err := os.WriteFile(filepath.Join(workspace, "nested"), []byte("file"), 0o600); err != nil {
					t.Fatalf("write working-directory file: %v", err)
				}
			},
			wantCode: "working_directory_create_failed", wantOutcome: "failure", wantError: true,
		},
		{
			name: "stdout is directory",
			prepare: func(t *testing.T, directory string, _ *protocol.AgentAssignment) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(directory, stdoutFilename), 0o700); err != nil {
					t.Fatalf("Mkdir(stdout) error = %v", err)
				}
			},
			wantCode: "stdout_open_failed", wantOutcome: "failure", wantError: true,
		},
		{
			name: "stderr is directory",
			prepare: func(t *testing.T, directory string, _ *protocol.AgentAssignment) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(directory, stderrFilename), 0o700); err != nil {
					t.Fatalf("Mkdir(stderr) error = %v", err)
				}
			},
			wantCode: "stderr_open_failed", wantOutcome: "failure", wantError: true,
		},
		{
			name: "runner context canceled", mutate: func(value *protocol.AgentAssignment) {
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Command.Executable = os.Args[0]
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Command.Args = []string{"-test.run=^TestAgentRunnerHelper$"}
				value.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Environment = &protocol.Environment{
					Values: map[string]string{"JOBMAN_AGENT_HELPER": "block"},
				}
			},
			context: func(t *testing.T) context.Context {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			wantOutcome: "aborted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDirectory := t.TempDir()
			value := assignment.Document
			if test.mutate != nil {
				test.mutate(&value)
			}
			sealed := resealAssignment(t, value)
			authorization := authorization
			authorization.Spec.EffectiveExecutionDigest = sealed.EffectiveExecutionDigest
			directory, err := prepareExecutionFiles(stateDirectory, sealed, authorization)
			if err != nil {
				t.Fatalf("prepareExecutionFiles() error = %v", err)
			}
			if test.prepare != nil {
				test.prepare(t, directory, &value)
			}
			ctx := t.Context()
			if test.context != nil {
				ctx = test.context(t)
			}
			err = RunExecution(ctx, stateDirectory, testExecutionID)
			if (err != nil) != test.wantError {
				t.Fatalf("RunExecution() error = %v, want error %t", err, test.wantError)
			}
			completion, readErr := readCompletionManifest(directory)
			if readErr != nil || completion.Result.Outcome != test.wantOutcome ||
				(test.wantCode != "" && completion.Result.FailureCode != test.wantCode) {
				t.Fatalf("completion = %#v, %v", completion, readErr)
			}
		})
	}

	if err := writeManifest(filepath.Join(t.TempDir(), "manifest"), make(chan int)); err == nil {
		t.Fatal("writeManifest() accepted an unencodable value")
	}
	if _, err := readStartManifest(t.TempDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readStartManifest(missing) error = %v", err)
	}
	if _, err := readCompletionManifest(t.TempDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readCompletionManifest(missing) error = %v", err)
	}
	if mapped, err := mapWorkspacePath(t.TempDir(), "workspace:/"); err != nil || mapped == "" {
		t.Fatalf("mapWorkspacePath(root) = %q, %v", mapped, err)
	}
}

func TestServiceStepAndDeliveryBoundaries(t *testing.T) {
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	assignment, authorization := testAssignment(t, "true", nil, nil)
	emptyAssignments := `{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[]}`
	emptyActions := `{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[]}`
	newClient := func(transport roundTripFunc) *Client {
		return &Client{
			baseURL: baseURL, httpClient: &http.Client{Transport: transport},
			credentials: credentialFiles{Metadata: metadata{
				AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
				CertificateExpiresAt: time.Now().Add(time.Hour),
			}},
		}
	}
	newSpool := func(t *testing.T) *Spool {
		t.Helper()
		spool, openErr := OpenSpool(t.Context(), t.TempDir())
		if openErr != nil {
			t.Fatalf("OpenSpool() error = %v", openErr)
		}
		t.Cleanup(func() { _ = spool.Close() })
		return spool
	}
	seedExecution := func(t *testing.T, spool *Spool) {
		t.Helper()
		if putErr := spool.PutAssignment(t.Context(), assignment); putErr != nil {
			t.Fatalf("PutAssignment() error = %v", putErr)
		}
		if acceptErr := spool.RecordAcceptance(t.Context(), testExecutionID, authorization); acceptErr != nil {
			t.Fatalf("RecordAcceptance() error = %v", acceptErr)
		}
	}

	t.Run("step reconcile failure", func(t *testing.T) {
		spool := newSpool(t)
		if closeErr := spool.Close(); closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
		client := newClient(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusOK, emptyAssignments), nil
		})
		if stepErr := (&service{client: client, spool: spool}).step(t.Context()); stepErr == nil {
			t.Fatal("step() accepted reconciliation failure")
		}
	})

	t.Run("step event delivery failure", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, openErr := OpenSpool(t.Context(), stateDirectory)
		if openErr != nil {
			t.Fatalf("OpenSpool() error = %v", openErr)
		}
		t.Cleanup(func() { _ = spool.Close() })
		seedExecution(t, spool)
		directory, prepareErr := prepareExecutionFiles(stateDirectory, assignment, authorization)
		if prepareErr != nil {
			t.Fatalf("prepareExecutionFiles() error = %v", prepareErr)
		}
		if manifestErr := writeManifest(filepath.Join(directory, startedFilename), StartManifest{
			ExecutionID: testExecutionID, NativeID: "42",
			Process: platform.ProcessIdentity{PID: 42}, StartedAt: time.Now().UTC(),
		}); manifestErr != nil {
			t.Fatalf("write start manifest: %v", manifestErr)
		}
		client := newClient(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
		})
		if stepErr := (&service{stateDirectory: stateDirectory, client: client, spool: spool}).step(t.Context()); stepErr == nil {
			t.Fatal("step() accepted event delivery failure")
		}
	})

	t.Run("step action poll failure", func(t *testing.T) {
		spool := newSpool(t)
		client := newClient(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path == "/v1/agent/actions" {
				return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
			}
			return responseWithBody(http.StatusOK, emptyAssignments), nil
		})
		if stepErr := (&service{client: client, spool: spool}).step(t.Context()); stepErr == nil {
			t.Fatal("step() accepted action poll failure")
		}
	})

	t.Run("step assignment poll failure", func(t *testing.T) {
		spool := newSpool(t)
		client := newClient(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path == "/v1/agent/actions" {
				return responseWithBody(http.StatusOK, emptyActions), nil
			}
			return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
		})
		if stepErr := (&service{client: client, spool: spool}).step(t.Context()); stepErr == nil {
			t.Fatal("step() accepted assignment poll failure")
		}
	})

	assignmentList := `{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[` + string(assignment.CanonicalJSON) + `]}`
	t.Run("step second reconciliation failure", func(t *testing.T) {
		stateDirectory := t.TempDir()
		spool, openErr := OpenSpool(t.Context(), stateDirectory)
		if openErr != nil {
			t.Fatalf("OpenSpool() error = %v", openErr)
		}
		t.Cleanup(func() { _ = spool.Close() })
		authorizationDocument, marshalErr := marshalStrictJSON(authorization)
		if marshalErr != nil {
			t.Fatalf("marshal authorization: %v", marshalErr)
		}
		client := newClient(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.URL.Path == "/v1/agent/actions":
				return responseWithBody(http.StatusOK, emptyActions), nil
			case strings.HasSuffix(request.URL.Path, "/accept"):
				return responseWithBody(http.StatusOK, string(authorizationDocument)), nil
			default:
				return responseWithBody(http.StatusOK, assignmentList), nil
			}
		})
		sut := &service{stateDirectory: stateDirectory, client: client, spool: spool}
		sut.startExecution = func(string, string) error { return errors.New("start failed") }
		if stepErr := sut.step(t.Context()); stepErr == nil || !strings.Contains(stepErr.Error(), "start failed") {
			t.Fatalf("step() error = %v", stepErr)
		}
	})
	t.Run("assignment identity mismatch", func(t *testing.T) {
		spool := newSpool(t)
		client := newClient(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusOK, assignmentList), nil
		})
		client.credentials.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		if pollErr := (&service{client: client, spool: spool}).pollAssignments(t.Context()); pollErr == nil {
			t.Fatal("pollAssignments() accepted mismatched identity")
		}
	})

	t.Run("assignment acceptance failure", func(t *testing.T) {
		spool := newSpool(t)
		client := newClient(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/accept") {
				return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
			}
			return responseWithBody(http.StatusOK, assignmentList), nil
		})
		if pollErr := (&service{client: client, spool: spool}).pollAssignments(t.Context()); pollErr == nil {
			t.Fatal("pollAssignments() accepted acceptance failure")
		}
	})

	t.Run("acceptance journal failure", func(t *testing.T) {
		spool := newSpool(t)
		authorizationDocument, marshalErr := marshalStrictJSON(authorization)
		if marshalErr != nil {
			t.Fatalf("marshal authorization: %v", marshalErr)
		}
		client := newClient(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/accept") {
				if closeErr := spool.Close(); closeErr != nil {
					t.Errorf("Close() error = %v", closeErr)
				}
				return responseWithBody(http.StatusOK, string(authorizationDocument)), nil
			}
			return responseWithBody(http.StatusOK, assignmentList), nil
		})
		if pollErr := (&service{client: client, spool: spool}).pollAssignments(t.Context()); pollErr == nil {
			t.Fatal("pollAssignments() accepted journal failure")
		}
	})

	t.Run("event delivery journal failure", func(t *testing.T) {
		spool := newSpool(t)
		seedExecution(t, spool)
		event := testCompletionEvent(t)
		if queueErr := spool.QueueEvent(t.Context(), event); queueErr != nil {
			t.Fatalf("QueueEvent() error = %v", queueErr)
		}
		client := newClient(func(*http.Request) (*http.Response, error) {
			if closeErr := spool.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
			value := event.Document.Metadata
			body := fmt.Sprintf(`{"apiVersion":"jobman.control/v1alpha1","kind":"ExecutionEventReceipt","eventId":%q,"sequence":%d}`,
				value.EventID, value.Sequence)
			return responseWithBody(http.StatusOK, body), nil
		})
		if flushErr := (&service{client: client, spool: spool}).flushEvents(t.Context()); flushErr == nil {
			t.Fatal("flushEvents() accepted journal failure")
		}
	})
}

func TestServiceRecentClaimTerminalAndObservationFailures(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	stateDirectory := t.TempDir()
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	executions, err := spool.ListExecutions(t.Context())
	if err != nil || len(executions) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", executions, err)
	}
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	claimPath := filepath.Join(directory, claimFilename)
	if err = writeNewPrivateFile(claimPath, []byte("recent")); err != nil {
		t.Fatalf("write claim: %v", err)
	}
	sut := &service{
		stateDirectory: stateDirectory,
		client:         &Client{credentials: credentialFiles{Metadata: metadata{AgentID: testAgentID}}},
		spool:          spool,
	}
	if err = sut.reconcileExecution(t.Context(), executions[0]); err != nil {
		t.Fatalf("reconcileExecution(recent claim) error = %v", err)
	}
	if err = removePrivateFile(claimPath); err != nil {
		t.Fatalf("remove claim: %v", err)
	}
	if err = spool.SetExecutionState(t.Context(), testExecutionID, "terminal"); err != nil {
		t.Fatalf("SetExecutionState(terminal) error = %v", err)
	}
	executions, err = spool.ListExecutions(t.Context())
	if err != nil {
		t.Fatalf("ListExecutions(terminal) error = %v", err)
	}
	if err = sut.reconcileExecution(t.Context(), executions[0]); err == nil {
		t.Fatal("reconcileExecution() accepted terminal execution without completion")
	}

	failingSpool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool(failing) error = %v", err)
	}
	if err = failingSpool.Close(); err != nil {
		t.Fatalf("Close(failing) error = %v", err)
	}
	started := StartManifest{
		ExecutionID: testExecutionID, NativeID: "42", Process: platform.ProcessIdentity{PID: 42},
		StartedAt: time.Now().UTC(),
	}
	if err = (&service{client: sut.client, spool: failingSpool}).observeStart(
		t.Context(), SpoolExecution{Assignment: assignment}, started,
	); err == nil {
		t.Fatal("observeStart() accepted closed journal")
	}
	invalidIdentityService := &service{client: &Client{}, spool: spool}
	if err = invalidIdentityService.observeStart(
		t.Context(), SpoolExecution{Assignment: assignment}, started,
	); err == nil {
		t.Fatal("observeStart() accepted empty agent identity")
	}
	if err = invalidIdentityService.observeCompletion(
		t.Context(), SpoolExecution{Assignment: assignment}, StartManifest{}, os.ErrNotExist,
		CompletionManifest{
			ExecutionID: testExecutionID, ObservedAt: time.Now().UTC(),
			Result: protocol.ProcessResult{Outcome: "lost"},
		},
	); err == nil {
		t.Fatal("observeCompletion() accepted empty agent identity")
	}
	if err = RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), PollInterval: 250 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler),
	}); err == nil {
		t.Fatal("RunService() accepted missing credentials")
	}
}

func testCompletionEvent(t *testing.T) protocol.SealedExecutionEvent {
	t.Helper()
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ExecutionID: testExecutionID,
			AgentID: testAgentID, Sequence: 1, ObservedAt: time.Now().UTC(),
		},
		Spec: protocol.ExecutionEventSpec{
			Type: "process.completed", Result: &protocol.ProcessResult{Outcome: "lost"},
		},
	})
	if err != nil {
		t.Fatalf("SealExecutionEvent() error = %v", err)
	}
	return event
}

func TestEnrollmentClientAndCLIFailureBoundaries(t *testing.T) {
	values := Enrollment{
		ServerURL: "https://control.example.test", TargetGenerationID: testTargetGenerationID,
		Hostname: "worker-a", ExecutionBackends: []string{"subprocess"}, Runtimes: []string{"native"},
	}
	if err := Enroll(nil, t.TempDir(), "token", values); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("Enroll() accepted nil context")
	}
	if err := Enroll(t.Context(), t.TempDir(), "bad token", values); err == nil {
		t.Fatal("Enroll() accepted invalid token")
	}
	if err := Enroll(t.Context(), "relative", "token", values); err == nil {
		t.Fatal("Enroll() accepted relative state directory")
	}
	alreadyEnrolled := t.TempDir()
	if err := os.WriteFile(filepath.Join(alreadyEnrolled, agentCredentialsFilename), []byte(`{}`), 0o600); err != nil {
		t.Fatalf("WriteFile(credentials) error = %v", err)
	}
	if err := Enroll(t.Context(), alreadyEnrolled, "token", values); err == nil {
		t.Fatal("Enroll() accepted existing enrollment")
	}
	invalidURLValues := values
	invalidURLValues.ServerURL = "http://control.example.test"
	if err := Enroll(t.Context(), t.TempDir(), "token", invalidURLValues); err == nil {
		t.Fatal("Enroll() accepted insecure server URL")
	}
	invalidCAValues := values
	invalidCAValues.ServerCAPEM = []byte("not a certificate")
	if err := Enroll(t.Context(), t.TempDir(), "token", invalidCAValues); err == nil {
		t.Fatal("Enroll() accepted invalid server CA")
	}

	pki := newTestPKI(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{}`)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	invalidSessionValues := values
	invalidSessionValues.ServerURL = server.URL
	invalidSessionValues.ServerCAPEM = pki.caPEM
	if err := Enroll(t.Context(), t.TempDir(), "token", invalidSessionValues); err == nil {
		t.Fatal("Enroll() accepted invalid server session")
	}

	invalidPendingDirectory := t.TempDir()
	if err := os.Mkdir(filepath.Join(invalidPendingDirectory, pendingEnrollmentFilename), 0o700); err != nil {
		t.Fatalf("Mkdir(pending enrollment) error = %v", err)
	}
	if _, err := loadOrCreatePendingEnrollment(invalidPendingDirectory, values); err == nil {
		t.Fatal("loadOrCreatePendingEnrollment() accepted directory document")
	}
	if err := saveCredentials(filepath.Join(t.TempDir(), "missing"), credentialFiles{}); err == nil {
		t.Fatal("saveCredentials() accepted missing directory")
	}

	badURLDirectory := t.TempDir()
	if err := saveCredentials(badURLDirectory, credentialFiles{
		Metadata: metadata{
			ServerURL: "http://control.example.test", AgentID: testAgentID,
			TargetGenerationID: testTargetGenerationID, CertificateExpiresAt: time.Now().Add(time.Hour),
		},
		PrivateKeyPEM: []byte("key"), CertificatePEM: []byte("certificate"),
	}); err != nil {
		t.Fatalf("save bad URL credentials: %v", err)
	}
	if _, err := OpenClient(badURLDirectory); err == nil {
		t.Fatal("OpenClient() accepted invalid server URL")
	}
	badKeyDirectory := t.TempDir()
	if err := saveCredentials(badKeyDirectory, credentialFiles{
		Metadata: metadata{
			ServerURL: "https://control.example.test", AgentID: testAgentID,
			TargetGenerationID: testTargetGenerationID, CertificateExpiresAt: time.Now().Add(time.Hour),
		},
		PrivateKeyPEM: []byte("key"), CertificatePEM: []byte("certificate"),
	}); err != nil {
		t.Fatalf("save bad key credentials: %v", err)
	}
	if _, err := OpenClient(badKeyDirectory); err == nil {
		t.Fatal("OpenClient() accepted invalid key pair")
	}

	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	failingClient := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
		})},
		credentials: credentialFiles{Metadata: metadata{AgentID: testAgentID}},
	}
	assignment, _ := testAssignment(t, "true", nil, nil)
	invalidAssignment := assignment
	invalidAssignment.Document.Metadata.DeliveryID = ""
	if _, err := failingClient.AcceptAssignment(t.Context(), invalidAssignment); err == nil {
		t.Fatal("AcceptAssignment() accepted invalid assignment")
	}
	event := testCompletionEvent(t)
	mismatchedEvent := event
	mismatchedEvent.Document.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := failingClient.RecordEvent(t.Context(), mismatchedEvent); err == nil {
		t.Fatal("RecordEvent() accepted mismatched agent")
	}
	if err := failingClient.RecordEvent(t.Context(), event); err == nil {
		t.Fatal("RecordEvent() accepted transport failure")
	}
	if err := failingClient.RenewCertificate(t.Context(), t.TempDir()); err == nil {
		t.Fatal("RenewCertificate() accepted transport failure")
	}

	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID: "99999999-9999-4999-8999-999999999999", ExecutionID: testExecutionID,
			AgentID: testAgentID, Revision: 1, RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	actionDocument, err := marshalStrictJSON(action)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}
	actionClient := staticResponseClient(t, baseURL,
		`{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[`+string(actionDocument)+`]}`)
	actionClient.credentials.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err := actionClient.ListActions(t.Context(), 1); err == nil {
		t.Fatal("ListActions() accepted mismatched action agent")
	}

	_, certificateRequest, err := generateKeyAndCSR(testAgentID)
	if err != nil {
		t.Fatalf("generateKeyAndCSR() error = %v", err)
	}
	certificate := pki.issueCSR(t, certificateRequest)
	certificateDocument, err := marshalStrictJSON(certificate)
	if err != nil {
		t.Fatalf("marshal certificate: %v", err)
	}
	badRenewClient := staticResponseClient(t, baseURL, string(certificateDocument))
	badRenewClient.credentials = failingClient.credentials
	if err := badRenewClient.RenewCertificate(t.Context(), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("RenewCertificate() accepted credential save failure")
	}
	invalidCertificate := certificate
	invalidCertificate.CertificatePEM = "not a certificate"
	invalidCertificateDocument, err := marshalStrictJSON(invalidCertificate)
	if err != nil {
		t.Fatalf("marshal invalid certificate: %v", err)
	}
	activateClient := staticResponseClient(t, baseURL, string(invalidCertificateDocument))
	activateClient.credentials = failingClient.credentials
	if err := activateClient.RenewCertificate(t.Context(), t.TempDir()); err == nil {
		t.Fatal("RenewCertificate() accepted invalid rotated certificate")
	}

	key, certificateRequest, err := generateKeyAndCSR(testAgentID)
	if err != nil {
		t.Fatalf("generateKeyAndCSR(client) error = %v", err)
	}
	issued := pki.issueCSR(t, certificateRequest)
	httpClient, err := newHTTPClientForCertificate(pki.caPEM, []byte(issued.CertificatePEM), key)
	if err != nil {
		t.Fatalf("newHTTPClientForCertificate() error = %v", err)
	}
	httpClient.CloseIdleConnections()

	if err := performRequest(t.Context(), http.DefaultClient, baseURL, "bad\nmethod", "/", "", nil, nil); err == nil {
		t.Fatal("performRequest() accepted invalid method")
	}
	readFailureClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: failingReadCloser{}}, nil
	})}
	if err := performRequest(t.Context(), readFailureClient, baseURL, http.MethodGet, "/", "", nil, &map[string]any{}); err == nil {
		t.Fatal("performRequest() accepted response read failure")
	}

	var stdout, stderr bytes.Buffer
	if err := executeEnroll(t.Context(), []string{"--unknown"}, strings.NewReader("token"), &stdout, &stderr); err == nil {
		t.Fatal("executeEnroll() accepted unknown flag")
	}
	if err := executeEnroll(t.Context(), []string{
		"--state-dir", t.TempDir(), "--server", "https://control.example.test",
		"--target-generation", testTargetGenerationID,
	}, strings.NewReader("bad token"), &stdout, &stderr); err == nil {
		t.Fatal("executeEnroll() accepted invalid token")
	}
	if err := executeEnroll(t.Context(), []string{
		"--state-dir", t.TempDir(), "--server", "https://control.example.test",
		"--target-generation", testTargetGenerationID, "--server-ca", filepath.Join(t.TempDir(), "missing"),
	}, strings.NewReader("token"), &stdout, &stderr); err == nil {
		t.Fatal("executeEnroll() accepted missing server CA")
	}
	if err := executeRunExecution(t.Context(), []string{"--unknown"}, &stderr); err == nil {
		t.Fatal("executeRunExecution() accepted unknown flag")
	}
	if _, err := readEnrollmentToken(failingReader{}, "-"); err == nil {
		t.Fatal("readEnrollmentToken() accepted read failure")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (failingReadCloser) Close() error             { return nil }

func TestSpoolCorruptionAndReplayBoundaries(t *testing.T) {
	assignment, authorization := testAssignment(t, "true", nil, nil)
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	changedAssignment := assignment
	changedAssignment.CanonicalJSON = append(append([]byte(nil), assignment.CanonicalJSON...), ' ')
	if err = spool.PutAssignment(t.Context(), changedAssignment); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("PutAssignment(changed replay) error = %v", err)
	}
	missingAuthorization := authorization
	missingAuthorization.Metadata.ExecutionID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err = spool.RecordAcceptance(t.Context(), missingAuthorization.Metadata.ExecutionID, missingAuthorization); err == nil {
		t.Fatal("RecordAcceptance() accepted missing execution")
	}
	if err = spool.SetExecutionState(t.Context(), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "running"); err == nil {
		t.Fatal("SetExecutionState() accepted missing execution")
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	event := testCompletionEvent(t)
	if err = spool.QueueEvent(t.Context(), event); err != nil {
		t.Fatalf("QueueEvent() error = %v", err)
	}
	changedEvent := event
	changedEvent.CanonicalJSON = append(append([]byte(nil), event.CanonicalJSON...), ' ')
	if err = spool.QueueEvent(t.Context(), changedEvent); !errors.Is(err, errSpoolConflict) {
		t.Fatalf("QueueEvent(changed replay) error = %v", err)
	}
	if _, err = spool.database.ExecContext(t.Context(),
		"UPDATE pending_events SET event_document = '{}' WHERE event_id = ?", event.Document.Metadata.EventID,
	); err != nil {
		t.Fatalf("corrupt pending event: %v", err)
	}
	if _, err = spool.PendingEvents(t.Context()); err == nil {
		t.Fatal("PendingEvents() accepted corrupt event")
	}
	missingAction := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID:    "99999999-9999-4999-8999-999999999999",
			ExecutionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", AgentID: testAgentID,
			Revision: 1, RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	if err = spool.PutAction(t.Context(), missingAction); err == nil {
		t.Fatal("PutAction() accepted missing execution")
	}
	if _, err = spool.database.ExecContext(t.Context(),
		"UPDATE executions SET authorization_document = '{}' WHERE execution_id = ?", testExecutionID,
	); err != nil {
		t.Fatalf("corrupt authorization: %v", err)
	}
	if _, err = spool.ListExecutions(t.Context()); err == nil {
		t.Fatal("ListExecutions() accepted corrupt authorization")
	}
}

func TestEnrollmentDurabilityFailureBoundaries(t *testing.T) {
	values := Enrollment{
		TargetGenerationID: testTargetGenerationID, Hostname: "worker-a",
		ExecutionBackends: []string{"subprocess"}, Runtimes: []string{"native"},
	}
	pendingDirectoryState := t.TempDir()
	if err := os.Mkdir(filepath.Join(pendingDirectoryState, pendingEnrollmentFilename), 0o700); err != nil {
		t.Fatalf("Mkdir(pending) error = %v", err)
	}
	values.ServerURL = "https://control.example.test"
	if err := Enroll(t.Context(), pendingDirectoryState, "token", values); err == nil {
		t.Fatal("Enroll() accepted unreadable pending enrollment")
	}
	malformedState := t.TempDir()
	if err := os.WriteFile(filepath.Join(malformedState, pendingEnrollmentFilename), []byte(`{`), 0o600); err != nil {
		t.Fatalf("WriteFile(pending) error = %v", err)
	}
	if _, err := loadOrCreatePendingEnrollment(malformedState, values); err == nil {
		t.Fatal("loadOrCreatePendingEnrollment() accepted malformed document")
	}
	if _, err := loadOrCreatePendingEnrollment(filepath.Join(t.TempDir(), "missing"), values); err == nil {
		t.Fatal("loadOrCreatePendingEnrollment() accepted missing state directory")
	}

	pki := newTestPKI(t)
	startServer := func(t *testing.T, handler http.Handler) *httptest.Server {
		t.Helper()
		server := httptest.NewUnstartedServer(handler)
		server.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate},
		}
		server.StartTLS()
		t.Cleanup(server.Close)
		return server
	}
	serverFailure := startServer(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	values.ServerURL = serverFailure.URL
	values.ServerCAPEM = pki.caPEM
	if err := Enroll(t.Context(), t.TempDir(), "token", values); err == nil {
		t.Fatal("Enroll() accepted control server failure")
	}

	validSession := func() sessionResponse {
		var response sessionResponse
		response.APIVersion = controlAPIVersion
		response.Kind = "AgentSession"
		response.Metadata.AgentID = testAgentID
		response.Metadata.SessionID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		response.Metadata.ExpiresAt = time.Now().UTC().Add(time.Hour)
		response.Spec.Token = "session-token"
		response.Spec.AuthScheme = "Jobman-Agent"
		response.Spec.Certificate.CertificatePEM = "certificate"
		response.Spec.Certificate.ExpiresAt = time.Now().UTC().Add(time.Hour)
		return response
	}
	t.Run("credential save", func(t *testing.T) {
		stateDirectory := t.TempDir()
		server := startServer(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			pendingPath := filepath.Join(stateDirectory, pendingEnrollmentFilename)
			if err := os.Remove(pendingPath); err != nil {
				t.Errorf("remove pending enrollment: %v", err)
			}
			if err := os.Remove(stateDirectory); err != nil {
				t.Errorf("remove state directory: %v", err)
			}
			if err := os.WriteFile(stateDirectory, []byte("file"), 0o600); err != nil {
				t.Errorf("replace state directory: %v", err)
			}
			writer.Header().Set("Content-Type", "application/json")
			writeTestJSON(t, writer, validSession())
		}))
		request := values
		request.ServerURL = server.URL
		if err := Enroll(t.Context(), stateDirectory, "token", request); err == nil {
			t.Fatal("Enroll() accepted credential save failure")
		}
	})
	t.Run("pending removal", func(t *testing.T) {
		stateDirectory := t.TempDir()
		server := startServer(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			pendingPath := filepath.Join(stateDirectory, pendingEnrollmentFilename)
			if err := os.Remove(pendingPath); err != nil {
				t.Errorf("remove pending enrollment: %v", err)
			}
			if err := os.Mkdir(pendingPath, 0o700); err != nil {
				t.Errorf("replace pending enrollment: %v", err)
			}
			if err := os.WriteFile(filepath.Join(pendingPath, "child"), []byte("child"), 0o600); err != nil {
				t.Errorf("write pending child: %v", err)
			}
			writer.Header().Set("Content-Type", "application/json")
			writeTestJSON(t, writer, validSession())
		}))
		request := values
		request.ServerURL = server.URL
		if err := Enroll(t.Context(), stateDirectory, "token", request); err == nil {
			t.Fatal("Enroll() accepted pending removal failure")
		}
	})
}

func TestWaitAndServiceFinalFailureBoundaries(t *testing.T) {
	startBlocking := func(t *testing.T) (*exec.Cmd, platform.ProcessIdentity) {
		t.Helper()
		command := exec.Command(os.Args[0], "-test.run=^TestAgentRunnerHelper$")
		command.Env = append(os.Environ(), "JOBMAN_AGENT_HELPER=block")
		platform.ConfigureTarget(command)
		if err := command.Start(); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		identity, err := finalizeProcessIdentity(command)
		if err != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("finalizeProcessIdentity() error = %v", err)
		}
		return command, identity
	}
	command, _ := startBlocking(t)
	if err := stopUnidentifiedProcess(command); err != nil {
		t.Fatalf("stopUnidentifiedProcess() error = %v", err)
	}
	command, identity := startBlocking(t)
	if err := stopIdentifiedProcess(command, identity); err != nil {
		t.Fatalf("stopIdentifiedProcess() error = %v", err)
	}
	startCompleted := func(t *testing.T) (*exec.Cmd, platform.ProcessIdentity) {
		t.Helper()
		childCommand := exec.Command(os.Args[0], "-test.run=^TestAgentRunnerHelper$")
		childCommand.Env = append(os.Environ(), "JOBMAN_AGENT_HELPER=1")
		platform.ConfigureTarget(childCommand)
		if err := childCommand.Start(); err != nil {
			t.Fatalf("Start(completed) error = %v", err)
		}
		childIdentity, err := finalizeProcessIdentity(childCommand)
		if err != nil {
			killErr := childCommand.Process.Kill()
			waitErr := childCommand.Wait()
			t.Fatalf("finalizeProcessIdentity(completed) error = %v; cleanup = %v", err, errors.Join(killErr, waitErr))
		}
		if err = childCommand.Wait(); err != nil {
			t.Fatalf("Wait(completed) error = %v", err)
		}
		return childCommand, childIdentity
	}
	command, _ = startCompleted(t)
	if err := stopUnidentifiedProcess(command); err == nil {
		t.Fatal("stopUnidentifiedProcess() accepted an already reaped process")
	}
	command, identity = startCompleted(t)
	if err := stopIdentifiedProcess(command, identity); err == nil {
		t.Fatal("stopIdentifiedProcess() accepted an already reaped process")
	}
	command, identity = startBlocking(t)
	result := waitForProcess(t.Context(), command, identity, filepath.Join(t.TempDir(), "cancel"), "invalid")
	if result.Outcome != "failure" || result.FailureCode != "invalid_timeout" {
		t.Fatalf("invalid timeout result = %#v", result)
	}
	command, identity = startBlocking(t)
	cancelParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(cancelParent, []byte("file"), 0o600); err != nil {
		t.Fatalf("WriteFile(cancel parent) error = %v", err)
	}
	result = waitForProcess(t.Context(), command, identity, filepath.Join(cancelParent, "child"), "")
	if result.Outcome != "failure" || result.FailureCode != "cancel_check_failed" {
		t.Fatalf("cancel check result = %#v", result)
	}

	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	failingClient := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return responseWithBody(http.StatusServiceUnavailable, `{}`), nil
		})},
		credentials: credentialFiles{Metadata: metadata{
			AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
			CertificateExpiresAt: time.Now().Add(time.Minute),
		}},
	}
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = (&service{client: failingClient, spool: spool}).step(t.Context()); err == nil {
		t.Fatal("step() accepted certificate renewal failure")
	}

	assignment, authorization := testAssignment(t, "true", nil, nil)
	stateDirectory := t.TempDir()
	actionSpool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool(action) error = %v", err)
	}
	t.Cleanup(func() { _ = actionSpool.Close() })
	if err = actionSpool.PutAssignment(t.Context(), assignment); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = actionSpool.RecordAcceptance(t.Context(), testExecutionID, authorization); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	if _, err = prepareExecutionFiles(stateDirectory, assignment, authorization); err != nil {
		t.Fatalf("prepareExecutionFiles() error = %v", err)
	}
	action := protocol.DesiredAction{
		APIVersion: protocol.V1Alpha1, Kind: protocol.DesiredActionKind,
		Metadata: protocol.DesiredActionMetadata{
			ActionID: "99999999-9999-4999-8999-999999999999", ExecutionID: testExecutionID,
			AgentID: testAgentID, Revision: 1, RequestedAt: time.Now().UTC(),
		},
		Spec: protocol.DesiredActionSpec{Type: "cancel"},
	}
	actionDocument, err := marshalStrictJSON(action)
	if err != nil {
		t.Fatalf("marshal action: %v", err)
	}
	actionList := `{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[` + string(actionDocument) + `]}`
	actionClient := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/acknowledge") {
				if closeErr := actionSpool.Close(); closeErr != nil {
					t.Errorf("Close() error = %v", closeErr)
				}
				return responseWithBody(http.StatusNoContent, ""), nil
			}
			return responseWithBody(http.StatusOK, actionList), nil
		})},
		credentials: credentialFiles{Metadata: metadata{
			AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
		}},
	}
	if err = (&service{stateDirectory: stateDirectory, client: actionClient, spool: actionSpool}).pollActions(t.Context()); err == nil {
		t.Fatal("pollActions() accepted acknowledgement journal failure")
	}

	closedSpool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("OpenSpool(closed) error = %v", err)
	}
	if err = closedSpool.Close(); err != nil {
		t.Fatalf("Close(closed) error = %v", err)
	}
	completion := CompletionManifest{
		ExecutionID: testExecutionID, ObservedAt: time.Now().UTC(),
		Result: protocol.ProcessResult{Outcome: "lost"},
	}
	if err = (&service{client: actionClient, spool: closedSpool}).observeCompletion(
		t.Context(), SpoolExecution{Assignment: assignment}, StartManifest{}, os.ErrNotExist, completion,
	); err == nil {
		t.Fatal("observeCompletion() accepted closed journal")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func responseWithBody(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(body)),
	}
}
