package agent

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

func TestClientEnrollmentAndExecutionProtocol(t *testing.T) {
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
	var authenticatedRequests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/v1/agent/enroll" {
			if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
				http.Error(writer, "client certificate required", http.StatusUnauthorized)
				return
			}
			authenticatedRequests.Add(1)
		}
		switch request.URL.Path {
		case "/v1/agent/enroll":
			if request.Header.Get("Authorization") != "Jobman-Enrollment enrollment-token" {
				http.Error(writer, "bad token", http.StatusUnauthorized)
				return
			}
			var enrollment enrollmentRequest
			decodeTestRequest(t, request, &enrollment)
			certificate := pki.issueCSR(t, enrollment.Spec.CertificateSigningRequest)
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "AgentSession",
				"metadata": map[string]any{
					"agentId": testAgentID, "sessionId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
					"expiresAt": time.Now().UTC().Add(time.Hour),
				},
				"spec": map[string]any{
					"token": "legacy-session", "authScheme": "Jobman-Agent", "inertOnly": false,
					"certificate": certificate,
				},
			})
		case "/v1/agent/assignments":
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "AgentAssignmentList",
				"requiresAcceptance": true, "items": []json.RawMessage{assignment.CanonicalJSON},
			})
		case "/v1/agent/assignments/66666666-6666-4666-8666-666666666666/accept":
			var acceptance protocol.AgentAcceptance
			decodeTestRequest(t, request, &acceptance)
			if acceptance.Metadata.ExecutionID != testExecutionID {
				t.Errorf("acceptance execution ID = %q", acceptance.Metadata.ExecutionID)
			}
			writeTestJSON(t, writer, authorization)
		case "/v1/agent/executions/33333333-3333-4333-8333-333333333333/events":
			var event protocol.ExecutionEvent
			decodeTestRequest(t, request, &event)
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "ExecutionEventReceipt",
				"eventId": event.Metadata.EventID, "sequence": event.Metadata.Sequence,
			})
		case "/v1/agent/executions/33333333-3333-4333-8333-333333333333/logs/stdout/chunks/1":
			var chunk agentLogChunk
			decodeTestRequest(t, request, &chunk)
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "LogChunkReceipt",
				"executionId": chunk.Metadata.ExecutionID, "stream": chunk.Metadata.Stream,
				"sequence": chunk.Metadata.Sequence,
			})
		case "/v1/agent/actions":
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": controlAPIVersion, "kind": "DesiredActionList", "items": []any{action},
			})
		case "/v1/agent/actions/99999999-9999-4999-8999-999999999999/acknowledge":
			var acknowledgement protocol.ActionAcknowledgement
			decodeTestRequest(t, request, &acknowledgement)
			writer.WriteHeader(http.StatusNoContent)
		case "/v1/agent/certificate/renew":
			var renewal struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Spec       struct {
					CertificateSigningRequest string `json:"certificateSigningRequest"`
				} `json:"spec"`
			}
			decodeTestRequest(t, request, &renewal)
			writeTestJSON(t, writer, pki.issueCSR(t, renewal.Spec.CertificateSigningRequest))
		default:
			http.NotFound(writer, request)
		}
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate},
		ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pki.roots,
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	stateDirectory := t.TempDir()
	serverCAFile := filepath.Join(t.TempDir(), "server-ca.pem")
	if err := os.WriteFile(serverCAFile, pki.caPEM, 0o600); err != nil {
		t.Fatalf("WriteFile(server CA) error = %v", err)
	}
	var enrollmentOutput bytes.Buffer
	if err := ExecuteCLI(t.Context(), []string{
		"enroll", "--state-dir", stateDirectory, "--server", server.URL,
		"--target-generation", testTargetGenerationID, "--server-ca", serverCAFile,
	}, strings.NewReader("enrollment-token\n"), &enrollmentOutput, &enrollmentOutput); err != nil {
		t.Fatalf("ExecuteCLI(enroll) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDirectory, pendingEnrollmentFilename)); !os.IsNotExist(err) {
		t.Fatalf("pending enrollment still exists: %v", err)
	}
	client, err := OpenClient(stateDirectory)
	if err != nil {
		t.Fatalf("OpenClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	if client.AgentID() != testAgentID || client.TargetGenerationID() != testTargetGenerationID ||
		client.CertificateExpiresAt().IsZero() {
		t.Fatalf("client identities are incomplete: %#v", client.credentials.Metadata)
	}
	assignments, err := client.ListAssignments(t.Context(), 10)
	if err != nil || len(assignments) != 1 {
		t.Fatalf("ListAssignments() = %#v, %v", assignments, err)
	}
	accepted, err := client.AcceptAssignment(t.Context(), assignments[0])
	if err != nil || accepted.Metadata.AuthorizationID != authorization.Metadata.AuthorizationID {
		t.Fatalf("AcceptAssignment() = %#v, %v", accepted, err)
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
	if err = client.RecordEvent(t.Context(), event); err != nil {
		t.Fatalf("RecordEvent() error = %v", err)
	}
	spool, err := OpenSpool(t.Context(), stateDirectory)
	if err != nil {
		t.Fatalf("OpenSpool() error = %v", err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	if err = spool.PutAssignment(t.Context(), assignments[0]); err != nil {
		t.Fatalf("PutAssignment() error = %v", err)
	}
	if err = spool.RecordAcceptance(t.Context(), testExecutionID, accepted); err != nil {
		t.Fatalf("RecordAcceptance() error = %v", err)
	}
	if err = spool.queueLogChunk(t.Context(), agentLogChunk{
		APIVersion: controlAPIVersion, Kind: "LogChunk",
		Metadata: logChunkMetadata{ExecutionID: testExecutionID, Stream: logStreamStdout, Sequence: 1},
		Spec: logChunkSpec{
			StoreName: "department-nfs", StoreVersion: 1, ObjectKey: "safe/key",
			ByteLength: 1, Checksum: "sha256:" + strings.Repeat("a", 64), CapturedAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("queueLogChunk() error = %v", err)
	}
	if err = (&service{client: client, spool: spool}).flushLogChunks(t.Context()); err != nil {
		t.Fatalf("flushLogChunks() error = %v", err)
	}
	if pending, pendingErr := spool.pendingLogChunks(t.Context()); pendingErr != nil || len(pending) != 0 {
		t.Fatalf("pendingLogChunks() = %#v, %v", pending, pendingErr)
	}
	actions, err := client.ListActions(t.Context(), 10)
	if err != nil || len(actions) != 1 {
		t.Fatalf("ListActions() = %#v, %v", actions, err)
	}
	if err = client.AcknowledgeAction(t.Context(), actions[0]); err != nil {
		t.Fatalf("AcknowledgeAction() error = %v", err)
	}
	priorExpiration := client.CertificateExpiresAt()
	if err = client.RenewCertificate(t.Context(), stateDirectory); err != nil {
		t.Fatalf("RenewCertificate() error = %v", err)
	}
	if !client.CertificateExpiresAt().After(priorExpiration.Add(-time.Second)) {
		t.Fatalf("renewed expiration = %v, prior = %v", client.CertificateExpiresAt(), priorExpiration)
	}
	if authenticatedRequests.Load() != 7 {
		t.Fatalf("authenticated requests = %d, want 7", authenticatedRequests.Load())
	}
}

func TestRenewSessionPersistsRotatedCredential(t *testing.T) {
	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	credentials, err := loadCredentials(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	baseURL, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().UTC().Add(2 * time.Hour)
	client := &Client{
		baseURL: baseURL, credentials: credentials,
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("Authorization") != "Jobman-Agent session" {
				t.Errorf("authorization = %q", request.Header.Get("Authorization"))
			}
			body, marshalErr := json.Marshal(map[string]any{
				"apiVersion": controlAPIVersion, "kind": "AgentSession",
				"metadata": map[string]any{
					"agentId": testAgentID, "sessionId": "99999999-9999-4999-8999-999999999999",
					"expiresAt": expires,
				},
				"spec": map[string]any{
					"token": "rotated", "authScheme": "Jobman-Agent", "inertOnly": true,
				},
			})
			if marshalErr != nil {
				return nil, marshalErr
			}
			return responseWithBody(http.StatusOK, string(body)), nil
		})},
	}
	if err = client.RenewSession(t.Context(), stateDirectory); err != nil {
		t.Fatalf("RenewSession() error = %v", err)
	}
	stored, err := loadCredentials(stateDirectory)
	if err != nil || stored.Metadata.SessionToken != "rotated" ||
		stored.Metadata.SessionID != "99999999-9999-4999-8999-999999999999" {
		t.Fatalf("rotated credentials = %#v, %v", stored.Metadata, err)
	}

	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return responseWithBody(http.StatusOK, `{}`), nil
	})}
	if err = client.RenewSession(t.Context(), stateDirectory); err == nil {
		t.Fatal("RenewSession() accepted invalid response")
	}
}

func TestClientRejectsInvalidLimitsAndIdentities(t *testing.T) {
	client := &Client{credentials: credentialFiles{Metadata: metadata{
		AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
	}}}
	if _, err := client.ListAssignments(t.Context(), 0); err == nil {
		t.Fatal("ListAssignments() accepted zero limit")
	}
	if _, err := client.ListActions(t.Context(), 101); err == nil {
		t.Fatal("ListActions() accepted excessive limit")
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ExecutionID: testExecutionID,
			AgentID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Sequence: 1,
			ObservedAt: time.Now().UTC(),
		},
		Spec: protocol.ExecutionEventSpec{
			Type: "process.completed", Result: &protocol.ProcessResult{Outcome: "lost"},
		},
	})
	if err != nil {
		t.Fatalf("SealExecutionEvent() error = %v", err)
	}
	if err = client.RecordEvent(t.Context(), event); err == nil {
		t.Fatal("RecordEvent() accepted a different agent")
	}
}

type testPKI struct {
	caCertificate     *x509.Certificate
	caKey             *ecdsa.PrivateKey
	caPEM             []byte
	roots             *x509.CertPool
	serverCertificate tls.Certificate
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(CA) error = %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "agent test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(CA) error = %v", err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("ParseCertificate(CA) error = %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	roots := x509.NewCertPool()
	roots.AddCert(caCertificate)
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(server) error = %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCertificate, serverKey.Public(), caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(server) error = %v", err)
	}
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey() error = %v", err)
	}
	serverCertificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
	)
	if err != nil {
		t.Fatalf("X509KeyPair(server) error = %v", err)
	}

	return testPKI{
		caCertificate: caCertificate, caKey: caKey, caPEM: caPEM,
		roots: roots, serverCertificate: serverCertificate,
	}
}

func (pki testPKI) issueCSR(t *testing.T, requestPEM string) certificateResponse {
	t.Helper()
	block, _ := pem.Decode([]byte(requestPEM))
	if block == nil {
		t.Fatal("certificate request did not contain PEM")
	}
	request, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || request.CheckSignature() != nil {
		t.Fatalf("ParseCertificateRequest() error = %v", err)
	}
	now := time.Now().UTC()
	identityURI, err := url.Parse("urn:jobman:agent:" + testAgentID)
	if err != nil {
		t.Fatalf("Parse(agent URI) error = %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: request.Subject,
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{identityURI},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, pki.caCertificate, request.PublicKey, pki.caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(client) error = %v", err)
	}

	return certificateResponse{
		CertificatePEM:   string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})),
		CACertificatePEM: string(pki.caPEM), Serial: template.SerialNumber.Text(16),
		ExpiresAt: template.NotAfter,
	}
}

func decodeTestRequest(t *testing.T, request *http.Request, target any) {
	t.Helper()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Errorf("decode request: %v", err)
	}
}

func writeTestJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}
