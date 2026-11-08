package agent

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func saveExpiredRecoveryCredentials(t *testing.T, pki testPKI, directory, endpoint string) credentialFiles {
	t.Helper()
	saveTestCredentials(t, pki, directory, endpoint)
	credentials, err := loadCredentials(directory)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := credentialCertificate(credentials)
	if err != nil {
		t.Fatal(err)
	}
	certificate.NotBefore = time.Now().Add(-2 * time.Hour)
	certificate.NotAfter = time.Now().Add(-time.Hour).Truncate(time.Second)
	encoded, err := x509.CreateCertificate(rand.Reader, certificate, pki.caCertificate, certificate.PublicKey, pki.caKey)
	if err != nil {
		t.Fatal(err)
	}
	credentials.CertificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded})
	credentials.Metadata.CertificateExpiresAt = certificate.NotAfter
	credentials.Metadata.SessionExpiresAt = certificate.NotAfter
	if err = saveCredentials(directory, credentials); err != nil {
		t.Fatal(err)
	}
	return credentials
}

func recoveryResponse(t *testing.T, pki testPKI, request recoveryRequest) sessionResponse {
	t.Helper()
	var response sessionResponse
	response.APIVersion, response.Kind = controlAPIVersion, "AgentSession"
	response.Metadata.AgentID = request.Metadata.AgentID
	response.Metadata.SessionID = "99999999-9999-4999-8999-999999999999"
	response.Metadata.ExpiresAt = time.Now().Add(15 * time.Minute)
	response.Spec.Token, response.Spec.AuthScheme = "recovered-session", "Jobman-Agent"
	response.Spec.Certificate = pki.issueCSR(t, request.Spec.CertificateSigningRequest)
	return response
}

func TestRecoveryPreservesJournalAndReplaysLostResponse(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	var mutex sync.Mutex
	var original []byte
	var response sessionResponse
	requests := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		requests++
		if request.URL.Path != "/v1/agent/recover" || request.Header.Get("Authorization") != "Jobman-Enrollment grant" || len(request.TLS.PeerCertificates) != 0 {
			t.Error("recovery did not use the explicit enrollment grant over server-authenticated TLS")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var document recoveryRequest
		if err = json.Unmarshal(body, &document); err != nil {
			t.Error(err)
			return
		}
		if document.Metadata.AgentID != testAgentID || document.Spec.TargetGenerationID != testTargetGenerationID || document.Spec.ExecutionUser != "alice" {
			t.Error("recovery identity changed")
		}
		if requests == 1 {
			original = append([]byte(nil), body...)
			response = recoveryResponse(t, pki, document)
			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				t.Error("test transport cannot drop response")
				return
			}
			connection, _, hijackErr := hijacker.Hijack()
			if hijackErr != nil {
				t.Error(hijackErr)
				return
			}
			_ = connection.Close()
			return
		}
		if !bytes.Equal(original, body) {
			t.Error("retry changed durable recovery request")
		}
		writeTestJSON(t, writer, response)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	old := saveExpiredRecoveryCredentials(t, pki, directory, server.URL)
	journal := filepath.Join(directory, agentDatabaseFilename)
	journalBytes := []byte("synthetic execution journal must remain unchanged")
	if err := os.WriteFile(journal, journalBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Recover(t.Context(), directory, "grant", "alice"); err == nil {
		t.Fatal("lost response unexpectedly succeeded")
	}
	stored, err := loadCredentials(directory)
	if err != nil || !bytes.Equal(stored.PrivateKeyPEM, old.PrivateKeyPEM) {
		t.Fatal("old credentials changed before successful recovery")
	}
	if err = Recover(t.Context(), directory, "different-grant", "alice"); err == nil {
		t.Fatal("pending recovery accepted different token")
	}
	if err = Recover(t.Context(), directory, "grant", "alice"); err != nil {
		t.Fatal(err)
	}
	if err = Recover(t.Context(), directory, "grant", "alice"); err != nil {
		t.Fatalf("completed local replay: %v", err)
	}
	mutex.Lock()
	gotRequests := requests
	mutex.Unlock()
	if gotRequests != 2 {
		t.Fatalf("requests = %d, want2", gotRequests)
	}
	stored, err = loadCredentials(directory)
	if err != nil || stored.Metadata.AgentID != testAgentID || stored.Metadata.TargetGenerationID != testTargetGenerationID || stored.Metadata.SessionToken != "recovered-session" || bytes.Equal(stored.PrivateKeyPEM, old.PrivateKeyPEM) {
		t.Fatal("recovery did not preserve identity and rotate credentials")
	}
	got, err := os.ReadFile(journal)
	if err != nil || !bytes.Equal(got, journalBytes) {
		t.Fatal("recovery changed execution journal")
	}
	receipt, err := os.ReadFile(filepath.Join(directory, recoveryFilename))
	if err != nil || bytes.Contains(receipt, []byte(`"grant"`)) {
		t.Fatal("receipt contains raw grant")
	}
}

func TestRecoveryRejectsInvalidResponseWithoutReplacingCredentials(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var document recoveryRequest
		decodeTestRequest(t, request, &document)
		response := recoveryResponse(t, pki, document)
		response.Metadata.AgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		writeTestJSON(t, writer, response)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	saveExpiredRecoveryCredentials(t, pki, directory, server.URL)
	before, err := os.ReadFile(filepath.Join(directory, agentCredentialsFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err = Recover(t.Context(), directory, "grant", "alice"); err == nil {
		t.Fatal("accepted different agent identity")
	}
	after, err := os.ReadFile(filepath.Join(directory, agentCredentialsFilename))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("invalid response replaced credentials")
	}
}

func TestRecoveryCompletionAfterCredentialWriteIsDurable(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	old := saveExpiredRecoveryCredentials(t, pki, directory, "https://control.example.test")
	pending, _, err := prepareRecovery(directory, old, "grant", "alice")
	if err != nil {
		t.Fatal(err)
	}
	updated, err := recoveredCredentials(old, pending, recoveryResponse(t, pki, pending.Request))
	if err != nil {
		t.Fatal(err)
	}
	if err = saveCredentials(directory, updated); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after credentials were persisted but before completion.
	if err = checkRecoveryBeforeService(directory, updated); err != nil {
		t.Fatal(err)
	}
	if err = Recover(t.Context(), directory, "grant", "alice"); err != nil {
		t.Fatal(err)
	}
	receipt, err := readRecovery(directory)
	if err != nil || receipt.CompletedCertificateSerial == "" {
		t.Fatal("completion was not recorded")
	}
}

func TestRecoveryRequiresStoppedServiceAndRejectsInvalidInputs(t *testing.T) {
	directory := t.TempDir()
	lock, err := lockState(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err = Recover(t.Context(), directory, "grant", "alice"); err == nil || !strings.Contains(err.Error(), "state is in use") {
		t.Fatalf("active service recovery error=%v", err)
	}
	if err = RunService(t.Context(), ServiceOptions{StateDirectory: directory, Logger: slog.New(slog.DiscardHandler)}); err == nil || !strings.Contains(err.Error(), "state is in use") {
		t.Fatalf("second service writer error=%v", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	//nolint:staticcheck // The public API must reject an explicitly nil context.
	if err = Recover(nil, directory, "grant", "alice"); err == nil {
		t.Fatal("nil context accepted")
	}
	if err = Recover(t.Context(), directory, "bad token", "alice"); err == nil {
		t.Fatal("bad token accepted")
	}
	var output bytes.Buffer
	if err = ExecuteCLI(t.Context(), []string{"recover"}, strings.NewReader("grant"), &output, &output); err == nil {
		t.Fatal("missing state accepted")
	}
}

func TestStateLockRejectsUnsafePathsAndAllowsReuse(t *testing.T) {
	directory := t.TempDir()
	first, err := lockState(directory)
	if err != nil {
		t.Fatal(err)
	}
	if second, secondErr := lockState(directory); secondErr == nil {
		_ = second.Close()
		t.Fatal("second writer acquired lock")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := lockState(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(directory, "agent.lock")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(directory, "agent.lock"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = lockState(directory); err == nil {
		t.Fatal("directory accepted as lock")
	}
	if err = checkRecoveryBeforeService(directory, credentialFiles{}); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestRecoveredCertificateValidation(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	old := saveExpiredRecoveryCredentials(t, pki, directory, "https://control.example.test")
	pending, _, err := prepareRecovery(directory, old, "grant", "alice")
	if err != nil {
		t.Fatal(err)
	}
	valid := recoveryResponse(t, pki, pending.Request)
	for _, test := range []struct {
		name   string
		change func(*sessionResponse)
	}{
		{"wrong CA", func(r *sessionResponse) { r.Spec.Certificate.CACertificatePEM = "invalid" }},
		{"wrong serial", func(r *sessionResponse) { r.Spec.Certificate.Serial = "123" }},
		{"wrong expiry", func(r *sessionResponse) { r.Spec.Certificate.ExpiresAt = r.Spec.Certificate.ExpiresAt.Add(time.Minute) }},
		{"expired session", func(r *sessionResponse) { r.Metadata.ExpiresAt = time.Now().Add(-time.Minute) }},
		{"different key", func(r *sessionResponse) { r.Spec.Certificate.CertificatePEM = string(old.CertificatePEM) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := valid
			test.change(&response)
			if _, err := recoveredCredentials(old, pending, response); err == nil {
				t.Fatal("accepted invalid recovered credentials")
			}
		})
	}
}

func TestRecoveryRejectsRedirectWithoutForwardingGrant(t *testing.T) {
	pki := newTestPKI(t)
	var forwarded bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/agent/recover" {
			http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
			return
		}
		forwarded = true
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	directory := t.TempDir()
	saveExpiredRecoveryCredentials(t, pki, directory, server.URL)
	if err := Recover(t.Context(), directory, "grant", "alice"); err == nil {
		t.Fatal("recovery followed redirect")
	}
	if forwarded {
		t.Fatal("enrollment grant was forwarded")
	}
}

func TestRecoveryCLIAndPendingStartupBoundary(t *testing.T) {
	pki := newTestPKI(t)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var document recoveryRequest
		decodeTestRequest(t, request, &document)
		if document.Spec.ExecutionUser == "" {
			t.Error("recovery omitted execution user")
		}
		writeTestJSON(t, writer, recoveryResponse(t, pki, document))
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	directory := t.TempDir()
	old := saveExpiredRecoveryCredentials(t, pki, directory, server.URL)
	if err := checkRecoveryBeforeService(directory, old); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := ExecuteCLI(t.Context(), []string{"recover", "--state-dir", directory}, strings.NewReader("grant\n"), &output, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "journal preserved") {
		t.Fatal("missing recovery confirmation")
	}
	current, err := loadCredentials(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err = checkRecoveryBeforeService(directory, current); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"recover", "--unknown"},
		{"recover", "--state-dir", directory, "--token-file", "missing-token"},
		{"recover", "--state-dir", "relative"},
	} {
		if err = ExecuteCLI(t.Context(), args, strings.NewReader("grant\n"), io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted invalid arguments %v", args)
		}
	}
	// A request prepared but not completed must not start a service with stale
	// credentials or silently substitute another user's enrollment token.
	second := t.TempDir()
	old = saveExpiredRecoveryCredentials(t, pki, second, server.URL)
	if _, _, err = prepareRecovery(second, old, "pending-grant", "alice"); err != nil {
		t.Fatal(err)
	}
	if err = checkRecoveryBeforeService(second, old); err == nil {
		t.Fatal("service accepted unresolved recovery")
	}
	if err = RunService(t.Context(), ServiceOptions{StateDirectory: second, Logger: slog.New(slog.DiscardHandler)}); err == nil || !strings.Contains(err.Error(), "recovery is pending") {
		t.Fatalf("service startup with unresolved recovery error=%v", err)
	}
	if _, _, err = prepareRecovery(second, old, "pending-grant", "bob"); err == nil {
		t.Fatal("recovery accepted different execution user")
	}
}

func TestRecoveryReceiptBoundaries(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, recoveryFilename)
	for _, data := range [][]byte{[]byte(`{"unexpected":true}`), bytes.Repeat([]byte("x"), 65537)} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readRecovery(directory); err == nil {
			t.Fatal("accepted invalid recovery receipt")
		}
		if err := checkRecoveryBeforeService(directory, credentialFiles{}); err == nil {
			t.Fatal("startup accepted invalid recovery receipt")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := readRecovery(directory); err == nil {
		t.Fatal("accepted directory as recovery receipt")
	}
}

func TestRecoveryReceiptExpiryAndCredentialMismatch(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	old := saveExpiredRecoveryCredentials(t, pki, directory, "https://control.example.test")
	pending, _, err := prepareRecovery(directory, old, "grant", "alice")
	if err != nil {
		t.Fatal(err)
	}
	updated, err := recoveredCredentials(old, pending, recoveryResponse(t, pki, pending.Request))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = resumeRecovery(pending, updated, pending.TokenHash); err == nil {
		t.Fatal("pending request accepted a changed certificate")
	}
	if err = completeRecovery(directory, &pending, old); err == nil {
		t.Fatal("completion accepted credentials from before recovery")
	}
	// A retry after the credential write must finalize its receipt itself.
	if _, done, prepareErr := prepareRecovery(directory, updated, "grant", "alice"); prepareErr != nil || !done {
		t.Fatalf("resume after credential write = %t, %v", done, prepareErr)
	}
	if _, _, err = prepareRecovery(directory, updated, "new-grant", "alice"); err == nil {
		t.Fatal("fresh grant replaced a still-valid certificate")
	}
	updated.Metadata.CertificateExpiresAt = time.Now().Add(-time.Hour)
	if _, _, err = prepareRecovery(directory, updated, "grant", "alice"); err == nil {
		t.Fatal("old grant replay accepted a later credential expiry")
	}
	// A later real certificate expiry requires and accepts a fresh grant.
	if next, done, prepareErr := prepareRecovery(directory, old, "new-grant", "alice"); prepareErr != nil || done || next.TokenHash == pending.TokenHash {
		t.Fatalf("new expiry recovery = %t, %v", done, prepareErr)
	}
}

func TestRecoveryRejectsDamagedCredentialsAndReceiptWrites(t *testing.T) {
	pki := newTestPKI(t)
	directory := t.TempDir()
	old := saveExpiredRecoveryCredentials(t, pki, directory, "https://control.example.test")
	pending, _, err := prepareRecovery(directory, old, "grant", "alice")
	if err != nil {
		t.Fatal(err)
	}
	broken := old
	broken.PrivateKeyPEM = []byte("invalid key")
	if _, _, err = newRecovery(directory, broken, "token-hash", "alice"); err == nil {
		t.Fatal("accepted damaged credentials for recovery")
	}
	if err = completeRecovery(directory, &pending, broken); err == nil {
		t.Fatal("accepted damaged completion credentials")
	}
	if err = os.WriteFile(filepath.Join(directory, recoveryFilename), []byte("invalid receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = prepareRecovery(directory, old, "grant", "alice"); err == nil {
		t.Fatal("accepted damaged receipt")
	}
	if _, _, err = newRecovery(filepath.Join(directory, "missing"), old, "token-hash", "alice"); err == nil {
		t.Fatal("recovery accepted a failed durable request write")
	}
	updated, err := recoveredCredentials(old, pending, recoveryResponse(t, pki, pending.Request))
	if err != nil {
		t.Fatal(err)
	}
	if err = completeRecovery(filepath.Join(directory, "missing"), &pending, updated); err == nil {
		t.Fatal("recovery accepted a failed durable completion write")
	}
	response := recoveryResponse(t, pki, pending.Request)
	response.Spec.Certificate.CACertificatePEM = string(newTestPKI(t).caPEM)
	if _, err = recoveredCredentials(old, pending, response); err == nil {
		t.Fatal("accepted certificate signed by a different CA")
	}
}
