package agent

import (
	"crypto/tls"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestServiceSessionRenewalFailureDoesNotStarvePolling(t *testing.T) {
	t.Parallel()
	endpoint, err := url.Parse("https://control.example.test")
	if err != nil {
		t.Fatal(err)
	}
	var requested []string
	client := &Client{
		baseURL: endpoint,
		credentials: credentialFiles{Metadata: metadata{
			AgentID: testAgentID, TargetGenerationID: testTargetGenerationID,
			SessionToken: "expired", SessionExpiresAt: time.Now().Add(-time.Minute),
			CertificateExpiresAt: time.Now().Add(time.Hour),
		}},
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requested = append(requested, request.URL.Path)
			switch request.URL.Path {
			case "/v1/agent/session/renew":
				return responseWithBody(http.StatusUnauthorized, ""), nil
			case "/v1/agent/actions":
				return responseWithBody(http.StatusOK, `{"apiVersion":"jobman.control/v1alpha1","kind":"DesiredActionList","items":[]}`), nil
			case "/v1/agent/assignments":
				return responseWithBody(http.StatusOK, `{"apiVersion":"jobman.control/v1alpha1","kind":"AgentAssignmentList","requiresAcceptance":true,"items":[]}`), nil
			default:
				t.Fatalf("unexpected request %q", request.URL.Path)
				return responseWithBody(http.StatusInternalServerError, ""), nil
			}
		})},
	}
	spool, err := OpenSpool(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	value := &service{stateDirectory: t.TempDir(), logger: slog.New(slog.DiscardHandler), client: client, spool: spool}
	for range 2 {
		if err = value.step(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"/v1/agent/session/renew", "/v1/agent/actions", "/v1/agent/assignments", "/v1/agent/actions", "/v1/agent/assignments"}
	if !slices.Equal(requested, want) {
		t.Fatalf("requests = %v, want %v", requested, want)
	}
	value.sessionRenewalTriedAt = time.Now().Add(-2 * sessionRenewalRetryInterval)
	if err = value.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if requested[len(want)] != "/v1/agent/session/renew" {
		t.Fatal("session renewal was not retried after backoff")
	}
}

func TestServicePrioritizesCertificateRenewalAndFailsClosed(t *testing.T) {
	for _, rejectCertificate := range []bool{false, true} {
		t.Run(map[bool]string{false: "session_failure", true: "certificate_failure"}[rejectCertificate], func(t *testing.T) {
			pki := newTestPKI(t)
			var mutex sync.Mutex
			var requested []string
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				mutex.Lock()
				defer mutex.Unlock()
				requested = append(requested, request.URL.Path)
				switch request.URL.Path {
				case "/v1/agent/certificate/renew":
					if rejectCertificate {
						writer.WriteHeader(http.StatusUnauthorized)
						return
					}
					var document struct {
						APIVersion string `json:"apiVersion"`
						Kind       string `json:"kind"`
						Spec       struct {
							CSR string `json:"certificateSigningRequest"`
						} `json:"spec"`
					}
					decodeTestRequest(t, request, &document)
					writeTestJSON(t, writer, pki.issueCSR(t, document.Spec.CSR))
				case "/v1/agent/session/renew":
					writer.WriteHeader(http.StatusUnauthorized)
				case "/v1/agent/actions":
					writeTestJSON(t, writer, map[string]any{"apiVersion": controlAPIVersion, "kind": "DesiredActionList", "items": []any{}})
				case "/v1/agent/assignments":
					writeTestJSON(t, writer, map[string]any{"apiVersion": controlAPIVersion, "kind": "AgentAssignmentList", "requiresAcceptance": true, "items": []any{}})
				default:
					t.Errorf("unexpected request %q", request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pki.serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
			server.StartTLS()
			t.Cleanup(server.Close)
			directory := t.TempDir()
			saveTestCredentials(t, pki, directory, server.URL)
			client, err := OpenClient(directory)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			client.credentials.Metadata.SessionExpiresAt = time.Now().Add(-time.Minute)
			client.credentials.Metadata.CertificateExpiresAt = time.Now().Add(time.Minute)
			spool, err := OpenSpool(t.Context(), directory)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = spool.Close() })
			value := &service{stateDirectory: directory, logger: slog.New(slog.DiscardHandler), client: client, spool: spool}
			err = value.step(t.Context())
			if (err != nil) != rejectCertificate {
				t.Fatalf("step error = %v, certificate rejected = %t", err, rejectCertificate)
			}
			mutex.Lock()
			defer mutex.Unlock()
			want := []string{"/v1/agent/certificate/renew"}
			if !rejectCertificate {
				want = append(want, "/v1/agent/session/renew", "/v1/agent/actions", "/v1/agent/assignments")
			}
			if !slices.Equal(requested, want) {
				t.Fatalf("requests = %v, want %v", requested, want)
			}
		})
	}
}
