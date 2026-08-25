package controlclient

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

const (
	testJobID    = "11111111-1111-4111-8111-111111111111"
	testTargetID = "22222222-2222-4222-8222-222222222222"
)

func TestClientLifecycleAndTargets(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("signed.jwt.value\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	var submitAttempts atomic.Int64
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer signed.jwt.value" ||
			request.Header.Get("Accept") != "application/json" {
			t.Errorf("request headers = %v", request.Header)
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/jobs":
			if submitAttempts.Add(1) == 1 {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if request.Header.Get("Idempotency-Key") != "submit-operation" ||
				request.Header.Get("Content-Type") != "application/json" {
				t.Errorf("submit headers = %v", request.Header)
			}
			if _, err := protocol.DecodeJobRequest(request.Body, protocol.DecodeLimits{}); err != nil {
				t.Errorf("decode submitted request: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			writeTestJSON(t, writer, testJob("accepted"))
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/collections":
			if request.Header.Get("Idempotency-Key") != "collection-operation" {
				t.Errorf("collection headers = %v", request.Header)
			}
			if _, err := protocol.DecodeCollectionRequest(request.Body, protocol.DecodeLimits{}); err != nil {
				t.Errorf("decode submitted collection: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			writeTestJSON(t, writer, testCollection())
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/collections/99999999-9999-4999-8999-999999999999":
			writeTestJSON(t, writer, testCollection())
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/graphs":
			if request.Header.Get("Idempotency-Key") != "graph-operation" {
				t.Errorf("graph headers = %v", request.Header)
			}
			if _, err := protocol.DecodeGraphRequest(request.Body, protocol.DecodeLimits{}); err != nil {
				t.Errorf("decode submitted graph: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			writeTestJSON(t, writer, testGraph())
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/graphs/88888888-8888-4888-8888-888888888888":
			writeTestJSON(t, writer, testGraph())
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/graphs/88888888-8888-4888-8888-888888888888/cancel":
			if request.Header.Get("Idempotency-Key") != "graph-cancel-operation" {
				t.Errorf("graph cancel headers = %v", request.Header)
			}
			writeTestJSON(t, writer, testGraph())
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/history/imports" && request.URL.Query().Get("dryRun") == "true":
			if request.Header.Get("Idempotency-Key") != "" {
				t.Errorf("dry-run history headers = %v", request.Header)
			}
			writeTestJSON(t, writer, map[string]any{
				"apiVersion": apiVersion, "kind": "CompletedHistoryImportPlan",
				"status": map[string]string{"result": "valid"},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/history/imports":
			if request.Header.Get("Idempotency-Key") != "history-operation" {
				t.Errorf("history headers = %v", request.Header)
			}
			job := testJob("terminal")
			job.Metadata.Name = "imported-job"
			job.Status.Outcome = "success"
			writer.WriteHeader(http.StatusCreated)
			writeTestJSON(t, writer, job)
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/jobs":
			if request.URL.Query().Get("limit") != "7" || request.URL.Query().Get("phase") != "running" ||
				request.URL.Query().Get("pageToken") != "next" {
				t.Errorf("job list query = %v", request.URL.Query())
			}
			writeTestJSON(t, writer, JobPage{
				APIVersion: apiVersion, Kind: "JobList",
				Items: []Job{testJob("running")}, NextPageToken: "following",
			})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/jobs/"+testJobID:
			job := testJob("running")
			job.Status.NativeID = "12345"
			job.Status.Scheduler = &SchedulerStatus{
				Backend: "slurm", State: "running", Cluster: "alpha",
				ObservedAt: time.Now().UTC(),
			}
			writeTestJSON(t, writer, job)
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/jobs/"+testJobID+"/logs":
			writeTestJSON(t, writer, testLogManifest())
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/jobs/"+testJobID+"/artifacts":
			writeTestJSON(t, writer, testArtifactManifest())
		case request.Method == http.MethodPost && request.URL.Path == "/v1/namespaces/research/jobs/"+testJobID+"/cancel":
			if request.Header.Get("Idempotency-Key") != "cancel-operation" {
				t.Errorf("cancel idempotency key = %q", request.Header.Get("Idempotency-Key"))
			}
			job := testJob("running")
			job.Status.DesiredState = "cancel"
			writeTestJSON(t, writer, job)
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/targets":
			writeTestJSON(t, writer, targetPage{
				APIVersion: apiVersion, Kind: "TargetList", Items: []Target{testTarget()},
			})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/namespaces/research/targets/workstation-a":
			writeTestJSON(t, writer, testTarget())
		case request.Method == http.MethodPut && request.URL.Path == "/v1/namespaces/research/targets/workstation-a/state":
			if request.Header.Get("If-Match") != `"revision-1"` ||
				request.Header.Get("Idempotency-Key") != "target-state-operation" {
				t.Errorf("target state headers = %v", request.Header)
			}
			target := testTarget()
			target.Status.State = "draining"
			writeTestJSON(t, writer, target)
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			writer.WriteHeader(http.StatusNotFound)
		}
	})
	client, err := New(Options{
		Endpoint: "https://control.example", Namespace: "research", TokenFile: tokenFile,
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: handler}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sealed := testRequest(t)
	job, err := client.SubmitJob(t.Context(), sealed, "submit-operation")
	if err != nil || job.Metadata.ID != testJobID || submitAttempts.Load() != 2 {
		t.Fatalf("SubmitJob() = %#v, %v attempts=%d", job, err, submitAttempts.Load())
	}
	collection, err := client.SubmitCollection(t.Context(), testCollectionRequest(t), "collection-operation")
	if err != nil || collection.Metadata.ID != "99999999-9999-4999-8999-999999999999" {
		t.Fatalf("SubmitCollection() = %#v, %v", collection, err)
	}
	collection, err = client.GetCollection(t.Context(), collection.Metadata.ID)
	if err != nil || collection.Status.ArrayMode != "slurm-array" || len(collection.Items) != 1 {
		t.Fatalf("GetCollection() = %#v, %v", collection, err)
	}
	graph, err := client.SubmitGraph(t.Context(), testGraphRequest(t), "graph-operation")
	if err != nil || graph.Metadata.ID != "88888888-8888-4888-8888-888888888888" {
		t.Fatalf("SubmitGraph() = %#v, %v", graph, err)
	}
	graph, err = client.GetGraph(t.Context(), graph.Metadata.ID)
	if err != nil || len(graph.Items) != 2 || !graph.Items[1].Dependencies[0].Satisfied {
		t.Fatalf("GetGraph() = %#v, %v", graph, err)
	}
	graph, err = client.CancelGraph(t.Context(), graph.Metadata.ID, "graph-cancel-operation")
	if err != nil || graph.Spec.UnsatisfiedPolicy != "skip" {
		t.Fatalf("CancelGraph() = %#v, %v", graph, err)
	}
	history := testCompletedHistoryImportRequest(t)
	if _, err = client.ImportCompletedHistory(t.Context(), history, true, ""); err != nil {
		t.Fatalf("ImportCompletedHistory(dry run) error = %v", err)
	}
	job, err = client.ImportCompletedHistory(t.Context(), history, false, "history-operation")
	if err != nil || job.Status.Outcome != "success" {
		t.Fatalf("ImportCompletedHistory() = %#v, %v", job, err)
	}
	page, err := client.ListJobs(t.Context(), 7, "running", "next")
	if err != nil || len(page.Items) != 1 || page.NextPageToken != "following" {
		t.Fatalf("ListJobs() = %#v, %v", page, err)
	}
	job, err = client.GetJob(t.Context(), testJobID)
	if err != nil || job.Status.Phase != "running" || job.Status.NativeID != "12345" ||
		job.Status.Scheduler == nil || job.Status.Scheduler.State != "running" {
		t.Fatalf("GetJob() = %#v, %v", job, err)
	}
	manifest, err := client.GetJobLogs(t.Context(), testJobID)
	if err != nil || len(manifest.Items) != 1 || manifest.Items[0].ByteLength != 6 {
		t.Fatalf("GetJobLogs() = %#v, %v", manifest, err)
	}
	artifactManifest, err := client.GetJobArtifacts(t.Context(), testJobID)
	if err != nil || len(artifactManifest.Items) != 1 || artifactManifest.Items[0].Name != "result" {
		t.Fatalf("GetJobArtifacts() = %#v, %v", artifactManifest, err)
	}
	job, err = client.CancelJob(t.Context(), testJobID, "cancel-operation")
	if err != nil || job.Status.DesiredState != "cancel" {
		t.Fatalf("CancelJob() = %#v, %v", job, err)
	}
	targets, err := client.ListTargets(t.Context())
	if err != nil || len(targets) != 1 || targets[0].Metadata.Name != "workstation-a" {
		t.Fatalf("ListTargets() = %#v, %v", targets, err)
	}
	target, err := client.GetTarget(t.Context(), "workstation-a")
	if err != nil || target.Metadata.ID != testTargetID {
		t.Fatalf("GetTarget() = %#v, %v", target, err)
	}
	target, err = client.UpdateTargetState(
		t.Context(), "workstation-a", "draining", 1, "target-state-operation",
	)
	if err != nil || target.Status.State != "draining" {
		t.Fatalf("UpdateTargetState() = %#v, %v", target, err)
	}
	if _, err = client.UpdateTargetState(t.Context(), "", "draining", 1, "operation"); err == nil {
		t.Fatal("UpdateTargetState() accepted invalid target")
	}
}

func TestCollectionResponseValidationFailures(t *testing.T) {
	t.Parallel()
	invalid := testCollection()
	invalid.Status.ArrayMode = "unknown"
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeTestJSON(t, writer, invalid)
	})
	client, err := New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: handler}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.SubmitCollection(t.Context(), testCollectionRequest(t), "operation"); err == nil {
		t.Fatal("SubmitCollection() accepted an invalid response")
	}
	if _, err = client.GetCollection(t.Context(), "99999999-9999-4999-8999-999999999999"); err == nil {
		t.Fatal("GetCollection() accepted an invalid response")
	}
	invalid = testCollection()
	invalid.Items[0].Index = 1
	if err = client.validateCollection(invalid, invalid.Metadata.ID); err == nil {
		t.Fatal("validateCollection() accepted an unordered item")
	}
}

func TestGraphAndHistoryResponseValidationFailures(t *testing.T) {
	t.Parallel()
	invalidGraph := testGraph()
	invalidGraph.Kind = "FutureGraph"
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeTestJSON(t, writer, invalidGraph)
	})
	client, err := New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: handler}},
	})
	if err != nil {
		t.Fatal(err)
	}
	graphID := "88888888-8888-4888-8888-888888888888"
	if _, err = client.SubmitGraph(t.Context(), testGraphRequest(t), "operation"); err == nil {
		t.Fatal("SubmitGraph() accepted an invalid response")
	}
	if _, err = client.GetGraph(t.Context(), graphID); err == nil {
		t.Fatal("GetGraph() accepted an invalid response")
	}
	if _, err = client.CancelGraph(t.Context(), graphID, "operation"); err == nil {
		t.Fatal("CancelGraph() accepted an invalid response")
	}
	failureHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	})
	failingClient, err := New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: failureHandler}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = failingClient.GetGraph(t.Context(), graphID); err == nil {
		t.Fatal("GetGraph() transport error = nil")
	}
	if _, err = failingClient.CancelGraph(t.Context(), graphID, "operation"); err == nil {
		t.Fatal("CancelGraph() transport error = nil")
	}

	invalidGraph = testGraph()
	invalidGraph.Items[1].Dependencies = []GraphDependency{
		{From: "missing", Predicate: "success"},
	}
	if err = client.validateGraph(invalidGraph, graphID); err == nil {
		t.Fatal("validateGraph() accepted an unknown dependency")
	}
	invalidGraph = testGraph()
	invalidGraph.Items[1].Dependencies = append(
		invalidGraph.Items[1].Dependencies, invalidGraph.Items[1].Dependencies[0],
	)
	if err = client.validateGraph(invalidGraph, graphID); err == nil {
		t.Fatal("validateGraph() accepted a duplicate dependency")
	}

	history := testCompletedHistoryImportRequest(t)
	history.Metadata.Namespace = "other"
	if _, err = client.ImportCompletedHistory(t.Context(), history, false, "operation"); err == nil {
		t.Fatal("ImportCompletedHistory() accepted a different namespace")
	}
	planHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeTestJSON(t, writer, map[string]any{
			"apiVersion": apiVersion, "kind": "CompletedHistoryImportPlan",
			"status": map[string]string{"result": "unsafe"},
		})
	})
	client, err = New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: planHandler}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.ImportCompletedHistory(
		t.Context(), testCompletedHistoryImportRequest(t), true, "",
	); err == nil {
		t.Fatal("ImportCompletedHistory() accepted an invalid dry-run plan")
	}
}

func testLogManifest() LogManifest {
	return LogManifest{
		APIVersion: apiVersion, Kind: "JobLogManifest", Namespace: "research", JobID: testJobID,
		Items: []LogStream{{
			ExecutionID: "33333333-3333-4333-8333-333333333333", RunNumber: 1,
			Stream: "stdout", State: "complete", ByteLength: 6,
			Chunks: []LogChunk{{
				Sequence: 1, StoreName: "department-nfs", StoreVersion: 1,
				ObjectKey: "namespaces/research/logs/one.chunk", ByteLength: 6,
				Checksum:   "sha256:" + strings.Repeat("a", 64),
				CapturedAt: time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC),
			}},
		}},
	}
}

func testArtifactManifest() ArtifactManifest {
	return ArtifactManifest{
		APIVersion: apiVersion, Kind: "JobArtifactManifest", Namespace: "research", JobID: testJobID,
		Items: []PublishedArtifact{{
			ExecutionID: "33333333-3333-4333-8333-333333333333", RunNumber: 1,
			Name: "result", StoreName: "department-nfs", StoreVersion: 2,
			ObjectKey: "research/results/result.txt", ByteLength: 7,
			Checksum:    "sha256:" + strings.Repeat("a", 64),
			PublishedAt: time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC),
		}},
	}
}

func testCollection() Collection {
	job := testJob("accepted")
	job.Metadata.Name = "trial"

	return Collection{
		APIVersion: apiVersion, Kind: "Collection",
		Metadata: CollectionMetadata{
			ID: "99999999-9999-4999-8999-999999999999", Namespace: "research", Name: "sweep", Revision: 1,
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
		Spec:   CollectionSpec{MaxActive: 1, FailurePolicy: "continue", ArrayPolicy: "prefer"},
		Status: CollectionStatus{Phase: "accepted", ArrayMode: "slurm-array", Total: 1},
		Items:  []CollectionItem{{Index: 0, Name: "trial", Job: job}},
	}
}

func testCollectionRequest(t *testing.T) protocol.SealedCollectionRequest {
	t.Helper()
	request := testRequest(t)
	sealed, err := protocol.SealCollectionRequest(protocol.CollectionRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.CollectionRequestKind,
		Metadata: protocol.CollectionRequestMetadata{Namespace: "research", Name: "sweep"},
		Spec: protocol.CollectionRequestSpec{
			MaxActive: 1, FailurePolicy: "continue", ArrayPolicy: "prefer",
			Items: []protocol.CollectionItem{{
				Name: "trial", Workload: request.Document.Spec.Workload, Placement: request.Document.Spec.Placement,
			}},
		},
	})
	if err != nil {
		t.Fatalf("SealCollectionRequest() error = %v", err)
	}

	return sealed
}

func testGraph() Graph {
	first := testJob("accepted")
	first.Metadata.Name = "prepare"
	second := testJob("accepted")
	second.Metadata.ID = "77777777-7777-4777-8777-777777777777"
	second.Metadata.Name = "analyze"

	return Graph{
		APIVersion: apiVersion, Kind: "Graph",
		Metadata: GraphMetadata{
			ID: "88888888-8888-4888-8888-888888888888", Namespace: "research", Name: "pipeline",
			Revision: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
		Spec:   GraphSpec{MaxActive: 1, UnsatisfiedPolicy: "skip"},
		Status: GraphStatus{Phase: "accepted", Total: 2, Waiting: 2},
		Items: []GraphItem{
			{Index: 0, Name: "prepare", Job: first},
			{
				Index: 1, Name: "analyze", Job: second,
				Dependencies: []GraphDependency{{From: "prepare", Predicate: "success", Satisfied: true}},
			},
		},
	}
}

func testGraphRequest(t *testing.T) protocol.SealedGraphRequest {
	t.Helper()
	request := testRequest(t)
	sealed, err := protocol.SealGraphRequest(protocol.GraphRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.GraphRequestKind,
		Metadata: protocol.GraphRequestMetadata{Namespace: "research", Name: "pipeline"},
		Spec: protocol.GraphRequestSpec{
			MaxActive: 1, UnsatisfiedPolicy: "skip",
			Nodes: []protocol.GraphNode{
				{Name: "prepare", Workload: request.Document.Spec.Workload, Placement: request.Document.Spec.Placement},
				{Name: "analyze", Workload: request.Document.Spec.Workload, Placement: request.Document.Spec.Placement},
			},
			Edges: []protocol.GraphEdge{{From: "prepare", To: "analyze", Predicate: "success"}},
		},
	})
	if err != nil {
		t.Fatalf("SealGraphRequest() error = %v", err)
	}

	return sealed
}

func testCompletedHistoryImportRequest(t *testing.T) CompletedHistoryImportRequest {
	t.Helper()
	request := testRequest(t)

	return CompletedHistoryImportRequest{
		APIVersion: apiVersion, Kind: "CompletedHistoryImport",
		Metadata: protocol.JobRequestMetadata{Namespace: "research", Name: "imported-job"},
		Spec: CompletedHistoryImportSpec{
			Outcome: "success", CompletedAt: time.Now().UTC().Add(-time.Hour),
			Source:   CompletedHistorySource{Store: "sqlite", Schema: 8, JobID: "local-job-1"},
			Workload: request.Document.Spec.Workload, Placement: request.Document.Spec.Placement,
		},
	}
}

func TestClientTLSWithConfiguredCA(t *testing.T) {
	t.Parallel()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	encoded := testCertificatePEM(t)
	if err := os.WriteFile(caFile, encoded, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	transport, err := newTransport(caFile)
	if err != nil || transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil {
		t.Fatalf("newTransport(CA) = %#v, %v", transport, err)
	}
}

func TestClientAPIAndResponseErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantAPI     bool
	}{
		{
			name: "structured API", status: http.StatusNotFound, contentType: "application/json",
			body: `{"error":{"code":"not_found","message":"resource not found"}}`, wantAPI: true,
		},
		{name: "opaque API", status: http.StatusBadGateway, contentType: "text/plain", body: "gateway"},
		{name: "content type", status: http.StatusOK, contentType: "text/plain", body: `{}`},
		{name: "malformed JSON", status: http.StatusOK, contentType: "application/json", body: `{`},
		{name: "trailing JSON", status: http.StatusOK, contentType: "application/json", body: `{} {}`},
		{name: "incompatible", status: http.StatusOK, contentType: "application/json", body: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", test.contentType)
				writer.WriteHeader(test.status)
				if _, writeErr := io.WriteString(writer, test.body); writeErr != nil {
					t.Errorf("write response: %v", writeErr)
				}
			})
			client, err := New(Options{
				Endpoint: "https://control.example", Namespace: "research",
				HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: handler}},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_, err = client.GetJob(t.Context(), testJobID)
			if err == nil {
				t.Fatal("GetJob() error = nil")
			}
			var apiError *APIError
			if test.wantAPI && (!errors.As(err, &apiError) || apiError.Code != "not_found") {
				t.Fatalf("GetJob() error = %T %v", err, err)
			}
		})
	}
}

func TestClientRejectsIncompatibleResources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body any
		call func(*Client) error
	}{
		{
			name: "job namespace", body: func() Job { job := testJob("accepted"); job.Metadata.Namespace = "other"; return job }(),
			call: func(client *Client) error { _, err := client.GetJob(t.Context(), testJobID); return err },
		},
		{
			name: "job list", body: JobPage{APIVersion: "future", Kind: "JobList"},
			call: func(client *Client) error { _, err := client.ListJobs(t.Context(), 1, "", ""); return err },
		},
		{
			name: "job list item", body: JobPage{APIVersion: apiVersion, Kind: "JobList", Items: []Job{{}}},
			call: func(client *Client) error { _, err := client.ListJobs(t.Context(), 1, "", ""); return err },
		},
		{
			name: "target list", body: targetPage{APIVersion: "future", Kind: "TargetList"},
			call: func(client *Client) error { _, err := client.ListTargets(t.Context()); return err },
		},
		{
			name: "target list item", body: targetPage{APIVersion: apiVersion, Kind: "TargetList", Items: []Target{{}}},
			call: func(client *Client) error { _, err := client.ListTargets(t.Context()); return err },
		},
		{
			name: "target name", body: testTarget(),
			call: func(client *Client) error { _, err := client.GetTarget(t.Context(), "different"); return err },
		},
		{
			name: "log offset", body: func() LogManifest {
				manifest := testLogManifest()
				manifest.Items[0].Chunks[0].ByteOffset = 1
				return manifest
			}(),
			call: func(client *Client) error { _, err := client.GetJobLogs(t.Context(), testJobID); return err },
		},
		{
			name: "artifact envelope", body: ArtifactManifest{APIVersion: "future"},
			call: func(client *Client) error { _, err := client.GetJobArtifacts(t.Context(), testJobID); return err },
		},
		{
			name: "artifact digest", body: func() ArtifactManifest {
				manifest := testArtifactManifest()
				manifest.Items[0].Checksum = "invalid"
				return manifest
			}(),
			call: func(client *Client) error { _, err := client.GetJobArtifacts(t.Context(), testJobID); return err },
		},
		{
			name: "duplicate artifact", body: func() ArtifactManifest {
				manifest := testArtifactManifest()
				manifest.Items = append(manifest.Items, manifest.Items[0])
				return manifest
			}(),
			call: func(client *Client) error { _, err := client.GetJobArtifacts(t.Context(), testJobID); return err },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writeTestJSON(t, writer, test.body)
			})
			client, err := New(Options{
				Endpoint: "https://control.example", Namespace: "research",
				HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: handler}},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err = test.call(client); err == nil {
				t.Fatal("client accepted incompatible resource")
			}
		})
	}
}

func TestClientConfigurationAndTokenErrors(t *testing.T) {
	t.Parallel()
	for _, options := range []Options{
		{Endpoint: "://bad", Namespace: "research"},
		{Endpoint: "https://example.com/base", Namespace: "research"},
		{Endpoint: "https://example.com", Namespace: ""},
		{Endpoint: "https://example.com", Namespace: "research/other"},
		{Endpoint: "http://127.0.0.1:8080", Namespace: "research", TokenFile: "/private/token"},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("New(%#v) accepted invalid options", options)
		}
	}
	root := t.TempDir()
	invalidCA := filepath.Join(root, "invalid-ca.pem")
	if err := os.WriteFile(invalidCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	if _, err := New(Options{Endpoint: "https://example.com", Namespace: "research", CAFile: invalidCA}); err == nil {
		t.Fatal("New() accepted invalid CA")
	}
	if _, err := New(Options{Endpoint: "https://example.com", Namespace: "research", CAFile: root}); err == nil {
		t.Fatal("New() accepted CA directory")
	}
	if _, err := x509.SystemCertPool(); err != nil {
		t.Logf("system pool unavailable: %v", err)
	}
	tests := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{name: "empty", data: nil, mode: 0o600},
		{name: "whitespace", data: []byte("bad token"), mode: 0o600},
		{name: "permissions", data: []byte("token"), mode: 0o644},
		{name: "oversize", data: bytes.Repeat([]byte("x"), maximumTokenBytes+1), mode: 0o600},
	}
	for _, test := range tests {
		if runtime.GOOS == "windows" && test.name == "permissions" {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(root, test.name)
			if err := os.WriteFile(file, test.data, test.mode); err != nil {
				t.Fatalf("write token: %v", err)
			}
			if err := os.Chmod(file, test.mode); err != nil {
				t.Fatalf("chmod token: %v", err)
			}
			if _, err := readTokenFile(file); err == nil {
				t.Fatal("readTokenFile() error = nil")
			}
		})
	}
	valid := filepath.Join(root, "valid")
	if err := os.WriteFile(valid, []byte("token\r\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	if token, err := readTokenFile(valid); err != nil || token != "token" {
		t.Fatalf("readTokenFile(valid) = %q, %v", token, err)
	}
	if _, err := readTokenFile(root); err == nil {
		t.Fatal("readTokenFile(directory) error = nil")
	}
	if runtime.GOOS != "windows" {
		link := filepath.Join(root, "token-link")
		if err := os.Symlink(valid, link); err != nil {
			t.Fatalf("create token symlink: %v", err)
		}
		if _, err := readTokenFile(link); err == nil {
			t.Fatal("readTokenFile(symlink) error = nil")
		}
	}
}

func TestClientTransportAndBodyFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		transport http.RoundTripper
	}{
		{
			name: "transport",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("offline")
			}),
		},
		{
			name: "read",
			transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(errorReader{}), Request: request,
				}, nil
			}),
		},
		{
			name: "oversize",
			transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), maximumResponseBytes+1))), Request: request,
				}, nil
			}),
		},
		{
			name: "close",
			transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: closeErrorBody{Reader: strings.NewReader(`{}`)}, Request: request,
				}, nil
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client, err := New(Options{
				Endpoint: "https://control.example", Namespace: "research",
				HTTPClient: &http.Client{Transport: test.transport},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if _, err = client.GetJob(t.Context(), testJobID); err == nil {
				t.Fatal("GetJob() error = nil")
			}
		})
	}
}

func TestClientMutationValidationAndCredentialFailures(t *testing.T) {
	t.Parallel()
	invalidHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if _, writeErr := io.WriteString(writer, `{}`); writeErr != nil {
			t.Errorf("write response: %v", writeErr)
		}
	})
	client, err := New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: invalidHandler}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err = client.SubmitJob(t.Context(), testRequest(t), "submit-key"); err == nil {
		t.Fatal("SubmitJob() accepted incompatible job")
	}
	if _, err = client.CancelJob(t.Context(), testJobID, "cancel-key"); err == nil {
		t.Fatal("CancelJob() accepted incompatible job")
	}

	missing := filepath.Join(t.TempDir(), "missing-token")
	client, err = New(Options{
		Endpoint: "https://control.example", Namespace: "research", TokenFile: missing,
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: invalidHandler}},
	})
	if err != nil {
		t.Fatalf("New(missing token) error = %v", err)
	}
	if _, err = client.GetJob(t.Context(), testJobID); err == nil || !strings.Contains(err.Error(), "token file") {
		t.Fatalf("GetJob(missing token) error = %v", err)
	}
	if _, err = newTransport(filepath.Join(t.TempDir(), "missing-ca")); err == nil {
		t.Fatal("newTransport(missing CA) error = nil")
	}

	forbiddenHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		if _, writeErr := io.WriteString(
			writer, `{"error":{"code":"forbidden","message":"not authorized"}}`,
		); writeErr != nil {
			t.Errorf("write response: %v", writeErr)
		}
	})
	client, err = New(Options{
		Endpoint: "https://control.example", Namespace: "research",
		HTTPClient: &http.Client{Transport: handlerRoundTripper{handler: forbiddenHandler}},
	})
	if err != nil {
		t.Fatalf("New(forbidden) error = %v", err)
	}
	if _, err = client.SubmitJob(t.Context(), testRequest(t), "submit-key"); err == nil {
		t.Fatal("SubmitJob(forbidden) error = nil")
	}
	if _, err = client.CancelJob(t.Context(), testJobID, "cancel-key"); err == nil {
		t.Fatal("CancelJob(forbidden) error = nil")
	}
	if _, err = client.ListTargets(t.Context()); err == nil {
		t.Fatal("ListTargets(forbidden) error = nil")
	}
	if _, err = client.GetTarget(t.Context(), "workstation-a"); err == nil {
		t.Fatal("GetTarget(forbidden) error = nil")
	}
}

func TestAPIErrorFormatting(t *testing.T) {
	t.Parallel()
	if got := (&APIError{StatusCode: 500}).Error(); got != "Jobman Control returned HTTP 500" {
		t.Fatalf("APIError.Error() = %q", got)
	}
	if got := (&APIError{StatusCode: 409, Code: "conflict", Message: "already exists"}).Error(); !strings.Contains(got, "conflict") {
		t.Fatalf("APIError.Error() = %q", got)
	}
}

func testRequest(t *testing.T) protocol.SealedJobRequest {
	t.Helper()
	sealed, err := protocol.SealJobRequest(protocol.JobRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.JobRequestKind,
		Metadata: protocol.JobRequestMetadata{Namespace: "research", Name: "test-job"},
		Spec: protocol.JobRequestSpec{
			Workload: protocol.WorkloadBinding{Document: protocol.Workload{
				APIVersion: protocol.V1Alpha1, Kind: protocol.WorkloadKind,
				Spec: protocol.WorkloadSpec{
					Command: protocol.Command{Executable: "echo", Args: []string{"hello"}},
					Runtime: protocol.Runtime{Kind: "native"},
					Policy:  protocol.ExecutionPolicy{Retry: protocol.RetryPolicy{MaxRuns: 1}, DuplicateRisk: "reject"},
				},
			}},
			Placement: protocol.Placement{Target: "workstation-a"},
		},
	})
	if err != nil {
		t.Fatalf("SealJobRequest() error = %v", err)
	}

	return sealed
}

func testJob(phase string) Job {
	timestamp := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	return Job{
		APIVersion: apiVersion, Kind: "Job",
		Metadata: JobMetadata{
			ID: testJobID, Namespace: "research", Name: "test-job", Revision: 1,
			CreatedAt: timestamp, UpdatedAt: timestamp,
		},
		Spec: JobSpec{
			WorkloadDigest: "sha256:" + strings.Repeat("a", 64),
			Placement:      JobPlacement{Target: "workstation-a"},
		},
		Status: JobStatus{Phase: phase, DesiredState: "run"},
	}
}

func testTarget() Target {
	timestamp := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	return Target{
		APIVersion: apiVersion, Kind: "Target",
		Metadata: TargetMetadata{
			ID: testTargetID, GenerationID: "33333333-3333-4333-8333-333333333333",
			Generation: 1, Namespace: "research", Name: "workstation-a", Revision: 1,
			CreatedAt: timestamp, UpdatedAt: timestamp,
		},
		Spec: TargetSpec{
			Kind: "host", ExecutionBackend: "subprocess", ControlTransport: "agent-api",
			Runtimes: []string{"native"},
		},
		Status: TargetStatus{State: "active"},
	}
}

func writeTestJSON(t *testing.T, writer io.Writer, value any) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

type closeErrorBody struct{ io.Reader }

func (closeErrorBody) Close() error { return errors.New("close failed") }

type handlerRoundTripper struct{ handler http.Handler }

func (transport handlerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	transport.handler.ServeHTTP(recorder, request)
	response := recorder.Result()
	response.Request = request

	return response, nil
}

func testCertificatePEM(t *testing.T) []byte {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate certificate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Jobman Control Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, template, &privateKey.PublicKey, privateKey,
	)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded})
}
