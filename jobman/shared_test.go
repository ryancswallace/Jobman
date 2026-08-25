package jobman

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/controlclient"
	"github.com/ryancswallace/jobman/protocol"
)

const sharedTestOperationID = "44444444-4444-4444-8444-444444444444"

type fakeSharedControl struct {
	submitResult     controlclient.Job
	submitErr        error
	submitted        protocol.SealedJobRequest
	submitKey        string
	listResult       controlclient.JobPage
	listErr          error
	listLimit        int
	listPhase        string
	listToken        string
	getResults       []controlclient.Job
	getErr           error
	getIDs           []string
	cancelResult     controlclient.Job
	cancelErr        error
	cancelID         string
	cancelKey        string
	targets          []controlclient.Target
	targetsErr       error
	target           controlclient.Target
	targetErr        error
	targetName       string
	targetState      string
	targetRev        int64
	targetKey        string
	logs             controlclient.LogManifest
	logResults       []controlclient.LogManifest
	logsErr          error
	artifacts        controlclient.ArtifactManifest
	artifactsErr     error
	collectionResult controlclient.Collection
	collectionErr    error
	collection       protocol.SealedCollectionRequest
	collectionKey    string
	collectionID     string
	graphResult      controlclient.Graph
	graphErr         error
	graph            protocol.SealedGraphRequest
	graphKey         string
	graphID          string
	historyRequest   controlclient.CompletedHistoryImportRequest
	historyDryRun    bool
	historyKey       string
}

func (client *fakeSharedControl) SubmitJob(
	_ context.Context,
	request protocol.SealedJobRequest,
	key string,
) (controlclient.Job, error) {
	client.submitted = request
	client.submitKey = key

	return client.submitResult, client.submitErr
}

func (client *fakeSharedControl) SubmitCollection(
	_ context.Context,
	request protocol.SealedCollectionRequest,
	key string,
) (controlclient.Collection, error) {
	client.collection = request
	client.collectionKey = key

	return client.collectionResult, client.collectionErr
}

func (client *fakeSharedControl) GetCollection(
	_ context.Context,
	id string,
) (controlclient.Collection, error) {
	client.collectionID = id

	return client.collectionResult, client.collectionErr
}

func (client *fakeSharedControl) SubmitGraph(
	_ context.Context,
	request protocol.SealedGraphRequest,
	key string,
) (controlclient.Graph, error) {
	client.graph = request
	client.graphKey = key

	return client.graphResult, client.graphErr
}

func (client *fakeSharedControl) GetGraph(_ context.Context, id string) (controlclient.Graph, error) {
	client.graphID = id

	return client.graphResult, client.graphErr
}

func (client *fakeSharedControl) CancelGraph(
	_ context.Context,
	id,
	key string,
) (controlclient.Graph, error) {
	client.graphID = id
	client.graphKey = key

	return client.graphResult, client.graphErr
}

func (client *fakeSharedControl) ImportCompletedHistory(
	_ context.Context,
	request controlclient.CompletedHistoryImportRequest,
	dryRun bool,
	key string,
) (controlclient.Job, error) {
	client.historyRequest = request
	client.historyDryRun = dryRun
	client.historyKey = key

	return client.submitResult, client.submitErr
}

func (client *fakeSharedControl) ListJobs(
	_ context.Context,
	limit int,
	phase,
	token string,
) (controlclient.JobPage, error) {
	client.listLimit = limit
	client.listPhase = phase
	client.listToken = token

	return client.listResult, client.listErr
}

func (client *fakeSharedControl) GetJob(_ context.Context, id string) (controlclient.Job, error) {
	client.getIDs = append(client.getIDs, id)
	if client.getErr != nil {
		return controlclient.Job{}, client.getErr
	}
	if len(client.getResults) == 0 {
		return sharedTestJob("running", ""), nil
	}
	result := client.getResults[0]
	if len(client.getResults) > 1 {
		client.getResults = client.getResults[1:]
	}

	return result, nil
}

func (client *fakeSharedControl) GetJobLogs(_ context.Context, _ string) (controlclient.LogManifest, error) {
	if len(client.logResults) != 0 {
		result := client.logResults[0]
		if len(client.logResults) > 1 {
			client.logResults = client.logResults[1:]
		}
		return result, client.logsErr
	}
	return client.logs, client.logsErr
}

func (client *fakeSharedControl) GetJobArtifacts(
	_ context.Context,
	_ string,
) (controlclient.ArtifactManifest, error) {
	return client.artifacts, client.artifactsErr
}

func TestSharedArtifactsListsLogicalOutputs(t *testing.T) {
	client := &fakeSharedControl{artifacts: controlclient.ArtifactManifest{
		APIVersion: "jobman.control/v1alpha1", Kind: "JobArtifactManifest",
		Namespace: "research", JobID: testJobID,
		Items: []controlclient.PublishedArtifact{{
			ExecutionID: "33333333-3333-4333-8333-333333333333", RunNumber: 1,
			Name: "result", StoreName: "department-nfs", StoreVersion: 3,
			ObjectKey: "research/results/result.txt", ByteLength: 7,
			Checksum: "sha256:" + strings.Repeat("a", 64), PublishedAt: time.Now().UTC(),
		}},
	}}
	output, err := executeShared(
		t, sharedTestDependencies(client, sharedTestConfiguration()), t.TempDir(),
		"shared", "artifacts", testJobID,
	)
	if err != nil || !strings.Contains(output, "artifact://department-nfs/research/results/result.txt") ||
		!strings.Contains(output, "result") {
		t.Fatalf("shared artifacts = %q, %v", output, err)
	}
}

func TestSharedCollectionSubmitAndShow(t *testing.T) {
	job := sharedTestJob("accepted", "")
	collection := controlclient.Collection{
		APIVersion: "jobman.control/v1alpha1", Kind: "Collection",
		Metadata: controlclient.CollectionMetadata{
			ID: "99999999-9999-4999-8999-999999999999", Namespace: "research", Name: "sweep", Revision: 1,
		},
		Spec:   controlclient.CollectionSpec{MaxActive: 1, FailurePolicy: "continue", ArrayPolicy: "prefer"},
		Status: controlclient.CollectionStatus{Phase: "accepted", ArrayMode: "slurm-array", Total: 1},
		Items:  []controlclient.CollectionItem{{Index: 0, Name: "trial", Job: job}},
	}
	client := &fakeSharedControl{collectionResult: collection}
	request := sharedTestRequest(t)
	sealed, err := protocol.SealCollectionRequest(protocol.CollectionRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.CollectionRequestKind,
		Metadata: protocol.CollectionRequestMetadata{Namespace: "research", Name: "sweep"},
		Spec: protocol.CollectionRequestSpec{
			MaxActive: 1, FailurePolicy: "continue", ArrayPolicy: "prefer",
			Items: []protocol.CollectionItem{{
				Name: "trial", Workload: request.Document.Spec.Workload,
				Placement: request.Document.Spec.Placement,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "collection.json")
	if err = os.WriteFile(filename, sealed.CanonicalJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	deps := sharedTestDependencies(client, sharedTestConfiguration())
	output, err := executeShared(t, deps, t.TempDir(), "shared", "collection", "submit", filename)
	if err != nil || !strings.Contains(output, collection.Metadata.ID) ||
		client.collection.Document.Metadata.Name != "sweep" ||
		!strings.HasPrefix(client.collectionKey, "collection-") {
		t.Fatalf("collection submit = %q, %v request=%#v key=%q", output, err, client.collection, client.collectionKey)
	}
	output, err = executeShared(
		t, deps, t.TempDir(), "shared", "collection", "show", "99999999-9999-4999-8999-999999999999",
	)
	if err != nil || !strings.Contains(output, "slurm-array") || client.collectionID != collection.Metadata.ID {
		t.Fatalf("collection show = %q, %v ID=%q", output, err, client.collectionID)
	}
}

func TestSharedGraphAndHistoryCommands(t *testing.T) {
	request := sharedTestRequest(t)
	first := sharedTestJob("accepted", "")
	first.Metadata.Name = "prepare"
	second := sharedTestJob("accepted", "")
	second.Metadata.ID = "77777777-7777-4777-8777-777777777777"
	second.Metadata.Name = "analyze"
	graph := controlclient.Graph{
		APIVersion: "jobman.control/v1alpha1", Kind: "Graph",
		Metadata: controlclient.GraphMetadata{
			ID: "88888888-8888-4888-8888-888888888888", Namespace: "research", Name: "pipeline", Revision: 1,
		},
		Spec:   controlclient.GraphSpec{MaxActive: 1, UnsatisfiedPolicy: "skip"},
		Status: controlclient.GraphStatus{Phase: "accepted", Total: 2, Waiting: 2},
		Items: []controlclient.GraphItem{
			{Index: 0, Name: "prepare", Job: first},
			{
				Index: 1, Name: "analyze", Job: second,
				Dependencies: []controlclient.GraphDependency{{From: "prepare", Predicate: "success", Satisfied: true}},
			},
		},
	}
	client := &fakeSharedControl{graphResult: graph}
	sealedGraph, err := protocol.SealGraphRequest(protocol.GraphRequest{
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
		t.Fatal(err)
	}
	graphFile := filepath.Join(t.TempDir(), "graph.json")
	if err = os.WriteFile(graphFile, sealedGraph.CanonicalJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	deps := sharedTestDependencies(client, sharedTestConfiguration())
	output, err := executeShared(t, deps, t.TempDir(), "shared", "graph", "submit", graphFile)
	if err != nil || !strings.Contains(output, graph.Metadata.ID) ||
		client.graph.Document.Metadata.Name != "pipeline" || !strings.HasPrefix(client.graphKey, "graph-") {
		t.Fatalf("graph submit = %q, %v graph=%#v key=%q", output, err, client.graph, client.graphKey)
	}
	output, err = executeShared(t, deps, t.TempDir(), "shared", "graph", "show", graph.Metadata.ID)
	if err != nil || !strings.Contains(output, "1 (1 satisfied)") || client.graphID != graph.Metadata.ID {
		t.Fatalf("graph show = %q, %v ID=%q", output, err, client.graphID)
	}
	output, err = executeShared(t, deps, t.TempDir(), "shared", "graph", "show", "--json", graph.Metadata.ID)
	if err != nil || !strings.Contains(output, `"kind":"Graph"`) {
		t.Fatalf("graph show JSON = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, t.TempDir(), "shared", "graph", "cancel", graph.Metadata.ID)
	if err != nil || !strings.HasPrefix(client.graphKey, "graph-cancel-") {
		t.Fatalf("graph cancel = %q, %v key=%q", output, err, client.graphKey)
	}

	history := controlclient.CompletedHistoryImportRequest{
		APIVersion: "jobman.control/v1alpha1", Kind: "CompletedHistoryImport",
		Metadata: protocol.JobRequestMetadata{Namespace: "research", Name: "imported-job"},
		Spec: controlclient.CompletedHistoryImportSpec{
			Outcome: "success", CompletedAt: time.Now().UTC().Add(-time.Hour),
			Source:   controlclient.CompletedHistorySource{Store: "sqlite", Schema: 8, JobID: "local-job-1"},
			Workload: request.Document.Spec.Workload, Placement: request.Document.Spec.Placement,
		},
	}
	encodedHistory, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	historyFile := filepath.Join(t.TempDir(), "history.json")
	if err = os.WriteFile(historyFile, encodedHistory, 0o600); err != nil {
		t.Fatal(err)
	}
	client.submitResult = sharedTestJob("terminal", "success")
	client.submitResult.Metadata.Name = "imported-job"
	output, err = executeShared(
		t, deps, t.TempDir(), "shared", "history", "import", "--dry-run", "--json", historyFile,
	)
	if err != nil || !strings.Contains(output, `"kind":"CompletedHistoryImportPlan"`) || !client.historyDryRun {
		t.Fatalf("history dry run = %q, %v request=%#v", output, err, client.historyRequest)
	}
	output, err = executeShared(t, deps, t.TempDir(), "shared", "history", "import", historyFile)
	if err != nil || !strings.Contains(output, "terminal\tsuccess") || client.historyDryRun ||
		!strings.HasPrefix(client.historyKey, "history-import-") {
		t.Fatalf("history import = %q, %v dryRun=%v key=%q", output, err, client.historyDryRun, client.historyKey)
	}

	history.Metadata.Namespace = "other"
	encodedHistory, err = json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(historyFile, encodedHistory, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = executeShared(t, deps, t.TempDir(), "shared", "history", "import", historyFile); err == nil ||
		!strings.Contains(err.Error(), "namespace does not match") {
		t.Fatalf("mismatched history error = %v", err)
	}
}

func TestSharedLogsReadsAndFollowsVerifiedChunks(t *testing.T) {
	root := t.TempDir()
	store, err := artifact.NewFilesystemStore("department-nfs", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	firstKey := "namespaces/research/jobs/11111111-1111-4111-8111-111111111111/executions/33333333-3333-4333-8333-333333333333/logs/stdout/00000001.chunk"
	secondKey := "namespaces/research/jobs/11111111-1111-4111-8111-111111111111/executions/33333333-3333-4333-8333-333333333333/logs/stdout/00000002.chunk"
	firstDigest, err := store.PutImmutable(firstKey, []byte("first\n"))
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := store.PutImmutable(secondKey, []byte("second\n"))
	if err != nil {
		t.Fatal(err)
	}
	captured := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	first := controlclient.LogChunk{
		Sequence: 1, StoreName: "department-nfs", StoreVersion: 1, ObjectKey: firstKey,
		ByteLength: 6, Checksum: firstDigest, CapturedAt: captured,
	}
	second := controlclient.LogChunk{
		Sequence: 2, StoreName: "department-nfs", StoreVersion: 1, ObjectKey: secondKey,
		ByteOffset: 6, ByteLength: 7, Checksum: secondDigest, CapturedAt: captured.Add(-time.Second),
	}
	manifest := func(state string, length int64, chunks []controlclient.LogChunk) controlclient.LogManifest {
		return controlclient.LogManifest{
			APIVersion: "jobman.control/v1alpha1", Kind: "JobLogManifest",
			Namespace: "research", JobID: testJobID,
			Items: []controlclient.LogStream{{
				ExecutionID: "33333333-3333-4333-8333-333333333333", RunNumber: 1,
				Stream: "stdout", State: state, ByteLength: length, Chunks: chunks,
			}},
		}
	}
	configuration := sharedTestConfiguration()
	profile := configuration.Shared.Profiles["department"]
	profile.ArtifactRoots = map[string]config.SharedArtifactRoot{
		"department-nfs": {Version: 1, Path: root},
	}
	configuration.Shared.Profiles["department"] = profile
	client := &fakeSharedControl{logs: manifest("complete", 13, []controlclient.LogChunk{first, second})}
	output, err := executeShared(
		t, sharedTestDependencies(client, configuration), t.TempDir(),
		"shared", "logs", "--stream", "stdout", testJobID,
	)
	if err != nil || output != "first\nsecond\n" {
		t.Fatalf("shared logs = %q, %v", output, err)
	}
	client.logResults = []controlclient.LogManifest{
		manifest("capturing", 6, []controlclient.LogChunk{first}),
		manifest("complete", 13, []controlclient.LogChunk{first, second}),
	}
	output, err = executeShared(
		t, sharedTestDependencies(client, configuration), t.TempDir(),
		"shared", "logs", "--follow", "--poll-interval", "1ms", "--stream", "stdout", testJobID,
	)
	if err != nil || output != "first\nsecond\n" {
		t.Fatalf("shared logs follow = %q, %v", output, err)
	}
	client.logResults = []controlclient.LogManifest{manifest("capturing", 6, []controlclient.LogChunk{first})}
	client.getResults = []controlclient.Job{sharedTestJob("terminal", "success")}
	if _, err = executeShared(
		t, sharedTestDependencies(client, configuration), t.TempDir(),
		"shared", "logs", "--follow", "--poll-interval", "1ms", "--stream", "stdout", testJobID,
	); err == nil || !strings.Contains(err.Error(), "before log publication completed") {
		t.Fatalf("shared logs incomplete terminal error = %v", err)
	}
}

func (client *fakeSharedControl) CancelJob(
	_ context.Context,
	id,
	key string,
) (controlclient.Job, error) {
	client.cancelID = id
	client.cancelKey = key

	return client.cancelResult, client.cancelErr
}

func (client *fakeSharedControl) ListTargets(context.Context) ([]controlclient.Target, error) {
	return client.targets, client.targetsErr
}

func (client *fakeSharedControl) GetTarget(_ context.Context, name string) (controlclient.Target, error) {
	client.targetName = name

	return client.target, client.targetErr
}

func (client *fakeSharedControl) UpdateTargetState(
	_ context.Context,
	name,
	state string,
	revision int64,
	key string,
) (controlclient.Target, error) {
	client.targetName = name
	client.targetState = state
	client.targetRev = revision
	client.targetKey = key

	return client.target, client.targetErr
}

func TestSharedCommandLifecycle(t *testing.T) {
	client := &fakeSharedControl{
		submitResult: sharedTestJob("accepted", ""),
		listResult: controlclient.JobPage{
			APIVersion: "jobman.control/v1alpha1", Kind: "JobList",
			Items: []controlclient.Job{sharedTestJob("running", "")}, NextPageToken: "next-page",
		},
		getResults:   []controlclient.Job{sharedTestJob("running", "")},
		cancelResult: sharedTestJob("running", ""),
		targets:      []controlclient.Target{sharedTestTarget()},
		target:       sharedTestTarget(),
	}
	client.cancelResult.Status.DesiredState = "cancel"
	stateDir := t.TempDir()
	deps := sharedTestDependencies(client, sharedTestConfiguration())

	output, err := executeShared(t, deps, stateDir,
		"shared", "run", "--name", "analysis", "--target", "workstation-a",
		"--env", "MODE=batch", "--timeout", "2m", "--cpu", "4", "--memory", "8GiB",
		"--gpu", "1", "--nodes", "1", "--tasks", "2", "--wall-time", "30m",
		"--container-image", "registry.example.edu/research/analyze@sha256:0123456789abcdef",
		"--container-pull-policy", "never", "--container-network", "none",
		"--input", "sample=artifact://department-nfs/research/sample=inputs:/sample",
		"--output", "result=outputs:/result=artifact://department-nfs/research/result=required",
		"--", "analyze", "--input", "sample.dat",
	)
	if err != nil || !strings.Contains(output, testJobID) {
		t.Fatalf("shared run = %q, %v", output, err)
	}
	if client.submitted.Document.Spec.Workload.Document.Spec.Command.Executable != "analyze" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Environment.Values["MODE"] != "batch" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Resources == nil ||
		client.submitted.Document.Spec.Workload.Document.Spec.Resources.CPU != 4 ||
		client.submitted.Document.Spec.Workload.Document.Spec.Resources.Memory != "8GiB" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Resources.GPU != 1 ||
		client.submitted.Document.Spec.Workload.Document.Spec.Resources.WallTime != "30m" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Runtime.Kind != "container" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Runtime.Container == nil ||
		client.submitted.Document.Spec.Workload.Document.Spec.Runtime.Container.PullPolicy != "never" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Runtime.Container.Network != "none" ||
		client.submitted.Document.Spec.Workload.Document.Spec.Artifacts == nil ||
		len(client.submitted.Document.Spec.Workload.Document.Spec.Artifacts.Inputs) != 1 ||
		!client.submitted.Document.Spec.Workload.Document.Spec.Artifacts.Outputs[0].Required ||
		client.submitted.Document.Spec.Placement.Target != "workstation-a" ||
		!strings.HasPrefix(client.submitKey, "submit-") {
		t.Fatalf("submitted request = %#v key=%q", client.submitted.Document, client.submitKey)
	}
	operations, err := listSharedOperations(stateDir)
	if err != nil || len(operations) != 1 || operations[0].Status != "completed" || operations[0].JobID != testJobID {
		t.Fatalf("operations after run = %#v, %v", operations, err)
	}
	operationID := operations[0].ID

	output, err = executeShared(t, deps, stateDir,
		"shared", "run", "--resume", operationID, "--json",
	)
	if err != nil || !strings.Contains(output, `"operation_id":"`+operationID+`"`) ||
		client.getIDs[len(client.getIDs)-1] != testJobID {
		t.Fatalf("resume completed = %q, %v IDs=%v", output, err, client.getIDs)
	}

	output, err = executeShared(t, deps, stateDir,
		"shared", "list", "--limit", "7", "--phase", "running", "--page-token", "prior",
	)
	if err != nil || !strings.Contains(output, "NEXT_PAGE_TOKEN") || client.listLimit != 7 ||
		client.listPhase != "running" || client.listToken != "prior" {
		t.Fatalf("shared list = %q, %v request=(%d,%q,%q)", output, err, client.listLimit, client.listPhase, client.listToken)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "list", "--json")
	if err != nil || !strings.Contains(output, `"kind":"JobList"`) {
		t.Fatalf("shared list JSON = %q, %v", output, err)
	}

	output, err = executeShared(t, deps, stateDir, "shared", "status", testJobID)
	if err != nil || !strings.Contains(output, "running") {
		t.Fatalf("shared status = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "show", testJobID)
	if err != nil || !strings.Contains(output, `"kind":"Job"`) {
		t.Fatalf("shared show = %q, %v", output, err)
	}

	output, err = executeShared(t, deps, stateDir, "shared", "cancel", testJobID)
	if err != nil || !strings.Contains(output, "cancel") || client.cancelID != testJobID ||
		!strings.HasPrefix(client.cancelKey, "cancel-") {
		t.Fatalf("shared cancel = %q, %v ID=%q key=%q", output, err, client.cancelID, client.cancelKey)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "cancel", "--json", testJobID)
	if err != nil || !strings.Contains(output, `"desiredState":"cancel"`) {
		t.Fatalf("shared cancel JSON = %q, %v", output, err)
	}

	output, err = executeShared(t, deps, stateDir, "shared", "target", "list")
	if err != nil || !strings.Contains(output, "workstation-a") {
		t.Fatalf("shared target list = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "target", "list", "--json")
	if err != nil || !strings.Contains(output, `"targets"`) {
		t.Fatalf("shared target list JSON = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "target", "show", "workstation-a")
	if err != nil || !strings.Contains(output, "gpu (default)") || client.targetName != "workstation-a" {
		t.Fatalf("shared target show = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, stateDir,
		"shared", "target", "show", "--json", "workstation-a",
	)
	if err != nil || !strings.Contains(output, `"kind":"Target"`) {
		t.Fatalf("shared target show JSON = %q, %v", output, err)
	}
	client.target.Status.State = "draining"
	client.target.Metadata.Revision = 2
	output, err = executeShared(t, deps, stateDir,
		"shared", "target", "state", "--revision", "1", "workstation-a", "draining",
	)
	if err != nil || !strings.Contains(output, "draining") || client.targetName != "workstation-a" ||
		client.targetState != "draining" || client.targetRev != 1 ||
		!strings.HasPrefix(client.targetKey, "target-state-") {
		t.Fatalf(
			"shared target state = %q, %v request=(%q,%q,%d,%q)",
			output, err, client.targetName, client.targetState, client.targetRev, client.targetKey,
		)
	}

	output, err = executeShared(t, deps, stateDir, "shared", "operations")
	if err != nil || !strings.Contains(output, operationID) {
		t.Fatalf("shared operations = %q, %v", output, err)
	}
	output, err = executeShared(t, deps, stateDir, "shared", "operations", "--json")
	if err != nil || !strings.Contains(output, `"operations"`) {
		t.Fatalf("shared operations JSON = %q, %v", output, err)
	}
}

func TestSharedPendingOperationResume(t *testing.T) {
	stateDir := t.TempDir()
	sealed := sharedTestRequest(t)
	operation, err := createSharedOperation(stateDir, "department", "research", sealed)
	if err != nil {
		t.Fatalf("createSharedOperation() error = %v", err)
	}
	client := &fakeSharedControl{submitResult: sharedTestJob("accepted", "")}
	output, err := executeShared(
		t, sharedTestDependencies(client, sharedTestConfiguration()), stateDir,
		"shared", "run", "--resume", operation.ID,
	)
	if err != nil || !strings.Contains(output, operation.ID) || client.submitKey != operation.IdempotencyKey {
		t.Fatalf("resume pending = %q, %v key=%q", output, err, client.submitKey)
	}
	loaded, err := loadSharedOperation(stateDir, operation.ID)
	if err != nil || loaded.Status != "completed" || loaded.JobID != testJobID {
		t.Fatalf("completed operation = %#v, %v", loaded, err)
	}
}

func TestSharedWaitOutcomes(t *testing.T) {
	stateDir := t.TempDir()
	configuration := sharedTestConfiguration()
	client := &fakeSharedControl{getResults: []controlclient.Job{
		sharedTestJob("running", ""), sharedTestJob("terminal", "success"),
	}}
	output, err := executeShared(
		t, sharedTestDependencies(client, configuration), stateDir,
		"shared", "wait", "--poll-interval", "1ns", testJobID,
	)
	if err != nil || !strings.Contains(output, "success") || len(client.getIDs) != 2 {
		t.Fatalf("shared wait success = %q, %v calls=%d", output, err, len(client.getIDs))
	}
	client.getResults = []controlclient.Job{sharedTestJob("terminal", "failure")}
	output, err = executeShared(
		t, sharedTestDependencies(client, configuration), stateDir,
		"shared", "wait", "--json", testJobID,
	)
	var outcomeError sharedOutcomeError
	if !errors.As(err, &outcomeError) || !outcomeError.Silent() ||
		!strings.Contains(outcomeError.Error(), "failure") || !strings.Contains(output, `"outcome":"failure"`) {
		t.Fatalf("shared wait failure = %q, %T %v", output, err, err)
	}
}

func TestSharedCommandValidationAndErrors(t *testing.T) {
	stateDir := t.TempDir()
	base := sharedTestConfiguration()
	client := &fakeSharedControl{submitResult: sharedTestJob("accepted", "")}
	tests := []struct {
		name          string
		configuration config.Config
		dependencies  dependencies
		arguments     []string
		want          string
	}{
		{
			name: "no profile", configuration: func() config.Config {
				value := base
				value.Shared.CurrentProfile = ""
				return value
			}(), arguments: []string{"shared", "list"}, want: "select a shared profile",
		},
		{
			name: "unknown profile", configuration: base,
			arguments: []string{"shared", "--profile", "missing", "list"}, want: "unknown shared profile",
		},
		{name: "missing target", configuration: base, arguments: []string{"shared", "run", "--", "echo"}, want: "--target is required"},
		{
			name: "duplicate environment", configuration: base,
			arguments: []string{"shared", "run", "--target", "workstation-a", "--env", "A=1", "--env", "A=2", "--", "echo"},
			want:      "duplicate name",
		},
		{
			name: "malformed environment", configuration: base,
			arguments: []string{"shared", "run", "--target", "workstation-a", "--env", "BAD", "--", "echo"},
			want:      "NAME=VALUE",
		},
		{
			name: "malformed input", configuration: base,
			arguments: []string{"shared", "run", "--target", "workstation-a", "--input", "bad", "--", "echo"},
			want:      "--input must be",
		},
		{
			name: "malformed output", configuration: base,
			arguments: []string{"shared", "run", "--target", "workstation-a", "--output", "a=outputs:/a=artifact://s/a=maybe", "--", "echo"},
			want:      "disposition",
		},
		{
			name: "container policy without image", configuration: base,
			arguments: []string{"shared", "run", "--target", "workstation-a", "--container-network", "none", "--", "echo"},
			want:      "require --container-image",
		},
		{name: "list limit", configuration: base, arguments: []string{"shared", "list", "--limit", "0"}, want: "--limit"},
		{
			name: "wait bounds", configuration: base,
			arguments: []string{"shared", "wait", "--poll-interval", "0s", testJobID}, want: "must be positive",
		},
		{
			name: "target state revision", configuration: base,
			arguments: []string{"shared", "target", "state", "workstation-a", "draining"}, want: "required flag",
		},
		{
			name: "target state value", configuration: base,
			arguments: []string{"shared", "target", "state", "--revision", "1", "workstation-a", "broken"},
			want:      "STATE must be",
		},
		{
			name: "resume with command", configuration: base,
			arguments: []string{"shared", "run", "--resume", sharedTestOperationID, "echo"}, want: "does not accept",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := test.dependencies
			if deps.LoadConfig == nil {
				deps = sharedTestDependencies(client, test.configuration)
			}
			_, err := executeShared(t, deps, stateDir, test.arguments...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	_, err := executeShared(t, dependencies{}, stateDir, "shared", "list")
	if err == nil || !strings.Contains(err.Error(), "configuration loader") {
		t.Fatalf("missing loader error = %v", err)
	}
	deps := sharedTestDependencies(client, base)
	deps.OpenControl = nil
	_, err = executeShared(t, deps, stateDir, "shared", "list")
	if err == nil || !strings.Contains(err.Error(), "client is unavailable") {
		t.Fatalf("missing client error = %v", err)
	}
	deps = sharedTestDependencies(client, base)
	deps.OpenControl = func(config.SharedProfile) (sharedControlClient, error) {
		return nil, errors.New("construction failed")
	}
	_, err = executeShared(t, deps, stateDir, "shared", "list")
	if err == nil || !strings.Contains(err.Error(), "construction failed") {
		t.Fatalf("client construction error = %v", err)
	}
	deps = sharedTestDependencies(client, base)
	deps.LoadConfig = func(*rootOptions) (config.Loaded, error) {
		return config.Loaded{}, errors.New("load failed")
	}
	_, err = executeShared(t, deps, stateDir, "shared", "list")
	if err == nil || !strings.Contains(err.Error(), "load failed") {
		t.Fatalf("load error = %v", err)
	}
}

func TestSharedOperationJournalValidation(t *testing.T) {
	stateDir := t.TempDir()
	request := sharedTestRequest(t)
	operation, err := createSharedOperation(stateDir, "department", "research", request)
	if err != nil {
		t.Fatalf("createSharedOperation() error = %v", err)
	}
	if !sharedOperationIDPattern.MatchString(operation.ID) || operation.Status != "pending" {
		t.Fatalf("operation = %#v", operation)
	}
	path, err := sharedOperationPath(stateDir, operation.ID)
	if err != nil {
		t.Fatalf("sharedOperationPath() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("operation permissions = %v, %v", info, err)
	}
	loaded, err := loadSharedOperation(stateDir, operation.ID)
	if err != nil || loaded.RequestDigest != request.RequestDigest {
		t.Fatalf("loadSharedOperation() = %#v, %v", loaded, err)
	}
	if err = completeSharedOperation(stateDir, &operation, testJobID); err != nil {
		t.Fatalf("completeSharedOperation() error = %v", err)
	}
	summaries, err := listSharedOperations(stateDir)
	if err != nil || len(summaries) != 1 || summaries[0].Status != "completed" {
		t.Fatalf("listSharedOperations() = %#v, %v", summaries, err)
	}
	if empty, emptyErr := listSharedOperations(filepath.Join(stateDir, "missing")); emptyErr != nil || len(empty) != 0 {
		t.Fatalf("listSharedOperations(missing) = %#v, %v", empty, emptyErr)
	}
	if _, err = sharedOperationPath(stateDir, "../escape"); err == nil {
		t.Fatal("sharedOperationPath() accepted traversal")
	}
	if _, err = loadSharedOperation(stateDir, sharedTestOperationID); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("load missing error = %v", err)
	}

	corrupt := operation
	corrupt.Status = "invalid"
	if err = validateSharedOperation(corrupt, corrupt.ID); err == nil {
		t.Fatal("validateSharedOperation() accepted invalid status")
	}
	corrupt = operation
	corrupt.JobID = ""
	if err = validateSharedOperation(corrupt, corrupt.ID); err == nil {
		t.Fatal("validateSharedOperation() accepted inconsistent completion")
	}
	corrupt = operation
	corrupt.RequestDigest = "sha256:" + strings.Repeat("0", 64)
	if err = validateSharedOperation(corrupt, corrupt.ID); err == nil {
		t.Fatal("validateSharedOperation() accepted request mismatch")
	}
	corrupt = operation
	corrupt.Version = 99
	if err = validateSharedOperation(corrupt, corrupt.ID); err == nil {
		t.Fatal("validateSharedOperation() accepted version")
	}

	encoded, err := json.Marshal(operation)
	if err != nil {
		t.Fatalf("marshal operation: %v", err)
	}
	if err = os.WriteFile(path, append(encoded, []byte(" {}")...), 0o600); err != nil {
		t.Fatalf("write trailing operation: %v", err)
	}
	if _, err = loadSharedOperation(stateDir, operation.ID); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("load trailing error = %v", err)
	}
}

func TestSharedHelpersAndWriterFailures(t *testing.T) {
	for input, want := range map[string]string{
		"/usr/bin/My Tool": "my-tool", "***": "job", strings.Repeat("a", 140): strings.Repeat("a", 128),
	} {
		if got := portableJobName(input); got != want {
			t.Fatalf("portableJobName(%q) = %q, want %q", input, got, want)
		}
	}
	if environment, err := parseSharedEnvironment([]string{"A=1", "EMPTY="}); err != nil ||
		environment.Values["EMPTY"] != "" {
		t.Fatalf("parseSharedEnvironment() = %#v, %v", environment, err)
	}
	if environment, err := parseSharedEnvironment(nil); err != nil || environment == nil {
		t.Fatalf("parseSharedEnvironment(nil) = %#v, %v", environment, err)
	}
	command := newRootCommand(sharedTestDependencies(&fakeSharedControl{}, sharedTestConfiguration()))
	command.SetOut(errorWriter{})
	if err := writeSharedRunResult(command, sharedTestJob("accepted", ""), sharedTestOperationID, false); err == nil {
		t.Fatal("writeSharedRunResult() write error = nil")
	}
	if err := writeSharedJobPage(command, controlclient.JobPage{}, false); err == nil {
		t.Fatal("writeSharedJobPage() write error = nil")
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	waitCommand := newRootCommand(sharedTestDependencies(&fakeSharedControl{}, sharedTestConfiguration()))
	waitCommand.SetContext(canceled)
	err := runSharedWait(waitCommand, sharedContext{
		client: &fakeSharedControl{}, profile: "department", namespace: "research", stateDir: t.TempDir(),
	}, testJobID, &sharedWaitOptions{pollInterval: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("runSharedWait(canceled) error = %v", err)
	}
}

func executeShared(
	t *testing.T,
	dependencies dependencies,
	stateDir string,
	arguments ...string,
) (string, error) {
	t.Helper()
	command := newRootCommand(dependencies)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(io.Discard)
	command.SetIn(strings.NewReader(""))
	command.SetArgs(append([]string{"--state-dir", stateDir}, arguments...))
	err := command.ExecuteContext(t.Context())

	return output.String(), err
}

func sharedTestDependencies(client sharedControlClient, configuration config.Config) dependencies {
	return dependencies{
		LoadConfig: func(*rootOptions) (config.Loaded, error) {
			return config.Loaded{Config: configuration}, nil
		},
		OpenControl: func(profile config.SharedProfile) (sharedControlClient, error) {
			if profile.Namespace != "research" {
				return nil, errors.New("unexpected profile")
			}
			return client, nil
		},
	}
}

func sharedTestConfiguration() config.Config {
	configuration := config.Default()
	configuration.Shared.CurrentProfile = "department"
	configuration.Shared.Profiles["department"] = config.SharedProfile{
		Endpoint: "https://control.example", Namespace: "research",
	}

	return configuration
}

func sharedTestRequest(t *testing.T) protocol.SealedJobRequest {
	t.Helper()
	sealed, err := protocol.SealJobRequest(protocol.JobRequest{
		APIVersion: protocol.V1Alpha1, Kind: protocol.JobRequestKind,
		Metadata: protocol.JobRequestMetadata{Namespace: "research", Name: "analysis"},
		Spec: protocol.JobRequestSpec{
			Workload: protocol.WorkloadBinding{Document: protocol.Workload{
				APIVersion: protocol.V1Alpha1, Kind: protocol.WorkloadKind,
				Spec: protocol.WorkloadSpec{
					Command: protocol.Command{Executable: "analyze"},
					Runtime: protocol.Runtime{Kind: "native"},
					Policy: protocol.ExecutionPolicy{
						Retry: protocol.RetryPolicy{MaxRuns: 1}, DuplicateRisk: "reject",
					},
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

func sharedTestJob(phase, outcome string) controlclient.Job {
	timestamp := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	return controlclient.Job{
		APIVersion: "jobman.control/v1alpha1", Kind: "Job",
		Metadata: controlclient.JobMetadata{
			ID: testJobID, Namespace: "research", Name: "analysis", Revision: 1,
			CreatedAt: timestamp, UpdatedAt: timestamp,
		},
		Spec: controlclient.JobSpec{
			WorkloadDigest: "sha256:" + strings.Repeat("a", 64),
			Placement: controlclient.JobPlacement{
				Target: "workstation-a", TargetID: "11111111-1111-4111-8111-111111111111",
				TargetGenerationID: "22222222-2222-4222-8222-222222222222",
				ExecutionBackend:   "subprocess",
			},
		},
		Status: controlclient.JobStatus{Phase: phase, DesiredState: "run", Outcome: outcome},
	}
}

func sharedTestTarget() controlclient.Target {
	timestamp := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	return controlclient.Target{
		APIVersion: "jobman.control/v1alpha1", Kind: "Target",
		Metadata: controlclient.TargetMetadata{
			ID:           "33333333-3333-4333-8333-333333333333",
			GenerationID: "44444444-4444-4444-8444-444444444444", Generation: 1,
			Namespace: "research", Name: "workstation-a", Revision: 1,
			CreatedAt: timestamp, UpdatedAt: timestamp,
		},
		Spec: controlclient.TargetSpec{
			Kind: "host", ExecutionBackend: "subprocess", ControlTransport: "agent-api",
			Runtimes:   []string{"native"},
			Partitions: []controlclient.Partition{{Name: "gpu", IsDefault: true}},
		},
		Status: controlclient.TargetStatus{State: "active"},
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
