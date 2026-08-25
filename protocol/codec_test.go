package protocol

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSealWorkloadNormalizesWithoutMutatingInput(t *testing.T) {
	t.Parallel()
	input := Workload{
		APIVersion: V1Alpha1,
		Kind:       WorkloadKind,
		Metadata:   WorkloadMetadata{Name: "minimal"},
		Spec: WorkloadSpec{
			Command: Command{Executable: "true"},
		},
	}

	sealed, err := SealWorkload(input)
	if err != nil {
		t.Fatalf("SealWorkload() error = %v", err)
	}
	if input.Spec.WorkingDirectory != "" || input.Spec.Runtime.Kind != "" ||
		input.Spec.Policy.Retry.MaxRuns != 0 {
		t.Fatal("SealWorkload() mutated its input")
	}
	if sealed.Document.Spec.WorkingDirectory != "workspace:/" {
		t.Fatalf("working directory = %q", sealed.Document.Spec.WorkingDirectory)
	}
	if sealed.Document.Spec.Runtime.Kind != "native" {
		t.Fatalf("runtime kind = %q", sealed.Document.Spec.Runtime.Kind)
	}
	if sealed.Document.Spec.Policy.Retry.MaxRuns != 1 {
		t.Fatalf("max runs = %d", sealed.Document.Spec.Policy.Retry.MaxRuns)
	}
	if sealed.Document.Spec.Policy.DuplicateRisk != "reject" {
		t.Fatalf("duplicate risk = %q", sealed.Document.Spec.Policy.DuplicateRisk)
	}
	if !digestPattern.MatchString(sealed.Digest) {
		t.Fatalf("digest = %q", sealed.Digest)
	}
	sealedAgain, err := SealWorkload(sealed.Document)
	if err != nil {
		t.Fatalf("SealWorkload(sealed) error = %v", err)
	}
	if sealed.Digest != sealedAgain.Digest || !bytes.Equal(sealed.CanonicalJSON, sealedAgain.CanonicalJSON) {
		t.Fatal("sealing is not idempotent")
	}
}

func TestSealWorkloadNormalizesUnorderedSets(t *testing.T) {
	t.Parallel()
	first := fullWorkload()
	first.Spec.Requirements.Capabilities = []string{"network", "gpu"}
	first.Spec.Artifacts.Inputs = []InputArtifact{
		{Name: "zeta", Source: "artifact://data/zeta", Target: "inputs:/zeta"},
		{Name: "alpha", Source: "artifact://data/alpha", Target: "inputs:/alpha"},
	}
	second := fullWorkload()
	second.Spec.Requirements.Capabilities = []string{"gpu", "network"}
	second.Spec.Artifacts.Inputs = []InputArtifact{
		{Name: "alpha", Source: "artifact://data/alpha", Target: "inputs:/alpha"},
		{Name: "zeta", Source: "artifact://data/zeta", Target: "inputs:/zeta"},
	}

	sealedFirst, err := SealWorkload(first)
	if err != nil {
		t.Fatalf("SealWorkload(first) error = %v", err)
	}
	sealedSecond, err := SealWorkload(second)
	if err != nil {
		t.Fatalf("SealWorkload(second) error = %v", err)
	}
	if sealedFirst.Digest != sealedSecond.Digest {
		t.Fatalf("digests differ: %q != %q", sealedFirst.Digest, sealedSecond.Digest)
	}
}

func TestDecodeWorkloadRejectsUnsafeJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		limits  DecodeLimits
		wantErr string
	}{
		{
			name:    "unknown field",
			input:   `{"apiVersion":"jobman/v1alpha1","kind":"Workload","metadata":{},"spec":{"command":{"executable":"true"},"unknown":true}}`,
			wantErr: "unknown field",
		},
		{
			name:    "duplicate key",
			input:   `{"kind":"Workload","kind":"Workload"}`,
			wantErr: "duplicate JSON object key",
		},
		{
			name:    "trailing value",
			input:   `{}` + "\n{}",
			wantErr: "trailing JSON value",
		},
		{
			name:    "bounded bytes",
			input:   `{}`,
			limits:  DecodeLimits{MaxBytes: 1},
			wantErr: "input exceeds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeWorkload(strings.NewReader(test.input), test.limits)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("DecodeWorkload() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestSealWorkloadCanonicalizesExtensionNumbers(t *testing.T) {
	t.Parallel()
	variants := []json.RawMessage{
		json.RawMessage(`{"priority":100}`),
		json.RawMessage(`{"priority":100.0}`),
		json.RawMessage(`{"priority":1e2}`),
		json.RawMessage(`{"priority":0.01e4}`),
	}
	var digest string
	for _, extension := range variants {
		workload := minimalWorkload()
		workload.Spec.Extensions = map[string]json.RawMessage{"example": extension}
		sealed, err := SealWorkload(workload)
		if err != nil {
			t.Fatalf("SealWorkload(%s) error = %v", extension, err)
		}
		if !bytes.Contains(sealed.CanonicalJSON, []byte(`"priority":1e2`)) {
			t.Fatalf("canonical JSON = %s", sealed.CanonicalJSON)
		}
		if digest == "" {
			digest = sealed.Digest
		} else if sealed.Digest != digest {
			t.Fatalf("digest for %s = %q, want %q", extension, sealed.Digest, digest)
		}
	}
}

func TestDecodeWorkloadRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()
	input := append([]byte(`{"apiVersion":"jobman/v1alpha1","kind":"Workload","metadata":{"name":"`), 0xff)
	input = append(input, []byte(`"},"spec":{}}`)...)
	if _, err := DecodeWorkload(bytes.NewReader(input), DecodeLimits{}); err == nil ||
		!strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("DecodeWorkload() error = %v", err)
	}
}

func TestSealJobRequestBindsWorkloadDigest(t *testing.T) {
	t.Parallel()
	request := JobRequest{
		APIVersion: V1Alpha1,
		Kind:       JobRequestKind,
		Metadata: JobRequestMetadata{
			Namespace: "research",
			Name:      "minimal-job",
		},
		Spec: JobRequestSpec{
			Workload:  WorkloadBinding{Document: minimalWorkload()},
			Placement: Placement{Target: "workstation-a"},
		},
	}

	sealed, err := SealJobRequest(request)
	if err != nil {
		t.Fatalf("SealJobRequest() error = %v", err)
	}
	if sealed.WorkloadDigest == "" || sealed.Document.Spec.Workload.Digest != sealed.WorkloadDigest {
		t.Fatalf("workload digest was not bound: %#v", sealed)
	}
	request.Spec.Workload.Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err := SealJobRequest(request); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("SealJobRequest(mismatch) error = %v", err)
	}
}

func TestSealCollectionRequestBindsChildrenAndDefaultsPolicy(t *testing.T) {
	t.Parallel()
	request := CollectionRequest{
		APIVersion: V1Alpha1, Kind: CollectionRequestKind,
		Metadata: CollectionRequestMetadata{Namespace: "research", Name: "sweep"},
		Spec: CollectionRequestSpec{Items: []CollectionItem{
			{Name: "trial-a", Workload: WorkloadBinding{Document: minimalWorkload()}, Placement: Placement{Target: "cluster", Partition: "cpu"}},
			{Name: "trial-b", Workload: WorkloadBinding{Document: minimalWorkload()}, Placement: Placement{Target: "cluster", Partition: "cpu"}},
		}},
	}
	sealed, err := SealCollectionRequest(request)
	if err != nil {
		t.Fatalf("SealCollectionRequest() error = %v", err)
	}
	if sealed.Document.Spec.MaxActive != 1 || sealed.Document.Spec.FailurePolicy != "continue" ||
		sealed.Document.Spec.ArrayPolicy != "prefer" || len(sealed.WorkloadDigests) != 2 ||
		sealed.WorkloadDigests[0] == "" || sealed.Document.Spec.Items[0].Workload.Digest != sealed.WorkloadDigests[0] {
		t.Fatalf("sealed collection = %#v", sealed)
	}
	decoded, err := DecodeCollectionRequest(bytes.NewReader(sealed.CanonicalJSON), DecodeLimits{})
	if err != nil || decoded.RequestDigest != sealed.RequestDigest {
		t.Fatalf("DecodeCollectionRequest() = %#v, %v", decoded, err)
	}
	request.Spec.Items[0].Workload.Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err = SealCollectionRequest(request); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("SealCollectionRequest(mismatch) error = %v", err)
	}
}

func TestCollectionRequestRejectsInvalidPolicyAndChildren(t *testing.T) {
	t.Parallel()
	valid := CollectionRequest{
		APIVersion: V1Alpha1, Kind: CollectionRequestKind,
		Metadata: CollectionRequestMetadata{Namespace: "research", Name: "sweep"},
		Spec: CollectionRequestSpec{
			MaxActive: 1, FailurePolicy: "fail-fast", ArrayPolicy: "require",
			Items: []CollectionItem{{
				Name: "trial", Workload: WorkloadBinding{Document: minimalWorkload()},
				Placement: Placement{Target: "cluster"},
			}},
		},
	}
	for _, mutate := range []func(*CollectionRequest){
		func(value *CollectionRequest) { value.Spec.Items = nil },
		func(value *CollectionRequest) { value.Spec.MaxActive = 2 },
		func(value *CollectionRequest) { value.Spec.FailurePolicy = "ignore" },
		func(value *CollectionRequest) { value.Spec.ArrayPolicy = "always" },
		func(value *CollectionRequest) { value.Spec.Items = append(value.Spec.Items, value.Spec.Items[0]) },
		func(value *CollectionRequest) { value.Spec.Items[0].Placement.Target = "Bad Target" },
	} {
		candidate := valid
		candidate.Spec.Items = append([]CollectionItem(nil), valid.Spec.Items...)
		mutate(&candidate)
		if _, err := SealCollectionRequest(candidate); err == nil {
			t.Fatalf("SealCollectionRequest(%#v) unexpectedly succeeded", candidate)
		}
	}
}

func TestSealGraphRequestBindsNodesDefaultsAndRejectsCycles(t *testing.T) {
	minimal := minimalWorkload()
	request := GraphRequest{
		APIVersion: V1Alpha1, Kind: GraphRequestKind,
		Metadata: GraphRequestMetadata{Namespace: "research", Name: "pipeline"},
		Spec: GraphRequestSpec{
			Nodes: []GraphNode{
				{Name: "prepare", Workload: WorkloadBinding{Document: minimal}, Placement: Placement{Target: "host-a"}},
				{Name: "analyze", Workload: WorkloadBinding{Document: minimal}, Placement: Placement{Target: "slurm-a"}},
			},
			Edges: []GraphEdge{{From: "prepare", To: "analyze", Predicate: "outcomes", Outcomes: []string{"failure", "success"}}},
		},
	}
	sealed, err := SealGraphRequest(request)
	if err != nil {
		t.Fatalf("SealGraphRequest() error = %v", err)
	}
	if sealed.Document.Spec.MaxActive != 1 || sealed.Document.Spec.UnsatisfiedPolicy != "skip" {
		t.Fatalf("graph defaults = %#v", sealed.Document.Spec)
	}
	if got := sealed.Document.Spec.Edges[0].Outcomes; !reflect.DeepEqual(got, []string{"failure", "success"}) {
		t.Fatalf("normalized outcomes = %v", got)
	}
	if len(sealed.WorkloadDigests) != 2 || sealed.WorkloadDigests[0] == "" {
		t.Fatalf("workload digests = %v", sealed.WorkloadDigests)
	}
	decoded, err := DecodeGraphRequest(bytes.NewReader(sealed.CanonicalJSON), DecodeLimits{})
	if err != nil || decoded.RequestDigest != sealed.RequestDigest {
		t.Fatalf("DecodeGraphRequest() = %#v, %v", decoded, err)
	}

	cycle := sealed.Document
	cycle.Spec.Edges = append(cycle.Spec.Edges, GraphEdge{From: "analyze", To: "prepare", Predicate: "success"})
	if _, err = SealGraphRequest(cycle); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("SealGraphRequest(cycle) error = %v", err)
	}
}

func TestGraphRequestRejectsInvalidEdgesAndPolicies(t *testing.T) {
	valid := GraphRequest{
		APIVersion: V1Alpha1, Kind: GraphRequestKind,
		Metadata: GraphRequestMetadata{Namespace: "research", Name: "pipeline"},
		Spec: GraphRequestSpec{
			MaxActive: 1, UnsatisfiedPolicy: "blocked",
			Nodes: []GraphNode{
				{Name: "first", Workload: WorkloadBinding{Document: minimalWorkload()}, Placement: Placement{Target: "host-a"}},
				{Name: "second", Workload: WorkloadBinding{Document: minimalWorkload()}, Placement: Placement{Target: "host-b"}},
			},
			Edges: []GraphEdge{{From: "first", To: "second", Predicate: "success"}},
		},
	}
	for _, mutate := range []func(*GraphRequest){
		func(value *GraphRequest) { value.APIVersion = "future" },
		func(value *GraphRequest) { value.Spec.Nodes = nil },
		func(value *GraphRequest) { value.Spec.MaxActive = 3 },
		func(value *GraphRequest) { value.Spec.UnsatisfiedPolicy = "wait" },
		func(value *GraphRequest) { value.Spec.Nodes[1].Name = "first" },
		func(value *GraphRequest) { value.Spec.Edges[0].From = "missing" },
		func(value *GraphRequest) { value.Spec.Edges[0].To = "first" },
		func(value *GraphRequest) { value.Spec.Edges[0].Predicate = "unknown" },
		func(value *GraphRequest) { value.Spec.Edges[0].Predicate = "outcomes" },
		func(value *GraphRequest) { value.Spec.Edges[0].Outcomes = []string{"success"} },
		func(value *GraphRequest) {
			value.Spec.Edges[0].Predicate = "outcomes"
			value.Spec.Edges[0].Outcomes = []string{"unknown"}
		},
		func(value *GraphRequest) {
			value.Spec.Edges[0].Predicate = "outcomes"
			value.Spec.Edges[0].Outcomes = []string{"success", "success"}
		},
		func(value *GraphRequest) { value.Spec.Edges = append(value.Spec.Edges, value.Spec.Edges[0]) },
	} {
		candidate := valid
		candidate.Spec.Nodes = append([]GraphNode(nil), valid.Spec.Nodes...)
		candidate.Spec.Edges = append([]GraphEdge(nil), valid.Spec.Edges...)
		mutate(&candidate)
		if _, err := SealGraphRequest(candidate); err == nil {
			t.Fatalf("SealGraphRequest(%#v) unexpectedly succeeded", candidate)
		}
	}
	if err := json.Unmarshal(GraphRequestSchema(), new(any)); err != nil {
		t.Fatalf("GraphRequestSchema() is invalid JSON: %v", err)
	}
}

func TestSchemaAccessorsReturnCopies(t *testing.T) {
	t.Parallel()
	first := WorkloadSchema()
	second := WorkloadSchema()
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("WorkloadSchema() returned an empty schema")
	}
	first[0] ^= 0xff
	if bytes.Equal(first, second) {
		t.Fatal("WorkloadSchema() did not return an independent copy")
	}
	var decoded map[string]any
	if err := json.Unmarshal(JobRequestSchema(), &decoded); err != nil {
		t.Fatalf("JobRequestSchema() is invalid JSON: %v", err)
	}
	if err := json.Unmarshal(CollectionRequestSchema(), &decoded); err != nil {
		t.Fatalf("CollectionRequestSchema() is invalid JSON: %v", err)
	}
}

func minimalWorkload() Workload {
	return Workload{
		APIVersion: V1Alpha1,
		Kind:       WorkloadKind,
		Metadata:   WorkloadMetadata{Name: "minimal"},
		Spec: WorkloadSpec{
			Command: Command{Executable: "true"},
		},
	}
}

func fullWorkload() Workload {
	return Workload{
		APIVersion: V1Alpha1,
		Kind:       WorkloadKind,
		Metadata:   WorkloadMetadata{Name: "full"},
		Spec: WorkloadSpec{
			Command:          Command{Executable: "python", Args: []string{"train.py"}},
			WorkingDirectory: "workspace:/",
			Runtime:          Runtime{Kind: "native"},
			Policy: ExecutionPolicy{
				Retry:         RetryPolicy{MaxRuns: 1},
				DuplicateRisk: "reject",
			},
			Artifacts: &Artifacts{},
			Requirements: &Requirements{
				Architectures: []string{"amd64"},
				Capabilities:  []string{"gpu"},
			},
		},
	}
}
