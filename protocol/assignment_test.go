package protocol

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEffectiveExecutionValidationFailures(t *testing.T) {
	t.Parallel()
	value := decodedEffectiveExecution(t)
	tests := []struct {
		name   string
		mutate func(*EffectiveExecution)
		want   string
	}{
		{name: "API version", mutate: func(document *EffectiveExecution) { document.APIVersion = "other/v1" }, want: "API version"},
		{name: "kind", mutate: func(document *EffectiveExecution) { document.Kind = "Other" }, want: "kind"},
		{name: "execution ID", mutate: func(document *EffectiveExecution) { document.Metadata.ExecutionID = "invalid" }, want: "execution ID"},
		{name: "namespace", mutate: func(document *EffectiveExecution) { document.Metadata.Namespace = "Bad Namespace" }, want: "namespace"},
		{name: "workload digest", mutate: func(document *EffectiveExecution) { document.Spec.Workload.Digest = "invalid" }, want: "workload"},
		{name: "target", mutate: func(document *EffectiveExecution) { document.Spec.Placement.Target = "" }, want: "target"},
		{name: "partition", mutate: func(document *EffectiveExecution) { document.Spec.Placement.Partition = "Bad Partition" }, want: "partition"},
		{name: "backend", mutate: func(document *EffectiveExecution) { document.Spec.Placement.ExecutionBackend = "ssh" }, want: "backend"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			document := value
			test.mutate(&document)
			err := ValidateEffectiveExecution(document)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateEffectiveExecution() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestEffectiveExecutionArtifactStoreBindings(t *testing.T) {
	base := decodedEffectiveExecution(t)
	base.Spec.Workload.Document.Spec.Artifacts = &Artifacts{
		Inputs: []InputArtifact{validInput("input")},
	}
	base.Spec.ArtifactStores = []ArtifactStoreBinding{{Name: "department-nfs", Version: 2}}
	sealedWorkload, err := SealWorkload(base.Spec.Workload.Document)
	if err != nil {
		t.Fatal(err)
	}
	base.Spec.Workload = WorkloadBinding{Digest: sealedWorkload.Digest, Document: sealedWorkload.Document}
	if err = ValidateEffectiveExecution(base); err != nil {
		t.Fatalf("ValidateEffectiveExecution(artifacts) error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*EffectiveExecution)
	}{
		{name: "invalid store", mutate: func(value *EffectiveExecution) { value.Spec.ArtifactStores[0].Name = "Bad" }},
		{name: "duplicate store", mutate: func(value *EffectiveExecution) {
			value.Spec.ArtifactStores = append(value.Spec.ArtifactStores, value.Spec.ArtifactStores[0])
		}},
		{name: "unsorted stores", mutate: func(value *EffectiveExecution) {
			value.Spec.ArtifactStores = append(value.Spec.ArtifactStores, ArtifactStoreBinding{Name: "aaa", Version: 1})
		}},
		{name: "missing stores", mutate: func(value *EffectiveExecution) { value.Spec.ArtifactStores = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Spec.ArtifactStores = append([]ArtifactStoreBinding(nil), base.Spec.ArtifactStores...)
			test.mutate(&value)
			if err := ValidateEffectiveExecution(value); err == nil {
				t.Fatal("ValidateEffectiveExecution() accepted invalid artifact binding")
			}
		})
	}
}

func TestEffectiveExecutionSlurmArrayBinding(t *testing.T) {
	t.Parallel()
	value := decodedEffectiveExecution(t)
	value.Spec.Placement.ExecutionBackend = "slurm"
	value.Metadata.SlurmArray = &SlurmArrayBinding{
		CollectionID: "77777777-7777-4777-8777-777777777777",
		TaskIndex:    1, TaskCount: 2, MaxParallel: 1,
	}
	if err := ValidateEffectiveExecution(value); err != nil {
		t.Fatalf("ValidateEffectiveExecution(array) error = %v", err)
	}
	for _, mutate := range []func(*EffectiveExecution){
		func(document *EffectiveExecution) { document.Metadata.SlurmArray.CollectionID = "invalid" },
		func(document *EffectiveExecution) { document.Metadata.SlurmArray.TaskIndex = 2 },
		func(document *EffectiveExecution) { document.Metadata.SlurmArray.MaxParallel = 3 },
		func(document *EffectiveExecution) { document.Spec.Placement.ExecutionBackend = "subprocess" },
	} {
		candidate := value
		binding := *value.Metadata.SlurmArray
		candidate.Metadata.SlurmArray = &binding
		mutate(&candidate)
		if err := ValidateEffectiveExecution(candidate); err == nil {
			t.Fatalf("ValidateEffectiveExecution(%#v) accepted invalid array binding", candidate.Metadata.SlurmArray)
		}
	}
}

func TestAgentAssignmentValidationFailures(t *testing.T) {
	t.Parallel()
	value := decodedAgentAssignment(t)
	tests := []struct {
		name   string
		mutate func(*AgentAssignment)
		want   string
	}{
		{name: "API version", mutate: func(document *AgentAssignment) { document.APIVersion = "other/v1" }, want: "API version"},
		{name: "kind", mutate: func(document *AgentAssignment) { document.Kind = "Other" }, want: "kind"},
		{name: "delivery ID", mutate: func(document *AgentAssignment) { document.Metadata.DeliveryID = "invalid" }, want: "delivery ID"},
		{name: "agent ID", mutate: func(document *AgentAssignment) { document.Metadata.AgentID = "invalid" }, want: "agent ID"},
		{name: "digest format", mutate: func(document *AgentAssignment) { document.Spec.EffectiveExecutionDigest = "invalid" }, want: "digest"},
		{name: "effective execution", mutate: func(document *AgentAssignment) { document.Spec.EffectiveExecution.Kind = "Other" }, want: "effective execution"},
		{name: "digest mismatch", mutate: func(document *AgentAssignment) {
			document.Spec.EffectiveExecution.Spec.Placement.Target = "workstation-b"
		}, want: "does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			document := value
			test.mutate(&document)
			err := ValidateAgentAssignment(document)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateAgentAssignment() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestExecutionAndAssignmentSealingPopulatesDigests(t *testing.T) {
	t.Parallel()
	effective := decodedEffectiveExecution(t)
	effective.Spec.Workload.Digest = ""
	sealedEffective, err := SealEffectiveExecution(effective)
	if err != nil || sealedEffective.Document.Spec.Workload.Digest == "" {
		t.Fatalf("SealEffectiveExecution() = %#v, %v", sealedEffective, err)
	}
	assignment := decodedAgentAssignment(t)
	assignment.Spec.EffectiveExecutionDigest = ""
	assignment.Spec.EffectiveExecution = sealedEffective.Document
	sealedAssignment, err := SealAgentAssignment(assignment)
	if err != nil || sealedAssignment.EffectiveExecutionDigest != sealedEffective.Digest {
		t.Fatalf("SealAgentAssignment() = %#v, %v", sealedAssignment, err)
	}
}

func TestExecutionAndAssignmentSealingRejectsMismatchedDigests(t *testing.T) {
	t.Parallel()
	effective := decodedEffectiveExecution(t)
	effective.Spec.Workload.Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err := SealEffectiveExecution(effective); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("SealEffectiveExecution() error = %v", err)
	}
	assignment := decodedAgentAssignment(t)
	assignment.Spec.EffectiveExecutionDigest = "sha256:" + strings.Repeat("0", 64)
	if _, err := SealAgentAssignment(assignment); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("SealAgentAssignment() error = %v", err)
	}
}

func TestAgentContractDecodersRejectUnknownFields(t *testing.T) {
	t.Parallel()
	for name, decode := range map[string]func() error{
		"effective": func() error {
			_, err := DecodeEffectiveExecution(strings.NewReader(`{"unknown":true}`), DecodeLimits{})
			return err
		},
		"assignment": func() error {
			_, err := DecodeAgentAssignment(strings.NewReader(`{"unknown":true}`), DecodeLimits{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := decode(); err == nil {
				t.Fatal("decoder accepted an unknown field")
			}
		})
	}
}

func TestAgentContractsRejectUnnormalizedEmbeddedWorkloads(t *testing.T) {
	t.Parallel()
	effective := decodedEffectiveExecution(t)
	effective.Spec.Workload.Document.Metadata.Labels = map[string]string{}
	if err := ValidateEffectiveExecution(effective); err == nil || !strings.Contains(err.Error(), "not normalized") {
		t.Fatalf("ValidateEffectiveExecution() error = %v", err)
	}

	assignment := decodedAgentAssignment(t)
	assignment.Spec.EffectiveExecution.Spec.Workload.Document.Metadata.Labels = map[string]string{}
	if err := ValidateAgentAssignment(assignment); err == nil || !strings.Contains(err.Error(), "not normalized") {
		t.Fatalf("ValidateAgentAssignment() error = %v", err)
	}
}

func TestAgentSchemasAreCopied(t *testing.T) {
	t.Parallel()
	for _, schema := range [][]byte{EffectiveExecutionSchema(), AgentAssignmentSchema()} {
		if len(schema) == 0 {
			t.Fatal("embedded schema is empty")
		}
		schema[0] = 'x'
	}
	if EffectiveExecutionSchema()[0] != '{' || AgentAssignmentSchema()[0] != '{' {
		t.Fatal("schema getter exposed mutable embedded storage")
	}
}

func decodedEffectiveExecution(t *testing.T) EffectiveExecution {
	t.Helper()
	contents := fixtureContents(t, "valid", "effective-execution-minimal.json")
	sealed, err := DecodeEffectiveExecution(bytes.NewReader(contents), DecodeLimits{})
	if err != nil {
		t.Fatalf("DecodeEffectiveExecution() error = %v", err)
	}

	return sealed.Document
}

func decodedAgentAssignment(t *testing.T) AgentAssignment {
	t.Helper()
	contents := fixtureContents(t, "valid", "agent-assignment-minimal.json")
	sealed, err := DecodeAgentAssignment(bytes.NewReader(contents), DecodeLimits{})
	if err != nil {
		t.Fatalf("DecodeAgentAssignment() error = %v", err)
	}

	return sealed.Document
}

func fixtureContents(t *testing.T, category, name string) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "conformance", category, name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	return contents
}
