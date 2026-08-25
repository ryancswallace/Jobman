package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateWorkloadRejectsInvalidComponents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Workload)
		want   string
	}{
		{name: "API version", mutate: func(value *Workload) { value.APIVersion = "other/v1" }, want: "API version"},
		{name: "kind", mutate: func(value *Workload) { value.Kind = "Other" }, want: "kind"},
		{name: "metadata name", mutate: func(value *Workload) { value.Metadata.Name = "Not Valid" }, want: "metadata"},
		{name: "description", mutate: func(value *Workload) { value.Metadata.Description = "bad\x00" }, want: "description"},
		{name: "label key", mutate: func(value *Workload) { value.Metadata.Labels = map[string]string{"Bad": "x"} }, want: "labels key"},
		{name: "label value", mutate: func(value *Workload) {
			value.Metadata.Labels = map[string]string{"key": strings.Repeat("x", maximumNameBytes+1)}
		}, want: "labels value"},
		{name: "too many labels", mutate: func(value *Workload) { value.Metadata.Labels = largeStringMap() }, want: "labels has too many"},
		{name: "annotation value", mutate: func(value *Workload) { value.Metadata.Annotations = map[string]string{"key": "bad\x00"} }, want: "annotations value"},
		{name: "no command", mutate: func(value *Workload) { value.Spec.Command = Command{} }, want: "exactly one"},
		{name: "both commands", mutate: func(value *Workload) { value.Spec.Command.Shell = &ShellCommand{} }, want: "exactly one"},
		{name: "too many arguments", mutate: func(value *Workload) { value.Spec.Command.Args = make([]string, maximumCommandArgs+1) }, want: "arguments"},
		{name: "argument value", mutate: func(value *Workload) { value.Spec.Command.Args = []string{"bad\x00"} }, want: "argument 0"},
		{name: "executable", mutate: func(value *Workload) { value.Spec.Command.Executable = "bad\x00" }, want: "executable"},
		{name: "shell arguments", mutate: func(value *Workload) { value.Spec.Command = Command{Args: []string{"x"}, Shell: validShell()} }, want: "direct arguments"},
		{name: "shell capability", mutate: func(value *Workload) { value.Spec.Command = Command{Shell: &ShellCommand{Script: "true"}} }, want: "shell capability"},
		{name: "shell script", mutate: func(value *Workload) { value.Spec.Command = Command{Shell: &ShellCommand{Capability: "posix-sh"}} }, want: "shell script"},
		{name: "working directory", mutate: func(value *Workload) { value.Spec.WorkingDirectory = "/tmp" }, want: "logical root"},
		{name: "working directory character", mutate: func(value *Workload) { value.Spec.WorkingDirectory = `workspace:/bad\path` }, want: "path character"},
		{name: "working directory normalization", mutate: func(value *Workload) { value.Spec.WorkingDirectory = "workspace:/a/../b" }, want: "not normalized"},
		{name: "environment profile", mutate: func(value *Workload) { value.Spec.Environment = &Environment{Profile: "Bad Profile"} }, want: "profile"},
		{name: "too many environment values", mutate: func(value *Workload) { value.Spec.Environment = &Environment{Values: largeStringMap()} }, want: "too many values"},
		{name: "environment name", mutate: func(value *Workload) { value.Spec.Environment = &Environment{Values: map[string]string{"1BAD": "x"}} }, want: "environment name"},
		{name: "environment value", mutate: func(value *Workload) {
			value.Spec.Environment = &Environment{Values: map[string]string{"GOOD": "bad\x00"}}
		}, want: "environment value"},
		{name: "too many secrets", mutate: func(value *Workload) {
			value.Spec.Environment = &Environment{Secrets: make([]SecretBinding, maximumMapEntries+1)}
		}, want: "too many secrets"},
		{name: "duplicate secret", mutate: func(value *Workload) {
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{validSecret("token"), validSecret("token")}}
		}, want: "duplicate secret"},
		{name: "secret name", mutate: func(value *Workload) {
			secret := validSecret("")
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "secret name"},
		{name: "secret source", mutate: func(value *Workload) {
			secret := validSecret("token")
			secret.Source = "https://example.com/token"
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "secret \"token\" source"},
		{name: "secret exposure absent", mutate: func(value *Workload) {
			secret := validSecret("token")
			secret.ExposeAs = SecretExposure{}
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "select one exposure"},
		{name: "secret exposure both", mutate: func(value *Workload) {
			secret := validSecret("token")
			secret.ExposeAs.File = "workspace:/token"
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "select one exposure"},
		{name: "secret environment exposure", mutate: func(value *Workload) {
			secret := validSecret("token")
			secret.ExposeAs.Environment = "1BAD"
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "environment exposure"},
		{name: "secret file exposure", mutate: func(value *Workload) {
			secret := validSecret("token")
			secret.ExposeAs = SecretExposure{File: "/token"}
			value.Spec.Environment = &Environment{Secrets: []SecretBinding{secret}}
		}, want: "file exposure"},
		{name: "negative resources", mutate: func(value *Workload) { value.Spec.Resources = &Resources{CPU: -1} }, want: "negative"},
		{name: "memory", mutate: func(value *Workload) { value.Spec.Resources = &Resources{Memory: "16G"} }, want: "memory"},
		{name: "temporary storage", mutate: func(value *Workload) { value.Spec.Resources = &Resources{TemporaryStorage: "lots"} }, want: "temporary storage"},
		{name: "wall time", mutate: func(value *Workload) { value.Spec.Resources = &Resources{WallTime: "0s"} }, want: "wall time"},
		{name: "native container", mutate: func(value *Workload) { value.Spec.Runtime.Container = &ContainerRuntime{} }, want: "native runtime"},
		{name: "container missing", mutate: func(value *Workload) { value.Spec.Runtime = Runtime{Kind: "container"} }, want: "settings are required"},
		{name: "container image", mutate: func(value *Workload) {
			value.Spec.Runtime = Runtime{Kind: "container", Container: &ContainerRuntime{PullPolicy: "never", Network: "none"}}
		}, want: "container image"},
		{name: "container pull", mutate: func(value *Workload) {
			value.Spec.Runtime = validContainerRuntime()
			value.Spec.Runtime.Container.PullPolicy = "sometimes"
		}, want: "pull policy"},
		{name: "container network", mutate: func(value *Workload) {
			value.Spec.Runtime = validContainerRuntime()
			value.Spec.Runtime.Container.Network = "public"
		}, want: "network policy"},
		{name: "runtime kind", mutate: func(value *Workload) { value.Spec.Runtime = Runtime{Kind: "virtual-machine"} }, want: "runtime kind"},
		{name: "too many artifacts", mutate: func(value *Workload) {
			value.Spec.Artifacts = &Artifacts{Inputs: make([]InputArtifact, maximumArtifacts+1)}
		}, want: "too many artifacts"},
		{name: "artifact name", mutate: func(value *Workload) {
			input := validInput("")
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "artifact name"},
		{name: "duplicate artifact", mutate: func(value *Workload) {
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{validInput("same")}, Outputs: []OutputArtifact{validOutput("same")}}
		}, want: "duplicate artifact"},
		{name: "input source", mutate: func(value *Workload) {
			input := validInput("input")
			input.Source = "artifact://store"
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "URI path"},
		{name: "input source store", mutate: func(value *Workload) {
			input := validInput("input")
			input.Source = "artifact://Bad/input"
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "artifact store"},
		{name: "input source segment", mutate: func(value *Workload) {
			input := validInput("input")
			input.Source = "artifact://store/a//b"
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "path segment"},
		{name: "input target", mutate: func(value *Workload) {
			input := validInput("input")
			input.Target = "workspace:/input"
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "inputs logical root"},
		{name: "input checksum", mutate: func(value *Workload) {
			input := validInput("input")
			input.Checksum = "sha256:no"
			value.Spec.Artifacts = &Artifacts{Inputs: []InputArtifact{input}}
		}, want: "checksum"},
		{name: "output source", mutate: func(value *Workload) {
			output := validOutput("output")
			output.Source = "inputs:/output"
			value.Spec.Artifacts = &Artifacts{Outputs: []OutputArtifact{output}}
		}, want: "outputs logical root"},
		{name: "output destination", mutate: func(value *Workload) {
			output := validOutput("output")
			output.Destination = "artifact://store"
			value.Spec.Artifacts = &Artifacts{Outputs: []OutputArtifact{output}}
		}, want: "URI path"},
		{name: "run timeout", mutate: func(value *Workload) { value.Spec.Policy.RunTimeout = "never" }, want: "run timeout"},
		{name: "maximum runs low", mutate: func(value *Workload) { value.Spec.Policy.Retry.MaxRuns = 0 }, want: "max runs"},
		{name: "maximum runs high", mutate: func(value *Workload) { value.Spec.Policy.Retry.MaxRuns = maximumRuns + 1 }, want: "max runs"},
		{name: "backoff", mutate: func(value *Workload) { value.Spec.Policy.Retry.Backoff = "-1s" }, want: "retry backoff"},
		{name: "duplicate risk", mutate: func(value *Workload) { value.Spec.Policy.DuplicateRisk = "always" }, want: "duplicate-risk"},
		{name: "too many requirements", mutate: func(value *Workload) {
			value.Spec.Requirements = &Requirements{OperatingSystems: make([]string, maximumMapEntries+1)}
		}, want: "too many operating systems"},
		{name: "unsorted requirements", mutate: func(value *Workload) { value.Spec.Requirements = &Requirements{Architectures: []string{"z", "a"}} }, want: "sorted and unique"},
		{name: "duplicate requirement", mutate: func(value *Workload) { value.Spec.Requirements = &Requirements{Capabilities: []string{"gpu", "gpu"}} }, want: "sorted and unique"},
		{name: "requirement name", mutate: func(value *Workload) { value.Spec.Requirements = &Requirements{OperatingSystems: []string{"Bad"}} }, want: "requirements"},
		{name: "too many extensions", mutate: func(value *Workload) { value.Spec.Extensions = largeRawMap() }, want: "too many extensions"},
		{name: "extension name", mutate: func(value *Workload) {
			value.Spec.Extensions = map[string]json.RawMessage{"Bad": json.RawMessage(`{}`)}
		}, want: "extension"},
		{name: "extension JSON", mutate: func(value *Workload) {
			value.Spec.Extensions = map[string]json.RawMessage{"valid": json.RawMessage(`{`)}
		}, want: "extension \"valid\""},
		{name: "extension canonical", mutate: func(value *Workload) {
			value.Spec.Extensions = map[string]json.RawMessage{"valid": json.RawMessage(`{ "x": 1 }`)}
		}, want: "not canonical"},
		{name: "extension object", mutate: func(value *Workload) {
			value.Spec.Extensions = map[string]json.RawMessage{"valid": json.RawMessage(`null`)}
		}, want: "must be an object"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := normalizedWorkload(t)
			test.mutate(&value)
			err := ValidateWorkload(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateWorkload() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateWorkloadAcceptsCompleteNormalizedDocument(t *testing.T) {
	t.Parallel()
	value := normalizedWorkload(t)
	value.Spec.Command = Command{Shell: validShell()}
	value.Spec.Environment = &Environment{
		Profile: "research",
		Values:  map[string]string{"THREADS": "4"},
		Secrets: []SecretBinding{
			validSecret("environment-token"),
			{
				Name: "file-token", Source: "secret://research/file-token",
				ExposeAs: SecretExposure{File: "workspace:/secrets/token"},
			},
		},
	}
	value.Spec.Resources = &Resources{
		CPU: 2, GPU: 1, Nodes: 1, Tasks: 1, Memory: "1GiB",
		TemporaryStorage: "1GB", WallTime: "1h",
	}
	value.Spec.Runtime = validContainerRuntime()
	value.Spec.Artifacts = &Artifacts{
		Inputs:  []InputArtifact{validInput("input")},
		Outputs: []OutputArtifact{validOutput("output")},
	}
	value.Spec.Policy = ExecutionPolicy{
		RunTimeout: "2h", Retry: RetryPolicy{MaxRuns: 2, Backoff: "1s"},
		DuplicateRisk: "allow-if-idempotent",
	}
	value.Spec.Requirements = &Requirements{
		OperatingSystems: []string{"linux"},
		Architectures:    []string{"amd64"},
		Capabilities:     []string{"container"},
	}
	value.Spec.Extensions = map[string]json.RawMessage{"example": json.RawMessage(`{"priority":1e2}`)}
	if err := ValidateWorkload(value); err != nil {
		t.Fatalf("ValidateWorkload() error = %v", err)
	}
}

func TestValidateJobRequestRejectsInvalidComponents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*JobRequest)
		want   string
	}{
		{name: "API version", mutate: func(value *JobRequest) { value.APIVersion = "other/v1" }, want: "API version"},
		{name: "kind", mutate: func(value *JobRequest) { value.Kind = "Other" }, want: "kind"},
		{name: "namespace", mutate: func(value *JobRequest) { value.Metadata.Namespace = "" }, want: "namespace"},
		{name: "name", mutate: func(value *JobRequest) { value.Metadata.Name = "Bad Name" }, want: "name"},
		{name: "labels", mutate: func(value *JobRequest) { value.Metadata.Labels = map[string]string{"Bad": "value"} }, want: "job labels"},
		{name: "digest format", mutate: func(value *JobRequest) { value.Spec.Workload.Digest = "invalid" }, want: "invalid workload digest"},
		{name: "workload invalid", mutate: func(value *JobRequest) { value.Spec.Workload.Document.Kind = "Other" }, want: "workload"},
		{name: "digest mismatch", mutate: func(value *JobRequest) { value.Spec.Workload.Digest = "sha256:" + strings.Repeat("0", 64) }, want: "does not match"},
		{name: "workload not normalized", mutate: func(value *JobRequest) { value.Spec.Workload.Document.Metadata.Labels = map[string]string{} }, want: "not normalized"},
		{name: "target", mutate: func(value *JobRequest) { value.Spec.Placement.Target = "" }, want: "target"},
		{name: "partition", mutate: func(value *JobRequest) { value.Spec.Placement.Partition = "Bad Partition" }, want: "partition"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := normalizedJobRequest(t)
			test.mutate(&value)
			err := ValidateJobRequest(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateJobRequest() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateJobRequestAcceptsPartition(t *testing.T) {
	t.Parallel()
	value := normalizedJobRequest(t)
	value.Spec.Placement.Partition = "gpu"
	if err := ValidateJobRequest(value); err != nil {
		t.Fatalf("ValidateJobRequest() error = %v", err)
	}
}

func normalizedWorkload(t *testing.T) Workload {
	t.Helper()
	sealed, err := SealWorkload(minimalWorkload())
	if err != nil {
		t.Fatalf("SealWorkload() error = %v", err)
	}

	return sealed.Document
}

func normalizedJobRequest(t *testing.T) JobRequest {
	t.Helper()
	workload := normalizedWorkload(t)
	sealedWorkload, err := SealWorkload(workload)
	if err != nil {
		t.Fatalf("SealWorkload() error = %v", err)
	}
	sealed, err := SealJobRequest(JobRequest{
		APIVersion: V1Alpha1,
		Kind:       JobRequestKind,
		Metadata: JobRequestMetadata{
			Namespace: "research",
			Name:      "job",
		},
		Spec: JobRequestSpec{
			Workload: WorkloadBinding{
				Digest:   sealedWorkload.Digest,
				Document: sealedWorkload.Document,
			},
			Placement: Placement{Target: "workstation"},
		},
	})
	if err != nil {
		t.Fatalf("SealJobRequest() error = %v", err)
	}

	return sealed.Document
}

func validShell() *ShellCommand {
	return &ShellCommand{Capability: "posix-sh", Script: "true"}
}

func validSecret(name string) SecretBinding {
	return SecretBinding{
		Name: name, Source: "secret://research/token",
		ExposeAs: SecretExposure{Environment: "TOKEN"},
	}
}

func validContainerRuntime() Runtime {
	return Runtime{
		Kind: "container",
		Container: &ContainerRuntime{
			Image:      "registry.example/research/image@sha256:abc",
			PullPolicy: "if-not-present",
			Network:    "restricted",
		},
	}
}

func validInput(name string) InputArtifact {
	return InputArtifact{
		Name: name, Source: "artifact://research/input", Target: "inputs:/input",
	}
}

func validOutput(name string) OutputArtifact {
	return OutputArtifact{
		Name: name, Source: "outputs:/output", Destination: "artifact://research/output",
	}
}

func largeStringMap() map[string]string {
	values := make(map[string]string, maximumMapEntries+1)
	for index := range maximumMapEntries + 1 {
		values["key-"+strings.Repeat("0", 4)+string(rune('a'+index%26))+strings.Repeat("x", index/26)] = "value"
	}

	return values
}

func largeRawMap() map[string]json.RawMessage {
	values := make(map[string]json.RawMessage, maximumMapEntries+1)
	for index := range maximumMapEntries + 1 {
		values["extension-"+strings.Repeat("x", index+1)] = json.RawMessage(`{}`)
	}

	return values
}
