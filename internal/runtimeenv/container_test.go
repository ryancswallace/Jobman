package runtimeenv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ryancswallace/jobman/protocol"
)

type probeRunner struct {
	name string
	args []string
	err  error
}

func (runner *probeRunner) Run(_ context.Context, name string, arguments ...string) ([]byte, error) {
	runner.name = name
	runner.args = append([]string(nil), arguments...)

	return []byte("version"), runner.err
}

func TestOCICommandUsesHardenedExactArguments(t *testing.T) {
	t.Parallel()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	workingDirectory := filepath.Join(workspace, "work")
	if err = os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	adapter, err := New(Options{Engine: "docker", Executable: binary})
	if err != nil {
		t.Fatal(err)
	}
	command, err := adapter.Command(CommandRequest{
		Runtime: protocol.ContainerRuntime{
			Image: "registry.example/research/tool@sha256:abcdef", PullPolicy: "never", Network: "restricted",
		},
		Executable: "python", Arguments: []string{"-c", "print('ok')"},
		Workspace: workspace, WorkingDirectory: workingDirectory,
		BaseEnvironment: []string{"PATH=" + filepath.Dir(binary)},
		Environment:     map[string]string{"B": "two", "A": "one"},
	})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	for _, sequence := range [][]string{
		{"--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges"},
		{"--pull=never", "--network=none"},
		{"--workdir", "/workspace/work"},
		{"--env", "A=one", "--env", "B=two"},
		{"registry.example/research/tool@sha256:abcdef", "python", "-c", "print('ok')"},
	} {
		if !containsArgumentSequence(command.Args, sequence) {
			t.Fatalf("command args %q do not contain %q", command.Args, sequence)
		}
	}
}

func TestApptainerRequiresPreStagedImageAndPolicy(t *testing.T) {
	t.Parallel()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	adapter, err := New(Options{Engine: "apptainer", Executable: binary})
	if err != nil {
		t.Fatal(err)
	}
	request := CommandRequest{
		Runtime: protocol.ContainerRuntime{
			Image: filepath.Join(t.TempDir(), "tool.sif"), PullPolicy: "never", Network: "none",
		},
		Executable: "tool", Workspace: workspace, WorkingDirectory: workspace,
		BaseEnvironment: []string{"PATH=" + filepath.Dir(binary)},
	}
	command, err := adapter.Command(request)
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if !containsArgumentSequence(command.Args, []string{
		"exec", "--containall", "--no-home", "--cleanenv", "--pwd", "/workspace",
	}) || !containsArgumentSequence(command.Args, []string{"--net", "--network", "none"}) {
		t.Fatalf("Apptainer args = %q", command.Args)
	}
	request.Runtime.PullPolicy = "always"
	if _, err = adapter.Command(request); err == nil || !strings.Contains(err.Error(), "pre-staged") {
		t.Fatalf("Command(always) error = %v", err)
	}
	request.Runtime.PullPolicy = "never"
	request.Runtime.Image = "docker://example/tool"
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command(remote image) unexpectedly succeeded")
	}
}

func TestContainerPolicyValidationProbeAndNetwork(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Engine: "unknown"}); err == nil {
		t.Fatal("New() accepted unknown engine")
	}
	runner := &probeRunner{}
	adapter, err := New(Options{Engine: "podman", Executable: "podman-custom", Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Probe(t.Context()); err != nil || runner.name != "podman-custom" ||
		!reflect.DeepEqual(runner.args, []string{"--version"}) {
		t.Fatalf("Probe() = %v, call %q %q", err, runner.name, runner.args)
	}
	runner.err = errors.New("missing")
	if err = adapter.Probe(t.Context()); err == nil {
		t.Fatal("Probe() accepted failure")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	adapter, err = New(Options{Engine: "podman", Executable: binary})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Command(CommandRequest{
		Runtime:    protocol.ContainerRuntime{Image: "tool:latest", PullPolicy: "if-not-present", Network: "host"},
		Executable: "tool", Workspace: workspace, WorkingDirectory: workspace,
		BaseEnvironment: []string{"PATH=" + filepath.Dir(binary)},
	})
	if err == nil || !strings.Contains(err.Error(), "host network") {
		t.Fatalf("Command(host network) error = %v", err)
	}
}

func TestContainerAdapterIdentityAndAdditionalPolicies(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Engine: "docker", Executable: "bad\x00engine"}); err == nil {
		t.Fatal("New() accepted a NUL executable")
	}
	adapter, err := New(Options{Engine: "docker", AllowHostNetwork: true})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Engine() != "docker" || adapter.Executable() != "docker" || !adapter.AllowHostNetwork() {
		t.Fatalf("adapter identity = %#v", adapter)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	adapter, err = New(Options{Engine: "docker", Executable: binary, AllowHostNetwork: true})
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	request := CommandRequest{
		Runtime: protocol.ContainerRuntime{
			Image: "example/tool:latest", PullPolicy: "always", Network: "host",
		},
		Executable: "tool", Workspace: workspace, WorkingDirectory: workspace,
	}
	command, err := adapter.Command(request)
	if err != nil || !containsArgumentSequence(command.Args, []string{"--pull=always", "--network=host"}) {
		t.Fatalf("Command(host) = %#v, %v", command, err)
	}
	request.Runtime.Image = "bad image"
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted an invalid OCI image")
	}
	request.Runtime.Image = "example/tool"
	request.Runtime.PullPolicy = "sometimes"
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted an invalid pull policy")
	}
	request.Runtime.PullPolicy = "never"
	request.Runtime.Network = "public"
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted an invalid network policy")
	}
	request.Runtime.Network = "none"
	request.Executable = "bad\x00tool"
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted a NUL executable")
	}
	request.Executable = "tool"
	request.Arguments = []string{"bad\x00argument"}
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted a NUL argument")
	}
	request.Arguments = nil
	request.WorkingDirectory = filepath.Join(t.TempDir(), "outside")
	if _, err = adapter.Command(request); err == nil {
		t.Fatal("Command() accepted a working directory outside its workspace")
	}
}

func TestApptainerProbeAndHostPolicy(t *testing.T) {
	t.Parallel()
	runner := &probeRunner{}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(Options{
		Engine: "apptainer", Executable: binary, AllowHostNetwork: true, Runner: runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Probe(t.Context()); err != nil || !reflect.DeepEqual(runner.args, []string{"version"}) {
		t.Fatalf("Probe() = %v, args=%q", err, runner.args)
	}
	workspace := t.TempDir()
	command, err := adapter.Command(CommandRequest{
		Runtime: protocol.ContainerRuntime{
			Image: filepath.Join(t.TempDir(), "tool.sif"), PullPolicy: "never", Network: "host",
		},
		Executable: "tool", Workspace: workspace, WorkingDirectory: workspace,
	})
	if err != nil || containsArgumentSequence(command.Args, []string{"--net", "--network", "none"}) {
		t.Fatalf("Command(host) = %#v, %v", command, err)
	}
	if _, err = (ExecCommandRunner{}).Run(t.Context(), os.Args[0], "-test.run=^$"); err != nil {
		t.Fatalf("ExecCommandRunner.Run() error = %v", err)
	}
}

func containsArgumentSequence(values, sequence []string) bool {
	for index := 0; index+len(sequence) <= len(values); index++ {
		if reflect.DeepEqual(values[index:index+len(sequence)], sequence) {
			return true
		}
	}

	return false
}
