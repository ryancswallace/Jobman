// Package runtimeenv translates portable runtime intent into bounded native
// process invocations selected by target policy.
package runtimeenv

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ryancswallace/jobman/internal/executor"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	engineApptainer   = "apptainer"
	pullPolicyNever   = "never"
	networkNone       = "none"
	networkRestricted = "restricted"
	networkHost       = "host"
)

// CommandRunner is the test seam for container-engine probes.
type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

// ExecCommandRunner invokes a configured engine directly without a shell.
type ExecCommandRunner struct{}

// Run executes one engine command with cancellation.
func (ExecCommandRunner) Run(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...) // #nosec G204,G702 -- executable is target policy.

	return command.CombinedOutput()
}

// Options is administrator-selected target policy for container execution.
type Options struct {
	Engine           string
	Executable       string
	AllowHostNetwork bool
	Runner           CommandRunner
}

// Adapter translates one validated portable runtime into a hardened engine
// invocation. Docker and Podman are supported for subprocess targets;
// Apptainer is supported for Slurm targets using pre-staged SIF images.
type Adapter struct {
	engine           string
	executable       string
	allowHostNetwork bool
	runner           CommandRunner
}

// New validates target container policy.
func New(options Options) (*Adapter, error) {
	if !contains([]string{"docker", "podman", engineApptainer}, options.Engine) {
		return nil, errors.New("container engine must be docker, podman, or apptainer")
	}
	if options.Executable == "" {
		options.Executable = options.Engine
	}
	if strings.ContainsRune(options.Executable, 0) {
		return nil, errors.New("container executable is invalid")
	}
	if options.Runner == nil {
		options.Runner = ExecCommandRunner{}
	}

	return &Adapter{
		engine: options.Engine, executable: options.Executable,
		allowHostNetwork: options.AllowHostNetwork, runner: options.Runner,
	}, nil
}

// Engine returns the selected target-side engine.
func (adapter *Adapter) Engine() string { return adapter.engine }

// Executable returns the selected target-side engine executable.
func (adapter *Adapter) Executable() string { return adapter.executable }

// AllowHostNetwork reports whether target policy permits host networking.
func (adapter *Adapter) AllowHostNetwork() bool { return adapter.allowHostNetwork }

// Probe verifies that the selected engine is callable.
func (adapter *Adapter) Probe(ctx context.Context) error {
	argument := "--version"
	if adapter.engine == engineApptainer {
		argument = "version"
	}
	if _, err := adapter.runner.Run(ctx, adapter.executable, argument); err != nil {
		return fmt.Errorf("probe %s container engine: %w", adapter.engine, err)
	}

	return nil
}

// CommandRequest contains one fully resolved private workspace and portable
// command. Environment values are passed as engine arguments only because
// secret bindings remain unsupported by this slice.
type CommandRequest struct {
	Runtime          protocol.ContainerRuntime
	Executable       string
	Arguments        []string
	Workspace        string
	WorkingDirectory string
	BaseEnvironment  []string
	Environment      map[string]string
}

// Command builds an exact-argument engine process without shell evaluation.
func (adapter *Adapter) Command(request CommandRequest) (*exec.Cmd, error) {
	if request.Executable == "" || strings.ContainsRune(request.Executable, 0) {
		return nil, errors.New("container workload executable is invalid")
	}
	for _, argument := range request.Arguments {
		if strings.ContainsRune(argument, 0) {
			return nil, errors.New("container workload argument is invalid")
		}
	}
	containerDirectory, err := containerWorkingDirectory(request.Workspace, request.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	var arguments []string
	switch adapter.engine {
	case "docker", "podman":
		arguments, err = adapter.ociArguments(request, containerDirectory)
	case engineApptainer:
		arguments, err = adapter.apptainerArguments(request, containerDirectory)
	default:
		err = errors.New("unsupported container engine")
	}
	if err != nil {
		return nil, err
	}
	command, _, err := executor.Command(executor.Request{
		Executable: adapter.executable, Arguments: arguments,
		Directory: request.Workspace, BaseEnv: request.BaseEnvironment,
	})
	if err != nil {
		return nil, err
	}

	return command, nil
}

func (adapter *Adapter) ociArguments(
	request CommandRequest,
	containerDirectory string,
) ([]string, error) {
	if !validOCIImage(request.Runtime.Image) {
		return nil, errors.New("container image reference is invalid")
	}
	pull := map[string]string{"always": "always", "if-not-present": "missing", pullPolicyNever: pullPolicyNever}[request.Runtime.PullPolicy]
	if pull == "" {
		return nil, errors.New("container pull policy is unsupported")
	}
	network, err := adapter.network(request.Runtime.Network)
	if err != nil {
		return nil, err
	}
	arguments := []string{
		"run", "--rm", "--init", "--read-only", "--cap-drop=ALL",
		"--security-opt=no-new-privileges", "--pull=" + pull, "--network=" + network,
		"--mount", "type=bind,src=" + request.Workspace + ",dst=/workspace,rw",
		"--workdir", containerDirectory,
	}
	if user := currentProcessUser(); user != "" {
		arguments = append(arguments, "--user", user)
	}
	arguments = appendEnvironment(arguments, request.Environment)
	arguments = append(arguments, request.Runtime.Image, request.Executable)
	arguments = append(arguments, request.Arguments...)

	return arguments, nil
}

func (adapter *Adapter) apptainerArguments(
	request CommandRequest,
	containerDirectory string,
) ([]string, error) {
	if request.Runtime.PullPolicy != pullPolicyNever {
		return nil, errors.New("apptainer requires pullPolicy never and a pre-staged SIF image")
	}
	if !filepath.IsAbs(request.Runtime.Image) || filepath.Clean(request.Runtime.Image) != request.Runtime.Image ||
		!strings.HasSuffix(strings.ToLower(request.Runtime.Image), ".sif") ||
		strings.ContainsRune(request.Runtime.Image, 0) {
		return nil, errors.New("apptainer image must be a normalized absolute SIF path")
	}
	arguments := []string{
		"exec", "--containall", "--no-home", "--cleanenv", "--pwd", containerDirectory,
		"--bind", request.Workspace + ":/workspace:rw",
	}
	switch request.Runtime.Network {
	case networkNone, networkRestricted:
		arguments = append(arguments, "--net", "--network", networkNone)
	case networkHost:
		if !adapter.allowHostNetwork {
			return nil, errors.New("container host network is not allowed by target policy")
		}
	default:
		return nil, errors.New("container network policy is unsupported")
	}
	arguments = appendEnvironment(arguments, request.Environment)
	arguments = append(arguments, request.Runtime.Image, request.Executable)
	arguments = append(arguments, request.Arguments...)

	return arguments, nil
}

func (adapter *Adapter) network(policy string) (string, error) {
	switch policy {
	case networkNone, networkRestricted:
		// A target may later map restricted to an approved egress network. Until
		// then, fail safely by giving it the stricter no-network semantics.
		return networkNone, nil
	case networkHost:
		if !adapter.allowHostNetwork {
			return "", errors.New("container host network is not allowed by target policy")
		}

		return networkHost, nil
	default:
		return "", errors.New("container network policy is unsupported")
	}
}

func appendEnvironment(arguments []string, values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		arguments = append(arguments, "--env", key+"="+values[key])
	}

	return arguments
}

func containerWorkingDirectory(workspace, workingDirectory string) (string, error) {
	if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace ||
		!filepath.IsAbs(workingDirectory) {
		return "", errors.New("container workspace paths must be absolute and normalized")
	}
	relative, err := filepath.Rel(workspace, workingDirectory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("container working directory escapes workspace")
	}
	if relative == "." {
		return "/workspace", nil
	}

	return "/workspace/" + filepath.ToSlash(relative), nil
}

func validOCIImage(value string) bool {
	if value == "" || len(value) > 4096 || strings.HasPrefix(value, "-") ||
		strings.ContainsAny(value, " \t\r\n\x00") {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/@:+-", character) {
			return false
		}
	}

	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}

	return false
}
