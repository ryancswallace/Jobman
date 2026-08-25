package sshbootstrap

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testGenerationID = "55555555-5555-4555-8555-555555555555"

func TestValidateOptionsDefaultsAndRejectsUnsafeInputs(t *testing.T) {
	options := Options{
		Host: "research-submit", AgentBinary: "/tmp/jobman-agent-linux",
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "enrollment-token",
	}
	if err := validateOptions(&options); err != nil {
		t.Fatalf("validateOptions() error = %v", err)
	}
	if options.SSHExecutable != "ssh" || options.RemoteBinary != ".local/bin/jobman-agent" ||
		options.RemoteStateDirectory != ".local/state/jobman-agent" || options.Runner == nil ||
		options.PollInterval != 2*time.Second || options.ArtifactStoreVersion != 1 {
		t.Fatalf("defaulted options = %#v", options)
	}
	tests := []Options{
		{Host: "-oProxyCommand=bad"},
		{Host: "host name"},
		{Host: "host", RemoteBinary: "../escape"},
		{Host: "host", ServerURL: "http://control.example.edu"},
		{Host: "host", EnrollmentToken: "secret token"},
	}
	for index := range tests {
		candidate := options
		if tests[index].Host != "" {
			candidate.Host = tests[index].Host
		}
		if tests[index].RemoteBinary != "" {
			candidate.RemoteBinary = tests[index].RemoteBinary
		}
		if tests[index].ServerURL != "" {
			candidate.ServerURL = tests[index].ServerURL
		}
		if tests[index].EnrollmentToken != "" {
			candidate.EnrollmentToken = tests[index].EnrollmentToken
		}
		if err := validateOptions(&candidate); err == nil {
			t.Fatalf("validateOptions(%d) accepted %#v", index, candidate)
		}
	}
	slurm := options
	slurm.Slurm = true
	if err := validateOptions(&slurm); err == nil {
		t.Fatal("validateOptions() accepted Slurm bootstrap without bundle configuration")
	}
	slurm.SlurmRoot = "/nfs/jobman/slurm"
	slurm.SlurmRunner = "/nfs/apps/jobman-agent"
	if err := validateOptions(&slurm); err != nil {
		t.Fatalf("validateOptions(Slurm) error = %v", err)
	}
	for name, mutate := range map[string]func(*Options){
		"poll interval": func(candidate *Options) { candidate.PollInterval = time.Nanosecond },
		"log limit":     func(candidate *Options) { candidate.MaximumLogBytes = -1 },
		"artifact limit": func(candidate *Options) {
			candidate.MaximumArtifactBytes = -1
		},
		"artifact pair": func(candidate *Options) { candidate.ArtifactStoreName = "safe" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := options
			mutate(&candidate)
			if err := validateOptions(&candidate); err == nil {
				t.Fatal("validateOptions() error = nil")
			}
		})
	}
}

type scriptedRunner struct {
	outputs [][]byte
	calls   [][]string
	inputs  []string
	failAt  int
}

func (runner *scriptedRunner) Run(
	_ context.Context,
	executable string,
	arguments []string,
	input io.Reader,
) ([]byte, error) {
	runner.calls = append(runner.calls, append([]string{executable}, arguments...))
	if runner.failAt == len(runner.calls) {
		return nil, errors.New("scripted SSH failure")
	}
	if input != nil {
		contents, err := io.ReadAll(input)
		if err != nil {
			return nil, err
		}
		runner.inputs = append(runner.inputs, string(contents))
	}
	if len(runner.outputs) == 0 {
		return nil, errors.New("unexpected SSH call")
	}
	result := runner.outputs[0]
	runner.outputs = runner.outputs[1:]
	return result, nil
}

func TestBootstrapSurfacesEveryRemoteStageFailure(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "jobman-agent")
	if err := os.WriteFile(binary, []byte("linux agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := Options{
		Host: "research-submit", AgentBinary: binary,
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "enrollment-token",
		inspectBinary: func(string) (binaryTarget, error) {
			return binaryTarget{architecture: "amd64"}, nil
		},
	}
	validStatus := []byte(`{
  "apiVersion":"jobman.agent/v1alpha1","kind":"AgentStatus",
  "metadata":{"agentId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","targetGenerationId":"` + testGenerationID + `"},
  "status":{"agentVersion":"dev"}
}`)
	tests := []struct {
		name    string
		outputs [][]byte
		failAt  int
	}{
		{name: "preflight", failAt: 1},
		{name: "invalid preflight", outputs: [][]byte{[]byte("unknown")}},
		{name: "agent transfer", outputs: [][]byte{[]byte("new")}, failAt: 2},
		{name: "enrollment", outputs: [][]byte{[]byte("new"), nil}, failAt: 3},
		{name: "service install", outputs: [][]byte{[]byte("new"), nil, nil}, failAt: 4},
		{name: "status", outputs: [][]byte{[]byte("new"), nil, nil, nil}, failAt: 5},
		{name: "invalid status", outputs: [][]byte{[]byte("new"), nil, nil, nil, []byte(`{}`)}},
		{name: "wrong status version", outputs: [][]byte{[]byte("new"), nil, nil, nil, validStatus}, failAt: 6},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := base
			options.Runner = &scriptedRunner{outputs: test.outputs, failAt: test.failAt}
			if test.name == "wrong status version" {
				options.ExpectedVersion = "other"
			}
			if _, err := Bootstrap(t.Context(), options); err == nil {
				t.Fatal("Bootstrap() error = nil")
			}
		})
	}
	if _, err := Bootstrap(nil, base); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("Bootstrap() accepted nil context")
	}
	inspectFailure := base
	inspectFailure.inspectBinary = func(string) (binaryTarget, error) {
		return binaryTarget{}, errors.New("inspect failed")
	}
	if _, err := Bootstrap(t.Context(), inspectFailure); err == nil {
		t.Fatal("Bootstrap() ignored binary inspection failure")
	}
}

func TestBootstrapServerCAFailures(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "jobman-agent")
	if err := os.WriteFile(binary, []byte("linux agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := Options{
		Host: "research-submit", AgentBinary: binary,
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "enrollment-token",
		inspectBinary: func(string) (binaryTarget, error) {
			return binaryTarget{architecture: "amd64"}, nil
		},
	}
	missing := base
	missing.ServerCAFile = filepath.Join(t.TempDir(), "missing.pem")
	missing.Runner = &scriptedRunner{outputs: [][]byte{[]byte("new"), nil}}
	if _, err := Bootstrap(t.Context(), missing); err == nil {
		t.Fatal("Bootstrap() accepted missing server CA")
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, []byte("certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	transfer := base
	transfer.ServerCAFile = ca
	transfer.Runner = &scriptedRunner{outputs: [][]byte{[]byte("new"), nil}, failAt: 3}
	if _, err := Bootstrap(t.Context(), transfer); err == nil {
		t.Fatal("Bootstrap() ignored server CA transfer failure")
	}
}

func TestBootstrapInstallsAndVerifiesAgent(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "jobman-agent")
	if err := os.WriteFile(binary, []byte("linux agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0o700); err != nil { //nolint:gosec // The fixture represents an executable agent binary.
		t.Fatal(err)
	}
	status := []byte(`{
  "apiVersion":"jobman.agent/v1alpha1","kind":"AgentStatus",
  "metadata":{"agentId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","targetGenerationId":"` + testGenerationID + `"},
  "status":{"agentVersion":"1.2.3"}
}`)
	runner := &scriptedRunner{outputs: [][]byte{[]byte("new\n"), nil, nil, nil, status}}
	result, err := Bootstrap(t.Context(), Options{
		Host: "research-submit", AgentBinary: binary,
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "enrollment-token", ExpectedVersion: "1.2.3",
		ArtifactStoreName: "department-nfs", ArtifactRoot: "/nfs/artifacts",
		Slurm: true, SlurmRoot: "/nfs/slurm", SlurmRunner: "/nfs/apps/jobman-agent",
		Runner: runner,
		inspectBinary: func(string) (binaryTarget, error) {
			return binaryTarget{architecture: "amd64"}, nil
		},
	})
	if err != nil || result.AgentVersion != "1.2.3" || result.ReusedEnrollment || len(runner.calls) != 5 {
		t.Fatalf("Bootstrap() = %#v, %v calls=%d", result, err, len(runner.calls))
	}
	if len(runner.inputs) < 2 || runner.inputs[1] != "enrollment-token\n" ||
		!strings.Contains(runner.calls[2][2], "--slurm") {
		t.Fatalf("bootstrap inputs/calls = %#v / %#v", runner.inputs, runner.calls)
	}
}

func TestBootstrapReusesEnrollmentAndTransfersCA(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "jobman-agent")
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(binary, []byte("linux agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0o700); err != nil { //nolint:gosec // The fixture represents an executable agent binary.
		t.Fatal(err)
	}
	if err := os.WriteFile(ca, []byte("certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	status := []byte(`{
  "apiVersion":"jobman.agent/v1alpha1","kind":"AgentStatus",
  "metadata":{"agentId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","targetGenerationId":"` + testGenerationID + `"},
  "status":{"agentVersion":"dev"}
}`)
	runner := &scriptedRunner{outputs: [][]byte{[]byte("enrolled"), nil, nil, nil, status}}
	result, err := Bootstrap(t.Context(), Options{
		Host: "research-submit", AgentBinary: binary, ServerCAFile: ca,
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "unused-token", Runner: runner,
		inspectBinary: func(string) (binaryTarget, error) {
			return binaryTarget{architecture: "arm64"}, nil
		},
	})
	if err != nil || !result.ReusedEnrollment || len(runner.calls) != 5 {
		t.Fatalf("Bootstrap(reuse) = %#v, %v calls=%d", result, err, len(runner.calls))
	}
}

func TestExecRunnerAndBinaryInspection(t *testing.T) {
	output, err := (ExecRunner{}).Run(t.Context(), "sh", []string{"-c", "printf ok"}, nil)
	if err != nil || string(output) != "ok" {
		t.Fatalf("ExecRunner.Run() = %q, %v", output, err)
	}
	if _, err = (ExecRunner{}).Run(t.Context(), "sh", []string{"-c", "printf failed; exit 1"}, nil); err == nil ||
		!strings.Contains(err.Error(), "failed") {
		t.Fatalf("ExecRunner.Run(failure) error = %v", err)
	}
	if _, err = (ExecRunner{}).Run(t.Context(), "sh", []string{"-c", "exit 1"}, nil); err == nil {
		t.Fatal("ExecRunner.Run(empty failure) error = nil")
	}
	target, inspectErr := inspectAgentBinary(os.Args[0])
	if inspectErr == nil && target.architecture == "" {
		t.Fatal("inspectAgentBinary() returned no architecture")
	}
	if inspectErr != nil && !strings.Contains(inspectErr.Error(), "target Linux") {
		t.Fatalf("inspectAgentBinary() error = %v", inspectErr)
	}
	if _, err = inspectAgentBinary(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("inspectAgentBinary() accepted missing file")
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err = os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = inspectAgentBinary(empty); err == nil {
		t.Fatal("inspectAgentBinary() accepted empty file")
	}
	textFile := filepath.Join(t.TempDir(), "text")
	if err = os.WriteFile(textFile, []byte("not a Go executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = inspectAgentBinary(textFile); err == nil {
		t.Fatal("inspectAgentBinary() accepted a non-Go file")
	}
}

func TestBootstrapUsesDefaultBinaryInspector(t *testing.T) {
	textFile := filepath.Join(t.TempDir(), "jobman-agent")
	if err := os.WriteFile(textFile, []byte("not a Go executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Host: "research-submit", AgentBinary: textFile,
		ServerURL: "https://control.example.edu", TargetGenerationID: testGenerationID,
		EnrollmentToken: "enrollment-token",
	}
	if _, err := Bootstrap(t.Context(), options); err == nil {
		t.Fatal("Bootstrap() accepted a non-Go agent binary")
	}
	options.Host = ""
	if _, err := Bootstrap(t.Context(), options); err == nil {
		t.Fatal("Bootstrap() accepted invalid options")
	}
}

func TestShellQuotingAndStatusValidation(t *testing.T) {
	quoted := quoteShell("a'b;$(touch bad)")
	if quoted != `'a'"'"'b;$(touch bad)'` {
		t.Fatalf("quoteShell() = %q", quoted)
	}
	options := Options{TargetGenerationID: testGenerationID, ExpectedVersion: "1.2.3"}
	document := `{
  "apiVersion":"jobman.agent/v1alpha1",
  "kind":"AgentStatus",
  "metadata":{"agentId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","targetGenerationId":"` + testGenerationID + `"},
  "status":{"agentVersion":"1.2.3"}
}`
	status, err := parseStatus([]byte(document), options)
	if err != nil || status.Metadata.TargetGenerationID != testGenerationID {
		t.Fatalf("parseStatus() = %#v, %v", status, err)
	}
	if _, err = parseStatus([]byte(strings.Replace(document, "1.2.3", "1.2.4", 1)), options); err == nil {
		t.Fatal("parseStatus() accepted unexpected version")
	}
}

func TestBoundedBuffer(t *testing.T) {
	var buffer boundedBuffer
	payload := strings.Repeat("x", maximumBootstrapOutput+100)
	written, err := buffer.Write([]byte(payload))
	if err != nil || written != len(payload) || buffer.Len() != maximumBootstrapOutput {
		t.Fatalf("boundedBuffer.Write() = %d, %v length=%d", written, err, buffer.Len())
	}
}
