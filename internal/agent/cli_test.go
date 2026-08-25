package agent

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecuteCLIInformationalAndValidationPaths(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantErr  bool
		contains string
	}{
		{name: "missing", wantErr: true, contains: "Usage:"},
		{name: "unknown", args: []string{"unknown"}, wantErr: true, contains: "Usage:"},
		{name: "help", args: []string{"help"}, contains: "jobman-agent"},
		{name: "version", args: []string{"version"}, contains: "jobman-agent"},
		{name: "enroll missing", args: []string{"enroll"}, wantErr: true},
		{name: "run missing", args: []string{"run"}, wantErr: true},
		{name: "bootstrap missing", args: []string{"bootstrap"}, wantErr: true},
		{name: "install missing", args: []string{"install-service"}, wantErr: true},
		{name: "status missing", args: []string{"status"}, wantErr: true},
		{name: "runner missing", args: []string{"run-execution"}, wantErr: true},
		{name: "bad flag", args: []string{"run", "--unknown"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := ExecuteCLI(t.Context(), test.args, strings.NewReader("token\n"), &stdout, &stderr)
			if (err != nil) != test.wantErr {
				t.Fatalf("ExecuteCLI() error = %v, want error %t", err, test.wantErr)
			}
			if test.contains != "" && !strings.Contains(stdout.String()+stderr.String(), test.contains) {
				t.Fatalf("output = %q, want containing %q", stdout.String()+stderr.String(), test.contains)
			}
		})
	}
}

func TestExecuteCLICommandsReachAgentBoundaries(t *testing.T) {
	stateDirectory := t.TempDir()
	if err := writePrivateFile(filepath.Join(stateDirectory, agentCredentialsFilename), []byte(`{}`)); err != nil {
		t.Fatalf("write credentials marker: %v", err)
	}
	var output bytes.Buffer
	err := ExecuteCLI(t.Context(), []string{
		"enroll", "--state-dir", stateDirectory, "--server", "https://control.example.test",
		"--target-generation", testTargetGenerationID,
	}, strings.NewReader("token\n"), &output, &output)
	if err == nil || !strings.Contains(err.Error(), "already enrolled") {
		t.Fatalf("ExecuteCLI(enroll) error = %v", err)
	}
	if err = ExecuteCLI(t.Context(), []string{
		"run", "--state-dir", "relative",
	}, strings.NewReader(""), &output, &output); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("ExecuteCLI(run) error = %v", err)
	}
	if err = ExecuteCLI(t.Context(), []string{
		"run-execution", "--state-dir", stateDirectory, "--execution-id", "invalid",
	}, strings.NewReader(""), &output, &output); err == nil || !strings.Contains(err.Error(), "invalid execution") {
		t.Fatalf("ExecuteCLI(run-execution) error = %v", err)
	}
}

func TestExecuteCLIExtendedAgentBoundaries(t *testing.T) {
	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	for _, arguments := range [][]string{
		{"status", "--state-dir", stateDirectory},
		{"status", "--state-dir", stateDirectory, "--json"},
	} {
		var output bytes.Buffer
		if err := ExecuteCLI(t.Context(), arguments, strings.NewReader(""), &output, &output); err != nil {
			t.Fatalf("ExecuteCLI(%q) error = %v", arguments[0], err)
		}
		if !strings.Contains(output.String(), testAgentID) {
			t.Fatalf("ExecuteCLI(%q) output = %q", arguments[0], output.String())
		}
	}
	var output bytes.Buffer
	if err := ExecuteCLI(t.Context(), []string{
		"bootstrap", "--host", "submit", "--agent-binary", filepath.Join(t.TempDir(), "missing"),
		"--server", "https://control.example.test", "--target-generation", testTargetGenerationID,
	}, strings.NewReader("token\n"), &output, &output); err == nil {
		t.Fatal("ExecuteCLI(bootstrap) accepted a missing agent binary")
	}
	output.Reset()
	if err := ExecuteCLI(t.Context(), []string{
		"bootstrap", "--host", "submit", "--agent-binary", filepath.Join(t.TempDir(), "missing"),
		"--server", "https://control.example.test", "--target-generation", testTargetGenerationID,
	}, strings.NewReader("\n"), &output, &output); err == nil {
		t.Fatal("ExecuteCLI(bootstrap) accepted an empty token")
	}
	for name, arguments := range map[string][]string{
		"status missing enrollment": {
			"status", "--state-dir", t.TempDir(),
		},
		"status bad flag": {
			"status", "--unknown",
		},
		"bootstrap bad flag": {
			"bootstrap", "--unknown",
		},
		"install bad flag": {
			"install-service", "--unknown",
		},
		"run artifact pair": {
			"run", "--state-dir", stateDirectory, "--artifact-store", "safe",
		},
		"run Slurm pair": {
			"run", "--state-dir", stateDirectory, "--slurm-root", "/nfs/slurm",
		},
		"runner artifact pair": {
			"run-execution", "--state-dir", stateDirectory, "--execution-id", testExecutionID,
			"--artifact-store", "safe",
		},
		"runner invalid store": {
			"run-execution", "--state-dir", stateDirectory, "--execution-id", testExecutionID,
			"--artifact-store", "Bad Name", "--artifact-root", t.TempDir(),
		},
		"install host boundary": {
			"install-service", "--state-dir", stateDirectory,
		},
		"enroll missing CA": {
			"enroll", "--state-dir", t.TempDir(), "--server", "https://control.example.test",
			"--target-generation", testTargetGenerationID, "--server-ca", filepath.Join(t.TempDir(), "missing"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			output.Reset()
			if err := ExecuteCLI(t.Context(), arguments, strings.NewReader("token\n"), &output, &output); err == nil {
				t.Fatal("ExecuteCLI() error = nil")
			}
		})
	}
}

func TestReadEnrollmentTokenFileErrors(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(filename, []byte("file-token\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if token, err := readEnrollmentToken(strings.NewReader(""), filename); err != nil || token != "file-token" {
		t.Fatalf("readEnrollmentToken(file) = %q, %v", token, err)
	}
	if _, err := readEnrollmentToken(strings.NewReader(""), filename+"-missing"); err == nil {
		t.Fatal("readEnrollmentToken() accepted missing file")
	}
	if _, err := readEnrollmentToken(strings.NewReader(strings.Repeat("x", 4097)), "-"); err == nil {
		t.Fatal("readEnrollmentToken() accepted oversized token")
	}
}

func TestRunServiceValidationAndCancelledStartup(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	if err := RunService(nil, ServiceOptions{Logger: logger}); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("RunService() accepted nil context")
	}
	if err := RunService(t.Context(), ServiceOptions{StateDirectory: t.TempDir()}); err == nil {
		t.Fatal("RunService() accepted nil logger")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger, PollInterval: time.Nanosecond,
	}); err == nil {
		t.Fatal("RunService() accepted invalid poll interval")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger, MaximumLogBytes: -1,
	}); err == nil {
		t.Fatal("RunService() accepted invalid maximum log bytes")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger, SlurmRoot: t.TempDir(),
	}); err == nil {
		t.Fatal("RunService() accepted incomplete Slurm configuration")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger,
		SlurmRoot: "relative", SlurmRunner: os.Args[0],
	}); err == nil {
		t.Fatal("RunService() accepted a relative Slurm root")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger,
		SlurmRoot: t.TempDir(), SlurmRunner: "relative",
	}); err == nil {
		t.Fatal("RunService() accepted a relative Slurm runner")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger, ArtifactStoreName: "logs",
	}); err == nil {
		t.Fatal("RunService() accepted incomplete artifact configuration")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: "relative", Logger: logger,
	}); err == nil {
		t.Fatal("RunService() accepted relative state directory")
	}
	if err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger,
	}); err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("RunService(missing credentials) error = %v", err)
	}

	stateDirectory := t.TempDir()
	pki := newTestPKI(t)
	saveTestCredentials(t, pki, stateDirectory, "https://127.0.0.1:1")
	canceledContext, cancelStartup := context.WithCancel(t.Context())
	cancelStartup()
	if err := RunService(canceledContext, ServiceOptions{
		StateDirectory: stateDirectory, Logger: logger, PollInterval: time.Second,
	}); err != nil {
		t.Fatalf("RunService(pre-canceled) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunService(ctx, ServiceOptions{
		StateDirectory: stateDirectory, Logger: logger, PollInterval: time.Second,
	}); err != nil {
		t.Fatalf("RunService(canceled) error = %v", err)
	}
}
