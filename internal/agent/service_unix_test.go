//go:build !windows

package agent

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestStartRunnerProcessBoundaries(t *testing.T) {
	executable, err := exec.LookPath("true")
	if err != nil {
		t.Skip("true executable is unavailable")
	}
	directory := t.TempDir()
	runnerService := &service{stateDirectory: directory, runnerExecutable: executable}
	if err = runnerService.startRunner(directory, testExecutionID); err != nil {
		t.Fatalf("startRunner() error = %v", err)
	}
	if err = runnerService.startRunner(filepath.Join(directory, "missing"), testExecutionID); err == nil {
		t.Fatal("startRunner() accepted missing log directory")
	}
	badExecutableService := &service{
		stateDirectory: directory, runnerExecutable: filepath.Join(directory, "missing"),
	}
	if err = badExecutableService.startRunner(directory, testExecutionID); err == nil {
		t.Fatal("startRunner() accepted missing executable")
	}
	if _, err = os.Stat(filepath.Join(directory, "runner.log")); err != nil {
		t.Fatalf("runner log missing: %v", err)
	}
}
