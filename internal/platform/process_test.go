package platform

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

const processHelperEnvironment = "JOBMAN_PLATFORM_PROCESS_HELPER"

func TestInspectCurrentProcess(t *testing.T) {
	t.Parallel()

	identity, err := Inspect(os.Getpid())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if identity.PID != os.Getpid() {
		t.Fatalf("Inspect().PID = %d, want %d", identity.PID, os.Getpid())
	}
	if identity.Creation == "" || identity.Boot == "" {
		t.Fatalf("Inspect() = %#v, want creation and boot identity", identity)
	}

	alive, err := Alive(identity)
	if err != nil {
		t.Fatalf("Alive() error = %v", err)
	}
	if !alive {
		t.Fatal("Alive() = false for current process")
	}
}

func TestFinalizeTargetStartRejectsInvalidPID(t *testing.T) {
	t.Parallel()
	if _, err := FinalizeTargetStart(0); err == nil {
		t.Fatal("FinalizeTargetStart(0) error = nil")
	}
}

func TestAliveRejectsIdentityMismatch(t *testing.T) {
	t.Parallel()

	identity, err := Inspect(os.Getpid())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	identity.Creation += "-different"

	alive, err := Alive(identity)
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("Alive() error = %v, want %v", err, ErrIdentityMismatch)
	}
	if alive {
		t.Fatal("Alive() = true for mismatched identity")
	}
	for name, operation := range map[string]func() error{
		"terminate": func() error { return Terminate(identity, false) },
		"pause":     func() error { return Pause(identity) },
		"resume":    func() error { return Resume(identity) },
	} {
		if err := operation(); !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("%s(mismatched identity) error = %v", name, err)
		}
	}
	if alive, err := Alive(ProcessIdentity{}); err == nil || alive {
		t.Fatalf("Alive(invalid PID) = %t, %v", alive, err)
	}
}

func TestProcessOperationsTreatGoneProcessAsComplete(t *testing.T) {
	t.Parallel()

	gone := ProcessIdentity{PID: 1 << 30, Creation: "missing", Boot: "missing"}
	if alive, err := Alive(gone); err != nil || alive {
		t.Fatalf("Alive(missing) = %t, %v", alive, err)
	}
	for name, operation := range map[string]func() error{
		"terminate": func() error { return Terminate(gone, false) },
		"pause":     func() error { return Pause(gone) },
		"resume":    func() error { return Resume(gone) },
	} {
		if err := operation(); err != nil {
			t.Errorf("%s(missing) error = %v", name, err)
		}
	}
}

func TestProcessOperationFailuresAreWrapped(t *testing.T) {
	t.Parallel()
	identity, err := Inspect(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("operation denied")
	operations := map[string]func() error{
		"terminate": func() error {
			return terminateWithOperation(identity, false, func(ProcessIdentity, bool) error { return failure })
		},
		"pause": func() error {
			return pauseWithOperation(identity, func(ProcessIdentity) error { return failure })
		},
		"resume": func() error {
			return resumeWithOperation(identity, func(ProcessIdentity) error { return failure })
		},
	}
	for name, operation := range operations {
		if operationErr := operation(); !errors.Is(operationErr, failure) {
			t.Errorf("%s error = %v", name, operationErr)
		}
	}
}

func TestProcessOperationsAndConfiguration(t *testing.T) {
	if !PauseResumeSupported() {
		t.Skip("managed-tree pause and resume are unavailable")
	}
	if _, err := Inspect(0); err == nil {
		t.Fatal("Inspect(0) error = nil")
	}

	supervisorCommand := exec.CommandContext(t.Context(), "unused")
	ConfigureSupervisor(supervisorCommand)
	if supervisorCommand.SysProcAttr == nil {
		t.Fatal("ConfigureSupervisor() did not set process attributes")
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	for _, force := range []bool{false, true} {
		command := exec.CommandContext(t.Context(), executable, "-test.run=^TestPlatformProcessHelper$") // #nosec G204 -- current test binary.
		command.Env = append(os.Environ(), processHelperEnvironment+"=1")
		ConfigureTarget(command)
		if command.SysProcAttr == nil {
			t.Fatal("ConfigureTarget() did not set process attributes")
		}
		stdin, pipeErr := command.StdinPipe()
		if pipeErr != nil {
			t.Fatalf("open helper stdin: %v", pipeErr)
		}
		if err := command.Start(); err != nil {
			t.Fatalf("start process helper: %v", err)
		}
		treeID, err := FinalizeTargetStart(command.Process.Pid)
		if err != nil {
			t.Fatalf("FinalizeTargetStart() error = %v", err)
		}
		waited := false
		t.Cleanup(func() {
			if waited {
				return
			}
			_ = stdin.Close()
			if killErr := command.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				t.Errorf("kill process helper: %v", killErr)
			}
			if waitErr := command.Wait(); waitErr != nil {
				var exitErr *exec.ExitError
				if !errors.As(waitErr, &exitErr) {
					t.Errorf("wait for killed process helper: %v", waitErr)
				}
			}
		})
		identity, err := Inspect(command.Process.Pid)
		if err != nil {
			t.Fatalf("Inspect(helper) error = %v", err)
		}
		identity.Tree = treeID
		if err := Pause(identity); err != nil {
			t.Fatalf("Pause() error = %v", err)
		}
		if err := Resume(identity); err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		if err := Terminate(identity, force); err != nil {
			t.Fatalf("Terminate(force=%t) error = %v", force, err)
		}
		var forcedTermination <-chan error
		if runtime.GOOS == "windows" && !force {
			result := make(chan error, 1)
			forcedTermination = result
			time.AfterFunc(100*time.Millisecond, func() {
				result <- Terminate(identity, true)
			})
		}
		if err := command.Wait(); err == nil {
			t.Fatalf("terminated helper Wait(force=%t) error = nil", force)
		}
		if forcedTermination != nil {
			if err := <-forcedTermination; err != nil {
				t.Fatalf("forced Windows termination error = %v", err)
			}
		}
		_ = stdin.Close()
		waited = true
	}
}

func TestPlatformProcessHelper(t *testing.T) {
	if os.Getenv(processHelperEnvironment) != "1" {
		t.Skip("helper process only")
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatalf("drain helper stdin: %v", err)
	}
}
