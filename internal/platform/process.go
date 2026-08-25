// Package platform isolates operating-system process management.
package platform

import (
	"errors"
	"fmt"
	"os/exec"
)

// ErrIdentityMismatch means a PID no longer identifies the process Jobman
// originally observed.
var ErrIdentityMismatch = errors.New("process identity mismatch")

// ErrUnsupported identifies an operation the current platform cannot perform
// safely for an entire managed process tree.
var ErrUnsupported = errors.New("platform operation is unsupported")

// ProcessIdentity is a PID plus platform creation and boot identity.
type ProcessIdentity struct {
	PID      int    `json:"pid"`
	Creation string `json:"creation"`
	Boot     string `json:"boot"`
	Tree     string `json:"tree,omitempty"`
}

// PauseResumeSupported reports whether this platform has a safe managed-tree
// suspend/resume primitive. Callers use it before recording active-run state.
func PauseResumeSupported() bool {
	return supportsPauseResume()
}

// ConfigureSupervisor detaches a supervisor from the submitting terminal.
func ConfigureSupervisor(cmd *exec.Cmd) {
	applySupervisorConfiguration(cmd)
}

// ConfigureTarget creates a separately addressable target process tree.
func ConfigureTarget(cmd *exec.Cmd) {
	applyTargetConfiguration(cmd)
}

// FinalizeTargetStart attaches a newly started target to the platform's
// process-tree primitive before it is allowed to execute user code.
func FinalizeTargetStart(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("finalize target start: invalid pid %d", pid)
	}

	return attachStartedTarget(pid)
}

// Inspect returns the current identity for pid.
func Inspect(pid int) (ProcessIdentity, error) {
	if pid <= 0 {
		return ProcessIdentity{}, fmt.Errorf("inspect process: invalid pid %d", pid)
	}

	return inspectProcess(pid)
}

// Alive reports whether identity still identifies a live process.
func Alive(identity ProcessIdentity) (bool, error) {
	current, err := Inspect(identity.PID)
	if err != nil {
		if isProcessGone(err) {
			return false, nil
		}

		return false, err
	}
	if current.Creation != identity.Creation || current.Boot != identity.Boot {
		return false, ErrIdentityMismatch
	}

	return processAlive(identity)
}

// Terminate sends a graceful or forced request to a verified process tree.
func Terminate(identity ProcessIdentity, force bool) error {
	return terminateWithOperation(identity, force, terminateProcess)
}

func terminateWithOperation(
	identity ProcessIdentity,
	force bool,
	operation func(ProcessIdentity, bool) error,
) error {
	alive, err := Alive(identity)
	if err != nil {
		return fmt.Errorf("verify process before termination: %w", err)
	}
	if !alive {
		return nil
	}

	if err := operation(identity, force); err != nil {
		return fmt.Errorf("terminate process tree %d: %w", identity.PID, err)
	}

	return nil
}

// Pause suspends a verified target process tree where the platform exposes a
// safe tree-level primitive.
func Pause(identity ProcessIdentity) error {
	return pauseWithOperation(identity, pauseProcess)
}

func pauseWithOperation(identity ProcessIdentity, operation func(ProcessIdentity) error) error {
	alive, err := Alive(identity)
	if err != nil {
		return fmt.Errorf("verify process before pause: %w", err)
	}
	if !alive {
		return nil
	}
	if err := operation(identity); err != nil {
		return fmt.Errorf("pause process tree %d: %w", identity.PID, err)
	}

	return nil
}

// Resume continues a verified target process tree where supported.
func Resume(identity ProcessIdentity) error {
	return resumeWithOperation(identity, resumeProcess)
}

func resumeWithOperation(identity ProcessIdentity, operation func(ProcessIdentity) error) error {
	alive, err := Alive(identity)
	if err != nil {
		return fmt.Errorf("verify process before resume: %w", err)
	}
	if !alive {
		return nil
	}
	if err := operation(identity); err != nil {
		return fmt.Errorf("resume process tree %d: %w", identity.PID, err)
	}

	return nil
}
