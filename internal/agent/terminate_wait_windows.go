//go:build windows

package agent

import (
	"errors"
	"os"
	"os/exec"
	"time"

	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/protocol"
)

func terminateAndWait(
	command *exec.Cmd,
	identity platform.ProcessIdentity,
	waited <-chan error,
	outcome, failureCode string,
) protocol.ProcessResult {
	if err := platform.Terminate(identity, false); err != nil {
		failureCode = "graceful_termination_failed"
	}
	timer := time.NewTimer(defaultTerminationGrace)
	defer timer.Stop()
	select {
	case <-waited:
	case <-timer.C:
		if err := platform.Terminate(identity, true); err != nil {
			failureCode = "forced_termination_failed"
		}
		forcedTimer := time.NewTimer(defaultTerminationGrace)
		select {
		case <-waited:
			forcedTimer.Stop()
		case <-forcedTimer.C:
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				failureCode = "forced_termination_failed"
			}
			finalTimer := time.NewTimer(defaultTerminationGrace)
			select {
			case <-waited:
				finalTimer.Stop()
			case <-finalTimer.C:
				failureCode = "process_wait_failed"
			}
		}
	}
	result := processResult(command.ProcessState, nil, failureCode)
	result.Outcome = outcome

	return result
}
