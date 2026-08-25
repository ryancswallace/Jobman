//go:build !windows

package agent

import (
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
		<-waited
	}
	result := processResult(command.ProcessState, nil, failureCode)
	result.Outcome = outcome

	return result
}
