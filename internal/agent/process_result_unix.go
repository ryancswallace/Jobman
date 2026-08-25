//go:build !windows

package agent

import (
	"os"
	"syscall"
)

func processSignal(state *os.ProcessState) string {
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() {
		return ""
	}

	return status.Signal().String()
}
