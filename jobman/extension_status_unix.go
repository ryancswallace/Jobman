//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package jobman

import (
	"os"
	"syscall"
)

func extensionProcessExitCode(state *os.ProcessState) int {
	status, ok := state.Sys().(syscall.WaitStatus)
	if ok && status.Signaled() {
		return 128 + int(status.Signal())
	}

	return state.ExitCode()
}
