//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package jobman

import "os"

func extensionProcessExitCode(state *os.ProcessState) int { return state.ExitCode() }
