//go:build windows

package jobman

import "os"

func extensionProcessExitCode(state *os.ProcessState) int { return state.ExitCode() }
