//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package jobman

import (
	"os"
	"syscall"
	"testing"
)

func TestRunExtensionMapsSignalStatus(t *testing.T) {
	t.Parallel()

	status, err := runExtension(t.Context(), extensionInvocation{
		Path: os.Args[0],
		Args: []string{"-test.run=^TestExtensionSignalHelperProcess$"},
		Env:  []string{"JOBMAN_EXTENSION_SIGNAL_HELPER=1"},
	})
	if err != nil || status != 128+int(syscall.SIGTERM) {
		t.Fatalf("signaled runExtension() status/error = %d/%v", status, err)
	}
}

//nolint:revive // This isolated test subprocess must terminate itself by signal.
func TestExtensionSignalHelperProcess(_ *testing.T) {
	if os.Getenv("JOBMAN_EXTENSION_SIGNAL_HELPER") != "1" {
		return
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		os.Exit(97)
	}
	os.Exit(98)
}
