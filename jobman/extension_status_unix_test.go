//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package jobman

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestRunExtensionMapsSignalStatus(t *testing.T) {
	t.Parallel()

	stdin, keepOpen, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := stdin.Close(); closeErr != nil {
			t.Errorf("close helper stdin: %v", closeErr)
		}
		if closeErr := keepOpen.Close(); closeErr != nil {
			t.Errorf("close helper stdin writer: %v", closeErr)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	status, err := runExtension(ctx, extensionInvocation{
		Path:  os.Args[0],
		Args:  []string{"-test.run=^TestExtensionSignalHelperProcess$"},
		Env:   []string{"JOBMAN_EXTENSION_SIGNAL_HELPER=1"},
		Stdin: stdin,
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
	var input [1]byte
	if _, err := os.Stdin.Read(input[:]); err != nil {
		os.Exit(96)
	}
	os.Exit(98)
}
