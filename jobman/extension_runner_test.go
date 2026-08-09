package jobman

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunExtensionPreservesStreamsAndExitStatus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		mode       string
		wantStatus int
	}{
		{name: "success", mode: "success", wantStatus: 0},
		{name: "nonzero", mode: "nonzero", wantStatus: 23},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			coverageDirectory := t.TempDir()
			status, err := runExtension(t.Context(), extensionInvocation{
				Path: os.Args[0],
				Args: []string{"-test.run=^TestExtensionRunnerHelperProcess$"},
				Env: []string{
					"JOBMAN_EXTENSION_RUNNER_HELPER=1",
					"JOBMAN_EXTENSION_RUNNER_MODE=" + test.mode,
					"GOCOVERDIR=" + coverageDirectory,
				},
				Stdin:  strings.NewReader("bounded input"),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if err != nil || status != test.wantStatus {
				t.Fatalf("runExtension() status/error = %d/%v, want %d/nil", status, err, test.wantStatus)
			}
			if test.mode == "success" && (stdout.String() != "stdout:bounded input" || stderr.String() != "stderr") {
				t.Fatalf("runExtension() stdout/stderr = %q/%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunExtensionReturnsContextAndLaunchErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	status, err := runExtension(ctx, extensionInvocation{Path: os.Args[0]})
	if status != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled runExtension() status/error = %d/%v", status, err)
	}

	missing := filepath.Join(t.TempDir(), "missing-extension")
	status, err = runExtension(t.Context(), extensionInvocation{Path: missing})
	if status != 0 || err == nil {
		t.Fatalf("missing runExtension() status/error = %d/%v", status, err)
	}
}

//nolint:revive // This isolated test subprocess must exit with controlled process statuses.
func TestExtensionRunnerHelperProcess(_ *testing.T) {
	if os.Getenv("JOBMAN_EXTENSION_RUNNER_HELPER") != "1" {
		return
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, "read stdin")
		os.Exit(97)
	}
	switch os.Getenv("JOBMAN_EXTENSION_RUNNER_MODE") {
	case "success":
		_, _ = fmt.Fprintf(os.Stdout, "stdout:%s", input)
		_, _ = fmt.Fprint(os.Stderr, "stderr")
		os.Exit(0)
	case "nonzero":
		os.Exit(23)
	default:
		os.Exit(98)
	}
}
