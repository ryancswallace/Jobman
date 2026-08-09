package resourceusage

import (
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/ryancswallace/jobman/diagnostic"
)

const resourceHelperEnvironment = "JOBMAN_RESOURCE_USAGE_TEST_HELPER"

func TestObserveReapedProcess(t *testing.T) {
	if os.Getenv(resourceHelperEnvironment) == "1" {
		for range 10000 {
			_ = make([]byte, 1024)
		}
		return
	}

	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestObserveReapedProcess$") // #nosec G204,G702 -- Executes this fixed test binary only.
	command.Env = append(os.Environ(), resourceHelperEnvironment+"=1")
	if err := command.Run(); err != nil {
		t.Fatalf("run accounting helper: %v", err)
	}
	observations := Observe(command.ProcessState)
	seen := make(map[string]bool, len(observations))
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			t.Fatalf("observation %#v is invalid: %v", observation, err)
		}
		if seen[observation.Metric] {
			t.Fatalf("duplicate metric %q", observation.Metric)
		}
		seen[observation.Metric] = true
		if observation.Scope != diagnostic.ResourceScopeProcess ||
			observation.Completeness != diagnostic.ResourceCompleteAtExit {
			t.Fatalf("observation overstates scope or completeness: %#v", observation)
		}
	}
	if !seen[diagnostic.ResourceCPUUserTime] || !seen[diagnostic.ResourceCPUSystemTime] {
		t.Fatalf("portable CPU observations = %#v", observations)
	}
	if (runtime.GOOS == "linux" || runtime.GOOS == "darwin") && !seen[diagnostic.ResourcePeakRSS] {
		t.Fatalf("native peak-resident-memory observation missing: %#v", observations)
	}
}

func TestObserveNilIsEmpty(t *testing.T) {
	t.Parallel()

	if observations := Observe(nil); len(observations) != 0 || observations == nil {
		t.Fatalf("Observe(nil) = %#v, want non-nil empty slice", observations)
	}
}

func TestResourceHelpersNormalizeAndOrder(t *testing.T) {
	t.Parallel()

	observation := newCPUObservation(diagnostic.ResourceCPUUserTime, fakeNanoseconds(-1))
	if observation.Value != 0 {
		t.Fatalf("negative CPU observation value = %d", observation.Value)
	}
	if stringCompare("a", "b") != -1 || stringCompare("b", "a") != 1 || stringCompare("a", "a") != 0 {
		t.Fatal("stringCompare() returned an unexpected ordering")
	}
}

type fakeNanoseconds int64

func (value fakeNanoseconds) Nanoseconds() int64 { return int64(value) }
