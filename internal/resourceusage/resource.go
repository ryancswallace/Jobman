// Package resourceusage converts portable post-wait process accounting into
// public, explicitly scoped diagnostic observations.
package resourceusage

import (
	"os"
	"slices"

	"github.com/ryancswallace/jobman/diagnostic"
)

// Observe returns facts available after a target has been reaped. CPU and
// memory observations are process-scoped; this package does not infer totals
// for descendants in the target process tree.
func Observe(state *os.ProcessState) []diagnostic.ResourceObservation {
	if state == nil {
		return []diagnostic.ResourceObservation{}
	}

	values := []diagnostic.ResourceObservation{
		newCPUObservation(diagnostic.ResourceCPUUserTime, state.UserTime()),
		newCPUObservation(diagnostic.ResourceCPUSystemTime, state.SystemTime()),
	}
	if bytes, ok := peakResidentMemory(state); ok {
		values = append(values, diagnostic.ResourceObservation{
			Metric:       diagnostic.ResourcePeakRSS,
			Value:        bytes,
			Unit:         diagnostic.ResourceUnitBytes,
			Scope:        diagnostic.ResourceScopeProcess,
			Source:       diagnostic.ResourceSourceWaitRusage,
			Completeness: diagnostic.ResourceCompleteAtExit,
		})
	}
	slices.SortFunc(values, func(left, right diagnostic.ResourceObservation) int {
		return stringCompare(left.Metric, right.Metric)
	})

	return values
}

func newCPUObservation(metric string, value interface{ Nanoseconds() int64 }) diagnostic.ResourceObservation {
	nanoseconds := value.Nanoseconds()
	if nanoseconds < 0 {
		nanoseconds = 0
	}

	return diagnostic.ResourceObservation{
		Metric:       metric,
		Value:        uint64(nanoseconds), // #nosec G115 -- negative durations are normalized above.
		Unit:         diagnostic.ResourceUnitNanoseconds,
		Scope:        diagnostic.ResourceScopeProcess,
		Source:       diagnostic.ResourceSourceProcessState,
		Completeness: diagnostic.ResourceCompleteAtExit,
	}
}

func stringCompare(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
