//go:build linux

package resourceusage

import (
	"math"
	"os"
	"syscall"
)

func peakResidentMemory(state *os.ProcessState) (uint64, bool) {
	usage, ok := state.SysUsage().(*syscall.Rusage)
	if !ok || usage.Maxrss < 0 {
		return 0, false
	}
	kibibytes := uint64(usage.Maxrss)
	if kibibytes > math.MaxUint64/1024 {
		return 0, false
	}

	return kibibytes * 1024, true
}
