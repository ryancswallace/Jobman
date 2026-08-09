//go:build !darwin && !linux

package resourceusage

import "os"

func peakResidentMemory(*os.ProcessState) (uint64, bool) { return 0, false }
