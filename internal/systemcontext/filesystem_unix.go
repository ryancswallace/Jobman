//go:build darwin || linux

package systemcontext

import (
	"math/bits"

	"golang.org/x/sys/unix"

	"github.com/ryancswallace/jobman/diagnostic"
)

func observeFilesystem(path string) *diagnostic.FilesystemCapacity {
	var statistics unix.Statfs_t
	if path == "" || unix.Statfs(path, &statistics) != nil || statistics.Bsize <= 0 {
		return nil
	}
	blockSize := uint64(statistics.Bsize)
	totalHigh, total := bits.Mul64(statistics.Blocks, blockSize)
	availableHigh, available := bits.Mul64(statistics.Bavail, blockSize)
	if totalHigh != 0 || availableHigh != 0 || total == 0 || available > total {
		return nil
	}

	return &diagnostic.FilesystemCapacity{
		Scope:          diagnostic.SystemFilesystemScope,
		Source:         diagnostic.SystemFilesystemStatfs,
		AvailableBytes: available,
		TotalBytes:     total,
	}
}
