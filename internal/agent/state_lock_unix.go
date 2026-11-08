//go:build darwin || linux

package agent

import (
	"os"

	"golang.org/x/sys/unix"
)

func tryLockStateFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
