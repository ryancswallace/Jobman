//go:build !darwin && !linux && !windows

package agent

import (
	"errors"
	"os"
)

func tryLockStateFile(_ *os.File) error {
	return errors.New("agent state locking is unsupported on this platform")
}
