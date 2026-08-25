//go:build darwin

package agent

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func requireLocalFilesystem(path string) error {
	var information unix.Statfs_t
	if err := unix.Statfs(path, &information); err != nil {
		return fmt.Errorf("inspect agent state filesystem: %w", err)
	}
	if information.Flags&unix.MNT_LOCAL == 0 {
		return errors.New("agent state directory must be on a host-local filesystem")
	}

	return nil
}
