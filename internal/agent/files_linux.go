//go:build linux

package agent

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const linuxNFSFilesystem = 0x6969

func requireLocalFilesystem(path string) error {
	var information unix.Statfs_t
	if err := unix.Statfs(path, &information); err != nil {
		return fmt.Errorf("inspect agent state filesystem: %w", err)
	}
	if information.Type == linuxNFSFilesystem {
		return errors.New("agent state directory must not be stored on NFS")
	}

	return nil
}
