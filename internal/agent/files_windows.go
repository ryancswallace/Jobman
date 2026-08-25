//go:build windows

package agent

import (
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func requireLocalFilesystem(path string) error {
	volume := filepath.VolumeName(path)
	root, err := windows.UTF16PtrFromString(volume + `\`)
	if err != nil {
		return fmt.Errorf("encode agent state volume: %w", err)
	}
	typeValue := windows.GetDriveType(root)
	if typeValue == windows.DRIVE_REMOTE {
		return errors.New("agent state directory must not be stored on a network drive")
	}
	if typeValue == windows.DRIVE_UNKNOWN || typeValue == windows.DRIVE_NO_ROOT_DIR {
		return errors.New("agent state directory filesystem could not be verified")
	}

	return nil
}
