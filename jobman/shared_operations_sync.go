//go:build !windows

package jobman

import (
	"errors"
	"os"
)

func syncSharedOperationDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}

	return errors.Join(directory.Sync(), directory.Close())
}
