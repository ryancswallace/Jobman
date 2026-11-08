package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// lockState serializes service and credential writers without locking the
// execution journal used by child runners. Keep the lock file across restarts;
// removing it would permit two processes to lock different files.
func lockState(directory string) (*os.File, error) {
	path := filepath.Join(directory, "agent.lock")
	if information, err := os.Lstat(path); err == nil {
		if !information.Mode().IsRegular() {
			return nil, errors.New("agent state lock must be a regular file")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inspect agent state lock: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open agent state lock: %w", err)
	}
	fail := func(err error) (*os.File, error) { return nil, errors.Join(err, file.Close()) }
	opened, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("inspect open agent state lock: %w", err))
	}
	named, err := os.Lstat(path)
	if err != nil {
		return fail(fmt.Errorf("inspect named agent state lock: %w", err))
	}
	if !opened.Mode().IsRegular() || !named.Mode().IsRegular() || !os.SameFile(opened, named) ||
		!privateFileMode(opened.Mode()) {
		return fail(errors.New("agent state lock must be a private regular file"))
	}
	if err = tryLockStateFile(file); err != nil {
		return fail(fmt.Errorf("agent state is in use; stop the agent service before enrollment or recovery: %w", err))
	}
	return file, nil
}

func privateFileMode(mode fs.FileMode) bool {
	return runtime.GOOS == operatingSystemWindows || mode.Perm()&0o077 == 0
}
