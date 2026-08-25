package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func prepareStateDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("agent state directory must be absolute")
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", fmt.Errorf("create agent state directory: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil { // #nosec G302 -- directories require owner traversal.
		return "", fmt.Errorf("protect agent state directory: %w", err)
	}
	if err := requireLocalFilesystem(path); err != nil {
		return "", err
	}

	return path, nil
}

func writePrivateFile(path string, contents []byte) (resultErr error) {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".jobman-agent-*")
	if err != nil {
		return fmt.Errorf("create private temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		removeErr := os.Remove(temporaryName)
		if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove private temporary file: %w", removeErr))
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return errors.Join(fmt.Errorf("protect private temporary file: %w", err), temporary.Close())
	}
	if _, err = temporary.Write(contents); err != nil {
		return errors.Join(fmt.Errorf("write private temporary file: %w", err), temporary.Close())
	}
	if err = temporary.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync private temporary file: %w", err), temporary.Close())
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close private temporary file: %w", err)
	}
	if err = os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("replace private file: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open private file directory: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil {
		return errors.Join(fmt.Errorf("sync private file directory: %w", syncErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close private file directory: %w", closeErr)
	}

	return nil
}

func writeNewPrivateFile(path string, contents []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(contents); err != nil {
		return errors.Join(err, file.Close())
	}
	if err = file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}

	return file.Close()
}

func removePrivateFile(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open private file directory: %w", err)
	}
	if err = directory.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync private file directory: %w", err), directory.Close())
	}
	if err = directory.Close(); err != nil {
		return fmt.Errorf("close private file directory: %w", err)
	}

	return nil
}
