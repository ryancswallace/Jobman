package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func prepareStateDir(path string) (string, error) {
	if path == "" {
		return "", errors.New("state directory is required")
	}

	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve state directory: %w", err)
	}
	absolute = filepath.Clean(absolute)

	_, inspectErr := os.Lstat(absolute)
	created := errors.Is(inspectErr, os.ErrNotExist)
	err = os.MkdirAll(absolute, 0o700)
	if err != nil {
		return "", fmt.Errorf("create state directory: %w", err)
	}
	if created {
		if hardenErr := hardenPath(absolute); hardenErr != nil {
			return "", fmt.Errorf("restrict new state directory: %w", hardenErr)
		}
	}

	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("canonicalize state directory: %w", err)
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return "", fmt.Errorf("resolve canonical state directory: %w", err)
	}

	info, err := os.Lstat(canonical)
	if err != nil {
		return "", fmt.Errorf("inspect state directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("state path is not a real directory")
	}
	if err := hardenExistingEmptyStateDirectory(canonical, created); err != nil {
		return "", err
	}
	if err := validatePermissions(info); err != nil {
		return "", fmt.Errorf("validate state directory permissions: %w", err)
	}
	if err := validateOwner(info); err != nil {
		return "", fmt.Errorf("validate state directory owner: %w", err)
	}
	if err := validatePathSecurity(canonical); err != nil {
		return "", fmt.Errorf("validate state directory access: %w", err)
	}

	return canonical, nil
}

func hardenExistingEmptyStateDirectory(path string, created bool) error {
	if created {
		return nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("inspect existing state directory contents: %w", err)
	}
	if len(entries) != 0 {
		return nil
	}
	if err := hardenPath(path); err != nil {
		return fmt.Errorf("restrict empty state directory: %w", err)
	}

	return nil
}

func prepareDatabaseFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- path is derived from the validated state root.
	if err == nil {
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("close new database file: %w", closeErr)
		}

		if hardenErr := hardenPath(path); hardenErr != nil {
			return fmt.Errorf("restrict new database file: %w", hardenErr)
		}

		return validateDatabaseFile(path)
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create database file: %w", err)
	}
	if err := hardenTrustedExistingDatabaseFile(path); err != nil {
		return fmt.Errorf("restrict existing database file: %w", err)
	}

	return validateDatabaseFile(path)
}

func validateDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect database file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("database path is not a regular file")
	}
	if err := validatePermissions(info); err != nil {
		return fmt.Errorf("validate database file permissions: %w", err)
	}
	if err := validateOwner(info); err != nil {
		return fmt.Errorf("validate database file owner: %w", err)
	}
	if err := validateSingleLink(info); err != nil {
		return fmt.Errorf("validate database file links: %w", err)
	}
	if err := validatePathSecurity(path); err != nil {
		return fmt.Errorf("validate database file access: %w", err)
	}

	return nil
}

func validateDatabaseSidecars(databasePath string) error {
	return validateDatabaseSidecarsWith(databasePath, validatePathSecurity)
}

func validateExistingDatabaseSidecars(databasePath string) error {
	return validateDatabaseSidecarsWith(databasePath, validateExistingDatabaseSidecarSecurity)
}

func validateDatabaseSidecarsWith(databasePath string, validateSecurity func(string) error) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		path := databasePath + suffix
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect database sidecar %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("database sidecar %q is not a regular file", path)
		}
		if err := validatePermissions(info); err != nil {
			return fmt.Errorf("validate database sidecar permissions: %w", err)
		}
		if err := validateOwner(info); err != nil {
			return fmt.Errorf("validate database sidecar owner: %w", err)
		}
		if err := validateSingleLink(info); err != nil {
			return fmt.Errorf("validate database sidecar links: %w", err)
		}
		if err := validateSecurity(path); err != nil {
			// SQLite can remove a sidecar when the final concurrent connection
			// closes, between Lstat and the Windows ACL query. Missing sidecars
			// are allowed, but never skip an access failure on a replacement.
			if databaseSidecarDisappeared(path, err) {
				continue
			}
			return fmt.Errorf("validate database sidecar access: %w", err)
		}
	}

	return nil
}

func databaseSidecarDisappeared(path string, validationErr error) bool {
	if !errors.Is(validationErr, os.ErrNotExist) {
		return false
	}
	_, err := os.Lstat(path)

	return errors.Is(err, os.ErrNotExist)
}

func hardenDatabaseSidecars(databasePath string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		path := databasePath + suffix
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect database sidecar %q before hardening: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("database sidecar %q is not a regular file", path)
		}
		if err := hardenPath(path); err != nil {
			return fmt.Errorf("restrict database sidecar %q: %w", path, err)
		}
	}

	return nil
}
