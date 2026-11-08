package logstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReleaseAbandonedRunDetectsMarkerMutation(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	run, err := CreateRun(stateDir, testJobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := run.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	marker := run.Paths().Active
	err = ReleaseAbandonedRun(t.Context(), stateDir, testJobID, 1, func(context.Context) (bool, error) {
		if removeErr := os.Remove(marker); removeErr != nil {
			t.Fatal(removeErr)
		}
		return true, nil
	})
	if err == nil {
		t.Fatal("ReleaseAbandonedRun() error = nil")
	}
}

func TestCleanupRunDetectsDirectoryMutationAfterEligibility(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*testing.T, string){
		"removed": func(t *testing.T, directory string) {
			t.Helper()
			if err := os.RemoveAll(directory); err != nil {
				t.Fatal(err)
			}
		},
		"active marker appeared": func(t *testing.T, directory string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(directory, activeFilename), nil, fileMode); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stateDir := filepath.Join(t.TempDir(), "state")
			run, err := CreateRun(stateDir, testJobID, 1)
			if err != nil {
				t.Fatal(err)
			}
			directory := run.Paths().Directory
			if closeErr := run.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			checks := 0
			_, err = CleanupRun(t.Context(), stateDir, testJobID, 1, func(context.Context) (bool, error) {
				checks++
				if checks == 2 {
					mutate(t, directory)
				}
				return true, nil
			})
			if err == nil {
				t.Fatal("CleanupRun() error = nil")
			}
		})
	}
}

func TestCleanupEntryAndSummaryFailureEdges(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, stdoutFilename)
	if err := os.WriteFile(path, []byte("data"), fileMode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	claim := cleanupClaim{path: root, entries: []cleanupEntry{{name: stdoutFilename, info: info}}}
	if _, err := removeCleanupClaim(claim); err == nil {
		t.Fatal("removeCleanupClaim(missing entry) error = nil")
	}

	summary := filepath.Join(root, cleanupSummaryFilename)
	if err := os.WriteFile(summary, []byte("corrupt"), fileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureCleanupSummary(root, nil); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("ensureCleanupSummary(corrupt without entries) error = %v", err)
	}
}
