package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateSidecarDisappearanceDuringSecurityCheck(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		remove    bool
		accessErr error
		wantErr   bool
	}{
		{name: "disappeared", remove: true, accessErr: os.ErrNotExist},
		{name: "replacement present", accessErr: os.ErrNotExist, wantErr: true},
		{name: "access denied", remove: true, accessErr: os.ErrPermission, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			databasePath := filepath.Join(t.TempDir(), "jobs.db")
			path := databasePath + "-wal"
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			checked := false
			err := validateDatabaseSidecarsWith(databasePath, func(observed string) error {
				checked = true
				if observed != path {
					t.Fatalf("validated %q, want %q", observed, path)
				}
				if test.remove {
					if err := os.Remove(observed); err != nil {
						t.Fatal(err)
					}
				}
				return fmt.Errorf("read security descriptor: %w", test.accessErr)
			})
			if !checked {
				t.Fatalf("security validation was not reached: %v", err)
			}
			if (err != nil) != test.wantErr {
				t.Fatalf("sidecar validation error = %v, want error %v", err, test.wantErr)
			}
			if test.wantErr && !errors.Is(err, test.accessErr) {
				t.Fatalf("sidecar validation error = %v, want %v", err, test.accessErr)
			}
		})
	}
}
