package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInstallUserServiceRejectsUnsafeUnitPublication(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		configure func(*testing.T, *InstallServiceOptions)
		want      string
	}{
		{
			name: "invalid execution policy",
			configure: func(_ *testing.T, options *InstallServiceOptions) {
				options.PollInterval = time.Nanosecond
			},
			want: "invalid polling",
		},
		{
			name: "newline in service argument",
			configure: func(_ *testing.T, options *InstallServiceOptions) {
				options.ArtifactStoreName = "department-nfs"
				options.ArtifactRoot = "/artifacts\nExecStart=/unexpected"
			},
			want: "single-line",
		},
		{
			name: "unit parent is a regular file",
			configure: func(t *testing.T, options *InstallServiceOptions) {
				t.Helper()
				if err := os.WriteFile(options.UnitDirectory, []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "create unit directory",
		},
		{
			name: "unit path is a directory",
			configure: func(t *testing.T, options *InstallServiceOptions) {
				t.Helper()
				if err := os.MkdirAll(filepath.Join(options.UnitDirectory, systemdUserUnitName), 0o700); err != nil {
					t.Fatal(err)
				}
			},
			want: "write unit",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateDirectory := t.TempDir()
			saveServiceTestCredentials(t, stateDirectory)
			options := InstallServiceOptions{
				StateDirectory: stateDirectory, AgentBinary: os.Args[0],
				UnitDirectory: filepath.Join(t.TempDir(), "units"), operatingSystem: "linux",
			}
			test.configure(t, &options)
			unit, err := InstallUserService(t.Context(), options)
			if unit != "" || err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("InstallUserService() = %q, %v, want %q", unit, err, test.want)
			}
			if _, err = loadCredentials(stateDirectory); err != nil {
				t.Fatalf("failed installation damaged enrollment: %v", err)
			}
			matches, err := filepath.Glob(filepath.Join(options.UnitDirectory, ".jobman-agent-*"))
			if err != nil || len(matches) != 0 {
				t.Fatalf("failed installation left temporary files: %v, %v", matches, err)
			}
		})
	}
}
