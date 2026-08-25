package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRenderSystemdUserUnitPreservesArguments(t *testing.T) {
	arguments, err := serviceRunArguments(InstallServiceOptions{
		PollInterval: 3 * time.Second, MaximumLogBytes: 1024,
		ArtifactStoreName: "department-nfs", ArtifactStoreVersion: 2,
		ArtifactRoot: "/nfs/research data", SlurmRoot: "/nfs/slurm",
		SlurmRunner: "/nfs/bin/jobman-agent",
	}, "/home/researcher/.local/state/jobman-agent")
	if err != nil {
		t.Fatalf("serviceRunArguments() error = %v", err)
	}
	unit, err := renderSystemdUserUnit("/home/researcher/bin/jobman-agent", arguments)
	if err != nil {
		t.Fatalf("renderSystemdUserUnit() error = %v", err)
	}
	for _, expected := range []string{
		`ExecStart="/home/researcher/bin/jobman-agent" "run"`,
		`"/nfs/research data"`,
		"NoNewPrivileges=true",
		"UMask=0077",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, expected) {
			t.Fatalf("unit missing %q:\n%s", expected, unit)
		}
	}
	quoted, err := quoteSystemdArgument(`/tmp/$HOME/%i/"agent"`)
	if err != nil || quoted != `"/tmp/$$HOME/%%i/\"agent\""` {
		t.Fatalf("quoteSystemdArgument() = %q, %v", quoted, err)
	}
	if _, err = quoteSystemdArgument("bad\nvalue"); err == nil {
		t.Fatal("quoteSystemdArgument() accepted a newline")
	}
}

func TestInstallUserServiceLifecycle(t *testing.T) {
	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	binary := filepath.Join(t.TempDir(), "jobman-agent")
	if err := os.WriteFile(binary, []byte("binary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0o700); err != nil { //nolint:gosec // The fixture must be executable to exercise installation.
		t.Fatal(err)
	}
	unitDirectory := filepath.Join(t.TempDir(), "systemd", "user")
	var systemctlCalls int
	unit, err := InstallUserService(t.Context(), InstallServiceOptions{
		StateDirectory: stateDirectory, AgentBinary: binary, UnitDirectory: unitDirectory,
		Start: true, operatingSystem: "linux",
		runSystemctl: func(_ context.Context, arguments []string) ([]byte, error) {
			systemctlCalls++
			if len(arguments) < 2 || arguments[0] != systemdUserArgument {
				t.Fatalf("systemctl arguments = %#v", arguments)
			}
			return nil, nil
		},
	})
	if err != nil || unit != filepath.Join(unitDirectory, systemdUserUnitName) || systemctlCalls != 3 {
		t.Fatalf("InstallUserService() = %q, %v calls=%d", unit, err, systemctlCalls)
	}
	contents, err := os.ReadFile(unit)
	if err != nil || !strings.Contains(string(contents), binary) {
		t.Fatalf("installed unit = %q, %v", contents, err)
	}
	information, err := os.Stat(unit)
	if err != nil {
		t.Fatalf("stat installed unit: %v", err)
	}
	if information.Mode().Perm() != 0o600 {
		t.Fatalf("unit mode = %v", information.Mode())
	}

	status, err := loadStatus(stateDirectory)
	if err != nil || status.Metadata.AgentID != testAgentID || status.Status.ServerURL == "" {
		t.Fatalf("loadStatus() = %#v, %v", status, err)
	}
}

func TestInstallUserServiceUsesHostDefaults(t *testing.T) {
	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	home := t.TempDir()
	t.Setenv("HOME", home)
	configurationRoot, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	unit, err := InstallUserService(t.Context(), InstallServiceOptions{
		StateDirectory: stateDirectory, operatingSystem: "linux",
	})
	if err != nil {
		t.Fatalf("InstallUserService(defaults) error = %v", err)
	}
	want := filepath.Join(configurationRoot, "systemd", "user", systemdUserUnitName)
	if unit != want {
		t.Fatalf("InstallUserService(defaults) = %q, want %q", unit, want)
	}
}

func TestInstallUserServiceRejectsInvalidHostState(t *testing.T) {
	if _, err := InstallUserService(nil, InstallServiceOptions{}); err == nil { //nolint:staticcheck // Nil is the validation case.
		t.Fatal("InstallUserService() accepted nil context")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{operatingSystem: "darwin"}); err == nil {
		t.Fatal("InstallUserService() accepted unsupported operating system")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: "relative",
	}); err == nil {
		t.Fatal("InstallUserService() accepted relative state directory")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: t.TempDir(),
	}); err == nil {
		t.Fatal("InstallUserService() accepted missing enrollment")
	}

	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory,
		executablePath: func() (string, error) { return "", errors.New("missing") },
	}); err == nil {
		t.Fatal("InstallUserService() ignored executable lookup failure")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory,
		AgentBinary: filepath.Join(t.TempDir(), "missing"),
	}); err == nil {
		t.Fatal("InstallUserService() accepted missing binary")
	}
	nonExecutable := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(nonExecutable, []byte("binary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory, AgentBinary: nonExecutable,
	}); err == nil {
		t.Fatal("InstallUserService() accepted non-executable binary")
	}
}

func TestInstallUserServiceSurfacesManagerFailures(t *testing.T) {
	stateDirectory := t.TempDir()
	saveServiceTestCredentials(t, stateDirectory)
	binary := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(binary, []byte("binary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0o700); err != nil { //nolint:gosec // The fixture must be executable to exercise validation.
		t.Fatal(err)
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory, AgentBinary: binary,
		userConfigDirectory: func() (string, error) { return "", errors.New("no config") },
	}); err == nil {
		t.Fatal("InstallUserService() ignored user configuration failure")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory, AgentBinary: binary,
		UnitDirectory: "relative",
	}); err == nil {
		t.Fatal("InstallUserService() accepted relative unit directory")
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory, AgentBinary: binary,
		UnitDirectory: t.TempDir(), Start: true,
		runSystemctl: func(context.Context, []string) ([]byte, error) {
			return []byte(strings.Repeat("x", 2048)), errors.New("failed")
		},
	}); err == nil || len(err.Error()) > 1200 {
		t.Fatalf("InstallUserService(systemctl) error = %v", err)
	}
	if _, err := InstallUserService(t.Context(), InstallServiceOptions{
		operatingSystem: "linux", StateDirectory: stateDirectory, AgentBinary: binary,
		UnitDirectory: t.TempDir(), Start: true,
		runSystemctl: func(context.Context, []string) ([]byte, error) {
			return nil, errors.New("failed")
		},
	}); err == nil {
		t.Fatal("InstallUserService() ignored an empty systemctl failure")
	}
}

func saveServiceTestCredentials(t *testing.T, stateDirectory string) {
	t.Helper()
	expires := time.Now().UTC().Add(time.Hour)
	if err := saveCredentials(stateDirectory, credentialFiles{
		Metadata: metadata{
			ServerURL: "https://control.example.test", AgentID: testAgentID,
			TargetGenerationID: testTargetGenerationID,
			SessionID:          "88888888-8888-4888-8888-888888888888", SessionToken: "session",
			SessionExpiresAt: expires, CertificateExpiresAt: expires,
		},
		PrivateKeyPEM: []byte("key"), CertificatePEM: []byte("certificate"),
	}); err != nil {
		t.Fatalf("saveCredentials() error = %v", err)
	}
}

func TestServiceRunArgumentValidation(t *testing.T) {
	if _, err := serviceRunArguments(InstallServiceOptions{PollInterval: time.Nanosecond}, "/state"); err == nil {
		t.Fatal("serviceRunArguments() accepted invalid polling interval")
	}
	if _, err := serviceRunArguments(InstallServiceOptions{
		ArtifactStoreName: "store",
	}, "/state"); err == nil {
		t.Fatal("serviceRunArguments() accepted incomplete artifact configuration")
	}
	if _, err := serviceRunArguments(InstallServiceOptions{
		SlurmRoot: "/nfs/slurm",
	}, "/state"); err == nil {
		t.Fatal("serviceRunArguments() accepted incomplete Slurm configuration")
	}
}
