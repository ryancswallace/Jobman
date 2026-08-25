package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/internal/runtimeenv"
)

type fakeDistributedCommandRunner struct {
	output []byte
	err    error
	calls  int
}

func (runner *fakeDistributedCommandRunner) Run(
	context.Context,
	string,
	...string,
) ([]byte, error) {
	runner.calls++

	return runner.output, runner.err
}

func TestArtifactStoreSelectionAndRunnerArguments(t *testing.T) {
	t.Parallel()
	store, capability, err := newArtifactStore(t.Context(), artifactStoreOptions{})
	if err != nil || store != nil || capability != "" {
		t.Fatalf("newArtifactStore(empty) = %#v, %q, %v", store, capability, err)
	}
	if _, _, err = newArtifactStore(t.Context(), artifactStoreOptions{name: "store", version: 1}); err == nil {
		t.Fatal("newArtifactStore() accepted no physical mapping")
	}
	if _, _, err = newArtifactStore(t.Context(), artifactStoreOptions{
		name: "store", version: 1, root: t.TempDir(), s3Region: "us-east-1",
	}); err == nil {
		t.Fatal("newArtifactStore() accepted S3 policy for a filesystem mapping")
	}
	filesystemStore, capability, err := newArtifactStore(t.Context(), artifactStoreOptions{
		name: "department-nfs", version: 2, root: t.TempDir(),
	})
	if err != nil || capability != "artifact-filesystem" {
		t.Fatalf("newArtifactStore(filesystem) = %#v, %q, %v", filesystemStore, capability, err)
	}
	selectedFilesystem, ok := filesystemStore.(*artifact.FilesystemStore)
	if !ok {
		t.Fatalf("filesystem store type = %T", filesystemStore)
	}
	arguments := appendArtifactRunnerArguments([]string{"run"}, filesystemStore, 99)
	if !containsStrings(arguments, "--artifact-root", selectedFilesystem.Root()) ||
		!containsStrings(arguments, "--max-artifact-bytes", "99") {
		t.Fatalf("filesystem arguments = %q", arguments)
	}
	runner := &fakeDistributedCommandRunner{output: []byte("aws-cli/2")}
	s3Store, capability, err := newArtifactStore(t.Context(), artifactStoreOptions{
		name: "department-s3", version: 4, s3Bucket: "department-artifacts",
		s3Prefix: "jobman", s3Region: "us-east-1", s3ExpectedOwner: "123456789012",
		awsExecutable: "/opt/aws", s3Runner: runner,
	})
	if err != nil || capability != "artifact-s3" || runner.calls != 1 {
		t.Fatalf("newArtifactStore(S3) = %#v, %q, %v calls=%d", s3Store, capability, err, runner.calls)
	}
	arguments = appendArtifactRunnerArguments(nil, s3Store, 0)
	for _, sequence := range [][]string{
		{"--artifact-store", "department-s3"},
		{"--artifact-s3-bucket", "department-artifacts"},
		{"--artifact-s3-prefix", "jobman"},
		{"--artifact-s3-region", "us-east-1"},
		{"--artifact-s3-expected-owner", "123456789012"},
		{"--aws-cli", "/opt/aws"},
	} {
		if !containsStrings(arguments, sequence...) {
			t.Fatalf("S3 arguments %q do not contain %q", arguments, sequence)
		}
	}
	if _, _, err = newArtifactStore(t.Context(), artifactStoreOptions{
		name: "department-s3", version: 1, s3Bucket: "department-artifacts",
		s3Runner: &fakeDistributedCommandRunner{err: errors.New("missing")},
	}); err == nil {
		t.Fatal("newArtifactStore() ignored an S3 probe failure")
	}
}

func TestContainerRuntimeSelectionAndRunnerArguments(t *testing.T) {
	t.Parallel()
	adapter, err := newContainerRuntime(t.Context(), "", "", false, nil)
	if err != nil || adapter != nil {
		t.Fatalf("newContainerRuntime(empty) = %#v, %v", adapter, err)
	}
	if _, err = newContainerRuntime(t.Context(), "", "custom", false, nil); err == nil {
		t.Fatal("newContainerRuntime() accepted an executable without an engine")
	}
	runner := &fakeDistributedCommandRunner{}
	adapter, err = newContainerRuntime(t.Context(), "docker", "/opt/docker", true, runner)
	if err != nil || runner.calls != 1 {
		t.Fatalf("newContainerRuntime() = %#v, %v calls=%d", adapter, err, runner.calls)
	}
	arguments := appendContainerRunnerArguments([]string{"run"}, adapter)
	if !reflect.DeepEqual(arguments, []string{
		"run", "--container-engine", "docker", "--container-runtime", "/opt/docker", "--container-host-network",
	}) {
		t.Fatalf("container arguments = %q", arguments)
	}
	if got := appendContainerRunnerArguments([]string{"run"}, nil); !reflect.DeepEqual(got, []string{"run"}) {
		t.Fatalf("native arguments = %q", got)
	}
	if _, err = newContainerRuntime(
		t.Context(), "podman", "podman", false, &fakeDistributedCommandRunner{err: errors.New("missing")},
	); err == nil {
		t.Fatal("newContainerRuntime() ignored a probe failure")
	}
	var _ runtimeenv.CommandRunner = runner
}

func TestDistributedServiceRunArguments(t *testing.T) {
	t.Parallel()
	arguments, err := serviceRunArguments(InstallServiceOptions{
		PollInterval: 3 * time.Second, ArtifactStoreName: "department-s3", ArtifactStoreVersion: 3,
		ArtifactS3Bucket: "department-artifacts", ArtifactS3Prefix: "jobman",
		ArtifactS3Region: "us-east-1", ArtifactS3Owner: "123456789012", AWSExecutable: "/opt/aws",
		ContainerEngine: "docker", ContainerExecutable: "/opt/docker", ContainerHostNetwork: true,
		MaximumLogBytes: 100, MaximumArtifactBytes: 200,
	}, "/state")
	if err != nil {
		t.Fatalf("serviceRunArguments() error = %v", err)
	}
	for _, sequence := range [][]string{
		{"--artifact-s3-bucket", "department-artifacts"},
		{"--artifact-s3-prefix", "jobman"},
		{"--artifact-s3-region", "us-east-1"},
		{"--artifact-s3-expected-owner", "123456789012"},
		{"--aws-cli", "/opt/aws"},
		{"--container-engine", "docker"},
		{"--container-runtime", "/opt/docker"},
		{"--container-host-network"},
	} {
		if !containsStrings(arguments, sequence...) {
			t.Fatalf("service arguments %q do not contain %q", arguments, sequence)
		}
	}
	if _, err = serviceRunArguments(InstallServiceOptions{ContainerExecutable: "docker"}, "/state"); err == nil {
		t.Fatal("serviceRunArguments() accepted container policy without an engine")
	}
	if _, err = serviceRunArguments(InstallServiceOptions{ContainerHostNetwork: true}, "/state"); err == nil {
		t.Fatal("serviceRunArguments() accepted host networking without an engine")
	}
}

func TestRunArrayTaskCLIValidationPath(t *testing.T) {
	t.Setenv("SLURM_ARRAY_TASK_ID", "0")
	if err := executeRunArrayTask(t.Context(), nil, &strings.Builder{}); err == nil {
		t.Fatal("executeRunArrayTask() accepted a missing manifest")
	}
	manifest := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := executeRunArrayTask(
		t.Context(), []string{"--manifest", manifest}, &strings.Builder{},
	); err == nil {
		t.Fatal("executeRunArrayTask() accepted an invalid manifest")
	}
	if err := executeRunArrayTask(t.Context(), []string{
		"--manifest", manifest, "--artifact-store", "incomplete",
	}, &strings.Builder{}); err == nil {
		t.Fatal("executeRunArrayTask() accepted an incomplete artifact store")
	}
	if err := executeRunArrayTask(t.Context(), []string{
		"--manifest", manifest, "--container-engine", "unknown",
	}, &strings.Builder{}); err == nil {
		t.Fatal("executeRunArrayTask() accepted an invalid container engine")
	}
}

func TestEnrollBuildsContainerCapabilitiesBeforeTransport(t *testing.T) {
	t.Parallel()
	err := executeEnroll(t.Context(), []string{
		"--state-dir", t.TempDir(), "--server", "https://127.0.0.1:1",
		"--target-generation", testTargetGenerationID,
		"--container-engine", "docker", "--container-runtime", "/usr/bin/true",
	}, strings.NewReader("token\n"), &strings.Builder{}, &strings.Builder{})
	if err == nil {
		t.Fatal("executeEnroll() unexpectedly reached an unavailable control plane")
	}
}

func TestRunServiceBuildsDistributedCapabilitiesBeforeEnrollment(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	runner := &fakeDistributedCommandRunner{output: []byte("version")}
	err := RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger,
		ArtifactStoreName: "department-s3", ArtifactStoreVersion: 1,
		ArtifactS3Bucket: "department-artifacts", s3CommandRunner: runner,
		ContainerEngine: "docker", ContainerExecutable: os.Args[0], containerCommandRunner: runner,
	})
	if err == nil || !strings.Contains(err.Error(), "credentials") || runner.calls != 2 {
		t.Fatalf("RunService(distributed) error = %v calls=%d", err, runner.calls)
	}
	if err = RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger, MaximumArtifactBytes: -1,
	}); err == nil {
		t.Fatal("RunService() accepted a negative artifact bound")
	}
	if err = RunService(t.Context(), ServiceOptions{
		StateDirectory: t.TempDir(), Logger: logger,
		ContainerEngine: "apptainer", ContainerExecutable: os.Args[0], containerCommandRunner: runner,
	}); err == nil || !strings.Contains(err.Error(), "required for Slurm") {
		t.Fatalf("RunService(apptainer subprocess) error = %v", err)
	}
}

func containsStrings(values []string, sequence ...string) bool {
	for index := 0; index+len(sequence) <= len(values); index++ {
		if reflect.DeepEqual(values[index:index+len(sequence)], sequence) {
			return true
		}
	}

	return false
}
