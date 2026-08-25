package agent

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/user"
	"runtime"
	"strings"

	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	sshbootstrap "github.com/ryancswallace/jobman/internal/bootstrap/ssh"
	"github.com/ryancswallace/jobman/internal/buildinfo"
)

// ExecuteCLI runs the standalone jobman-agent command surface.
func ExecuteCLI(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		writeAgentUsage(stderr)

		return errors.New("agent command is required")
	}
	switch args[0] {
	case "enroll":
		return executeEnroll(ctx, args[1:], stdin, stdout, stderr)
	case "run":
		return executeRun(ctx, args[1:], stderr)
	case "install-service":
		return executeInstallService(ctx, args[1:], stdout, stderr)
	case "status":
		return executeStatus(args[1:], stdout, stderr)
	case "bootstrap":
		return executeBootstrap(ctx, args[1:], stdin, stdout, stderr)
	case "run-execution":
		return executeRunExecution(ctx, args[1:], stderr)
	case "run-array-task":
		return executeRunArrayTask(ctx, args[1:], stderr)
	case "version", "--version", "-version":
		_, err := fmt.Fprintln(stdout, "jobman-agent "+buildinfo.Display())

		return err
	case "help", "--help", "-h":
		writeAgentUsage(stdout)

		return nil
	default:
		writeAgentUsage(stderr)

		return fmt.Errorf("unknown agent command %q", args[0])
	}
}

//nolint:cyclop,gocognit // Enrollment validates each explicit capability and credential input.
func executeEnroll(
	ctx context.Context,
	args []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	flags := flag.NewFlagSet("jobman-agent enroll", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDirectory := flags.String("state-dir", "", "absolute host-local agent state directory")
	serverURL := flags.String("server", "", "Jobman Control HTTPS origin")
	targetGenerationID := flags.String("target-generation", "", "target generation UUID")
	tokenFile := flags.String("token-file", "-", "enrollment token file, or - for standard input")
	serverCAFile := flags.String("server-ca", "", "optional PEM server CA certificate")
	slurmEnabled := flags.Bool("slurm", false, "enroll as a Slurm CLI submit-host agent")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" || *serverURL == "" || *targetGenerationID == "" {
		return errors.New("enroll requires --state-dir, --server, and --target-generation")
	}
	token, err := readEnrollmentToken(stdin, *tokenFile)
	if err != nil {
		return err
	}
	var serverCA []byte
	if *serverCAFile != "" {
		serverCA, err = os.ReadFile(*serverCAFile)
		if err != nil {
			return fmt.Errorf("read server CA: %w", err)
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("read hostname: %w", err)
	}
	currentUser, err := user.Current()
	if err != nil {
		return fmt.Errorf("read execution user: %w", err)
	}
	executionBackends := []string{executionBackendProcess}
	var capabilities []string
	if *slurmEnabled {
		if runtime.GOOS != "linux" {
			return errors.New("slurm agent enrollment is supported only on Linux")
		}
		adapter, adapterErr := slurmbackend.New(slurmbackend.ExecRunner{})
		if adapterErr != nil {
			return adapterErr
		}
		if probeErr := adapter.Probe(ctx); probeErr != nil {
			return probeErr
		}
		executionBackends = []string{executionBackendSlurm}
		capabilities = []string{"slurm-accounting", "slurm-arrays", "slurm-cli"}
	}
	runtimes := []string{runtimeNative}
	containerRuntime, err := newContainerRuntime(
		ctx, *containerEngine, *containerExecutable, *containerHostNetwork, nil,
	)
	if err != nil {
		return err
	}
	if containerRuntime != nil {
		if *slurmEnabled != (containerRuntime.Engine() == "apptainer") {
			return errors.New("apptainer is required for Slurm and unsupported for subprocess targets")
		}
		runtimes = append(runtimes, runtimeContainer)
		capabilities = append(capabilities, "container-"+containerRuntime.Engine())
	}
	if enrollErr := Enroll(ctx, *stateDirectory, token, Enrollment{
		ServerURL: *serverURL, TargetGenerationID: *targetGenerationID,
		AgentVersion: buildinfo.Version, OperatingSystem: runtime.GOOS,
		Architecture: runtime.GOARCH, Hostname: hostname, ExecutionUser: currentUser.Username,
		ExecutionBackends: executionBackends, Runtimes: runtimes,
		Capabilities: capabilities,
		ServerCAPEM:  serverCA,
	}); enrollErr != nil {
		return enrollErr
	}
	_, err = fmt.Fprintln(stdout, "agent enrolled")

	return err
}

func executeRun(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDirectory := flags.String("state-dir", "", "absolute host-local agent state directory")
	pollInterval := flags.Duration("poll-interval", defaultPollInterval, "control-plane poll interval")
	artifactStore := flags.String("artifact-store", "", "logical local or NFS artifact store name")
	artifactStoreVersion := flags.Int64("artifact-store-version", 1, "artifact store mapping version")
	artifactRoot := flags.String("artifact-root", "", "absolute local or NFS artifact store root")
	artifactS3Bucket := flags.String("artifact-s3-bucket", "", "private S3 artifact bucket")
	artifactS3Prefix := flags.String("artifact-s3-prefix", "", "optional S3 object prefix")
	artifactS3Region := flags.String("artifact-s3-region", "", "optional AWS region")
	artifactS3Owner := flags.String("artifact-s3-expected-owner", "", "expected 12-digit AWS account ID")
	awsExecutable := flags.String("aws-cli", "aws", "AWS CLI executable")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	maximumLogBytes := flags.Int64("max-log-bytes", defaultMaximumLogBytes, "maximum retained bytes per log stream")
	maximumArtifactBytes := flags.Int64("max-artifact-bytes", defaultMaximumArtifactBytes, "maximum staged artifact bytes per execution")
	slurmRoot := flags.String("slurm-root", "", "absolute private NFS execution-bundle root")
	slurmRunner := flags.String("slurm-runner", "", "absolute jobman-agent path visible on compute nodes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" {
		return errors.New("run requires --state-dir")
	}
	if (*slurmRoot == "") != (*slurmRunner == "") {
		return errors.New("run requires --slurm-root and --slurm-runner together")
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	return RunService(ctx, ServiceOptions{
		StateDirectory: *stateDirectory, PollInterval: *pollInterval, Logger: logger,
		ArtifactStoreName: *artifactStore, ArtifactStoreVersion: *artifactStoreVersion,
		ArtifactRoot: *artifactRoot, ArtifactS3Bucket: *artifactS3Bucket,
		ArtifactS3Prefix: *artifactS3Prefix, ArtifactS3Region: *artifactS3Region,
		ArtifactS3Owner: *artifactS3Owner, AWSExecutable: *awsExecutable,
		ContainerEngine: *containerEngine, ContainerExecutable: *containerExecutable,
		ContainerHostNetwork: *containerHostNetwork,
		MaximumLogBytes:      *maximumLogBytes,
		MaximumArtifactBytes: *maximumArtifactBytes,
		SlurmRoot:            *slurmRoot, SlurmRunner: *slurmRunner,
	})
}

func executeInstallService(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent install-service", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDirectory := flags.String("state-dir", "", "absolute host-local agent state directory")
	agentBinary := flags.String("agent-binary", "", "absolute agent binary path (defaults to this executable)")
	unitDirectory := flags.String("unit-dir", "", "systemd user unit directory")
	start := flags.Bool("start", false, "enable and start the systemd user service")
	pollInterval := flags.Duration("poll-interval", defaultPollInterval, "control-plane poll interval")
	artifactStore := flags.String("artifact-store", "", "logical local or NFS artifact store name")
	artifactStoreVersion := flags.Int64("artifact-store-version", 1, "artifact store mapping version")
	artifactRoot := flags.String("artifact-root", "", "absolute local or NFS artifact store root")
	artifactS3Bucket := flags.String("artifact-s3-bucket", "", "private S3 artifact bucket")
	artifactS3Prefix := flags.String("artifact-s3-prefix", "", "optional S3 object prefix")
	artifactS3Region := flags.String("artifact-s3-region", "", "optional AWS region")
	artifactS3Owner := flags.String("artifact-s3-expected-owner", "", "expected 12-digit AWS account ID")
	awsExecutable := flags.String("aws-cli", "aws", "AWS CLI executable")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	maximumLogBytes := flags.Int64("max-log-bytes", defaultMaximumLogBytes, "maximum retained bytes per log stream")
	maximumArtifactBytes := flags.Int64("max-artifact-bytes", defaultMaximumArtifactBytes, "maximum staged artifact bytes per execution")
	slurmRoot := flags.String("slurm-root", "", "absolute private NFS execution-bundle root")
	slurmRunner := flags.String("slurm-runner", "", "absolute jobman-agent path visible on compute nodes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" {
		return errors.New("install-service requires --state-dir")
	}
	unit, err := InstallUserService(ctx, InstallServiceOptions{
		StateDirectory: *stateDirectory, AgentBinary: *agentBinary, UnitDirectory: *unitDirectory,
		Start: *start, PollInterval: *pollInterval, ArtifactStoreName: *artifactStore,
		ArtifactStoreVersion: *artifactStoreVersion, ArtifactRoot: *artifactRoot,
		ArtifactS3Bucket: *artifactS3Bucket, ArtifactS3Prefix: *artifactS3Prefix,
		ArtifactS3Region: *artifactS3Region, ArtifactS3Owner: *artifactS3Owner,
		AWSExecutable:   *awsExecutable,
		ContainerEngine: *containerEngine, ContainerExecutable: *containerExecutable,
		ContainerHostNetwork: *containerHostNetwork,
		MaximumLogBytes:      *maximumLogBytes, SlurmRoot: *slurmRoot, SlurmRunner: *slurmRunner,
		MaximumArtifactBytes: *maximumArtifactBytes,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, unit)

	return err
}

func executeStatus(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDirectory := flags.String("state-dir", "", "absolute host-local agent state directory")
	jsonOutput := flags.Bool("json", false, "emit versioned JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" {
		return errors.New("status requires --state-dir")
	}
	status, err := loadStatus(*stateDirectory)
	if err != nil {
		return err
	}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)

		return encoder.Encode(status)
	}
	_, err = fmt.Fprintf(stdout, "%s\t%s\t%s\n",
		status.Metadata.AgentID, status.Metadata.TargetGenerationID, status.Status.AgentVersion)

	return err
}

func executeBootstrap(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent bootstrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	host := flags.String("host", "", "OpenSSH destination or configured host alias")
	agentBinary := flags.String("agent-binary", "", "Linux jobman-agent binary to transfer")
	sshExecutable := flags.String("ssh", "ssh", "OpenSSH client executable")
	remoteBinary := flags.String("remote-binary", ".local/bin/jobman-agent", "user-relative remote agent path")
	remoteState := flags.String("remote-state-dir", ".local/state/jobman-agent", "user-relative remote state path")
	serverURL := flags.String("server", "", "Jobman Control HTTPS origin")
	targetGeneration := flags.String("target-generation", "", "target generation UUID")
	tokenFile := flags.String("token-file", "-", "enrollment token file, or - for standard input")
	serverCA := flags.String("server-ca", "", "optional local PEM server CA file")
	expectedVersion := flags.String("expected-version", "", "required reported agent version")
	pollInterval := flags.Duration("poll-interval", defaultPollInterval, "control-plane poll interval")
	artifactStore := flags.String("artifact-store", "", "logical local or NFS artifact store name")
	artifactStoreVersion := flags.Int64("artifact-store-version", 1, "artifact store mapping version")
	artifactRoot := flags.String("artifact-root", "", "absolute remote local or NFS artifact root")
	artifactS3Bucket := flags.String("artifact-s3-bucket", "", "private S3 artifact bucket")
	artifactS3Prefix := flags.String("artifact-s3-prefix", "", "optional S3 object prefix")
	artifactS3Region := flags.String("artifact-s3-region", "", "optional AWS region")
	artifactS3Owner := flags.String("artifact-s3-expected-owner", "", "expected 12-digit AWS account ID")
	awsExecutable := flags.String("aws-cli", "aws", "remote AWS CLI executable")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "remote container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	maximumLogBytes := flags.Int64("max-log-bytes", defaultMaximumLogBytes, "maximum retained bytes per log stream")
	maximumArtifactBytes := flags.Int64("max-artifact-bytes", defaultMaximumArtifactBytes, "maximum staged artifact bytes per execution")
	slurmEnabled := flags.Bool("slurm", false, "enroll as a Slurm CLI submit-host agent")
	slurmRoot := flags.String("slurm-root", "", "absolute private NFS execution-bundle root")
	slurmRunner := flags.String("slurm-runner", "", "absolute agent path visible on compute nodes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *host == "" || *agentBinary == "" || *serverURL == "" || *targetGeneration == "" {
		return errors.New("bootstrap requires --host, --agent-binary, --server, and --target-generation")
	}
	token, err := readEnrollmentToken(stdin, *tokenFile)
	if err != nil {
		return err
	}
	result, err := sshbootstrap.Bootstrap(ctx, sshbootstrap.Options{
		Host: *host, AgentBinary: *agentBinary, SSHExecutable: *sshExecutable,
		RemoteBinary: *remoteBinary, RemoteStateDirectory: *remoteState,
		ServerURL: *serverURL, ServerCAFile: *serverCA, TargetGenerationID: *targetGeneration,
		EnrollmentToken: token, ExpectedVersion: *expectedVersion, PollInterval: *pollInterval,
		ArtifactStoreName: *artifactStore, ArtifactStoreVersion: *artifactStoreVersion,
		ArtifactRoot: *artifactRoot, ArtifactS3Bucket: *artifactS3Bucket,
		ArtifactS3Prefix: *artifactS3Prefix, ArtifactS3Region: *artifactS3Region,
		ArtifactS3Owner: *artifactS3Owner, AWSExecutable: *awsExecutable,
		ContainerEngine: *containerEngine, ContainerExecutable: *containerExecutable,
		ContainerHostNetwork: *containerHostNetwork,
		MaximumLogBytes:      *maximumLogBytes,
		MaximumArtifactBytes: *maximumArtifactBytes, Slurm: *slurmEnabled,
		SlurmRoot: *slurmRoot, SlurmRunner: *slurmRunner,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)

	return encoder.Encode(result)
}

func executeRunExecution(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent run-execution", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDirectory := flags.String("state-dir", "", "absolute host-local agent state directory")
	executionID := flags.String("execution-id", "", "execution UUID")
	maximumLogBytes := flags.Int64("max-log-bytes", defaultMaximumLogBytes, "maximum retained bytes per log stream")
	maximumArtifactBytes := flags.Int64("max-artifact-bytes", defaultMaximumArtifactBytes, "maximum staged artifact bytes per execution")
	artifactStoreName := flags.String("artifact-store", "", "logical local or NFS artifact store name")
	artifactStoreVersion := flags.Int64("artifact-store-version", 1, "artifact store mapping version")
	artifactRoot := flags.String("artifact-root", "", "absolute local or NFS artifact store root")
	artifactS3Bucket := flags.String("artifact-s3-bucket", "", "private S3 artifact bucket")
	artifactS3Prefix := flags.String("artifact-s3-prefix", "", "optional S3 object prefix")
	artifactS3Region := flags.String("artifact-s3-region", "", "optional AWS region")
	artifactS3Owner := flags.String("artifact-s3-expected-owner", "", "expected 12-digit AWS account ID")
	awsExecutable := flags.String("aws-cli", "aws", "AWS CLI executable")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" || *executionID == "" {
		return errors.New("run-execution requires --state-dir and --execution-id")
	}

	store, _, err := newArtifactStore(ctx, artifactStoreOptions{
		name: *artifactStoreName, version: *artifactStoreVersion, root: *artifactRoot,
		s3Bucket: *artifactS3Bucket, s3Prefix: *artifactS3Prefix,
		s3Region: *artifactS3Region, s3ExpectedOwner: *artifactS3Owner,
		awsExecutable: *awsExecutable,
	})
	if err != nil {
		return err
	}
	containerRuntime, err := newContainerRuntime(
		ctx, *containerEngine, *containerExecutable, *containerHostNetwork, nil,
	)
	if err != nil {
		return err
	}

	return RunExecutionWithOptions(ctx, *stateDirectory, *executionID, ExecutionOptions{
		MaximumLogBytes: *maximumLogBytes, MaximumArtifactBytes: *maximumArtifactBytes,
		ArtifactStore: store, ContainerRuntime: containerRuntime,
	})
}

func executeRunArrayTask(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("jobman-agent run-array-task", flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifest := flags.String("manifest", "", "absolute immutable array manifest path")
	maximumLogBytes := flags.Int64("max-log-bytes", defaultMaximumLogBytes, "maximum retained bytes per log stream")
	maximumArtifactBytes := flags.Int64("max-artifact-bytes", defaultMaximumArtifactBytes, "maximum staged artifact bytes per execution")
	artifactStoreName := flags.String("artifact-store", "", "logical artifact store name")
	artifactStoreVersion := flags.Int64("artifact-store-version", 1, "artifact store mapping version")
	artifactRoot := flags.String("artifact-root", "", "absolute local or NFS artifact store root")
	artifactS3Bucket := flags.String("artifact-s3-bucket", "", "private S3 artifact bucket")
	artifactS3Prefix := flags.String("artifact-s3-prefix", "", "optional S3 object prefix")
	artifactS3Region := flags.String("artifact-s3-region", "", "optional AWS region")
	artifactS3Owner := flags.String("artifact-s3-expected-owner", "", "expected 12-digit AWS account ID")
	awsExecutable := flags.String("aws-cli", "aws", "AWS CLI executable")
	containerEngine := flags.String("container-engine", "", "docker, podman, or apptainer runtime")
	containerExecutable := flags.String("container-runtime", "", "container engine executable")
	containerHostNetwork := flags.Bool("container-host-network", false, "allow portable host-network requests")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *manifest == "" {
		return errors.New("run-array-task requires --manifest")
	}
	store, _, err := newArtifactStore(ctx, artifactStoreOptions{
		name: *artifactStoreName, version: *artifactStoreVersion, root: *artifactRoot,
		s3Bucket: *artifactS3Bucket, s3Prefix: *artifactS3Prefix,
		s3Region: *artifactS3Region, s3ExpectedOwner: *artifactS3Owner,
		awsExecutable: *awsExecutable,
	})
	if err != nil {
		return err
	}
	containerRuntime, err := newContainerRuntime(
		ctx, *containerEngine, *containerExecutable, *containerHostNetwork, nil,
	)
	if err != nil {
		return err
	}

	return RunSlurmArrayTask(ctx, *manifest, os.Getenv("SLURM_ARRAY_TASK_ID"), ExecutionOptions{
		MaximumLogBytes: *maximumLogBytes, MaximumArtifactBytes: *maximumArtifactBytes,
		ArtifactStore: store, ContainerRuntime: containerRuntime,
	})
}

func readEnrollmentToken(stdin io.Reader, filename string) (string, error) {
	source := stdin
	var file *os.File
	var err error
	if filename != "-" {
		file, err = os.Open(filename)
		if err != nil {
			return "", fmt.Errorf("read enrollment token: %w", err)
		}
		defer func() { _ = file.Close() }()
		source = file
	}
	contents, err := io.ReadAll(io.LimitReader(source, 4097))
	if err != nil {
		return "", fmt.Errorf("read enrollment token: %w", err)
	}
	if len(contents) > 4096 {
		return "", errors.New("read enrollment token: token exceeds size limit")
	}
	token := strings.TrimSpace(string(contents))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("read enrollment token: token is invalid")
	}

	return token, nil
}

func writeAgentUsage(destination io.Writer) {
	_, _ = fmt.Fprintln(destination, `Usage: jobman-agent <command>

Commands:
  bootstrap       install, enroll, and start an agent through OpenSSH
  enroll          enroll this host with Jobman Control
  install-service install the per-user systemd service
  run             run the polling agent service
  run-array-task  internal Slurm array task runner
  run-execution   internal isolated execution runner
  status          inspect local enrollment identity without secrets
  version         print build version`)
}
