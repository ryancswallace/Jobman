package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	systemdUserUnitName = "jobman-agent.service"
	systemdUserArgument = "--user"
)

// InstallServiceOptions describes a per-user systemd installation. It never
// grants privileges or installs a system-wide unit.
type InstallServiceOptions struct {
	StateDirectory       string
	AgentBinary          string
	UnitDirectory        string
	PollInterval         time.Duration
	ArtifactStoreName    string
	ArtifactStoreVersion int64
	ArtifactRoot         string
	ArtifactS3Bucket     string
	ArtifactS3Prefix     string
	ArtifactS3Region     string
	ArtifactS3Owner      string
	AWSExecutable        string
	ContainerEngine      string
	ContainerExecutable  string
	ContainerHostNetwork bool
	MaximumLogBytes      int64
	MaximumArtifactBytes int64
	SlurmRoot            string
	SlurmRunner          string
	Start                bool
	operatingSystem      string
	executablePath       func() (string, error)
	userConfigDirectory  func() (string, error)
	runSystemctl         func(context.Context, []string) ([]byte, error)
}

// InstallUserService writes and optionally enables the agent's user service.
//
//nolint:cyclop,gocognit // Installation validates every path and service-manager boundary before mutation.
func InstallUserService(ctx context.Context, options InstallServiceOptions) (string, error) {
	if ctx == nil {
		return "", errors.New("install agent service: context is required")
	}
	operatingSystem := options.operatingSystem
	if operatingSystem == "" {
		operatingSystem = runtime.GOOS
	}
	if operatingSystem != "linux" {
		return "", errors.New("install agent service: systemd user services are supported only on Linux")
	}
	stateDirectory, err := filepath.Abs(options.StateDirectory)
	if err != nil || options.StateDirectory == "" || stateDirectory != filepath.Clean(options.StateDirectory) {
		return "", errors.New("install agent service: state directory must be an absolute normalized path")
	}
	if _, err = loadCredentials(stateDirectory); err != nil {
		return "", fmt.Errorf("install agent service: %w", err)
	}
	binary := options.AgentBinary
	if binary == "" {
		executablePath := options.executablePath
		if executablePath == nil {
			executablePath = os.Executable
		}
		binary, err = executablePath()
		if err != nil {
			return "", fmt.Errorf("install agent service: locate agent binary: %w", err)
		}
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return "", fmt.Errorf("install agent service: resolve agent binary: %w", err)
	}
	information, err := os.Stat(binary)
	if err != nil {
		return "", fmt.Errorf("install agent service: inspect agent binary: %w", err)
	}
	if !isOwnerExecutableRegular(information) {
		return "", errors.New("install agent service: agent binary must be an owner-executable regular file")
	}
	arguments, err := serviceRunArguments(options, stateDirectory)
	if err != nil {
		return "", err
	}
	unit, err := renderSystemdUserUnit(binary, arguments)
	if err != nil {
		return "", err
	}
	unitDirectory := options.UnitDirectory
	if unitDirectory == "" {
		userConfigDirectory := options.userConfigDirectory
		if userConfigDirectory == nil {
			userConfigDirectory = os.UserConfigDir
		}
		configurationDirectory, configErr := userConfigDirectory()
		if configErr != nil {
			return "", fmt.Errorf("install agent service: locate user configuration directory: %w", configErr)
		}
		unitDirectory = filepath.Join(configurationDirectory, "systemd", "user")
	}
	if !filepath.IsAbs(unitDirectory) || filepath.Clean(unitDirectory) != unitDirectory {
		return "", errors.New("install agent service: unit directory must be an absolute normalized path")
	}
	if err = os.MkdirAll(unitDirectory, 0o700); err != nil {
		return "", fmt.Errorf("install agent service: create unit directory: %w", err)
	}
	unitPath := filepath.Join(unitDirectory, systemdUserUnitName)
	if err = writePrivateFile(unitPath, []byte(unit)); err != nil {
		return "", fmt.Errorf("install agent service: write unit: %w", err)
	}
	if !options.Start {
		return unitPath, nil
	}
	commands := [][]string{
		{systemdUserArgument, "daemon-reload"},
		{systemdUserArgument, "enable", "--now", systemdUserUnitName},
		{systemdUserArgument, "is-active", "--quiet", systemdUserUnitName},
	}
	runSystemctl := options.runSystemctl
	if runSystemctl == nil {
		runSystemctl = func(ctx context.Context, arguments []string) ([]byte, error) {
			return exec.CommandContext(ctx, "systemctl", arguments...).CombinedOutput()
		}
	}
	for _, arguments := range commands {
		if output, commandErr := runSystemctl(ctx, arguments); commandErr != nil {
			message := strings.TrimSpace(string(output))
			if len(message) > 1024 {
				message = message[:1024]
			}
			if message == "" {
				return "", fmt.Errorf("install agent service: systemctl %s: %w", arguments[1], commandErr)
			}
			return "", fmt.Errorf("install agent service: systemctl %s: %w: %s", arguments[1], commandErr, message)
		}
	}

	return unitPath, nil
}

//nolint:cyclop,gocognit,nestif // Arguments mirror explicit administrator-selected service policy.
func serviceRunArguments(options InstallServiceOptions, stateDirectory string) ([]string, error) {
	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}
	maximumLogBytes := options.MaximumLogBytes
	if maximumLogBytes == 0 {
		maximumLogBytes = defaultMaximumLogBytes
	}
	maximumArtifactBytes := options.MaximumArtifactBytes
	if maximumArtifactBytes == 0 {
		maximumArtifactBytes = defaultMaximumArtifactBytes
	}
	storeVersion := options.ArtifactStoreVersion
	if storeVersion == 0 {
		storeVersion = 1
	}
	if pollInterval < 250*time.Millisecond || pollInterval > time.Minute ||
		maximumLogBytes < 1 || maximumArtifactBytes < 1 {
		return nil, errors.New("install agent service: invalid polling or log limit")
	}
	artifactConfigured := options.ArtifactStoreName != "" || options.ArtifactRoot != "" ||
		options.ArtifactS3Bucket != "" || options.ArtifactS3Prefix != "" ||
		options.ArtifactS3Region != "" || options.ArtifactS3Owner != ""
	if artifactConfigured && (options.ArtifactStoreName == "" ||
		(options.ArtifactRoot == "") == (options.ArtifactS3Bucket == "")) {
		return nil, errors.New("install agent service: artifact store requires exactly one filesystem root or S3 bucket")
	}
	if (options.SlurmRoot == "") != (options.SlurmRunner == "") {
		return nil, errors.New("install agent service: Slurm root and runner are required together")
	}
	arguments := []string{
		"run", "--state-dir", stateDirectory,
		"--poll-interval", pollInterval.String(),
		maximumLogBytesFlag, strconv.FormatInt(maximumLogBytes, 10),
		"--max-artifact-bytes", strconv.FormatInt(maximumArtifactBytes, 10),
	}
	if options.ArtifactStoreName != "" {
		arguments = append(arguments,
			"--artifact-store", options.ArtifactStoreName,
			"--artifact-store-version", strconv.FormatInt(storeVersion, 10),
		)
		if options.ArtifactRoot != "" {
			arguments = append(arguments, "--artifact-root", options.ArtifactRoot)
		} else {
			arguments = append(arguments, "--artifact-s3-bucket", options.ArtifactS3Bucket)
			if options.ArtifactS3Prefix != "" {
				arguments = append(arguments, "--artifact-s3-prefix", options.ArtifactS3Prefix)
			}
			if options.ArtifactS3Region != "" {
				arguments = append(arguments, "--artifact-s3-region", options.ArtifactS3Region)
			}
			if options.ArtifactS3Owner != "" {
				arguments = append(arguments, "--artifact-s3-expected-owner", options.ArtifactS3Owner)
			}
			if options.AWSExecutable != "" && options.AWSExecutable != "aws" {
				arguments = append(arguments, "--aws-cli", options.AWSExecutable)
			}
		}
	}
	if options.SlurmRoot != "" {
		arguments = append(arguments,
			"--slurm-root", options.SlurmRoot,
			"--slurm-runner", options.SlurmRunner,
		)
	}
	if options.ContainerEngine != "" {
		arguments = append(arguments, "--container-engine", options.ContainerEngine)
		if options.ContainerExecutable != "" {
			arguments = append(arguments, "--container-runtime", options.ContainerExecutable)
		}
		if options.ContainerHostNetwork {
			arguments = append(arguments, "--container-host-network")
		}
	} else if options.ContainerExecutable != "" || options.ContainerHostNetwork {
		return nil, errors.New("install agent service: container engine is required for container policy")
	}

	return arguments, nil
}

func renderSystemdUserUnit(binary string, arguments []string) (string, error) {
	command := make([]string, 0, 1+len(arguments))
	for _, value := range append([]string{binary}, arguments...) {
		quoted, err := quoteSystemdArgument(value)
		if err != nil {
			return "", err
		}
		command = append(command, quoted)
	}

	return `[Unit]
Description=Jobman per-user execution agent
Documentation=https://github.com/ryancswallace/jobman
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=` + strings.Join(command, " ") + `
Restart=on-failure
RestartSec=5s
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectControlGroups=true
ProtectKernelModules=true
ProtectKernelTunables=true
RestrictSUIDSGID=true
LockPersonality=true

[Install]
WantedBy=default.target
`, nil
}

func quoteSystemdArgument(value string) (string, error) {
	if value == "" || strings.ContainsAny(value, "\x00\r\n") {
		return "", errors.New("install agent service: service arguments must be nonempty single-line values")
	}
	replacer := strings.NewReplacer(
		`\`, `\\`, `"`, `\"`, `%`, `%%`, `$`, `$$`,
	)

	return `"` + replacer.Replace(value) + `"`, nil
}
