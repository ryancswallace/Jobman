// Package sshbootstrap installs and enrolls a per-user Jobman agent through
// the user's OpenSSH client. SSH is never used for steady-state execution.
package sshbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maximumBootstrapOutput = 64 * 1024

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Runner is the narrow local process boundary used for OpenSSH invocations.
type Runner interface {
	Run(context.Context, string, []string, io.Reader) ([]byte, error)
}

// ExecRunner invokes the installed OpenSSH client without changing its config,
// host-key policy, authentication agent, or jump-host behavior.
type ExecRunner struct{}

// Run invokes OpenSSH with bounded combined output and context cancellation.
func (ExecRunner) Run(ctx context.Context, executable string, arguments []string, input io.Reader) ([]byte, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Stdin = input
	var output boundedBuffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	if err != nil {
		message := strings.TrimSpace(output.String())
		if message == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", err, message)
	}

	return output.Bytes(), nil
}

type boundedBuffer struct{ bytes.Buffer }

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	original := len(value)
	remaining := maximumBootstrapOutput - buffer.Len()
	if remaining > 0 {
		if remaining < len(value) {
			value = value[:remaining]
		}
		_, _ = buffer.Buffer.Write(value)
	}
	return original, nil
}

// Options configures one bounded, noninteractive bootstrap operation.
type Options struct {
	Host                 string
	AgentBinary          string
	SSHExecutable        string
	RemoteBinary         string
	RemoteStateDirectory string
	ServerURL            string
	ServerCAFile         string
	TargetGenerationID   string
	EnrollmentToken      string
	ExpectedVersion      string
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
	Slurm                bool
	SlurmRoot            string
	SlurmRunner          string
	Runner               Runner
	inspectBinary        func(string) (binaryTarget, error)
}

// Result identifies the enrolled installation without exposing credentials.
type Result struct {
	Host               string `json:"host"`
	AgentID            string `json:"agentId"`
	TargetGenerationID string `json:"targetGenerationId"`
	AgentVersion       string `json:"agentVersion"`
	ReusedEnrollment   bool   `json:"reusedEnrollment"`
}

type remoteStatus struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		AgentID            string `json:"agentId"`
		TargetGenerationID string `json:"targetGenerationId"`
	} `json:"metadata"`
	Status struct {
		AgentVersion string `json:"agentVersion"`
	} `json:"status"`
}

// Bootstrap validates, transfers, enrolls, and starts a Linux user agent.
// The enrollment token is supplied only on the remote process's standard input.
//
//nolint:cyclop,gocognit // Bootstrap verifies each transfer, enrollment, and service boundary explicitly.
func Bootstrap(ctx context.Context, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("SSH bootstrap: context is required")
	}
	if err := validateOptions(&options); err != nil {
		return Result{}, err
	}
	inspectBinary := options.inspectBinary
	if inspectBinary == nil {
		inspectBinary = inspectAgentBinary
	}
	binaryInfo, err := inspectBinary(options.AgentBinary)
	if err != nil {
		return Result{}, err
	}
	binary, err := os.Open(options.AgentBinary)
	if err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap: open agent binary: %w", err)
	}
	defer binary.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, binary); err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap: checksum agent binary: %w", err)
	}
	if _, err = binary.Seek(0, io.SeekStart); err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap: rewind agent binary: %w", err)
	}
	binaryDigest := hex.EncodeToString(digest.Sum(nil))

	state, err := runSSH(ctx, options, preflightScript,
		[]string{options.RemoteBinary, options.RemoteStateDirectory, binaryInfo.architecture}, nil)
	if err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap preflight: %w", err)
	}
	state = bytes.TrimSpace(state)
	if !bytes.Equal(state, []byte("new")) && !bytes.Equal(state, []byte("enrolled")) {
		return Result{}, errors.New("SSH bootstrap preflight returned an invalid state")
	}
	if _, err = runSSH(ctx, options, uploadScript,
		[]string{options.RemoteBinary, binaryDigest, "700"}, binary); err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap transfer agent: %w", err)
	}
	remoteCA := ""
	if options.ServerCAFile != "" {
		ca, readErr := os.Open(options.ServerCAFile)
		if readErr != nil {
			return Result{}, fmt.Errorf("SSH bootstrap: open server CA: %w", readErr)
		}
		defer ca.Close()
		caDigest := sha256.New()
		if _, readErr = io.Copy(caDigest, ca); readErr != nil {
			return Result{}, fmt.Errorf("SSH bootstrap: checksum server CA: %w", readErr)
		}
		if _, readErr = ca.Seek(0, io.SeekStart); readErr != nil {
			return Result{}, fmt.Errorf("SSH bootstrap: rewind server CA: %w", readErr)
		}
		remoteCA = ".config/jobman-agent/control-ca.pem"
		if _, err = runSSH(ctx, options, uploadScript,
			[]string{remoteCA, hex.EncodeToString(caDigest.Sum(nil)), "600"}, ca); err != nil {
			return Result{}, fmt.Errorf("SSH bootstrap transfer server CA: %w", err)
		}
	}
	reused := bytes.Equal(state, []byte("enrolled"))
	if !reused {
		enrollArguments := []string{
			options.RemoteBinary, options.RemoteStateDirectory, options.ServerURL,
			options.TargetGenerationID, remoteCA, strconv.FormatBool(options.Slurm),
			options.ContainerEngine, options.ContainerExecutable,
			strconv.FormatBool(options.ContainerHostNetwork),
		}
		if _, err = runSSH(ctx, options, enrollScript, enrollArguments,
			strings.NewReader(options.EnrollmentToken+"\n")); err != nil {
			return Result{}, fmt.Errorf("SSH bootstrap enroll agent: %w", err)
		}
	}
	installArguments := []string{
		options.RemoteBinary, options.RemoteStateDirectory,
		options.PollInterval.String(), strconv.FormatInt(options.MaximumLogBytes, 10),
		strconv.FormatInt(options.MaximumArtifactBytes, 10), options.ArtifactStoreName,
		strconv.FormatInt(options.ArtifactStoreVersion, 10), options.ArtifactRoot,
		options.ArtifactS3Bucket, options.ArtifactS3Prefix, options.ArtifactS3Region,
		options.ArtifactS3Owner, options.AWSExecutable, options.SlurmRoot, options.SlurmRunner,
		options.ContainerEngine, options.ContainerExecutable,
		strconv.FormatBool(options.ContainerHostNetwork),
	}
	if _, err = runSSH(ctx, options, installScript, installArguments, nil); err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap start agent: %w", err)
	}
	statusDocument, err := runSSH(ctx, options, statusScript,
		[]string{options.RemoteBinary, options.RemoteStateDirectory}, nil)
	if err != nil {
		return Result{}, fmt.Errorf("SSH bootstrap verify agent: %w", err)
	}
	status, err := parseStatus(statusDocument, options)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Host: options.Host, AgentID: status.Metadata.AgentID,
		TargetGenerationID: status.Metadata.TargetGenerationID,
		AgentVersion:       status.Status.AgentVersion, ReusedEnrollment: reused,
	}, nil
}

type binaryTarget struct{ architecture string }

func inspectAgentBinary(filename string) (binaryTarget, error) {
	//nolint:gosec // The caller supplies an explicit local binary path after CLI and regular-file validation.
	information, err := os.Stat(filename)
	if err != nil {
		return binaryTarget{}, fmt.Errorf("SSH bootstrap: inspect agent binary: %w", err)
	}
	if !information.Mode().IsRegular() || information.Size() < 1 || information.Size() > 512*1024*1024 {
		return binaryTarget{}, errors.New("SSH bootstrap: agent binary must be a nonempty regular file no larger than 512 MiB")
	}
	metadata, err := buildinfo.ReadFile(filename)
	if err != nil {
		return binaryTarget{}, fmt.Errorf("SSH bootstrap: read agent binary build information: %w", err)
	}
	settings := make(map[string]string, len(metadata.Settings))
	for _, setting := range metadata.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["GOOS"] != "linux" {
		return binaryTarget{}, errors.New("SSH bootstrap: agent binary must target Linux")
	}
	architecture := settings["GOARCH"]
	if architecture != "amd64" && architecture != "arm64" && architecture != "386" {
		return binaryTarget{}, fmt.Errorf("SSH bootstrap: unsupported agent architecture %q", architecture)
	}

	return binaryTarget{architecture: architecture}, nil
}

//nolint:cyclop,gocognit // Bootstrap rejects each unsafe local and remote configuration independently.
func validateOptions(options *Options) error {
	if options.Host == "" || strings.HasPrefix(options.Host, "-") || strings.ContainsAny(options.Host, " \t\r\n\x00") {
		return errors.New("SSH bootstrap: host must be one OpenSSH destination without whitespace")
	}
	if options.AgentBinary == "" || !uuidPattern.MatchString(options.TargetGenerationID) {
		return errors.New("SSH bootstrap: agent binary and valid target generation are required")
	}
	if options.EnrollmentToken == "" || len(options.EnrollmentToken) > 4096 ||
		strings.ContainsAny(options.EnrollmentToken, " \t\r\n") {
		return errors.New("SSH bootstrap: enrollment token is invalid")
	}
	endpoint, err := url.Parse(options.ServerURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("SSH bootstrap: server must be an HTTPS origin")
	}
	if options.SSHExecutable == "" {
		options.SSHExecutable = "ssh"
	}
	if options.RemoteBinary == "" {
		options.RemoteBinary = ".local/bin/jobman-agent"
	}
	if options.RemoteStateDirectory == "" {
		options.RemoteStateDirectory = ".local/state/jobman-agent"
	}
	if !safeRelativePath(options.RemoteBinary) || !safeRelativePath(options.RemoteStateDirectory) {
		return errors.New("SSH bootstrap: remote binary and state directory must be normalized user-relative paths")
	}
	if options.PollInterval == 0 {
		options.PollInterval = 2 * time.Second
	}
	if options.PollInterval < 250*time.Millisecond || options.PollInterval > time.Minute {
		return errors.New("SSH bootstrap: poll interval must be between 250ms and 1m")
	}
	if options.MaximumLogBytes == 0 {
		options.MaximumLogBytes = 64 * 1024 * 1024
	}
	if options.MaximumLogBytes < 1 {
		return errors.New("SSH bootstrap: maximum log bytes must be positive")
	}
	if options.MaximumArtifactBytes == 0 {
		options.MaximumArtifactBytes = 1024 * 1024 * 1024
	}
	if options.MaximumArtifactBytes < 1 {
		return errors.New("SSH bootstrap: maximum artifact bytes must be positive")
	}
	if options.ArtifactStoreVersion == 0 {
		options.ArtifactStoreVersion = 1
	}
	artifactConfigured := options.ArtifactStoreName != "" || options.ArtifactRoot != "" ||
		options.ArtifactS3Bucket != "" || options.ArtifactS3Prefix != "" ||
		options.ArtifactS3Region != "" || options.ArtifactS3Owner != ""
	if artifactConfigured && (options.ArtifactStoreName == "" ||
		(options.ArtifactRoot == "") == (options.ArtifactS3Bucket == "")) {
		return errors.New("SSH bootstrap: artifact store requires exactly one filesystem root or S3 bucket")
	}
	if (options.SlurmRoot == "") != (options.SlurmRunner == "") {
		return errors.New("SSH bootstrap: incomplete artifact or Slurm configuration")
	}
	if options.Slurm != (options.SlurmRoot != "") {
		return errors.New("SSH bootstrap: --slurm requires the Slurm root and runner")
	}
	if options.ContainerEngine == "" && (options.ContainerExecutable != "" || options.ContainerHostNetwork) {
		return errors.New("SSH bootstrap: container engine is required for container policy")
	}
	if options.Runner == nil {
		options.Runner = ExecRunner{}
	}

	return nil
}

func safeRelativePath(value string) bool {
	return value != "" && !path.IsAbs(value) && path.Clean(value) == value && value != "." &&
		value != ".." && !strings.HasPrefix(value, "../") && !strings.ContainsAny(value, "\x00\r\n")
}

func runSSH(
	ctx context.Context,
	options Options,
	script string,
	arguments []string,
	input io.Reader,
) ([]byte, error) {
	var remote strings.Builder
	remote.WriteString("sh -c ")
	remote.WriteString(quoteShell(script))
	remote.WriteString(" sh")
	for _, argument := range arguments {
		remote.WriteByte(' ')
		remote.WriteString(quoteShell(argument))
	}

	return options.Runner.Run(ctx, options.SSHExecutable, []string{options.Host, remote.String()}, input)
}

func quoteShell(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `'"'"'`) + `'`
}

func parseStatus(document []byte, options Options) (remoteStatus, error) {
	var status remoteStatus
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		return remoteStatus{}, fmt.Errorf("SSH bootstrap verify agent: decode status: %w", err)
	}
	if status.APIVersion != "jobman.agent/v1alpha1" || status.Kind != "AgentStatus" ||
		status.Metadata.AgentID == "" || status.Metadata.TargetGenerationID != options.TargetGenerationID ||
		status.Status.AgentVersion == "" ||
		(options.ExpectedVersion != "" && status.Status.AgentVersion != options.ExpectedVersion) {
		return remoteStatus{}, errors.New("SSH bootstrap verify agent: remote identity or version does not match")
	}

	return status, nil
}

const preflightScript = `set -eu
binary_rel=$1
state_rel=$2
expected_arch=$3
test "$(uname -s)" = Linux
case "$(uname -m)" in
  x86_64) remote_arch=amd64 ;;
  aarch64|arm64) remote_arch=arm64 ;;
  i386|i686) remote_arch=386 ;;
  *) exit 12 ;;
esac
test "$remote_arch" = "$expected_arch"
command -v sha256sum >/dev/null
command -v systemctl >/dev/null
home=$(cd "$HOME" && pwd -P)
case "$binary_rel:$state_rel" in *../*|*/..:*|*:*/..|/*|*:/*) exit 13 ;; esac
if test -f "$home/$state_rel/credentials.json"; then printf enrolled; else printf new; fi`

const uploadScript = `set -eu
relative=$1
expected=$2
mode=$3
home=$(cd "$HOME" && pwd -P)
destination=$home/$relative
directory=$(dirname "$destination")
umask 077
mkdir -p "$directory"
temporary=$directory/.jobman-bootstrap.$$
trap 'rm -f "$temporary"' EXIT HUP INT TERM
cat >"$temporary"
actual=$(sha256sum "$temporary" | awk '{print $1}')
test "$actual" = "$expected"
chmod "$mode" "$temporary"
if test -e "$destination"; then
  existing=$(sha256sum "$destination" | awk '{print $1}')
  test "$existing" = "$expected"
else
  ln "$temporary" "$destination"
fi`

const enrollScript = `set -eu
binary_rel=$1
state_rel=$2
server=$3
generation=$4
ca_rel=$5
slurm=$6
container_engine=$7
container_runtime=$8
container_host_network=$9
home=$(cd "$HOME" && pwd -P)
set -- "$home/$binary_rel" enroll --state-dir "$home/$state_rel" --server "$server" --target-generation "$generation"
if test -n "$ca_rel"; then set -- "$@" --server-ca "$home/$ca_rel"; fi
if test "$slurm" = true; then set -- "$@" --slurm; fi
if test -n "$container_engine"; then
  set -- "$@" --container-engine "$container_engine"
  if test -n "$container_runtime"; then set -- "$@" --container-runtime "$container_runtime"; fi
  if test "$container_host_network" = true; then set -- "$@" --container-host-network; fi
fi
exec "$@"`

const installScript = `set -eu
binary_rel=$1
state_rel=$2
poll=$3
max_logs=$4
max_artifacts=$5
store=$6
store_version=$7
artifact_root=$8
s3_bucket=$9
shift 9
s3_prefix=$1
s3_region=$2
s3_owner=$3
aws_cli=$4
slurm_root=$5
slurm_runner=$6
container_engine=$7
container_runtime=$8
container_host_network=$9
home=$(cd "$HOME" && pwd -P)
set -- "$home/$binary_rel" install-service --start --state-dir "$home/$state_rel" --poll-interval "$poll" --max-log-bytes "$max_logs" --max-artifact-bytes "$max_artifacts"
if test -n "$store"; then
  set -- "$@" --artifact-store "$store" --artifact-store-version "$store_version"
  if test -n "$artifact_root"; then
    set -- "$@" --artifact-root "$artifact_root"
  else
    set -- "$@" --artifact-s3-bucket "$s3_bucket"
    if test -n "$s3_prefix"; then set -- "$@" --artifact-s3-prefix "$s3_prefix"; fi
    if test -n "$s3_region"; then set -- "$@" --artifact-s3-region "$s3_region"; fi
    if test -n "$s3_owner"; then set -- "$@" --artifact-s3-expected-owner "$s3_owner"; fi
    if test -n "$aws_cli"; then set -- "$@" --aws-cli "$aws_cli"; fi
  fi
fi
if test -n "$slurm_root"; then set -- "$@" --slurm-root "$slurm_root" --slurm-runner "$slurm_runner"; fi
if test -n "$container_engine"; then
  set -- "$@" --container-engine "$container_engine"
  if test -n "$container_runtime"; then set -- "$@" --container-runtime "$container_runtime"; fi
  if test "$container_host_network" = true; then set -- "$@" --container-host-network; fi
fi
exec "$@"`

const statusScript = `set -eu
home=$(cd "$HOME" && pwd -P)
exec "$home/$1" status --json --state-dir "$home/$2"`
