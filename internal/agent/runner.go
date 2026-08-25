package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/internal/executor"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/runtimeenv"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	executionsDirectoryName = "executions"
	assignmentFilename      = "assignment.json"
	authorizationFilename   = "authorization.json"
	claimFilename           = "launch.claim"
	startedFilename         = "started.json"
	completionFilename      = "completed.json"
	cancelFilename          = "cancel.requested"
	stdoutFilename          = "stdout.log"
	stderrFilename          = "stderr.log"
	cancelledOutcome        = "cancelled" //nolint:misspell // Frozen v1alpha1 wire value.
	defaultTerminationGrace = 5 * time.Second
	defaultMaximumLogBytes  = 64 * 1024 * 1024
	executionBackendProcess = "subprocess"
	executionBackendSlurm   = "slurm"
	processOutcomeFailure   = "failure"
	runtimeNative           = "native"
	runtimeContainer        = "container"
	maximumLogBytesFlag     = "--max-log-bytes"
)

// StartManifest is the durable target identity written immediately after a
// process tree has been established.
type StartManifest struct {
	ExecutionID string                   `json:"executionId"`
	NativeID    string                   `json:"nativeId"`
	Process     platform.ProcessIdentity `json:"process"`
	StartedAt   time.Time                `json:"startedAt"`
}

// CompletionManifest is the runner's durable terminal observation.
type CompletionManifest struct {
	ExecutionID string                       `json:"executionId"`
	ObservedAt  time.Time                    `json:"observedAt"`
	Result      protocol.ProcessResult       `json:"result"`
	Logs        map[string]LogCapture        `json:"logs,omitempty"`
	Artifacts   []protocol.PublishedArtifact `json:"artifacts,omitempty"`
}

// LogCapture records the exact retained length and whether later output was
// discarded by the runner's configured bound.
type LogCapture struct {
	ByteLength int64 `json:"byteLength"`
	Truncated  bool  `json:"truncated"`
}

// ExecutionOptions bounds target-side log capture.
type ExecutionOptions struct {
	MaximumLogBytes      int64
	MaximumArtifactBytes int64
	ArtifactStore        artifact.Store
	ContainerRuntime     *runtimeenv.Adapter
}

func executionDirectory(stateDirectory, executionID string) (string, error) {
	if !validUUID(executionID) {
		return "", errors.New("invalid execution ID")
	}

	return filepath.Join(stateDirectory, executionsDirectoryName, executionID), nil
}

func prepareExecutionFiles(
	stateDirectory string,
	assignment protocol.SealedAgentAssignment,
	authorization protocol.LaunchAuthorization,
) (string, error) {
	executionID := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	directory, err := executionDirectory(stateDirectory, executionID)
	if err != nil {
		return "", fmt.Errorf("prepare execution: %w", err)
	}
	if err = os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("prepare execution: create directory: %w", err)
	}
	if err = os.Chmod(directory, 0o700); err != nil { // #nosec G302 -- directories require owner traversal.
		return "", fmt.Errorf("prepare execution: protect directory: %w", err)
	}
	if err = writePrivateFile(filepath.Join(directory, assignmentFilename), assignment.CanonicalJSON); err != nil {
		return "", fmt.Errorf("prepare execution: write assignment: %w", err)
	}
	authorizationDocument, err := json.Marshal(authorization)
	if err != nil {
		return "", fmt.Errorf("prepare execution: encode authorization: %w", err)
	}
	if err = writePrivateFile(filepath.Join(directory, authorizationFilename), authorizationDocument); err != nil {
		return "", fmt.Errorf("prepare execution: write authorization: %w", err)
	}

	return directory, nil
}

// RunExecution is the isolated runner entry point. The launch claim uses
// O_EXCL, so only one runner can ever create target-side effects for an
// execution directory.
func RunExecution(ctx context.Context, stateDirectory, executionID string) error {
	return RunExecutionWithOptions(ctx, stateDirectory, executionID, ExecutionOptions{
		MaximumLogBytes: defaultMaximumLogBytes, MaximumArtifactBytes: defaultMaximumArtifactBytes,
	})
}

// RunExecutionWithOptions runs one accepted execution using explicit bounded
// capture policy.
//
//nolint:gocognit,cyclop // Linear fail-closed stages record distinct durable failure codes.
func RunExecutionWithOptions(
	ctx context.Context,
	stateDirectory, executionID string,
	options ExecutionOptions,
) error {
	if ctx == nil {
		return errors.New("run agent execution: nil context")
	}
	if options.MaximumLogBytes < 1 {
		return errors.New("run agent execution: maximum log bytes must be positive")
	}
	if options.MaximumArtifactBytes == 0 {
		options.MaximumArtifactBytes = defaultMaximumArtifactBytes
	}
	if options.MaximumArtifactBytes < 1 {
		return errors.New("run agent execution: maximum artifact bytes must be positive")
	}
	directory, err := executionDirectory(stateDirectory, executionID)
	if err != nil {
		return fmt.Errorf("run agent execution: %w", err)
	}
	if err = writeNewPrivateFile(filepath.Join(directory, claimFilename), []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n")); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return errors.New("run agent execution: execution was already claimed")
		}
		return fmt.Errorf("run agent execution: claim launch: %w", err)
	}
	assignment, authorization, err := loadExecutionDocuments(directory)
	if err != nil {
		return writeRunnerFailure(directory, executionID, "invalid_execution", err)
	}
	workload := assignment.Document.Spec.EffectiveExecution.Spec.Workload.Document
	if err = validateExecutableAssignment(assignment, authorization, options.ContainerRuntime); err != nil {
		return writeRunnerFailure(directory, executionID, "unsupported_workload", err)
	}
	workspace := filepath.Join(directory, "workspace")
	if err = os.MkdirAll(workspace, 0o700); err != nil {
		return writeRunnerFailure(directory, executionID, "workspace_create_failed", err)
	}
	if workload.Spec.Artifacts != nil {
		if err = validateArtifactStoreBinding(
			assignment.Document.Spec.EffectiveExecution.Spec.ArtifactStores, options.ArtifactStore,
		); err != nil {
			return writeRunnerFailure(directory, executionID, "artifact_store_unavailable", err)
		}
		if err = artifact.StageInputs(
			ctx, options.ArtifactStore, workspace, workload.Spec.Artifacts.Inputs, options.MaximumArtifactBytes,
		); err != nil {
			return writeRunnerFailure(directory, executionID, "artifact_stage_failed", err)
		}
	}
	workingDirectory, err := mapWorkspacePath(workspace, workload.Spec.WorkingDirectory)
	if err != nil {
		return writeRunnerFailure(directory, executionID, "invalid_working_directory", err)
	}
	if err = os.MkdirAll(workingDirectory, 0o700); err != nil {
		return writeRunnerFailure(directory, executionID, "working_directory_create_failed", err)
	}
	stdoutFile, err := os.OpenFile(filepath.Join(directory, stdoutFilename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return writeRunnerFailure(directory, executionID, "stdout_open_failed", err)
	}
	stdout := &boundedLogWriter{file: stdoutFile, maximum: options.MaximumLogBytes}
	defer func() { _ = stdoutFile.Close() }()
	stderrFile, err := os.OpenFile(filepath.Join(directory, stderrFilename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = stdoutFile.Close()
		return writeRunnerFailure(directory, executionID, "stderr_open_failed", err)
	}
	stderr := &boundedLogWriter{file: stderrFile, maximum: options.MaximumLogBytes}
	defer func() { _ = stderrFile.Close() }()

	environment := map[string]string(nil)
	if workload.Spec.Environment != nil {
		environment = workload.Spec.Environment.Values
	}
	baseEnvironment := executionBaseEnvironment()
	if assignment.Document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend == executionBackendSlurm {
		baseEnvironment = append(baseEnvironment, slurmExecutionEnvironment()...)
	}
	var command *exec.Cmd
	if workload.Spec.Runtime.Kind == runtimeContainer {
		command, err = options.ContainerRuntime.Command(runtimeenv.CommandRequest{
			Runtime:    *workload.Spec.Runtime.Container,
			Executable: workload.Spec.Command.Executable, Arguments: workload.Spec.Command.Args,
			Workspace: workspace, WorkingDirectory: workingDirectory,
			BaseEnvironment: baseEnvironment, Environment: environment,
		})
	} else {
		command, _, err = executor.Command(executor.Request{
			Executable: workload.Spec.Command.Executable, Arguments: workload.Spec.Command.Args,
			Directory: workingDirectory, BaseEnv: baseEnvironment, AddEnv: environment,
		})
	}
	if err != nil {
		failureCode := "process_prepare_failed"
		if kind, classified := executor.ClassifyFailure(err); classified {
			failureCode = string(kind)
		}

		return writeRunnerFailure(directory, executionID, failureCode, err)
	}
	command.Stdout = stdout
	command.Stderr = stderr
	command.Stdin = nil
	platform.ConfigureTarget(command)
	if err = command.Start(); err != nil {
		return writeRunnerFailure(directory, executionID, "process_start_failed", err)
	}
	identity, err := finalizeProcessIdentity(command)
	if err != nil {
		cleanupErr := stopUnidentifiedProcess(command)

		return writeRunnerFailure(
			directory, executionID, "process_identity_failed", errors.Join(err, cleanupErr),
		)
	}
	startedAt := time.Now().UTC()
	started := StartManifest{
		ExecutionID: executionID, NativeID: strconv.Itoa(identity.PID), Process: identity,
		StartedAt: startedAt,
	}
	if err = writeManifest(filepath.Join(directory, startedFilename), started); err != nil {
		cleanupErr := stopIdentifiedProcess(command, identity)

		return writeRunnerFailure(
			directory, executionID, "start_manifest_failed", errors.Join(err, cleanupErr),
		)
	}

	result := waitForProcess(ctx, command, identity, filepath.Join(directory, cancelFilename), workload.Spec.Policy.RunTimeout)
	if err = errors.Join(stdout.close(), stderr.close()); err != nil {
		return writeRunnerFailure(directory, executionID, "log_close_failed", err)
	}
	var published []protocol.PublishedArtifact
	if workload.Spec.Artifacts != nil {
		published, err = artifact.PublishOutputs(
			ctx, options.ArtifactStore, workspace, workload.Spec.Artifacts.Outputs, options.MaximumArtifactBytes,
		)
		if err != nil {
			result.Outcome = processOutcomeFailure
			result.FailureCode = "artifact_publish_failed"
		}
	}
	completion := CompletionManifest{
		ExecutionID: executionID, ObservedAt: time.Now().UTC(), Result: result,
		Logs:      map[string]LogCapture{logStreamStdout: stdout.capture(), logStreamStderr: stderr.capture()},
		Artifacts: published,
	}
	if err = writeManifest(filepath.Join(directory, completionFilename), completion); err != nil {
		return fmt.Errorf("run agent execution: write completion manifest: %w", err)
	}

	return nil
}

func validateArtifactStoreBinding(
	bindings []protocol.ArtifactStoreBinding,
	store artifact.Store,
) error {
	if store == nil || len(bindings) != 1 || bindings[0].Name != store.Name() ||
		bindings[0].Version != store.Version() {
		return errors.New("effective artifact store does not match the configured target mapping")
	}

	return nil
}

type boundedLogWriter struct {
	file      *os.File
	maximum   int64
	written   int64
	truncated bool
}

func (writer *boundedLogWriter) Write(contents []byte) (int, error) {
	originalLength := len(contents)
	remaining := writer.maximum - writer.written
	if remaining <= 0 {
		writer.truncated = writer.truncated || originalLength != 0

		return originalLength, nil
	}
	selected := contents
	if int64(len(selected)) > remaining {
		selected = selected[:remaining]
		writer.truncated = true
	}
	written, err := writer.file.Write(selected)
	writer.written += int64(written)
	if err != nil {
		return written, err
	}
	if written != len(selected) {
		return written, io.ErrShortWrite
	}

	return originalLength, nil
}

func (writer *boundedLogWriter) close() error {
	if err := writer.file.Sync(); err != nil {
		return err
	}

	return writer.file.Close()
}

func (writer *boundedLogWriter) capture() LogCapture {
	return LogCapture{ByteLength: writer.written, Truncated: writer.truncated}
}

func stopUnidentifiedProcess(command *exec.Cmd) error {
	var result error
	if err := command.Process.Kill(); err != nil {
		result = errors.Join(result, fmt.Errorf("kill unidentified process: %w", err))
	}
	if err := command.Wait(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			result = errors.Join(result, fmt.Errorf("wait for unidentified process: %w", err))
		}
	}

	return result
}

func stopIdentifiedProcess(command *exec.Cmd, identity platform.ProcessIdentity) error {
	var result error
	if err := platform.Terminate(identity, true); err != nil {
		result = errors.Join(result, fmt.Errorf("terminate identified process: %w", err))
	}
	if err := command.Wait(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			result = errors.Join(result, fmt.Errorf("wait for identified process: %w", err))
		}
	}

	return result
}

func loadExecutionDocuments(
	directory string,
) (protocol.SealedAgentAssignment, protocol.LaunchAuthorization, error) {
	assignmentDocument, err := os.ReadFile(filepath.Join(directory, assignmentFilename))
	if err != nil {
		return protocol.SealedAgentAssignment{}, protocol.LaunchAuthorization{}, fmt.Errorf("read assignment: %w", err)
	}
	assignment, err := protocol.DecodeAgentAssignment(bytes.NewReader(assignmentDocument), protocol.DecodeLimits{})
	if err != nil {
		return protocol.SealedAgentAssignment{}, protocol.LaunchAuthorization{}, err
	}
	authorizationDocument, err := os.ReadFile(filepath.Join(directory, authorizationFilename))
	if err != nil {
		return protocol.SealedAgentAssignment{}, protocol.LaunchAuthorization{}, fmt.Errorf("read authorization: %w", err)
	}
	var authorization protocol.LaunchAuthorization
	if err = decodeStrictJSON(authorizationDocument, &authorization); err != nil {
		return protocol.SealedAgentAssignment{}, protocol.LaunchAuthorization{}, fmt.Errorf("decode authorization: %w", err)
	}
	if err := protocol.ValidateLaunchAuthorization(authorization); err != nil {
		return protocol.SealedAgentAssignment{}, protocol.LaunchAuthorization{}, err
	}

	return assignment, authorization, nil
}

//nolint:cyclop // Each rejected feature has a distinct compatibility error.
func validateSubprocessAssignment(
	assignment protocol.SealedAgentAssignment,
	authorization protocol.LaunchAuthorization,
	containerRuntime *runtimeenv.Adapter,
) error {
	value := assignment.Document
	effective := value.Spec.EffectiveExecution
	workload := effective.Spec.Workload.Document
	if value.Metadata.AgentID != authorization.Metadata.AgentID ||
		effective.Metadata.ExecutionID != authorization.Metadata.ExecutionID ||
		effective.Spec.Placement.TargetGenerationID != authorization.Spec.TargetGenerationID ||
		assignment.EffectiveExecutionDigest != authorization.Spec.EffectiveExecutionDigest {
		return errors.New("launch authorization does not match assignment")
	}
	if effective.Spec.Placement.ExecutionBackend != executionBackendProcess ||
		(workload.Spec.Runtime.Kind != runtimeNative && workload.Spec.Runtime.Kind != runtimeContainer) ||
		(workload.Spec.Runtime.Kind == runtimeContainer && containerRuntime == nil) {
		return errors.New("execution requires an unsupported backend or runtime")
	}
	if workload.Spec.Command.Executable == "" || workload.Spec.Command.Shell != nil {
		return errors.New("only direct executable commands are supported")
	}
	if workload.Spec.Resources != nil || len(workload.Spec.Extensions) != 0 {
		return errors.New("resource controls and extensions are not supported in this slice")
	}
	if workload.Spec.Environment != nil &&
		(workload.Spec.Environment.Profile != "" || len(workload.Spec.Environment.Secrets) != 0) {
		return errors.New("environment profiles and secrets are not supported in this slice")
	}
	if workload.Spec.Policy.Retry.MaxRuns != 1 {
		return errors.New("agent-side retries are not supported")
	}

	return nil
}

//nolint:cyclop,gocognit // Each rejected feature has a distinct compatibility error.
func validateSlurmAssignment(
	assignment protocol.SealedAgentAssignment,
	authorization protocol.LaunchAuthorization,
	containerRuntime *runtimeenv.Adapter,
) error {
	value := assignment.Document
	effective := value.Spec.EffectiveExecution
	workload := effective.Spec.Workload.Document
	if value.Metadata.AgentID != authorization.Metadata.AgentID ||
		effective.Metadata.ExecutionID != authorization.Metadata.ExecutionID ||
		effective.Spec.Placement.TargetGenerationID != authorization.Spec.TargetGenerationID ||
		assignment.EffectiveExecutionDigest != authorization.Spec.EffectiveExecutionDigest {
		return errors.New("launch authorization does not match assignment")
	}
	if effective.Spec.Placement.ExecutionBackend != executionBackendSlurm ||
		(workload.Spec.Runtime.Kind != runtimeNative && workload.Spec.Runtime.Kind != runtimeContainer) ||
		(workload.Spec.Runtime.Kind == runtimeContainer && containerRuntime == nil) {
		return errors.New("execution requires an unsupported backend or runtime")
	}
	if workload.Spec.Command.Executable == "" || workload.Spec.Command.Shell != nil {
		return errors.New("only direct executable commands are supported")
	}
	if len(workload.Spec.Extensions) != 0 {
		return errors.New("extensions are not supported in this slice")
	}
	if err := slurm.ValidateResources(workload.Spec.Resources); err != nil {
		return fmt.Errorf("unsupported Slurm resources: %w", err)
	}
	if workload.Spec.Environment != nil &&
		(workload.Spec.Environment.Profile != "" || len(workload.Spec.Environment.Secrets) != 0) {
		return errors.New("environment profiles and secrets are not supported in this slice")
	}
	if workload.Spec.Environment != nil {
		for name := range workload.Spec.Environment.Values {
			if strings.HasPrefix(name, "SLURM_") || name == "CUDA_VISIBLE_DEVICES" ||
				name == "ROCR_VISIBLE_DEVICES" {
				return fmt.Errorf("environment value %q conflicts with scheduler state", name)
			}
		}
	}
	if workload.Spec.Policy.Retry.MaxRuns != 1 {
		return errors.New("agent-side retries are not supported")
	}

	return nil
}

func validateExecutableAssignment(
	assignment protocol.SealedAgentAssignment,
	authorization protocol.LaunchAuthorization,
	containerRuntime *runtimeenv.Adapter,
) error {
	switch assignment.Document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend {
	case executionBackendProcess:
		return validateSubprocessAssignment(assignment, authorization, containerRuntime)
	case executionBackendSlurm:
		return validateSlurmAssignment(assignment, authorization, containerRuntime)
	default:
		return errors.New("execution requires an unsupported backend")
	}
}

func mapWorkspacePath(workspace, logical string) (string, error) {
	const prefix = "workspace:/"
	if !strings.HasPrefix(logical, prefix) {
		return "", errors.New("working directory is not a workspace path")
	}
	relative := strings.TrimPrefix(logical, prefix)
	if relative == "" {
		return workspace, nil
	}
	if filepath.FromSlash(relative) != filepath.Clean(filepath.FromSlash(relative)) ||
		strings.Contains(relative, "\\") {
		return "", errors.New("working directory is not normalized")
	}
	result := filepath.Join(workspace, filepath.FromSlash(relative))
	relativeCheck, err := filepath.Rel(workspace, result)
	if err != nil || relativeCheck == ".." || strings.HasPrefix(relativeCheck, ".."+string(filepath.Separator)) {
		return "", errors.New("working directory escapes workspace")
	}

	return result, nil
}

func finalizeProcessIdentity(command *exec.Cmd) (platform.ProcessIdentity, error) {
	tree, err := platform.FinalizeTargetStart(command.Process.Pid)
	if err != nil {
		return platform.ProcessIdentity{}, err
	}
	identity, err := platform.Inspect(command.Process.Pid)
	if err != nil {
		return platform.ProcessIdentity{}, err
	}
	identity.Tree = tree

	return identity, nil
}

func waitForProcess(
	ctx context.Context,
	command *exec.Cmd,
	identity platform.ProcessIdentity,
	cancelPath, timeoutText string,
) protocol.ProcessResult {
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	var timeout <-chan time.Time
	var timeoutTimer *time.Timer
	if timeoutText != "" {
		duration, err := time.ParseDuration(timeoutText)
		if err != nil {
			return terminateAndWait(command, identity, waited, processOutcomeFailure, "invalid_timeout")
		}
		timeoutTimer = time.NewTimer(duration)
		timeout = timeoutTimer.C
		defer timeoutTimer.Stop()
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case waitErr := <-waited:
			return processResult(command.ProcessState, waitErr, "")
		case <-ctx.Done():
			return terminateAndWait(command, identity, waited, "aborted", "runner_interrupted")
		case <-timeout:
			return terminateAndWait(command, identity, waited, "timed_out", "")
		case <-ticker.C:
			if _, err := os.Stat(cancelPath); err == nil {
				return terminateAndWait(command, identity, waited, cancelledOutcome, "")
			} else if !errors.Is(err, fs.ErrNotExist) {
				return terminateAndWait(command, identity, waited, processOutcomeFailure, "cancel_check_failed")
			}
		}
	}
}

func processResult(state *os.ProcessState, waitErr error, failureCode string) protocol.ProcessResult {
	result := protocol.ProcessResult{Outcome: "success", FailureCode: failureCode}
	if state != nil {
		exitCode := state.ExitCode()
		if exitCode >= 0 && exitCode <= 255 {
			result.ExitCode = &exitCode
		} else if exitCode > 255 && failureCode == "" {
			result.FailureCode = "exit_code_unrepresentable"
		}
		result.Signal = processSignal(state)
	}
	if waitErr != nil || result.ExitCode == nil || *result.ExitCode != 0 || result.Signal != "" || failureCode != "" {
		result.Outcome = processOutcomeFailure
	}

	return result
}

func writeRunnerFailure(directory, executionID, failureCode string, source error) error {
	completion := CompletionManifest{
		ExecutionID: executionID, ObservedAt: time.Now().UTC(),
		Result: protocol.ProcessResult{Outcome: processOutcomeFailure, FailureCode: failureCode},
	}
	if err := writeManifest(filepath.Join(directory, completionFilename), completion); err != nil {
		return errors.Join(fmt.Errorf("run agent execution: %w", source), err)
	}

	return fmt.Errorf("run agent execution: %w", source)
}

func writeManifest(path string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	return writePrivateFile(path, encoded)
}

func readStartManifest(directory string) (StartManifest, error) {
	var result StartManifest
	if err := readManifest(filepath.Join(directory, startedFilename), &result); err != nil {
		return result, err
	}
	if result.ExecutionID == "" || result.NativeID == "" || result.Process.PID <= 0 || result.StartedAt.IsZero() {
		return result, errors.New("invalid start manifest")
	}

	return result, nil
}

func readCompletionManifest(directory string) (CompletionManifest, error) {
	var result CompletionManifest
	if err := readManifest(filepath.Join(directory, completionFilename), &result); err != nil {
		return result, err
	}
	if result.ExecutionID == "" || result.ObservedAt.IsZero() {
		return result, errors.New("invalid completion manifest")
	}

	return result, nil
}

func readManifest(path string, target any) error {
	document, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return decodeStrictJSON(document, target)
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}

	return true
}

func executionBaseEnvironment() []string {
	allowed := map[string]struct{}{
		"COMSPEC": {}, "HOME": {}, "LANG": {}, "LC_ALL": {}, "LOGNAME": {},
		"PATH": {}, "PATHEXT": {}, "SYSTEMROOT": {}, "TEMP": {}, "TMP": {},
		"TMPDIR": {}, "USER": {}, "USERPROFILE": {},
	}
	result := make([]string, 0, len(allowed))
	for _, value := range os.Environ() {
		name, _, found := strings.Cut(value, "=")
		if _, include := allowed[strings.ToUpper(name)]; found && include {
			result = append(result, value)
		}
	}

	return result
}

func slurmExecutionEnvironment() []string {
	var result []string
	for _, assignment := range os.Environ() {
		name, _, found := strings.Cut(assignment, "=")
		if found && (strings.HasPrefix(name, "SLURM_") || name == "CUDA_VISIBLE_DEVICES" ||
			name == "ROCR_VISIBLE_DEVICES") {
			result = append(result, assignment)
		}
	}
	sort.Strings(result)

	return result
}
