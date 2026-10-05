package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/ryancswallace/jobman/internal/artifact"
	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/runtimeenv"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	defaultPollInterval         = 2 * time.Second
	staleLaunchClaim            = 30 * time.Second
	capabilityInterval          = 5 * time.Minute
	defaultMaximumArtifactBytes = 1024 * 1024 * 1024
)

// ServiceOptions configures a per-user named-host execution agent.
type ServiceOptions struct {
	StateDirectory         string
	RunnerExecutable       string
	PollInterval           time.Duration
	Logger                 *slog.Logger
	ArtifactStoreName      string
	ArtifactStoreVersion   int64
	ArtifactRoot           string
	ArtifactS3Bucket       string
	ArtifactS3Prefix       string
	ArtifactS3Region       string
	ArtifactS3Owner        string
	AWSExecutable          string
	ContainerEngine        string
	ContainerExecutable    string
	ContainerHostNetwork   bool
	MaximumArtifactBytes   int64
	MaximumLogBytes        int64
	SlurmRoot              string
	SlurmRunner            string
	slurmCommandRunner     slurmbackend.Runner
	s3CommandRunner        artifact.CommandRunner
	containerCommandRunner runtimeenv.CommandRunner
}

// RunService polls Jobman Control, maintains the host-local journal, and
// reconciles isolated runner manifests until ctx is canceled.
//
//nolint:cyclop,gocognit,nestif // Startup validates and acquires each resource in one fail-closed sequence.
func RunService(ctx context.Context, options ServiceOptions) error {
	if ctx == nil {
		return errors.New("run agent service: nil context")
	}
	if options.Logger == nil {
		return errors.New("run agent service: logger is required")
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.PollInterval < 250*time.Millisecond || options.PollInterval > time.Minute {
		return errors.New("run agent service: poll interval must be between 250ms and 1m")
	}
	if options.MaximumLogBytes == 0 {
		options.MaximumLogBytes = defaultMaximumLogBytes
	}
	if options.MaximumLogBytes < 1 {
		return errors.New("run agent service: maximum log bytes must be positive")
	}
	if options.MaximumArtifactBytes == 0 {
		options.MaximumArtifactBytes = defaultMaximumArtifactBytes
	}
	if options.MaximumArtifactBytes < 1 {
		return errors.New("run agent service: maximum artifact bytes must be positive")
	}
	if (options.SlurmRoot == "") != (options.SlurmRunner == "") {
		return errors.New("run agent service: Slurm root and runner are required together")
	}
	var err error
	var scheduler *slurmbackend.Adapter
	currentUser, err := user.Current()
	if err != nil {
		return fmt.Errorf("run agent service: read execution user: %w", err)
	}
	executionUser := currentUser.Username
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("run agent service: read hostname: %w", err)
	}
	executionBackends := []string{executionBackendProcess}
	capabilities := []string(nil)
	if options.SlurmRoot != "" {
		options.SlurmRoot, err = prepareSharedExecutionRoot(options.SlurmRoot)
		if err != nil {
			return fmt.Errorf("run agent service: %w", err)
		}
		if err = validateSlurmRunner(options.SlurmRunner); err != nil {
			return fmt.Errorf("run agent service: %w", err)
		}
		commandRunner := options.slurmCommandRunner
		if commandRunner == nil {
			commandRunner = slurmbackend.ExecRunner{}
		}
		scheduler, err = slurmbackend.New(commandRunner)
		if err != nil {
			return fmt.Errorf("run agent service: %w", err)
		}
		if err = scheduler.Probe(ctx); err != nil {
			return fmt.Errorf("run agent service: %w", err)
		}
		executionBackends = []string{executionBackendSlurm}
		capabilities = []string{"slurm-accounting", "slurm-arrays", "slurm-cli"}
	}
	artifactStore, artifactCapability, err := newArtifactStore(ctx, artifactStoreOptions{
		name: options.ArtifactStoreName, version: options.ArtifactStoreVersion,
		root: options.ArtifactRoot, s3Bucket: options.ArtifactS3Bucket,
		s3Prefix: options.ArtifactS3Prefix, s3Region: options.ArtifactS3Region,
		s3ExpectedOwner: options.ArtifactS3Owner, awsExecutable: options.AWSExecutable,
		s3Runner: options.s3CommandRunner,
	})
	if err != nil {
		return fmt.Errorf("run agent service: %w", err)
	}
	if artifactCapability != "" {
		capabilities = append(capabilities, artifactCapability)
	}
	containerRuntime, err := newContainerRuntime(
		ctx, options.ContainerEngine, options.ContainerExecutable,
		options.ContainerHostNetwork, options.containerCommandRunner,
	)
	if err != nil {
		return fmt.Errorf("run agent service: %w", err)
	}
	runtimes := []string{runtimeNative}
	if containerRuntime != nil {
		if (options.SlurmRoot != "") != (containerRuntime.Engine() == "apptainer") {
			return errors.New("run agent service: apptainer is required for Slurm and unsupported for subprocess targets")
		}
		runtimes = append(runtimes, "container")
		capabilities = append(capabilities, "container-"+containerRuntime.Engine())
	}
	stateDirectory, err := prepareStateDirectory(options.StateDirectory)
	if err != nil {
		return fmt.Errorf("run agent service: %w", err)
	}
	if options.RunnerExecutable == "" {
		options.RunnerExecutable, err = os.Executable()
		if err != nil {
			return fmt.Errorf("run agent service: locate runner executable: %w", err)
		}
	}
	client, err := OpenClient(stateDirectory)
	if err != nil {
		return fmt.Errorf("run agent service: %w", err)
	}
	defer client.Close()
	spool, err := OpenSpool(ctx, stateDirectory)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("run agent service: %w", err)
	}
	defer func() { _ = spool.Close() }()
	service := &service{
		stateDirectory: stateDirectory, runnerExecutable: options.RunnerExecutable,
		logger: options.Logger, client: client, spool: spool, artifactStore: artifactStore,
		maximumLogBytes: options.MaximumLogBytes, slurm: scheduler, container: containerRuntime,
		maximumArtifactBytes: options.MaximumArtifactBytes,
		slurmRoot:            options.SlurmRoot, slurmRunner: options.SlurmRunner,
		executionUser: executionUser,
		capabilityReport: &CapabilityReport{
			AcceptingAssignments: true, AgentVersion: buildinfo.Version,
			OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH,
			Hostname: hostname, ExecutionUser: executionUser,
			ExecutionBackends: executionBackends, Runtimes: runtimes,
			Capabilities: capabilities,
		},
	}

	service.stepAndReport(ctx)
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			service.stepAndReport(ctx)
		}
	}
}

type service struct {
	stateDirectory       string
	runnerExecutable     string
	logger               *slog.Logger
	client               *Client
	spool                *Spool
	artifactStore        artifact.Store
	maximumLogBytes      int64
	maximumArtifactBytes int64
	slurm                slurmScheduler
	container            *runtimeenv.Adapter
	slurmRoot            string
	slurmRunner          string
	executionUser        string
	capabilityReport     *CapabilityReport
	capabilitySentAt     time.Time
	startExecution       func(string, string) error
}

func (service *service) stepAndReport(ctx context.Context) {
	if err := service.step(ctx); err != nil && ctx.Err() == nil {
		service.logger.WarnContext(ctx, "agent reconciliation failed", "error", err)
	}
}

//nolint:cyclop // One polling step deliberately sequences every durable control-plane queue.
func (service *service) step(ctx context.Context) error {
	sessionExpiresAt := service.client.SessionExpiresAt()
	if !sessionExpiresAt.IsZero() && time.Until(sessionExpiresAt) <= 5*time.Minute {
		if err := service.client.RenewSession(ctx, service.stateDirectory); err != nil {
			return err
		}
	}
	if time.Until(service.client.CertificateExpiresAt()) <= 20*time.Minute {
		if err := service.client.RenewCertificate(ctx, service.stateDirectory); err != nil {
			return err
		}
	}
	if service.capabilityReport != nil && (service.capabilitySentAt.IsZero() ||
		time.Since(service.capabilitySentAt) >= capabilityInterval) {
		report := *service.capabilityReport
		report.ObservedAt = time.Now().UTC()
		if err := service.client.ReportCapabilities(ctx, report); err != nil {
			return err
		}
		service.capabilitySentAt = report.ObservedAt
	}
	// An unavailable scheduler observation must not starve durable sibling
	// events or cancellation polling. Keep publication's log-before-event
	// ordering, and still return every reconciliation failure to the caller.
	reconcileErr := service.reconcileExecutions(ctx)
	if err := ctx.Err(); err != nil {
		return errors.Join(reconcileErr, err)
	}
	if err := service.flushLogChunks(ctx); err != nil {
		return errors.Join(reconcileErr, err)
	}
	if err := service.flushEvents(ctx); err != nil {
		return errors.Join(reconcileErr, err)
	}
	if err := service.pollActions(ctx); err != nil {
		return errors.Join(reconcileErr, err)
	}
	if err := service.pollAssignments(ctx); err != nil {
		return errors.Join(reconcileErr, err)
	}
	reconcileErr = errors.Join(reconcileErr, service.reconcileExecutions(ctx))
	if err := ctx.Err(); err != nil {
		return errors.Join(reconcileErr, err)
	}
	if err := service.flushLogChunks(ctx); err != nil {
		return errors.Join(reconcileErr, err)
	}

	return errors.Join(reconcileErr, service.flushEvents(ctx))
}

func (service *service) pollAssignments(ctx context.Context) error {
	assignments, err := service.client.ListAssignments(ctx, 100)
	if err != nil {
		return err
	}
	for _, assignment := range assignments {
		if err = validateOfferedAssignment(assignment, service.client, service.container); err != nil {
			return fmt.Errorf("validate offered assignment: %w", err)
		}
		if assignment.Document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend == executionBackendSlurm &&
			service.slurm == nil {
			return errors.New("validate offered assignment: Slurm execution is not configured")
		}
		if err := service.spool.PutAssignment(ctx, assignment); err != nil {
			return err
		}
		authorization, acceptErr := service.client.AcceptAssignment(ctx, assignment)
		if acceptErr != nil {
			return acceptErr
		}
		executionID := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
		if err := service.spool.RecordAcceptance(ctx, executionID, authorization); err != nil {
			return err
		}
	}

	return nil
}

func validateOfferedAssignment(
	assignment protocol.SealedAgentAssignment,
	client *Client,
	containerRuntime *runtimeenv.Adapter,
) error {
	value := assignment.Document
	if value.Metadata.AgentID != client.AgentID() ||
		value.Spec.EffectiveExecution.Spec.Placement.TargetGenerationID != client.TargetGenerationID() {
		return errors.New("assignment is for a different agent or target generation")
	}
	authorization := protocol.LaunchAuthorization{
		Metadata: protocol.LaunchAuthorizationMetadata{
			ExecutionID: value.Spec.EffectiveExecution.Metadata.ExecutionID,
			AgentID:     value.Metadata.AgentID,
		},
		Spec: protocol.LaunchAuthorizationSpec{
			TargetGenerationID:       value.Spec.EffectiveExecution.Spec.Placement.TargetGenerationID,
			EffectiveExecutionDigest: assignment.EffectiveExecutionDigest,
		},
	}

	return validateExecutableAssignment(assignment, authorization, containerRuntime)
}

func (service *service) pollActions(ctx context.Context) error {
	actions, err := service.client.ListActions(ctx, 25)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if journalErr := service.spool.PutAction(ctx, action); journalErr != nil {
			return journalErr
		}
		directory, directoryErr := executionDirectory(service.stateDirectory, action.Metadata.ExecutionID)
		if directoryErr != nil {
			return fmt.Errorf("apply desired action: %w", directoryErr)
		}
		if err = writePrivateFile(filepath.Join(directory, cancelFilename), []byte(action.Metadata.ActionID+"\n")); err != nil {
			return fmt.Errorf("apply desired action: %w", err)
		}
		if err := service.client.AcknowledgeAction(ctx, action); err != nil {
			return err
		}
		if err := service.spool.MarkActionAcknowledged(ctx, action.Metadata.ActionID); err != nil {
			return err
		}
	}

	return nil
}

func (service *service) reconcileExecutions(ctx context.Context) error {
	executions, err := service.spool.ListExecutions(ctx)
	if err != nil {
		return err
	}
	reconcileErr := service.reconcileSlurmArrays(ctx, executions)
	for _, execution := range executions {
		if err := ctx.Err(); err != nil {
			return errors.Join(reconcileErr, err)
		}
		effective := execution.Assignment.Document.Spec.EffectiveExecution
		if effective.Metadata.SlurmArray != nil {
			directory, directoryErr := executionDirectory(service.stateDirectory, effective.Metadata.ExecutionID)
			if directoryErr != nil {
				reconcileErr = errors.Join(reconcileErr, directoryErr)
				continue
			}
			if _, submissionErr := readSlurmSubmission(directory, effective.Metadata.ExecutionID); errors.Is(submissionErr, fs.ErrNotExist) {
				continue
			} else if submissionErr != nil {
				reconcileErr = errors.Join(reconcileErr, submissionErr)
				continue
			}
		}
		if err := service.reconcileExecution(ctx, execution); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}

	return reconcileErr
}

//nolint:cyclop,gocognit // Reconciliation handles each durable runner-manifest state explicitly.
func (service *service) reconcileExecution(ctx context.Context, execution SpoolExecution) error {
	assignment := execution.Assignment
	executionID := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	directory, err := prepareExecutionFiles(service.stateDirectory, assignment, *execution.Authorization)
	if err != nil {
		return err
	}
	if assignment.Document.Spec.EffectiveExecution.Spec.Placement.ExecutionBackend == executionBackendSlurm {
		if service.slurm == nil {
			return errors.New("reconcile Slurm execution: backend is not configured")
		}
		if execution.State == "accepted" {
			if stateErr := service.spool.SetExecutionState(ctx, executionID, "launching"); stateErr != nil {
				return stateErr
			}
		}

		return service.reconcileSlurmExecution(ctx, execution, directory)
	}
	completion, completionErr := readCompletionManifest(directory)
	started, startedErr := readStartManifest(directory)
	if completionErr == nil {
		if publishErr := service.publishLogs(ctx, execution, directory, &completion); publishErr != nil {
			return publishErr
		}
		return service.observeCompletion(ctx, execution, started, startedErr, completion)
	}
	if !errors.Is(completionErr, fs.ErrNotExist) {
		return fmt.Errorf("reconcile execution %s: %w", executionID, completionErr)
	}
	if startedErr == nil {
		if observeErr := service.observeStart(ctx, execution, started); observeErr != nil {
			return observeErr
		}
		if publishErr := service.publishLogs(ctx, execution, directory, nil); publishErr != nil {
			return publishErr
		}

		return service.spool.SetExecutionState(ctx, executionID, "running")
	}
	if !errors.Is(startedErr, fs.ErrNotExist) {
		return fmt.Errorf("reconcile execution %s: %w", executionID, startedErr)
	}
	claimPath := filepath.Join(directory, claimFilename)
	claim, claimErr := os.Stat(claimPath)
	if claimErr == nil {
		if time.Since(claim.ModTime()) < staleLaunchClaim {
			return nil
		}
		lost := CompletionManifest{
			ExecutionID: executionID, ObservedAt: time.Now().UTC(),
			Result: protocol.ProcessResult{Outcome: "lost"},
		}
		if err = writeManifest(filepath.Join(directory, completionFilename), lost); err != nil {
			return fmt.Errorf("record lost execution: %w", err)
		}

		return service.observeCompletion(ctx, execution, StartManifest{}, fs.ErrNotExist, lost)
	}
	if !errors.Is(claimErr, fs.ErrNotExist) {
		return fmt.Errorf("inspect launch claim: %w", claimErr)
	}
	if execution.State == "terminal" {
		return errors.New("terminal execution has no completion manifest")
	}
	if err := service.spool.SetExecutionState(ctx, executionID, "launching"); err != nil {
		return err
	}

	if service.startExecution != nil {
		return service.startExecution(directory, executionID)
	}

	return service.startRunner(directory, executionID)
}

func (service *service) observeStart(
	ctx context.Context,
	execution SpoolExecution,
	started StartManifest,
) error {
	executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	if started.ExecutionID != executionID {
		return errors.New("start manifest execution identity mismatch")
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: derivedEventID(executionID, "process.started"), ExecutionID: executionID,
			AgentID: service.client.AgentID(), Sequence: 1, ObservedAt: started.StartedAt,
		},
		Spec: protocol.ExecutionEventSpec{Type: "process.started", NativeID: started.NativeID},
	})
	if err != nil {
		return fmt.Errorf("observe process start: %w", err)
	}

	return service.spool.QueueEvent(ctx, event)
}

func (service *service) observeCompletion(
	ctx context.Context,
	execution SpoolExecution,
	started StartManifest,
	startedErr error,
	completion CompletionManifest,
) error {
	executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	if completion.ExecutionID != executionID {
		return errors.New("completion manifest execution identity mismatch")
	}
	sequence := int64(1)
	if startedErr == nil {
		if err := service.observeStart(ctx, execution, started); err != nil {
			return err
		}
		sequence = 2
	} else if !errors.Is(startedErr, fs.ErrNotExist) {
		return fmt.Errorf("observe process completion: %w", startedErr)
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: derivedEventID(executionID, "process.completed"), ExecutionID: executionID,
			AgentID: service.client.AgentID(), Sequence: sequence, ObservedAt: completion.ObservedAt,
		},
		Spec: protocol.ExecutionEventSpec{
			Type: "process.completed", Result: &completion.Result, Artifacts: completion.Artifacts,
		},
	})
	if err != nil {
		return fmt.Errorf("observe process completion: %w", err)
	}
	if err := service.spool.QueueEvent(ctx, event); err != nil {
		return err
	}

	return service.spool.SetExecutionState(ctx, executionID, "terminal")
}

func (service *service) flushEvents(ctx context.Context) error {
	events, err := service.spool.PendingEvents(ctx)
	if err != nil {
		return err
	}
	for _, pending := range events {
		if err := service.client.RecordEvent(ctx, pending.Event); err != nil {
			return err
		}
		if err := service.spool.MarkEventDelivered(ctx, pending.Event.Document.Metadata.EventID); err != nil {
			return err
		}
	}

	return nil
}

func (service *service) startRunner(directory, executionID string) error {
	maximumLogBytes := service.maximumLogBytes
	if maximumLogBytes == 0 {
		maximumLogBytes = defaultMaximumLogBytes
	}
	logFile, err := os.OpenFile(filepath.Join(directory, "runner.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("start execution runner: open log: %w", err)
	}
	command := exec.Command( //nolint:noctx // The isolated runner must outlive service cancellation.
		service.runnerExecutable, "run-execution", "--state-dir", service.stateDirectory,
		"--execution-id", executionID, maximumLogBytesFlag, strconv.FormatInt(maximumLogBytes, 10),
	) // #nosec G204 -- executable is the configured agent binary and arguments are fixed or validated UUIDs.
	command.Args = appendArtifactRunnerArguments(command.Args, service.artifactStore, service.maximumArtifactBytes)
	command.Args = appendContainerRunnerArguments(command.Args, service.container)
	command.Stdin = nil
	command.Stdout = logFile
	command.Stderr = logFile
	platform.ConfigureSupervisor(command)
	if err = command.Start(); err != nil {
		return errors.Join(fmt.Errorf("start execution runner: %w", err), logFile.Close())
	}
	go func() {
		if waitErr := command.Wait(); waitErr != nil && service.logger != nil {
			service.logger.Warn("execution runner exited with an error", "execution-id", executionID, "error", waitErr)
		}
		if closeErr := logFile.Close(); closeErr != nil && service.logger != nil {
			service.logger.Warn("close execution runner log", "execution-id", executionID, "error", closeErr)
		}
	}()

	return nil
}

func derivedEventID(executionID, eventType string) string {
	digest := sha256.Sum256([]byte(executionID + "\x00" + eventType))
	value := digest[:16]
	value[6] = (value[6] & 0x0f) | 0x50
	value[8] = (value[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}
