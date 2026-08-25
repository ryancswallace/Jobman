package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
	"github.com/ryancswallace/jobman/protocol"
)

const (
	slurmAttemptFilename       = "slurm-attempt.json"
	slurmSubmissionFilename    = "slurm-submission.json"
	slurmCancelReceiptFilename = "slurm-cancel-requested"
	slurmScriptFilename        = "jobman-slurm.sh"
	slurmStdoutFilename        = "slurm.stdout.log"
	slurmStderrFilename        = "slurm.stderr.log"
	slurmBatchScript           = "#!/bin/sh\nset -eu\nexec \"$@\"\n"
)

type slurmScheduler interface {
	Submit(context.Context, slurmbackend.SubmitRequest) (slurmbackend.Submission, error)
	SubmitArray(context.Context, slurmbackend.ArraySubmitRequest) (slurmbackend.Submission, error)
	FindByName(context.Context, string, string, time.Time) (slurmbackend.Submission, error)
	Observe(context.Context, string) (slurmbackend.Observation, error)
	Cancel(context.Context, string) error
}

type slurmAttemptManifest struct {
	ExecutionID   string    `json:"executionId"`
	JobName       string    `json:"jobName"`
	ExecutionUser string    `json:"executionUser"`
	AttemptedAt   time.Time `json:"attemptedAt"`
}

type slurmSubmissionManifest struct {
	ExecutionID string    `json:"executionId"`
	JobID       string    `json:"jobId"`
	Cluster     string    `json:"cluster,omitempty"`
	SubmittedAt time.Time `json:"submittedAt"`
}

func prepareSharedExecutionRoot(root string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", errors.New("slurm execution root must be an absolute normalized path")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("create Slurm execution root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", fmt.Errorf("inspect Slurm execution root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("slurm execution root must be a real directory")
	}
	if err = os.Chmod(root, 0o700); err != nil { // #nosec G302 -- owner traversal is required.
		return "", fmt.Errorf("protect Slurm execution root: %w", err)
	}

	return root, nil
}

func validateSlurmRunner(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("slurm runner must be an absolute normalized path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect Slurm runner: %w", err)
	}
	if !isOwnerExecutableRegular(info) {
		return errors.New("slurm runner must be an owner-executable regular file")
	}

	return nil
}

func prepareSlurmBundle(
	root string,
	assignment protocol.SealedAgentAssignment,
	authorization protocol.LaunchAuthorization,
) (string, error) {
	executionID := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	directory, err := executionDirectory(root, executionID)
	if err != nil {
		return "", fmt.Errorf("prepare Slurm bundle: %w", err)
	}
	parent := filepath.Dir(directory)
	if err = os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("prepare Slurm bundle: create parent: %w", err)
	}
	if err = os.Chmod(parent, 0o700); err != nil { // #nosec G302 -- owner traversal is required.
		return "", fmt.Errorf("prepare Slurm bundle: protect parent: %w", err)
	}
	if err = os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", fmt.Errorf("prepare Slurm bundle: create directory: %w", err)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return "", fmt.Errorf("prepare Slurm bundle: inspect directory: %w", err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("prepare Slurm bundle: execution directory must be a real directory")
	}
	if err = os.Chmod(directory, 0o700); err != nil { // #nosec G302 -- owner traversal is required.
		return "", fmt.Errorf("prepare Slurm bundle: protect directory: %w", err)
	}
	authorizationDocument, err := marshalStrictJSON(authorization)
	if err != nil {
		return "", fmt.Errorf("prepare Slurm bundle: encode authorization: %w", err)
	}
	for path, contents := range map[string][]byte{
		filepath.Join(directory, assignmentFilename):    assignment.CanonicalJSON,
		filepath.Join(directory, authorizationFilename): authorizationDocument,
		filepath.Join(directory, slurmScriptFilename):   []byte(slurmBatchScript),
	} {
		if err = writeImmutablePrivateFile(path, contents); err != nil {
			return "", fmt.Errorf("prepare Slurm bundle: %w", err)
		}
	}

	return directory, nil
}

func writeImmutablePrivateFile(path string, contents []byte) error {
	err := writeNewPrivateFile(path, contents)
	if err == nil {
		return nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	existing, readErr := os.ReadFile(path)
	if readErr != nil {
		return readErr
	}
	if !bytes.Equal(existing, contents) {
		return errSpoolConflict
	}

	return nil
}

func (service *service) reconcileSlurmExecution(
	ctx context.Context,
	execution SpoolExecution,
	localDirectory string,
) error {
	assignment := execution.Assignment
	executionID := assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	sharedDirectory, err := prepareSlurmBundle(service.slurmRoot, assignment, *execution.Authorization)
	if err != nil {
		return err
	}
	submission, err := readSlurmSubmission(localDirectory, executionID)
	if errors.Is(err, fs.ErrNotExist) {
		submission, err = service.ensureSlurmSubmission(ctx, execution, localDirectory, sharedDirectory)
	}
	if err != nil {
		return err
	}
	if submission.JobID == "" {
		return nil
	}
	if queueErr := service.queueSchedulerEvent(
		ctx, executionID, "scheduler.submitted", submission.SubmittedAt,
		submission.JobID, "queued", "", submission.Cluster,
	); queueErr != nil {
		return queueErr
	}
	observation, err := service.slurm.Observe(ctx, submission.JobID)
	if err != nil {
		return err
	}
	cluster := observation.Cluster
	if cluster == "" {
		cluster = submission.Cluster
	}
	if observation.Terminal {
		return service.completeSlurmExecution(
			ctx, execution, sharedDirectory, observation, cluster,
		)
	}
	if queueErr := service.queueSchedulerEvent(
		ctx, executionID, "scheduler.observed:"+observation.State,
		time.Now().UTC(), observation.JobID, observation.State, observation.Reason, cluster,
	); queueErr != nil {
		return queueErr
	}
	if observation.State == "running" {
		if stateErr := service.spool.SetExecutionState(ctx, executionID, "running"); stateErr != nil {
			return stateErr
		}
	}
	if _, statErr := os.Stat(filepath.Join(localDirectory, cancelFilename)); statErr == nil {
		if cancelErr := service.cancelSlurmExecution(
			ctx, localDirectory, sharedDirectory, submission.JobID,
		); cancelErr != nil {
			return cancelErr
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return fmt.Errorf("inspect Slurm cancellation: %w", statErr)
	}

	return nil
}

//nolint:nestif // The submit-once ambiguity boundary is deliberately explicit.
func (service *service) ensureSlurmSubmission(
	ctx context.Context,
	execution SpoolExecution,
	localDirectory, sharedDirectory string,
) (slurmSubmissionManifest, error) {
	assignment := execution.Assignment
	effective := assignment.Document.Spec.EffectiveExecution
	executionID := effective.Metadata.ExecutionID
	attempt, err := readSlurmAttempt(localDirectory, executionID)
	if errors.Is(err, fs.ErrNotExist) {
		attempt = slurmAttemptManifest{
			ExecutionID: executionID, JobName: "jobman-" + executionID,
			ExecutionUser: service.executionUser, AttemptedAt: time.Now().UTC(),
		}
		if err = writeManifest(filepath.Join(localDirectory, slurmAttemptFilename), attempt); err != nil {
			return slurmSubmissionManifest{}, fmt.Errorf("record Slurm submission attempt: %w", err)
		}
		request := slurmbackend.SubmitRequest{
			JobName: attempt.JobName, Partition: effective.Spec.Placement.Partition,
			ScriptPath: filepath.Join(sharedDirectory, slurmScriptFilename),
			StdoutPath: filepath.Join(sharedDirectory, slurmStdoutFilename),
			StderrPath: filepath.Join(sharedDirectory, slurmStderrFilename),
			RunnerPath: service.slurmRunner,
			RunnerArgs: []string{
				"run-execution", "--state-dir", service.slurmRoot,
				"--execution-id", executionID,
				maximumLogBytesFlag, strconv.FormatInt(service.maximumLogBytes, 10),
			},
			Resources: effective.Spec.Workload.Document.Spec.Resources,
		}
		request.RunnerArgs = appendArtifactRunnerArguments(
			request.RunnerArgs, service.artifactStore, service.maximumArtifactBytes,
		)
		request.RunnerArgs = appendContainerRunnerArguments(request.RunnerArgs, service.container)
		native, submitErr := service.slurm.Submit(ctx, request)
		if submitErr != nil {
			if queueErr := service.queueSchedulerEvent(
				ctx, executionID, "scheduler.uncertain", time.Now().UTC(), "", "uncertain",
				"submission outcome is uncertain", "",
			); queueErr != nil {
				return slurmSubmissionManifest{}, errors.Join(submitErr, queueErr)
			}
			if service.logger != nil {
				service.logger.WarnContext(ctx, "Slurm submission outcome is uncertain", "execution-id", executionID)
			}

			return slurmSubmissionManifest{}, nil
		}
		return persistSlurmSubmission(localDirectory, executionID, native, time.Now().UTC())
	}
	if err != nil {
		return slurmSubmissionManifest{}, err
	}
	native, findErr := service.slurm.FindByName(
		ctx, attempt.JobName, attempt.ExecutionUser, attempt.AttemptedAt.Add(-time.Minute),
	)
	if findErr != nil {
		if queueErr := service.queueSchedulerEvent(
			ctx, executionID, "scheduler.uncertain", time.Now().UTC(), "", "uncertain",
			"submission identity has not been reconciled", "",
		); queueErr != nil {
			return slurmSubmissionManifest{}, queueErr
		}
		if service.logger != nil {
			service.logger.WarnContext(ctx, "Slurm submission remains uncertain", "execution-id", executionID)
		}

		return slurmSubmissionManifest{}, nil
	}

	return persistSlurmSubmission(localDirectory, executionID, native, time.Now().UTC())
}

func persistSlurmSubmission(
	directory, executionID string,
	submission slurmbackend.Submission,
	submittedAt time.Time,
) (slurmSubmissionManifest, error) {
	manifest := slurmSubmissionManifest{
		ExecutionID: executionID, JobID: submission.JobID,
		Cluster: submission.Cluster, SubmittedAt: submittedAt,
	}
	if err := writeManifest(filepath.Join(directory, slurmSubmissionFilename), manifest); err != nil {
		return slurmSubmissionManifest{}, fmt.Errorf("record Slurm submission: %w", err)
	}

	return manifest, nil
}

func readSlurmAttempt(directory, executionID string) (slurmAttemptManifest, error) {
	var manifest slurmAttemptManifest
	if err := readManifest(filepath.Join(directory, slurmAttemptFilename), &manifest); err != nil {
		return manifest, err
	}
	if manifest.ExecutionID != executionID || manifest.JobName == "" ||
		manifest.ExecutionUser == "" || manifest.AttemptedAt.IsZero() {
		return manifest, errors.New("invalid Slurm attempt manifest")
	}

	return manifest, nil
}

func readSlurmSubmission(directory, executionID string) (slurmSubmissionManifest, error) {
	var manifest slurmSubmissionManifest
	if err := readManifest(filepath.Join(directory, slurmSubmissionFilename), &manifest); err != nil {
		return manifest, err
	}
	if manifest.ExecutionID != executionID || manifest.JobID == "" || manifest.SubmittedAt.IsZero() {
		return manifest, errors.New("invalid Slurm submission manifest")
	}

	return manifest, nil
}

func (service *service) cancelSlurmExecution(
	ctx context.Context,
	localDirectory, sharedDirectory, jobID string,
) error {
	if err := writePrivateFile(
		filepath.Join(sharedDirectory, cancelFilename), []byte("cancel requested\n"),
	); err != nil {
		return fmt.Errorf("stage Slurm cancellation: %w", err)
	}
	receipt := filepath.Join(localDirectory, slurmCancelReceiptFilename)
	if _, err := os.Stat(receipt); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect Slurm cancel receipt: %w", err)
	}
	if err := service.slurm.Cancel(ctx, jobID); err != nil {
		return err
	}

	return writeNewPrivateFile(receipt, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"))
}

func (service *service) completeSlurmExecution(
	ctx context.Context,
	execution SpoolExecution,
	sharedDirectory string,
	observation slurmbackend.Observation,
	cluster string,
) error {
	executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
	completion, err := readCompletionManifest(sharedDirectory)
	if errors.Is(err, fs.ErrNotExist) {
		if observation.Result == nil {
			return errors.New("terminal slurm observation has no result")
		}
		completion = CompletionManifest{
			ExecutionID: executionID, ObservedAt: time.Now().UTC(), Result: *observation.Result,
		}
		if err = writeManifest(filepath.Join(sharedDirectory, completionFilename), completion); err != nil {
			return fmt.Errorf("record synthetic Slurm completion: %w", err)
		}
	} else if err != nil {
		return err
	}
	if completion.ExecutionID != executionID {
		return errors.New("slurm completion manifest execution identity mismatch")
	}
	if publishErr := service.publishLogs(ctx, execution, sharedDirectory, &completion); publishErr != nil {
		return publishErr
	}
	if queueErr := service.queueSchedulerEventWithArtifacts(
		ctx, executionID, "scheduler.completed", completion.ObservedAt,
		observation.JobID, observation.State, observation.Reason, cluster, &completion.Result,
		completion.Artifacts,
	); queueErr != nil {
		return queueErr
	}

	return service.spool.SetExecutionState(ctx, executionID, "terminal")
}

func (service *service) queueSchedulerEvent(
	ctx context.Context,
	executionID, eventKey string,
	observedAt time.Time,
	nativeID, state, reason, cluster string,
) error {
	return service.queueSchedulerEventWithArtifacts(
		ctx, executionID, eventKey, observedAt, nativeID, state, reason, cluster, nil, nil,
	)
}

func (service *service) queueSchedulerEventWithArtifacts(
	ctx context.Context,
	executionID, eventKey string,
	observedAt time.Time,
	nativeID, state, reason, cluster string,
	result *protocol.ProcessResult,
	artifacts []protocol.PublishedArtifact,
) error {
	eventID := derivedEventID(executionID, eventKey)
	exists, err := service.spool.EventExists(ctx, eventID)
	if err != nil || exists {
		return err
	}
	sequence, err := service.spool.NextEventSequence(ctx, executionID)
	if err != nil {
		return err
	}
	eventType := eventKey
	if len(eventType) > len("scheduler.observed") && eventType[:len("scheduler.observed")] == "scheduler.observed" {
		eventType = "scheduler.observed"
	}
	event, err := protocol.SealExecutionEvent(protocol.ExecutionEvent{
		APIVersion: protocol.V1Alpha1, Kind: protocol.ExecutionEventKind,
		Metadata: protocol.ExecutionEventMetadata{
			EventID: eventID, ExecutionID: executionID, AgentID: service.client.AgentID(),
			Sequence: sequence, ObservedAt: observedAt.UTC(),
		},
		Spec: protocol.ExecutionEventSpec{
			Type: eventType, NativeID: nativeID,
			Scheduler: &protocol.SchedulerObservation{
				Backend: executionBackendSlurm, State: state, Reason: reason, Cluster: cluster,
			},
			Result: result, Artifacts: artifacts,
		},
	})
	if err != nil {
		return fmt.Errorf("observe Slurm scheduler: %w", err)
	}

	return service.spool.QueueEvent(ctx, event)
}
