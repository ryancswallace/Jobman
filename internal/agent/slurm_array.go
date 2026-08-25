package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	slurmbackend "github.com/ryancswallace/jobman/internal/backend/slurm"
)

const (
	slurmArraysDirectoryName   = "arrays"
	slurmArrayAttemptFilename  = "slurm-array-attempt.json"
	slurmArraySubmitFilename   = "slurm-array-submission.json"
	slurmArrayManifestFilename = "array-manifest.json"
)

type slurmArrayAttemptManifest struct {
	CollectionID  string    `json:"collectionId"`
	JobName       string    `json:"jobName"`
	ExecutionUser string    `json:"executionUser"`
	TaskCount     int       `json:"taskCount"`
	AttemptedAt   time.Time `json:"attemptedAt"`
}

type slurmArraySubmissionManifest struct {
	CollectionID string    `json:"collectionId"`
	JobID        string    `json:"jobId"`
	Cluster      string    `json:"cluster,omitempty"`
	TaskCount    int       `json:"taskCount"`
	SubmittedAt  time.Time `json:"submittedAt"`
}

// reconcileSlurmArrays compiles complete accepted collection groups before
// ordinary per-execution reconciliation. It is safe to repeat after a crash:
// one array attempt is recorded before sbatch and every child submission
// manifest is derived from the recovered parent identity.
func (service *service) reconcileSlurmArrays(ctx context.Context, executions []SpoolExecution) error {
	groups := make(map[string][]SpoolExecution)
	for _, execution := range executions {
		binding := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray
		if binding != nil {
			groups[binding.CollectionID] = append(groups[binding.CollectionID], execution)
		}
	}
	identifiers := make([]string, 0, len(groups))
	for identifier := range groups {
		identifiers = append(identifiers, identifier)
	}
	sort.Strings(identifiers)
	for _, identifier := range identifiers {
		group := groups[identifier]
		if len(group) == 0 {
			continue
		}
		expected := group[0].Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray.TaskCount
		if len(group) < expected {
			continue
		}
		if len(group) != expected {
			return errors.New("reconcile Slurm array: collection task count changed")
		}
		sort.Slice(group, func(left, right int) bool {
			return group[left].Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray.TaskIndex <
				group[right].Assignment.Document.Spec.EffectiveExecution.Metadata.SlurmArray.TaskIndex
		})
		if err := service.ensureSlurmArraySubmission(ctx, identifier, group); err != nil {
			return err
		}
	}

	return nil
}

//nolint:cyclop,gocognit // Array compilation validates every child before the single submit boundary.
func (service *service) ensureSlurmArraySubmission(
	ctx context.Context,
	collectionID string,
	executions []SpoolExecution,
) error {
	if service.slurm == nil {
		return errors.New("reconcile Slurm array: backend is not configured")
	}
	localDirectory, err := prepareArrayDirectory(service.stateDirectory, collectionID)
	if err != nil {
		return fmt.Errorf("reconcile Slurm array: prepare local directory: %w", err)
	}
	sharedDirectory, err := prepareArrayDirectory(service.slurmRoot, collectionID)
	if err != nil {
		return fmt.Errorf("reconcile Slurm array: prepare shared directory: %w", err)
	}
	manifest := SlurmArrayManifest{
		APIVersion: controlAPIVersion, Kind: "SlurmArrayManifest",
		Tasks: make([]SlurmArrayManifestTask, len(executions)),
	}
	var resourcesJSON []byte
	var partition string
	for index, execution := range executions {
		effective := execution.Assignment.Document.Spec.EffectiveExecution
		binding := effective.Metadata.SlurmArray
		if binding == nil || binding.CollectionID != collectionID || binding.TaskIndex != index ||
			binding.TaskCount != len(executions) || binding.MaxParallel < 1 ||
			effective.Spec.Placement.ExecutionBackend != executionBackendSlurm {
			return errors.New("reconcile Slurm array: inconsistent task binding")
		}
		if index == 0 {
			manifest.MaxActive = binding.MaxParallel
			partition = effective.Spec.Placement.Partition
		} else if binding.MaxParallel != manifest.MaxActive || effective.Spec.Placement.Partition != partition {
			return errors.New("reconcile Slurm array: tasks have incompatible scheduler policy")
		}
		encodedResources, encodeErr := json.Marshal(effective.Spec.Workload.Document.Spec.Resources)
		if encodeErr != nil {
			return fmt.Errorf("reconcile Slurm array: encode resources: %w", encodeErr)
		}
		if index == 0 {
			resourcesJSON = encodedResources
		} else if !bytes.Equal(encodedResources, resourcesJSON) {
			return errors.New("reconcile Slurm array: tasks have incompatible resources")
		}
		executionID := effective.Metadata.ExecutionID
		if _, prepareErr := prepareExecutionFiles(
			service.stateDirectory, execution.Assignment, *execution.Authorization,
		); prepareErr != nil {
			return prepareErr
		}
		if _, prepareErr := prepareSlurmBundle(
			service.slurmRoot, execution.Assignment, *execution.Authorization,
		); prepareErr != nil {
			return prepareErr
		}
		manifest.Tasks[index] = SlurmArrayManifestTask{
			Index: index, ExecutionID: executionID, StateDirectory: service.slurmRoot,
		}
	}
	manifestPath := filepath.Join(sharedDirectory, slurmArrayManifestFilename)
	if err = writeImmutableArrayManifest(manifestPath, manifest); err != nil {
		return fmt.Errorf("reconcile Slurm array: write manifest: %w", err)
	}
	if err = writeImmutablePrivateFile(
		filepath.Join(sharedDirectory, slurmScriptFilename), []byte(slurmBatchScript),
	); err != nil {
		return fmt.Errorf("reconcile Slurm array: write script: %w", err)
	}

	submission, err := readSlurmArraySubmission(localDirectory, collectionID, len(executions))
	if errors.Is(err, fs.ErrNotExist) {
		submission, err = service.submitOrRecoverSlurmArray(
			ctx, localDirectory, sharedDirectory, manifestPath, collectionID, executions,
		)
	}
	if err != nil {
		return err
	}
	if submission.JobID == "" {
		return nil
	}
	for index, execution := range executions {
		executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
		taskID, taskErr := slurmbackend.ArrayTaskID(submission.JobID, index)
		if taskErr != nil {
			return taskErr
		}
		directory, directoryErr := executionDirectory(service.stateDirectory, executionID)
		if directoryErr != nil {
			return directoryErr
		}
		if _, persistErr := persistSlurmSubmission(directory, executionID, slurmbackend.Submission{
			JobID: taskID, Cluster: submission.Cluster,
		}, submission.SubmittedAt); persistErr != nil {
			return persistErr
		}
	}

	return nil
}

//nolint:gocognit // The attempt-before-submit and ambiguity-recovery boundary is intentionally explicit.
func (service *service) submitOrRecoverSlurmArray(
	ctx context.Context,
	localDirectory, sharedDirectory, manifestPath, collectionID string,
	executions []SpoolExecution,
) (slurmArraySubmissionManifest, error) {
	attempt, err := readSlurmArrayAttempt(localDirectory, collectionID, len(executions))
	if errors.Is(err, fs.ErrNotExist) {
		attempt = slurmArrayAttemptManifest{
			CollectionID: collectionID, JobName: "jobman-array-" + collectionID,
			ExecutionUser: service.executionUser, TaskCount: len(executions), AttemptedAt: time.Now().UTC(),
		}
		if err = writeManifest(filepath.Join(localDirectory, slurmArrayAttemptFilename), attempt); err != nil {
			return slurmArraySubmissionManifest{}, fmt.Errorf("record Slurm array attempt: %w", err)
		}
		first := executions[0].Assignment.Document.Spec.EffectiveExecution
		arguments := []string{
			"run-array-task", "--manifest", manifestPath,
			maximumLogBytesFlag, strconv.FormatInt(service.maximumLogBytes, 10),
		}
		arguments = appendArtifactRunnerArguments(arguments, service.artifactStore, service.maximumArtifactBytes)
		arguments = appendContainerRunnerArguments(arguments, service.container)
		native, submitErr := service.slurm.SubmitArray(ctx, slurmbackend.ArraySubmitRequest{
			SubmitRequest: slurmbackend.SubmitRequest{
				JobName: attempt.JobName, Partition: first.Spec.Placement.Partition,
				ScriptPath: filepath.Join(sharedDirectory, slurmScriptFilename),
				StdoutPath: filepath.Join(sharedDirectory, "slurm-%A_%a.stdout.log"),
				StderrPath: filepath.Join(sharedDirectory, "slurm-%A_%a.stderr.log"),
				RunnerPath: service.slurmRunner, RunnerArgs: arguments,
				Resources: first.Spec.Workload.Document.Spec.Resources,
			},
			TaskCount: len(executions), MaxParallel: first.Metadata.SlurmArray.MaxParallel,
		})
		if submitErr != nil {
			for _, execution := range executions {
				executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
				if queueErr := service.queueSchedulerEvent(
					ctx, executionID, "scheduler.uncertain", time.Now().UTC(), "", "uncertain",
					"array submission outcome is uncertain", "",
				); queueErr != nil {
					return slurmArraySubmissionManifest{}, errors.Join(submitErr, queueErr)
				}
			}

			return slurmArraySubmissionManifest{}, nil
		}

		return persistSlurmArraySubmission(localDirectory, collectionID, len(executions), native, time.Now().UTC())
	}
	if err != nil {
		return slurmArraySubmissionManifest{}, err
	}
	native, findErr := service.slurm.FindByName(
		ctx, attempt.JobName, attempt.ExecutionUser, attempt.AttemptedAt.Add(-time.Minute),
	)
	if findErr != nil {
		for _, execution := range executions {
			executionID := execution.Assignment.Document.Spec.EffectiveExecution.Metadata.ExecutionID
			if queueErr := service.queueSchedulerEvent(
				ctx, executionID, "scheduler.uncertain", time.Now().UTC(), "", "uncertain",
				"array submission identity has not been reconciled", "",
			); queueErr != nil {
				return slurmArraySubmissionManifest{}, queueErr
			}
		}
		if service.logger != nil {
			service.logger.WarnContext(ctx, "Slurm array submission remains uncertain", "collection-id", collectionID)
		}

		return slurmArraySubmissionManifest{}, nil
	}

	return persistSlurmArraySubmission(localDirectory, collectionID, len(executions), native, time.Now().UTC())
}

func prepareArrayDirectory(root, collectionID string) (string, error) {
	if !validUUID(collectionID) {
		return "", errors.New("invalid collection ID")
	}
	directory := filepath.Join(root, slurmArraysDirectoryName, collectionID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	information, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	if !information.IsDir() || information.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("array directory must be a real directory")
	}
	if chmodErr := os.Chmod(directory, 0o700); chmodErr != nil { // #nosec G302 -- owner traversal is required.
		return "", chmodErr
	}

	return directory, nil
}

func writeImmutableArrayManifest(path string, manifest SlurmArrayManifest) error {
	if err := validateSlurmArrayManifest(manifest); err != nil {
		return err
	}
	contents, err := marshalStrictJSON(manifest)
	if err != nil {
		return err
	}

	return writeImmutablePrivateFile(path, contents)
}

func persistSlurmArraySubmission(
	directory, collectionID string,
	taskCount int,
	submission slurmbackend.Submission,
	submittedAt time.Time,
) (slurmArraySubmissionManifest, error) {
	manifest := slurmArraySubmissionManifest{
		CollectionID: collectionID, JobID: submission.JobID, Cluster: submission.Cluster,
		TaskCount: taskCount, SubmittedAt: submittedAt,
	}
	if err := writeManifest(filepath.Join(directory, slurmArraySubmitFilename), manifest); err != nil {
		return slurmArraySubmissionManifest{}, fmt.Errorf("record Slurm array submission: %w", err)
	}

	return manifest, nil
}

func readSlurmArrayAttempt(
	directory, collectionID string,
	taskCount int,
) (slurmArrayAttemptManifest, error) {
	var manifest slurmArrayAttemptManifest
	if err := readManifest(filepath.Join(directory, slurmArrayAttemptFilename), &manifest); err != nil {
		return manifest, err
	}
	if manifest.CollectionID != collectionID || manifest.JobName == "" || manifest.ExecutionUser == "" ||
		manifest.TaskCount != taskCount || manifest.AttemptedAt.IsZero() {
		return manifest, errors.New("invalid Slurm array attempt manifest")
	}

	return manifest, nil
}

func readSlurmArraySubmission(
	directory, collectionID string,
	taskCount int,
) (slurmArraySubmissionManifest, error) {
	var manifest slurmArraySubmissionManifest
	if err := readManifest(filepath.Join(directory, slurmArraySubmitFilename), &manifest); err != nil {
		return manifest, err
	}
	if manifest.CollectionID != collectionID || manifest.JobID == "" ||
		manifest.TaskCount != taskCount || manifest.SubmittedAt.IsZero() {
		return manifest, errors.New("invalid Slurm array submission manifest")
	}

	return manifest, nil
}
