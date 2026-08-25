package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ryancswallace/jobman/internal/backend/slurm"
)

const maximumArrayManifestBytes = 2 * 1024 * 1024

// SlurmArrayManifest maps scheduler indices to existing independently
// accepted execution directories. It contains no credentials or commands.
type SlurmArrayManifest struct {
	APIVersion string                   `json:"apiVersion"`
	Kind       string                   `json:"kind"`
	Tasks      []SlurmArrayManifestTask `json:"tasks"`
	MaxActive  int                      `json:"maxActive"`
}

// SlurmArrayManifestTask binds one task index to one private execution spool.
type SlurmArrayManifestTask struct {
	Index          int    `json:"index"`
	ExecutionID    string `json:"executionId"`
	StateDirectory string `json:"stateDirectory"`
}

// WriteSlurmArrayManifest validates and durably writes one private compiler
// result before sbatch is invoked.
func WriteSlurmArrayManifest(filename string, manifest SlurmArrayManifest) error {
	if err := validateSlurmArrayManifest(manifest); err != nil {
		return err
	}
	return writeManifest(filename, manifest)
}

// RunSlurmArrayTask resolves one scheduler-provided index through a validated
// immutable manifest and invokes the ordinary single-execution runner.
func RunSlurmArrayTask(
	ctx context.Context,
	manifestPath, taskIndex string,
	options ExecutionOptions,
) error {
	if !filepath.IsAbs(manifestPath) || filepath.Clean(manifestPath) != manifestPath {
		return errors.New("run Slurm array task: manifest path must be absolute and normalized")
	}
	information, err := os.Lstat(manifestPath)
	if err != nil {
		return fmt.Errorf("run Slurm array task: inspect manifest: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode()&os.ModeSymlink != 0 ||
		information.Size() > maximumArrayManifestBytes {
		return errors.New("run Slurm array task: manifest is not a bounded regular file")
	}
	file, err := os.Open(manifestPath)
	if err != nil {
		return fmt.Errorf("run Slurm array task: open manifest: %w", err)
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, maximumArrayManifestBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("run Slurm array task: read manifest: %w", errors.Join(readErr, closeErr))
	}
	var manifest SlurmArrayManifest
	if err = decodeStrictJSON(contents, &manifest); err != nil {
		return fmt.Errorf("run Slurm array task: decode manifest: %w", err)
	}
	if validationErr := validateSlurmArrayManifest(manifest); validationErr != nil {
		return validationErr
	}
	index, err := strconv.Atoi(taskIndex)
	if err != nil || index < 0 || index >= len(manifest.Tasks) {
		return errors.New("run Slurm array task: scheduler task index is invalid")
	}
	task := manifest.Tasks[index]

	return RunExecutionWithOptions(ctx, task.StateDirectory, task.ExecutionID, options)
}

func validateSlurmArrayManifest(manifest SlurmArrayManifest) error {
	if manifest.APIVersion != controlAPIVersion || manifest.Kind != "SlurmArrayManifest" {
		return errors.New("slurm array manifest version or kind is invalid")
	}
	tasks := make([]slurm.ArrayTask, len(manifest.Tasks))
	for index, task := range manifest.Tasks {
		if !filepath.IsAbs(task.StateDirectory) || filepath.Clean(task.StateDirectory) != task.StateDirectory {
			return errors.New("slurm array manifest state directory is invalid")
		}
		tasks[index] = slurm.ArrayTask{Index: task.Index, ExecutionID: task.ExecutionID}
	}
	plan, err := slurm.CompileArray(tasks, manifest.MaxActive)
	if err != nil {
		return err
	}
	for index := range plan.Tasks {
		if plan.Tasks[index] != tasks[index] {
			return errors.New("slurm array manifest tasks must already be index ordered")
		}
	}

	return nil
}
