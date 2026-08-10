package jobman

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
)

func newCancelCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	runCommand := &cobra.Command{
		Use:   "run JOB RUN",
		Short: "Cancel the selected active run and its job",
		Long: "Cancel the selected active run. The v1 contract also cancels the owning job, " +
			"so no subsequent retry is started.",
		Args: usageArgs(exactRunSelectorArgs),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withBackend(command, dependencies, root, func(backend app.Backend) error {
				details, err := backend.Inspect(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				run, err := selectRun(details.Runs, arguments[1])
				if err != nil {
					return err
				}
				if details.Job.ActiveRunID != run.ID {
					return fmt.Errorf("cancel run %d: run is not active: %w", run.Number, app.ErrConflict)
				}

				return cancelWithBackend(command, backend, arguments[0])
			})
		},
	}
	runCommand.Flags().SetInterspersed(false)
	runCommand.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)
	jobCommand := &cobra.Command{
		Use:   "job JOB",
		Short: "Cancel a managed job and prevent future runs",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return cancelJob(command, dependencies, root, arguments[0])
		},
	}
	jobCommand.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)
	command := &cobra.Command{
		Use:   "cancel JOB",
		Short: "Cancel a managed job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return cancelJob(command, dependencies, root, arguments[0])
		},
	}
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)
	command.AddCommand(
		jobCommand,
		runCommand,
	)

	return command
}

func cancelJob(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	selector string,
) error {
	return withBackend(command, dependencies, root, func(backend app.Backend) error {
		return cancelWithBackend(command, backend, selector)
	})
}

func cancelWithBackend(command *cobra.Command, backend app.Backend, selector string) error {
	job, err := backend.Cancel(command.Context(), selector)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(command.OutOrStdout(), "%s\t%s\n", job.ID, job.Phase); err != nil {
		return fmt.Errorf("write cancellation result: %w", err)
	}

	return nil
}
