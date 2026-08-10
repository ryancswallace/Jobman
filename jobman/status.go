package jobman

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
)

func newStatusCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "status JOB",
		Short: "Show concise current job status",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withBackend(command, dependencies, root, func(backend app.Backend) error {
				value, err := backend.Inspect(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, summary(value.Job))
				}
				_, err = fmt.Fprintf(
					command.OutOrStdout(),
					"%s\t%s\t%s\t%s\n",
					value.Job.ID,
					redactField(command, "name", value.Job.Spec.Name()),
					value.Job.Phase,
					value.Job.Outcome,
				)
				if err != nil {
					return fmt.Errorf("write job status: %w", err)
				}

				return nil
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}
