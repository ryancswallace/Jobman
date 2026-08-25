package jobman

import (
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/controlclient"
	"github.com/ryancswallace/jobman/protocol"
)

func newSharedGraphCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "graph",
		Short: "Submit and control immutable dependency graphs",
		Args:  usageArgs(cobra.NoArgs),
	}
	command.AddCommand(
		newSharedGraphSubmitCommand(dependencies, root, shared),
		newSharedGraphShowCommand(dependencies, root, shared),
		newSharedGraphCancelCommand(dependencies, root, shared),
	)

	return command
}

func newSharedGraphSubmitCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	return newSharedFileSubmitCommand(
		dependencies, root, shared, "submit FILE", "Submit a portable GraphRequest document",
		func(command *cobra.Command, sharedContext sharedContext, filename string, jsonOutput bool) error {
			sealed, err := decodeGraphFile(command, filename)
			if err != nil {
				return err
			}
			if sealed.Document.Metadata.Namespace != sharedContext.namespace {
				return usageError(errors.New("graph namespace does not match the selected shared profile"))
			}
			operationID, err := newSharedOperationID()
			if err != nil {
				return err
			}
			graph, err := sharedContext.client.SubmitGraph(command.Context(), sealed, "graph-"+operationID)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(command, graph)
			}

			return writeGraphSummary(command, graph)
		},
	)
}

func decodeGraphFile(command *cobra.Command, filename string) (protocol.SealedGraphRequest, error) {
	reader := command.InOrStdin()
	var file *os.File
	var err error
	if filename != "-" {
		file, err = os.Open(filename)
		if err != nil {
			return protocol.SealedGraphRequest{}, fmt.Errorf("open graph request: %w", err)
		}
		reader = file
	}
	sealed, err := protocol.DecodeGraphRequest(reader, protocol.DecodeLimits{})
	if err != nil {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		return protocol.SealedGraphRequest{}, err
	}
	if file != nil {
		if err = file.Close(); err != nil {
			return protocol.SealedGraphRequest{}, fmt.Errorf("close graph request: %w", err)
		}
	}

	return sealed, nil
}

func newSharedGraphShowCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "show GRAPH",
		Short: "Show a graph, dependencies, and node jobs",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				graph, err := sharedContext.client.GetGraph(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, graph)
				}

				return writeGraphSummary(command, graph)
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func newSharedGraphCancelCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "cancel GRAPH",
		Short: "Cancel every nonterminal node in a graph",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				operationID, err := newSharedOperationID()
				if err != nil {
					return err
				}
				graph, err := sharedContext.client.CancelGraph(command.Context(), arguments[0], "graph-cancel-"+operationID)
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, graph)
				}

				return writeGraphSummary(command, graph)
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func writeGraphSummary(command *cobra.Command, graph controlclient.Graph) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		writer, "GRAPH\t%s\t%s\t%s\t%d/%d terminal\t%s\n",
		graph.Metadata.ID, redactField(command, "name", graph.Metadata.Name),
		graph.Status.Phase, graph.Status.Terminal, graph.Status.Total, graph.Status.Outcome,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "INDEX\tNAME\tJOB\tPHASE\tOUTCOME\tDISPOSITION\tDEPENDENCIES"); err != nil {
		return err
	}
	for _, item := range graph.Items {
		dependencies := "-"
		if len(item.Dependencies) > 0 {
			dependencies = fmt.Sprintf("%d (%d satisfied)", len(item.Dependencies), satisfiedDependencies(item.Dependencies))
		}
		if _, err := fmt.Fprintf(
			writer, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", item.Index,
			redactField(command, "name", item.Name), item.Job.Metadata.ID,
			item.Job.Status.Phase, item.Job.Status.Outcome, item.Disposition, dependencies,
		); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func satisfiedDependencies(dependencies []controlclient.GraphDependency) int {
	count := 0
	for _, dependency := range dependencies {
		if dependency.Satisfied {
			count++
		}
	}

	return count
}
