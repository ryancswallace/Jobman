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

func newSharedCollectionCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "collection",
		Short: "Submit and inspect portable job collections",
		Args:  usageArgs(cobra.NoArgs),
	}
	command.AddCommand(
		newSharedCollectionSubmitCommand(dependencies, root, shared),
		newSharedCollectionShowCommand(dependencies, root, shared),
	)

	return command
}

func newSharedCollectionSubmitCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	return newSharedFileSubmitCommand(
		dependencies, root, shared, "submit FILE", "Submit a portable CollectionRequest document",
		func(command *cobra.Command, sharedContext sharedContext, filename string, jsonOutput bool) error {
			sealed, err := decodeCollectionFile(command, filename)
			if err != nil {
				return err
			}
			if sealed.Document.Metadata.Namespace != sharedContext.namespace {
				return usageError(errors.New("collection namespace does not match the selected shared profile"))
			}
			operationID, err := newSharedOperationID()
			if err != nil {
				return err
			}
			collection, err := sharedContext.client.SubmitCollection(
				command.Context(), sealed, "collection-"+operationID,
			)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(command, collection)
			}

			return writeCollectionSummary(command, collection)
		},
	)
}

func decodeCollectionFile(
	command *cobra.Command,
	filename string,
) (protocol.SealedCollectionRequest, error) {
	reader := command.InOrStdin()
	var file *os.File
	var err error
	if filename != "-" {
		file, err = os.Open(filename)
		if err != nil {
			return protocol.SealedCollectionRequest{}, fmt.Errorf("open collection request: %w", err)
		}
		reader = file
	}
	sealed, err := protocol.DecodeCollectionRequest(reader, protocol.DecodeLimits{})
	if err != nil {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		return protocol.SealedCollectionRequest{}, err
	}
	if file != nil {
		if err = file.Close(); err != nil {
			return protocol.SealedCollectionRequest{}, fmt.Errorf("close collection request: %w", err)
		}
	}

	return sealed, nil
}

func newSharedCollectionShowCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "show COLLECTION",
		Short: "Show a collection and its independent child jobs",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				collection, err := sharedContext.client.GetCollection(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, collection)
				}

				return writeCollectionSummary(command, collection)
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func writeCollectionSummary(command *cobra.Command, collection controlclient.Collection) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		writer, "COLLECTION\t%s\t%s\t%s\t%d/%d terminal\t%s\t%s\n",
		collection.Metadata.ID, redactField(command, "name", collection.Metadata.Name),
		collection.Status.Phase, collection.Status.Terminal, collection.Status.Total,
		collection.Status.Outcome, collection.Status.ArrayMode,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "INDEX\tNAME\tJOB\tPHASE\tOUTCOME"); err != nil {
		return err
	}
	for _, item := range collection.Items {
		if _, err := fmt.Fprintf(
			writer, "%d\t%s\t%s\t%s\t%s\n", item.Index,
			redactField(command, "name", item.Name), item.Job.Metadata.ID,
			item.Job.Status.Phase, item.Job.Status.Outcome,
		); err != nil {
			return err
		}
	}

	return writer.Flush()
}
