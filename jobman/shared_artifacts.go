package jobman

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/controlclient"
)

func newSharedArtifactsCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "artifacts JOB",
		Short: "List immutable outputs published by a shared job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(shared sharedContext) error {
				manifest, err := shared.client.GetJobArtifacts(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, manifest)
				}
				return writeSharedArtifacts(command, manifest.Items)
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit the versioned artifact manifest")
	return command
}

func writeSharedArtifacts(command *cobra.Command, artifacts []controlclient.PublishedArtifact) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "RUN\tNAME\tARTIFACT URI\tBYTES\tCHECKSUM"); err != nil {
		return fmt.Errorf("write shared artifact heading: %w", err)
	}
	for _, artifact := range artifacts {
		if _, err := fmt.Fprintf(
			writer, "%d\t%s\tartifact://%s/%s\t%d\t%s\n",
			artifact.RunNumber, artifact.Name, artifact.StoreName, artifact.ObjectKey,
			artifact.ByteLength, artifact.Checksum,
		); err != nil {
			return fmt.Errorf("write shared artifact: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush shared artifacts: %w", err)
	}
	return nil
}
