package jobman

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/controlclient"
)

const maximumHistoryImportBytes = 2 * 1024 * 1024

func newSharedHistoryCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "history",
		Short: "Migrate quiescent completed standalone history",
		Args:  usageArgs(cobra.NoArgs),
	}
	command.AddCommand(newSharedHistoryImportCommand(dependencies, root, shared))

	return command
}

func newSharedHistoryImportCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var dryRun, jsonOutput bool
	command := &cobra.Command{
		Use:   "import FILE",
		Short: "Validate or import one completed standalone history record",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				return importCompletedHistory(command, sharedContext, arguments[0], dryRun, jsonOutput)
			})
		},
	}
	command.Flags().BoolVar(&dryRun, "dry-run", false, "validate without creating shared state")
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func importCompletedHistory(
	command *cobra.Command,
	sharedContext sharedContext,
	filename string,
	dryRun bool,
	jsonOutput bool,
) error {
	document, err := decodeCompletedHistoryFile(command, filename)
	if err != nil {
		return err
	}
	if document.Metadata.Namespace != sharedContext.namespace {
		return usageError(errors.New("history namespace does not match the selected shared profile"))
	}
	operationID, err := newSharedOperationID()
	if err != nil {
		return err
	}
	job, err := sharedContext.client.ImportCompletedHistory(
		command.Context(), document, dryRun, "history-import-"+operationID,
	)
	if err != nil {
		return err
	}
	if dryRun {
		return writeHistoryImportPlan(command, jsonOutput)
	}
	if jsonOutput {
		return writeJSON(command, job)
	}
	_, err = fmt.Fprintf(command.OutOrStdout(), "%s\t%s\t%s\n", job.Metadata.ID, job.Status.Phase, job.Status.Outcome)

	return err
}

func writeHistoryImportPlan(command *cobra.Command, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(command, map[string]any{
			"apiVersion": "jobman.control/v1alpha1", "kind": "CompletedHistoryImportPlan",
			"status": map[string]string{"result": "valid"},
		})
	}
	_, err := fmt.Fprintln(command.OutOrStdout(), "history import is valid")

	return err
}

func decodeCompletedHistoryFile(
	command *cobra.Command,
	filename string,
) (controlclient.CompletedHistoryImportRequest, error) {
	reader := command.InOrStdin()
	var file *os.File
	var err error
	if filename != "-" {
		file, err = os.Open(filename)
		if err != nil {
			return controlclient.CompletedHistoryImportRequest{}, fmt.Errorf("open completed history import: %w", err)
		}
		reader = file
	}
	encoded, readErr := io.ReadAll(io.LimitReader(reader, maximumHistoryImportBytes+1))
	if file != nil {
		readErr = errors.Join(readErr, file.Close())
	}
	if readErr != nil {
		return controlclient.CompletedHistoryImportRequest{}, fmt.Errorf("read completed history import: %w", readErr)
	}
	if len(encoded) > maximumHistoryImportBytes {
		return controlclient.CompletedHistoryImportRequest{}, errors.New("completed history import exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var document controlclient.CompletedHistoryImportRequest
	if err = decoder.Decode(&document); err != nil {
		return controlclient.CompletedHistoryImportRequest{}, fmt.Errorf("decode completed history import: %w", err)
	}
	if err = decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return controlclient.CompletedHistoryImportRequest{}, errors.New("completed history import contains trailing data")
	}

	return document, nil
}
