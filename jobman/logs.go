package jobman

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
)

const allRunsSelection = "all"

//nolint:gocognit,cyclop // Log presentation combines selection, following, retained-run iteration, and tailing modes.
func newLogsCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	stream := string(app.LogBoth)
	var follow bool
	var runSelection string
	var allRuns bool
	var lines int64
	var raw bool
	command := &cobra.Command{
		Use:   "logs JOB",
		Short: "Read output captured for a job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			selected := app.LogStream(stream)
			if selected != app.LogStdout && selected != app.LogStderr && selected != app.LogBoth {
				return usageError(fmt.Errorf("invalid --stream %q: expected stdout, stderr, or both", stream))
			}

			if runSelection == allRunsSelection {
				allRuns = true
			}
			if follow && allRuns {
				return usageError(errors.New("--follow and --all cannot be used together"))
			}
			if runSelection != "" && runSelection != allRunsSelection && allRuns {
				return usageError(errors.New("a numbered --run and --all cannot be used together"))
			}
			if lines < -1 {
				return usageError(errors.New("--lines must be -1 or a nonnegative integer"))
			}

			return withBackend(command, dependencies, root, func(backend app.Backend) error {
				extended, extendedOK := backend.(app.FollowBackend)
				var runNumber uint64
				if runSelection != "" && runSelection != allRunsSelection {
					details, inspectErr := backend.Inspect(command.Context(), arguments[0])
					if inspectErr != nil {
						return inspectErr
					}
					resolved, resolveErr := resolveRunSelection(runSelection, details)
					if resolveErr != nil {
						return resolveErr
					}
					runNumber = resolved
				}
				if follow {
					if !extendedOK {
						return errors.New("application backend does not support log following")
					}

					return extended.FollowLogs(
						command.Context(), arguments[0], selected, runNumber, command.OutOrStdout(),
					)
				}
				var content []byte
				var err error
				switch {
				case allRuns:
					if !extendedOK {
						return errors.New("application backend does not support run selection")
					}
					details, inspectErr := backend.Inspect(command.Context(), arguments[0])
					if inspectErr != nil {
						return inspectErr
					}
					var combined bytes.Buffer
					for _, run := range details.Runs {
						part, readErr := extended.ReadRunLogs(
							command.Context(), arguments[0], selected, run.Number,
						)
						if readErr != nil {
							return readErr
						}
						if !raw {
							_, _ = fmt.Fprintf(&combined, "==> run %d <==\n", run.Number)
						}
						_, _ = combined.Write(part)
					}
					content = combined.Bytes()
				case runNumber != 0:
					if !extendedOK {
						return errors.New("application backend does not support run selection")
					}
					content, err = extended.ReadRunLogs(command.Context(), arguments[0], selected, runNumber)
				default:
					content, err = backend.ReadLogs(command.Context(), arguments[0], selected)
				}
				if err != nil {
					return err
				}
				if lines >= 0 {
					content = lastLines(content, uint64(lines))
				}
				if _, err := command.OutOrStdout().Write(content); err != nil {
					return fmt.Errorf("write job logs: %w", err)
				}

				return nil
			})
		},
	}
	command.Flags().StringVar(&stream, "stream", stream, "select stdout, stderr, or both")
	command.Flags().BoolVarP(&follow, "follow", "f", false, "follow output until the selected run completes")
	command.Flags().StringVar(&runSelection, "run", "", "select a run number, negative index, or all")
	command.Flags().BoolVar(&allRuns, "all", false, "read every retained run")
	command.Flags().Int64VarP(&lines, "lines", "n", -1, "show the last N lines (-1 means all)")
	command.Flags().BoolVar(&raw, "raw", false, "omit run presentation headers")
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}

func lastLines(content []byte, count uint64) []byte {
	if len(content) == 0 {
		return content
	}
	if count == 0 {
		return content[:0]
	}
	end := len(content)
	if content[end-1] == '\n' {
		end--
	}
	position := end
	for count > 0 && position > 0 {
		index := bytes.LastIndexByte(content[:position], '\n')
		if index < 0 {
			position = 0
			break
		}
		position = index
		count--
	}
	if position > 0 {
		position++
	}

	return content[position:]
}

func resolveRunSelection(selection string, details app.JobDetails) (uint64, error) {
	value, err := strconv.ParseInt(selection, 10, 64)
	if err != nil || value == 0 {
		return 0, usageError(
			fmt.Errorf("invalid --run %q: expected a nonzero integer or %s", selection, allRunsSelection),
		)
	}
	if value > 0 {
		for _, run := range details.Runs {
			if run.Number == uint64(value) {
				return run.Number, nil
			}
		}
		return 0, fmt.Errorf("logs run %d: %w", value, app.ErrNotFound)
	}
	index := int64(len(details.Runs)) + value
	if index < 0 || index >= int64(len(details.Runs)) {
		return 0, fmt.Errorf("logs run index %d: %w", value, app.ErrNotFound)
	}

	return details.Runs[index].Number, nil
}
