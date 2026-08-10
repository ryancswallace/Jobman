package jobman

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/config"
)

func newPauseCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	return lifecycleCommand("pause", "Pause a managed job", dependencies, root, func(
		command *cobra.Command,
		backend app.LifecycleBackend,
		selector string,
	) error {
		job, err := backend.Pause(command.Context(), selector)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "%s\t%s\n", job.ID, job.Phase)
		return err
	})
}

func newResumeCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	return lifecycleCommand("resume", "Resume a paused job", dependencies, root, func(
		command *cobra.Command,
		backend app.LifecycleBackend,
		selector string,
	) error {
		job, err := backend.Resume(command.Context(), selector)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "%s\t%s\n", job.ID, job.Phase)
		return err
	})
}

func newWaitCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	return lifecycleCommand("wait", "Wait for a job to finish", dependencies, root, func(
		command *cobra.Command,
		backend app.LifecycleBackend,
		selector string,
	) error {
		job, err := backend.Wait(command.Context(), selector)
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintf(command.OutOrStdout(), "%s\t%s\n", job.ID, job.Outcome); err != nil {
			return err
		}

		return completedJobError(job)
	})
}

func lifecycleCommand(
	use,
	short string,
	dependencies dependencies,
	root *rootOptions,
	operation func(*cobra.Command, app.LifecycleBackend, string) error,
) *cobra.Command {
	command := &cobra.Command{
		Use:   use + " JOB",
		Short: short,
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withBackend(command, dependencies, root, func(backend app.Backend) error {
				extended, ok := backend.(app.LifecycleBackend)
				if !ok {
					return errors.New("application backend does not support lifecycle controls")
				}

				return operation(command, extended, arguments[0])
			})
		},
	}
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}

func newInputCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	var sendEOF bool
	command := &cobra.Command{
		Use:   "input JOB",
		Short: "Send bytes to a live-input job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			source, err := liveInputSource(command.InOrStdin(), sendEOF)
			if err != nil {
				return err
			}
			return withBackend(command, dependencies, root, func(backend app.Backend) error {
				inputBackend, ok := backend.(app.InputBackend)
				if !ok {
					return errors.New("application backend does not support live input")
				}
				result, err := inputBackend.SendInput(command.Context(), arguments[0], source, sendEOF)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(command.OutOrStdout(), "%d\n", result.Delivered)
				return err
			})
		},
	}
	command.Flags().BoolVar(&sendEOF, "eof", false, "close the target's standard input")
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}

type inputStatReader interface {
	io.Reader
	Stat() (os.FileInfo, error)
}

func liveInputSource(source io.Reader, sendEOF bool) (io.Reader, error) {
	if !sendEOF {
		return source, nil
	}
	statReader, ok := source.(inputStatReader)
	if !ok {
		return source, nil
	}
	information, err := statReader.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect live-input source: %w", err)
	}
	if information.Mode()&os.ModeCharDevice != 0 {
		return strings.NewReader(""), nil
	}

	return source, nil
}

func newRerunCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	var name string
	var waitForCompletion bool
	command := &cobra.Command{
		Use:   "rerun JOB",
		Short: "Submit a new job from an existing specification",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withConfiguredBackend(command, dependencies, root, func(backend app.Backend, _ config.Loaded) error {
				extended, ok := backend.(app.LifecycleBackend)
				if !ok {
					return errors.New("application backend does not support rerun")
				}
				job, err := extended.Rerun(command.Context(), arguments[0], app.RerunRequest{Name: name})
				if err != nil {
					return err
				}
				if _, err = fmt.Fprintln(command.OutOrStdout(), job.ID); err != nil {
					return fmt.Errorf("write rerun job ID: %w", err)
				}
				if waitForCompletion {
					return waitForSubmittedJob(command.Context(), backend, job.ID.String())
				}

				return nil
			})
		},
	}
	command.Flags().StringVar(&name, "name", "", "override the new job's display name")
	command.Flags().BoolVar(&waitForCompletion, "wait", false, "wait for the terminal job outcome")
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}

func newCleanCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	var olderThan time.Duration
	var all bool
	var dryRun bool
	var force bool
	command := &cobra.Command{
		Use:   "clean [JOB]",
		Short: "Safely remove completed logs and eligible metadata",
		Args:  usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			request, err := cleanRequest(command, arguments, olderThan, all, dryRun, force)
			if err != nil {
				return err
			}
			clean := func(backend app.Backend) error {
				cleaner, ok := backend.(app.CleanupBackend)
				if !ok {
					return errors.New("application backend does not support cleanup")
				}
				result, err := cleaner.Clean(command.Context(), request)
				if err != nil {
					return err
				}
				mode := "removed"
				if request.DryRun {
					mode = "would remove"
				}
				suffix := ""
				if !force {
					suffix = "; run with `--force` to remove"
				}
				_, err = fmt.Fprintf(
					command.OutOrStdout(),
					"%s %d runs, %d files, %d bytes, %d jobs%s\n",
					mode,
					result.Runs,
					result.Files,
					result.Bytes,
					result.Jobs,
					suffix,
				)
				return err
			}
			if request.UsePolicy {
				return withConfiguredBackend(command, dependencies, root, func(backend app.Backend, _ config.Loaded) error {
					return clean(backend)
				})
			}

			return withBackend(command, dependencies, root, clean)
		},
	}
	durationFlag(command.Flags(), &olderThan, "older-than", "select logs completed at least this long ago")
	command.Flags().BoolVar(&all, "all", false, "select every completed job and its logs")
	command.Flags().BoolVar(&dryRun, "dry-run", true, "report eligible logs without removing them")
	command.Flags().BoolVar(&force, "force", false, "apply cleanup instead of the default dry run")
	command.ValidArgsFunction = jobSelectorArgumentCompletion(dependencies, root)

	return command
}

func cleanRequest(
	command *cobra.Command,
	arguments []string,
	olderThan time.Duration,
	all bool,
	dryRun bool,
	force bool,
) (app.CleanRequest, error) {
	effectiveDryRun := dryRun
	if force && !command.Flags().Changed("dry-run") {
		effectiveDryRun = false
	}
	if !effectiveDryRun && !force {
		return app.CleanRequest{}, usageError(errors.New("destructive cleanup requires --force or use --dry-run"))
	}
	if all && len(arguments) != 0 {
		return app.CleanRequest{}, usageError(errors.New("--all cannot be combined with a job selector"))
	}
	if all && command.Flags().Changed("older-than") {
		return app.CleanRequest{}, usageError(errors.New("--all cannot be combined with --older-than"))
	}
	selector := ""
	if len(arguments) == 1 {
		selector = arguments[0]
	}

	return app.CleanRequest{
		Selector: selector, OlderThan: olderThan, DryRun: effectiveDryRun,
		UsePolicy: selector == "" && !all && !command.Flags().Changed("older-than"), All: all,
	}, nil
}
