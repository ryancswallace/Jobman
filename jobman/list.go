package jobman

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/store"
)

const listCommandName = "list"

type listCommandOptions struct {
	jsonOutput      bool
	all             bool
	active          bool
	completed       bool
	showRuns        bool
	limit           int
	phase           string
	outcome         string
	name            string
	group           string
	submittedAfter  string
	submittedBefore string
}

func newListCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	options := &listCommandOptions{}
	command := &cobra.Command{
		Use:   listCommandName,
		Short: "List known jobs",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			return runListCommand(command, dependencies, root, options)
		},
	}
	command.Flags().BoolVar(&options.jsonOutput, "json", false, "emit versioned JSON")
	command.Flags().BoolVar(&options.all, "all", false, "list up to the store maximum of 1000 jobs")
	command.Flags().BoolVar(&options.active, "active", false, "include only nonterminal jobs")
	command.Flags().BoolVar(&options.completed, "completed", false, "include only completed jobs")
	command.Flags().IntVar(&options.limit, "limit", store.DefaultListLimit, "maximum number of jobs to return")
	command.Flags().StringVar(&options.phase, "phase", "", "include only jobs in this phase")
	command.Flags().StringVar(&options.outcome, "outcome", "", "include only jobs with this terminal outcome")
	command.Flags().StringVar(&options.name, "name", "", "include only jobs with this exact name")
	command.Flags().StringVar(&options.group, "group", "", "include only jobs in this group")
	command.Flags().StringVar(&options.submittedAfter, "submitted-after", "", "include jobs submitted after this RFC3339 time")
	command.Flags().StringVar(&options.submittedBefore, "submitted-before", "", "include jobs submitted before this RFC3339 time")
	command.Flags().BoolVar(&options.showRuns, "show-runs", false, "include ordered run summaries")

	return command
}

func runListCommand(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	options *listCommandOptions,
) error {
	request, err := options.request(command)
	if err != nil {
		return usageError(err)
	}
	return withBackend(command, dependencies, root, func(backend app.Backend) error {
		listed, listErr := queryListedJobs(command, backend, request)
		if listErr != nil {
			return listErr
		}

		return writeJobList(command, listed, options.jsonOutput, options.showRuns)
	})
}

func (options *listCommandOptions) request(command *cobra.Command) (app.ListRequest, error) {
	if options.active && options.completed {
		return app.ListRequest{}, errors.New("--active and --completed are mutually exclusive")
	}
	if options.all && command.Flags().Changed("limit") {
		return app.ListRequest{}, errors.New("--all and --limit are mutually exclusive")
	}
	if options.all {
		options.limit = store.MaximumListLimit
	}
	if options.limit < 1 || options.limit > store.MaximumListLimit {
		return app.ListRequest{}, fmt.Errorf("--limit must be between 1 and %d", store.MaximumListLimit)
	}
	if options.phase != "" && !model.JobPhase(options.phase).Valid() {
		return app.ListRequest{}, fmt.Errorf("invalid --phase %q", options.phase)
	}
	if options.outcome != "" && !model.JobOutcome(options.outcome).Valid() {
		return app.ListRequest{}, fmt.Errorf("invalid --outcome %q", options.outcome)
	}
	after, err := parseListTime("--submitted-after", options.submittedAfter)
	if err != nil {
		return app.ListRequest{}, err
	}
	before, err := parseListTime("--submitted-before", options.submittedBefore)
	if err != nil {
		return app.ListRequest{}, err
	}

	return app.ListRequest{
		Phase: model.JobPhase(options.phase), Outcome: model.JobOutcome(options.outcome),
		Name: options.name, Group: options.group, SubmittedAfter: after, SubmittedBefore: before,
		Active: options.active, Completed: options.completed, Limit: options.limit, ShowRuns: options.showRuns,
	}, nil
}

func queryListedJobs(command *cobra.Command, backend app.Backend, request app.ListRequest) ([]app.ListedJob, error) {
	if query, ok := backend.(app.QueryBackend); ok {
		return query.ListJobs(command.Context(), request)
	}
	jobs, err := backend.List(command.Context())
	if err != nil {
		return nil, err
	}
	listed := make([]app.ListedJob, 0, len(jobs))
	for _, job := range jobs {
		listed = append(listed, app.ListedJob{Job: job})
	}

	return listed, nil
}

func writeJobList(command *cobra.Command, jobs []app.ListedJob, jsonOutput, showRuns bool) error {
	if jsonOutput {
		if !showRuns {
			values := make([]model.JobState, 0, len(jobs))
			for _, listed := range jobs {
				values = append(values, listed.Job)
			}

			return writeJSON(command, struct {
				Jobs []jobSummary `json:"jobs"`
			}{Jobs: summaries(values)})
		}
		return writeJSON(command, struct {
			Jobs []listedJobDetail `json:"jobs"`
		}{Jobs: presentListedJobs(jobs, showRuns)})
	}

	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "ID\tNAME\tPHASE\tOUTCOME\tSUBMITTED"); err != nil {
		return fmt.Errorf("write job list header: %w", err)
	}
	for _, listed := range jobs {
		job := listed.Job
		if _, err := fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%s\t%s\n",
			job.ID,
			redactField(command, "name", job.Spec.Name()),
			job.Phase,
			job.Outcome,
			job.SubmittedAt.UTC().Format(timeFormat),
		); err != nil {
			return fmt.Errorf("write job list row: %w", err)
		}
		if showRuns {
			for _, run := range listed.Runs {
				if _, err := fmt.Fprintf(
					writer,
					"  run %s\t#%d\t%s\t%s\t%s\n",
					run.ID,
					run.Number,
					run.Phase,
					run.Outcome,
					strconv.FormatUint(run.Revision, 10),
				); err != nil {
					return fmt.Errorf("write run list row: %w", err)
				}
			}
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush job list: %w", err)
	}

	return nil
}

func parseListTime(flag, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 timestamp: %w", flag, err)
	}

	return parsed.UTC(), nil
}

func writeJSON(command *cobra.Command, data any) error {
	data, err := redactedJSON(command, data)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(command.OutOrStdout())
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(struct {
		SchemaVersion int `json:"schema_version"`
		Data          any `json:"data"`
	}{SchemaVersion: 1, Data: data}); err != nil {
		return fmt.Errorf("encode JSON output: %w", err)
	}

	return nil
}
