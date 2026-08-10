package jobman

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/store"
)

func newShowCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	var jsonOutput bool
	evidenceCommand := newShowEvidenceCommand(dependencies, root, &jsonOutput)
	runCommand := &cobra.Command{
		Use:   "run JOB RUN",
		Short: "Show one run by number or negative index",
		Args:  usageArgs(exactRunSelectorArgs),
		RunE: func(command *cobra.Command, arguments []string) error {
			return showRun(command, dependencies, root, arguments[0], arguments[1], jsonOutput)
		},
	}
	runCommand.Flags().SetInterspersed(false)
	runCommand.ValidArgsFunction = jobIDArgumentCompletion(dependencies, root)
	jobCommand := &cobra.Command{
		Use:   "job JOB",
		Short: "Show a job and its run history",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return showJob(command, dependencies, root, arguments[0], jsonOutput)
		},
	}
	jobCommand.ValidArgsFunction = jobIDArgumentCompletion(dependencies, root)
	command := &cobra.Command{
		Use:   "show JOB",
		Short: "Show a job and its run history",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return showJob(command, dependencies, root, arguments[0], jsonOutput)
		},
	}
	command.PersistentFlags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")
	command.ValidArgsFunction = jobIDArgumentCompletion(dependencies, root)
	command.AddCommand(
		evidenceCommand,
		jobCommand,
		runCommand,
	)

	return command
}

func newShowEvidenceCommand(
	dependencies dependencies,
	root *rootOptions,
	jsonOutput *bool,
) *cobra.Command {
	request := diagnostic.EvidenceRequest{Logs: diagnostic.LogsMetadata}
	command := &cobra.Command{
		Use:   "evidence JOB",
		Short: "Collect bounded diagnostic evidence for a job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			request.Selector = arguments[0]
			if flagChanged(command, "run") && request.Run == 0 {
				return usageError(errors.New("--run must be a nonzero run number or negative index"))
			}
			return showEvidence(command, dependencies, root, request, *jsonOutput)
		},
	}
	command.Flags().Int64Var(&request.Run, "run", 0, "select a run number or negative index")
	command.Flags().BoolVar(&request.AllRuns, "all-runs", false, "select the bounded run history")
	command.Flags().BoolVar(
		&request.IncludeCommand,
		"command",
		false,
		"include direct executable identities and ordered arguments",
	)
	command.Flags().BoolVar(&request.IncludePaths, "paths", false,
		"include working directories, configured paths, and resolved executables")
	command.Flags().BoolVar(&request.IncludeEnvironmentNames, "environment-names", false,
		"include environment variable names and roles, never values")
	command.Flags().BoolVar(&request.IncludeSystem, "system", false,
		"include bounded point-in-time filesystem and cgroup constraints")
	command.Flags().Var(newEvidenceLogModeValue(&request.Logs), "logs", "collect logs as metadata, tail, or none")
	byteSizeFlag(command.Flags(), &request.LogBytes, "log-bytes", "maximum bytes per selected log stream")
	command.Flags().Uint64Var(&request.Similar, "similar", 0, "request up to N same-fingerprint histories")
	command.ValidArgsFunction = jobIDArgumentCompletion(dependencies, root)

	return command
}

type evidenceLogModeValue diagnostic.LogMode

func newEvidenceLogModeValue(target *diagnostic.LogMode) *evidenceLogModeValue {
	return (*evidenceLogModeValue)(target)
}

func (value *evidenceLogModeValue) Set(encoded string) error {
	mode := diagnostic.LogMode(strings.ToLower(strings.TrimSpace(encoded)))
	switch mode {
	case diagnostic.LogsMetadata, diagnostic.LogsTail, diagnostic.LogsNone:
		*value = evidenceLogModeValue(mode)
		return nil
	default:
		return errors.New("must be metadata, tail, or none")
	}
}

func (value *evidenceLogModeValue) String() string {
	if value == nil {
		return ""
	}

	return string(*value)
}

func (*evidenceLogModeValue) Type() string { return "log-mode" }

func showEvidence(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	request diagnostic.EvidenceRequest,
	jsonOutput bool,
) error {
	return withLoadedBackend(command, dependencies, root, func(backend app.Backend, _ config.Loaded) error {
		diagnostics, ok := backend.(app.DiagnosticBackend)
		if !ok {
			return errors.New("application backend does not support diagnostic evidence")
		}
		evidence, err := diagnostics.DiagnosticEvidence(
			command.Context(), request, commandEvidenceSanitizer{redactor: commandRedactor(command)},
		)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeEvidenceJSON(command, evidence)
		}

		return writeEvidenceSummary(command, evidence)
	})
}

type commandEvidenceSanitizer struct{ redactor *config.Redactor }

func (sanitizer commandEvidenceSanitizer) Sanitize(field string, value []byte) ([]byte, bool) {
	redacted := []byte(sanitizer.redactor.RedactField(field, string(value)))

	return redacted, !bytes.Equal(redacted, value)
}

func (sanitizer commandEvidenceSanitizer) ValueRedactionConfigured() bool {
	return sanitizer.redactor.ProtectsValues()
}

func writeEvidenceJSON(command *cobra.Command, evidence diagnostic.Evidence) error {
	if err := diagnostic.Verify(evidence); err != nil {
		return fmt.Errorf("verify diagnostic evidence: %w", err)
	}
	encoder := json.NewEncoder(command.OutOrStdout())
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(struct {
		SchemaVersion int `json:"schema_version"`
		Data          struct {
			Evidence diagnostic.Evidence `json:"evidence"`
		} `json:"data"`
	}{SchemaVersion: 1, Data: struct {
		Evidence diagnostic.Evidence `json:"evidence"`
	}{Evidence: evidence}}); err != nil {
		return fmt.Errorf("encode diagnostic evidence: %w", err)
	}

	return nil
}

func writeEvidenceSummary(command *cobra.Command, evidence diagnostic.Evidence) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	selected := make([]string, 0, len(evidence.Subject.SelectedRuns))
	for _, run := range evidence.Subject.SelectedRuns {
		selected = append(selected, strconv.FormatUint(run, 10))
	}
	if len(selected) == 0 {
		selected = append(selected, "none")
	}
	fields := [][2]string{
		{"Evidence ID", evidence.EvidenceID},
		{"Evidence schema", strconv.Itoa(evidence.SchemaVersion)},
		{"Job", evidence.Subject.JobID},
		{"State", strings.TrimSpace(evidence.Subject.Phase + " " + evidence.Subject.Outcome)},
		{"Selected runs", strings.Join(selected, ", ")},
		{"Facts", strconv.FormatUint(evidence.Limits.ItemCount, 10)},
		{"Artifacts", strconv.FormatUint(evidence.Limits.ArtifactCount, 10)},
		{"Log bytes", strconv.FormatUint(evidence.Limits.LogBytes, 10)},
		{"Omissions", strconv.Itoa(len(evidence.Omissions))},
		{"Redactions", strconv.Itoa(len(evidence.RedactionNotices))},
		{"Artifact consistency", string(evidence.Consistency.Artifacts)},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(writer, "%s:\t%s\n", field[0], field[1]); err != nil {
			return fmt.Errorf("write diagnostic evidence summary: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush diagnostic evidence summary: %w", err)
	}

	return nil
}

func exactRunSelectorArgs(command *cobra.Command, arguments []string) error {
	if len(arguments) <= 2 {
		return cobra.ExactArgs(2)(command, arguments)
	}
	if err := command.ParseFlags(arguments[2:]); err != nil {
		return err
	}
	if remaining := command.Flags().Args(); len(remaining) != 0 {
		return fmt.Errorf("accepts 2 arg(s), received %d", 2+len(remaining))
	}

	return nil
}

func showJob(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	selector string,
	jsonOutput bool,
) error {
	return withBackend(command, dependencies, root, func(backend app.Backend) error {
		value, err := backend.Inspect(command.Context(), selector)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(command, detail(value))
		}

		return writeJobDetails(command, value)
	})
}

func showRun(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	selector,
	runSelector string,
	jsonOutput bool,
) error {
	return withBackend(command, dependencies, root, func(backend app.Backend) error {
		value, err := backend.Inspect(command.Context(), selector)
		if err != nil {
			return err
		}
		run, err := selectRun(value.Runs, runSelector)
		if err != nil {
			return err
		}
		presented := presentRuns([]model.RunState{run})[0]
		if jsonOutput {
			return writeJSON(command, presented)
		}

		return writeRunDetails(command, run)
	})
}

func selectRun(runs []model.RunState, selector string) (model.RunState, error) {
	value, err := strconv.ParseInt(selector, 10, 64)
	if err != nil || value == 0 {
		return model.RunState{}, usageError(errors.New("RUN must be a nonzero run number or negative index"))
	}
	if value < 0 {
		index := int64(len(runs)) + value
		if index < 0 || index >= int64(len(runs)) {
			return model.RunState{}, fmt.Errorf("show run %s: %w", selector, app.ErrNotFound)
		}

		return runs[index], nil
	}
	for _, run := range runs {
		if run.Number == uint64(value) {
			return run, nil
		}
	}

	return model.RunState{}, fmt.Errorf("show run %s: %w", selector, app.ErrNotFound)
}

func writeRunDetails(command *cobra.Command, run model.RunState) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	fields := [][2]string{
		{"ID", run.ID.String()},
		{"Number", strconv.FormatUint(run.Number, 10)},
		{"Phase", string(run.Phase)},
		{"Outcome", string(run.Outcome)},
		{"Revision", strconv.FormatUint(run.Revision, 10)},
		{"Resolved executable", run.ResolvedExecutable},
		{"Reserved", run.ReservedAt.UTC().Format(timeFormat)},
		{"Started", formatOptionalTime(run.StartedAt)},
		{"Completed", formatOptionalTime(run.CompletedAt)},
		{"Logs", formatLogAvailability(run.Logs.Available(), run.Logs.PrunedAt)},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(
			writer,
			"%s:\t%s\n",
			field[0],
			redactField(command, field[0], field[1]),
		); err != nil {
			return fmt.Errorf("write run details: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush run details: %w", err)
	}

	return nil
}

func writeJobDetails(command *cobra.Command, value app.JobDetails) error {
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	fields := [][2]string{
		{"ID", value.Job.ID.String()},
		{"Name", value.Job.Spec.Name()},
		{"Phase", string(value.Job.Phase)},
		{"Outcome", string(value.Job.Outcome)},
		{"Submitted", value.Job.SubmittedAt.UTC().Format(timeFormat)},
		{"Executable", value.Job.Spec.Executable()},
		{"Working directory", value.Job.Spec.WorkingDirectory()},
		{"Completed runs", strconv.FormatUint(value.Runtime.RunCount, 10)},
		{"Successful runs", strconv.FormatUint(value.Runtime.SuccessCount, 10)},
		{"Failed runs", strconv.FormatUint(value.Runtime.FailureCount, 10)},
		{"Dependencies", strconv.Itoa(len(value.Dependencies))},
		{"Wait evaluations", strconv.Itoa(len(value.WaitEvaluations))},
		{"Admission", formatAdmission(value.Admission)},
		{"Notification deliveries", strconv.Itoa(len(value.NotificationDeliveries))},
		{"Pending notifications", strconv.Itoa(pendingNotificationDeliveries(value.NotificationDeliveries))},
		{"Notification attempts", strconv.Itoa(len(value.NotificationAttempts))},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(
			writer,
			"%s:\t%s\n",
			field[0],
			redactField(command, field[0], field[1]),
		); err != nil {
			return fmt.Errorf("write job details: %w", err)
		}
	}
	if len(value.Runs) > 0 {
		if _, err := fmt.Fprintln(writer, "\nRUN\tPHASE\tOUTCOME\tSTARTED\tCOMPLETED\tLOGS"); err != nil {
			return fmt.Errorf("write run header: %w", err)
		}
	}
	for _, run := range value.Runs {
		if _, err := fmt.Fprintf(
			writer,
			"%d\t%s\t%s\t%s\t%s\t%s\n",
			run.Number,
			run.Phase,
			run.Outcome,
			formatOptionalTime(run.StartedAt),
			formatOptionalTime(run.CompletedAt),
			formatLogAvailability(run.Logs.Available(), run.Logs.PrunedAt),
		); err != nil {
			return fmt.Errorf("write run details: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush job details: %w", err)
	}

	return nil
}

func formatAdmission(admission *store.Admission) string {
	if admission == nil {
		return "none"
	}
	scope := "global"
	if admission.Pool != "" {
		scope = "pool " + admission.Pool
	}
	state := "active"
	if admission.ReleasedAt != nil {
		state = "released"
	}

	return fmt.Sprintf("%s, %s, %d slot(s)", state, scope, admission.Slots)
}

func pendingNotificationDeliveries(deliveries []store.NotificationDelivery) int {
	pending := 0
	for _, delivery := range deliveries {
		if delivery.Status == store.NotificationDeliveryPending ||
			delivery.Status == store.NotificationDeliveryDelivering {
			pending++
		}
	}

	return pending
}

func formatLogAvailability(available bool, prunedAt *time.Time) string {
	if available {
		return "available"
	}
	if prunedAt == nil {
		return "unavailable"
	}

	return "pruned " + prunedAt.UTC().Format(timeFormat)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}

	return value.UTC().Format(timeFormat)
}
