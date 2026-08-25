package jobman

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/controlclient"
	"github.com/ryancswallace/jobman/protocol"
)

const defaultSharedListLimit = 50

type sharedOptions struct {
	profile string
}

type sharedContext struct {
	client        sharedControlClient
	profile       string
	namespace     string
	stateDir      string
	artifactRoots map[string]config.SharedArtifactRoot
}

type sharedControlClient interface {
	SubmitJob(context.Context, protocol.SealedJobRequest, string) (controlclient.Job, error)
	SubmitCollection(context.Context, protocol.SealedCollectionRequest, string) (controlclient.Collection, error)
	GetCollection(context.Context, string) (controlclient.Collection, error)
	SubmitGraph(context.Context, protocol.SealedGraphRequest, string) (controlclient.Graph, error)
	GetGraph(context.Context, string) (controlclient.Graph, error)
	CancelGraph(context.Context, string, string) (controlclient.Graph, error)
	ImportCompletedHistory(context.Context, controlclient.CompletedHistoryImportRequest, bool, string) (controlclient.Job, error)
	ListJobs(context.Context, int, string, string) (controlclient.JobPage, error)
	GetJob(context.Context, string) (controlclient.Job, error)
	GetJobLogs(context.Context, string) (controlclient.LogManifest, error)
	GetJobArtifacts(context.Context, string) (controlclient.ArtifactManifest, error)
	CancelJob(context.Context, string, string) (controlclient.Job, error)
	ListTargets(context.Context) ([]controlclient.Target, error)
	GetTarget(context.Context, string) (controlclient.Target, error)
	UpdateTargetState(context.Context, string, string, int64, string) (controlclient.Target, error)
}

type openSharedControlFunc func(config.SharedProfile) (sharedControlClient, error)

func newSharedCommand(dependencies dependencies, root *rootOptions) *cobra.Command {
	options := &sharedOptions{}
	command := &cobra.Command{
		Use:   "shared",
		Short: "Use the pre-release Jobman Control client",
		Long:  "Use the pre-release shared-mode client. Shared jobs are a separate state universe from standalone SQLite jobs.",
		Args:  usageArgs(cobra.NoArgs),
	}
	command.PersistentFlags().StringVar(
		&options.profile, "profile", "", "select a shared control-service profile",
	)
	command.AddCommand(
		newSharedRunCommand(dependencies, root, options),
		newSharedListCommand(dependencies, root, options),
		newSharedStatusCommand(dependencies, root, options),
		newSharedShowCommand(dependencies, root, options),
		newSharedLogsCommand(dependencies, root, options),
		newSharedArtifactsCommand(dependencies, root, options),
		newSharedWaitCommand(dependencies, root, options),
		newSharedCancelCommand(dependencies, root, options),
		newSharedCollectionCommand(dependencies, root, options),
		newSharedGraphCommand(dependencies, root, options),
		newSharedHistoryCommand(dependencies, root, options),
		newSharedTargetCommand(dependencies, root, options),
		newSharedOperationsCommand(root),
	)

	return command
}

func withSharedContext(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	options *sharedOptions,
	operation func(sharedContext) error,
) error {
	if dependencies.LoadConfig == nil {
		return errors.New("shared configuration loader is unavailable")
	}
	loaded, err := dependencies.LoadConfig(root)
	if err != nil {
		return err
	}
	if redactionErr := configureRedactor(command, loaded.Config); redactionErr != nil {
		return redactionErr
	}
	profileName := options.profile
	if profileName == "" {
		profileName = loaded.Config.Shared.CurrentProfile
	}
	if profileName == "" {
		return usageError(errors.New("select a shared profile with --profile or shared.current_profile"))
	}
	profile, found := loaded.Config.Shared.Profiles[profileName]
	if !found {
		return usageError(fmt.Errorf("unknown shared profile %q", profileName))
	}
	if dependencies.OpenControl == nil {
		return errors.New("shared control client is unavailable")
	}
	client, err := dependencies.OpenControl(profile)
	if err != nil {
		return redactCommandError(command, fmt.Errorf("configure shared client: %w", err))
	}
	stateDir, err := config.StateDir(root.stateDir)
	if err != nil {
		return redactCommandError(command, err)
	}

	return redactCommandError(command, operation(sharedContext{
		client: client, profile: profileName, namespace: profile.Namespace, stateDir: stateDir,
		artifactRoots: profile.ArtifactRoots,
	}))
}

type sharedFileSubmitFunc func(*cobra.Command, sharedContext, string, bool) error

func newSharedFileSubmitCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
	use string,
	short string,
	submit sharedFileSubmitFunc,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use: use, Short: short, Args: usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(context sharedContext) error {
				return submit(command, context, arguments[0], jsonOutput)
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

type sharedRunOptions struct {
	name                string
	target              string
	partition           string
	workingDir          string
	environment         []string
	inputs              []string
	outputs             []string
	runTimeout          string
	cpu                 int
	memory              string
	gpu                 int
	nodes               int
	tasks               int
	wallTime            string
	containerImage      string
	containerPullPolicy string
	containerNetwork    string
	resume              string
	jsonOutput          bool
}

func newSharedRunCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	options := &sharedRunOptions{}
	command := &cobra.Command{
		Use:   "run [flags] -- COMMAND [ARG...]",
		Short: "Submit one portable job to a named target",
		Args: func(command *cobra.Command, arguments []string) error {
			if options.resume != "" {
				if len(arguments) != 0 {
					return usageError(errors.New("--resume does not accept a command"))
				}

				return nil
			}
			return usageArgs(cobra.MinimumNArgs(1))(command, arguments)
		},
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				return runSharedRun(command, sharedContext, options, arguments)
			})
		},
	}
	flags := command.Flags()
	flags.SetInterspersed(false)
	flags.StringVar(&options.name, "name", "", "set the portable job name")
	flags.StringVar(&options.target, "target", "", "select a named target")
	flags.StringVar(&options.partition, "partition", "", "select a target partition")
	flags.StringVar(&options.workingDir, "working-directory", "workspace:/", "set the logical workspace directory")
	flags.StringArrayVar(&options.environment, "env", nil, "set one explicit environment value as NAME=VALUE")
	flags.StringArrayVar(&options.inputs, "input", nil, "stage NAME=ARTIFACT_URI=inputs:/PATH[=SHA256]")
	flags.StringArrayVar(&options.outputs, "output", nil, "publish NAME=outputs:/PATH=ARTIFACT_URI[=required|optional]")
	flags.StringVar(&options.runTimeout, "timeout", "", "set the portable run timeout")
	flags.IntVar(&options.cpu, "cpu", 0, "request CPUs per task on a Slurm target")
	flags.StringVar(&options.memory, "memory", "", "request portable memory on a Slurm target (for example 4GiB)")
	flags.IntVar(&options.gpu, "gpu", 0, "request GPUs on a Slurm target")
	flags.IntVar(&options.nodes, "nodes", 0, "request nodes on a Slurm target")
	flags.IntVar(&options.tasks, "tasks", 0, "request tasks on a Slurm target")
	flags.StringVar(&options.wallTime, "wall-time", "", "request Slurm allocation wall time")
	flags.StringVar(&options.containerImage, "container-image", "", "run in this target-approved container image")
	flags.StringVar(&options.containerPullPolicy, "container-pull-policy", "if-not-present", "container pull policy: always, if-not-present, or never")
	flags.StringVar(&options.containerNetwork, "container-network", "restricted", "container network policy: restricted, none, or host")
	flags.StringVar(&options.resume, "resume", "", "resume a durable submission operation")
	flags.BoolVar(&options.jsonOutput, "json", false, "emit versioned JSON")

	return command
}

//nolint:cyclop // Submission validation projects each explicit portable flag without hidden defaults.
func runSharedRun(
	command *cobra.Command,
	shared sharedContext,
	options *sharedRunOptions,
	arguments []string,
) error {
	if options.resume != "" {
		return resumeSharedRun(command, shared, options)
	}
	if options.target == "" {
		return usageError(errors.New("--target is required"))
	}
	environment, err := parseSharedEnvironment(options.environment)
	if err != nil {
		return usageError(err)
	}
	artifacts, err := parseSharedArtifacts(options.inputs, options.outputs)
	if err != nil {
		return usageError(err)
	}
	name := options.name
	if name == "" {
		name = portableJobName(arguments[0])
	}
	var resources *protocol.Resources
	if options.cpu != 0 || options.memory != "" || options.gpu != 0 || options.nodes != 0 ||
		options.tasks != 0 || options.wallTime != "" {
		resources = &protocol.Resources{
			CPU: options.cpu, Memory: options.memory, GPU: options.gpu,
			Nodes: options.nodes, Tasks: options.tasks, WallTime: options.wallTime,
		}
	}
	runtime := protocol.Runtime{Kind: "native"}
	if options.containerImage != "" {
		runtime = protocol.Runtime{Kind: "container", Container: &protocol.ContainerRuntime{
			Image: options.containerImage, PullPolicy: options.containerPullPolicy,
			Network: options.containerNetwork,
		}}
	} else if options.containerPullPolicy != "if-not-present" || options.containerNetwork != "restricted" {
		return usageError(errors.New("container policy flags require --container-image"))
	}
	workload := protocol.Workload{
		APIVersion: protocol.V1Alpha1,
		Kind:       protocol.WorkloadKind,
		Metadata:   protocol.WorkloadMetadata{Name: name},
		Spec: protocol.WorkloadSpec{
			Command:          protocol.Command{Executable: arguments[0], Args: append([]string(nil), arguments[1:]...)},
			WorkingDirectory: options.workingDir,
			Environment:      environment,
			Resources:        resources,
			Artifacts:        artifacts,
			Runtime:          runtime,
			Policy: protocol.ExecutionPolicy{
				RunTimeout: options.runTimeout,
				Retry:      protocol.RetryPolicy{MaxRuns: 1}, DuplicateRisk: "reject",
			},
		},
	}
	sealed, err := protocol.SealJobRequest(protocol.JobRequest{
		APIVersion: protocol.V1Alpha1,
		Kind:       protocol.JobRequestKind,
		Metadata: protocol.JobRequestMetadata{
			Namespace: shared.namespace, Name: name,
		},
		Spec: protocol.JobRequestSpec{
			Workload: protocol.WorkloadBinding{Document: workload},
			Placement: protocol.Placement{
				Target: options.target, Partition: options.partition,
			},
		},
	})
	if err != nil {
		return usageError(err)
	}
	operation, err := createSharedOperation(
		shared.stateDir, shared.profile, shared.namespace, sealed,
	)
	if err != nil {
		return err
	}
	job, err := shared.client.SubmitJob(command.Context(), sealed, operation.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("submit shared job (resume with --resume %s): %w", operation.ID, err)
	}
	if err = completeSharedOperation(shared.stateDir, &operation, job.Metadata.ID); err != nil {
		return fmt.Errorf("record completed shared submission %s: %w", operation.ID, err)
	}

	return writeSharedRunResult(command, job, operation.ID, options.jsonOutput)
}

//nolint:nilnil // A nil artifact declaration is the canonical representation of no artifacts.
func parseSharedArtifacts(inputs, outputs []string) (*protocol.Artifacts, error) {
	if len(inputs) == 0 && len(outputs) == 0 {
		return nil, nil
	}
	result := &protocol.Artifacts{}
	for _, value := range inputs {
		parts := strings.SplitN(value, "=", 4)
		if len(parts) < 3 {
			return nil, errors.New("--input must be NAME=ARTIFACT_URI=inputs:/PATH[=SHA256]")
		}
		input := protocol.InputArtifact{Name: parts[0], Source: parts[1], Target: parts[2]}
		if len(parts) == 4 {
			input.Checksum = parts[3]
		}
		result.Inputs = append(result.Inputs, input)
	}
	for _, value := range outputs {
		parts := strings.SplitN(value, "=", 4)
		if len(parts) < 3 {
			return nil, errors.New("--output must be NAME=outputs:/PATH=ARTIFACT_URI[=required|optional]")
		}
		output := protocol.OutputArtifact{Name: parts[0], Source: parts[1], Destination: parts[2]}
		if len(parts) == 4 {
			switch parts[3] {
			case "required":
				output.Required = true
			case "optional":
			default:
				return nil, errors.New("--output disposition must be required or optional")
			}
		}
		result.Outputs = append(result.Outputs, output)
	}

	return result, nil
}

func resumeSharedRun(command *cobra.Command, shared sharedContext, options *sharedRunOptions) error {
	if err := validateSharedResumeOptions(options); err != nil {
		return err
	}
	operation, err := loadSharedOperation(shared.stateDir, options.resume)
	if err != nil {
		return err
	}
	if operation.Profile != shared.profile || operation.Namespace != shared.namespace {
		return errors.New("shared submission operation belongs to a different profile or namespace")
	}
	if operation.Status == sharedOperationCompleted {
		job, getErr := shared.client.GetJob(command.Context(), operation.JobID)
		if getErr != nil {
			return getErr
		}

		return writeSharedRunResult(command, job, operation.ID, options.jsonOutput)
	}
	sealed, err := protocol.DecodeJobRequest(bytes.NewReader(operation.Request), protocol.DecodeLimits{})
	if err != nil || sealed.RequestDigest != operation.RequestDigest {
		return errors.New("shared submission operation request is invalid")
	}
	job, err := shared.client.SubmitJob(command.Context(), sealed, operation.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("resume shared submission %s: %w", operation.ID, err)
	}
	if err = completeSharedOperation(shared.stateDir, &operation, job.Metadata.ID); err != nil {
		return fmt.Errorf("record completed shared submission %s: %w", operation.ID, err)
	}

	return writeSharedRunResult(command, job, operation.ID, options.jsonOutput)
}

//nolint:cyclop // Every workload flag is explicitly rejected during resume.
func validateSharedResumeOptions(options *sharedRunOptions) error {
	if options.target != "" || options.name != "" || options.partition != "" ||
		len(options.environment) != 0 || options.runTimeout != "" ||
		options.cpu != 0 || options.memory != "" || options.gpu != 0 ||
		options.nodes != 0 || options.tasks != 0 || options.wallTime != "" ||
		options.containerImage != "" || options.containerPullPolicy != "if-not-present" ||
		options.containerNetwork != "restricted" ||
		options.workingDir != "workspace:/" {
		return usageError(errors.New("--resume is mutually exclusive with workload and placement flags"))
	}

	return nil
}

func writeSharedRunResult(
	command *cobra.Command,
	job controlclient.Job,
	operationID string,
	jsonOutput bool,
) error {
	if jsonOutput {
		return writeJSON(command, struct {
			Job         controlclient.Job `json:"job"`
			OperationID string            `json:"operation_id"`
		}{Job: job, OperationID: operationID})
	}
	_, err := fmt.Fprintf(
		command.OutOrStdout(), "%s\t%s\t%s\t%s\n",
		job.Metadata.ID, job.Status.Phase, job.Spec.Placement.Target, operationID,
	)

	return err
}

func parseSharedEnvironment(values []string) (*protocol.Environment, error) {
	if len(values) == 0 {
		return &protocol.Environment{}, nil
	}
	environment := &protocol.Environment{Values: make(map[string]string, len(values))}
	for _, assignment := range values {
		name, value, found := strings.Cut(assignment, "=")
		if !found || name == "" {
			return nil, fmt.Errorf("--env %q must use NAME=VALUE", assignment)
		}
		if _, duplicate := environment.Values[name]; duplicate {
			return nil, fmt.Errorf("--env contains duplicate name %q", name)
		}
		environment.Values[name] = value
	}

	return environment, nil
}

func portableJobName(executable string) string {
	name := strings.ToLower(filepath.Base(executable))
	var result strings.Builder
	lastSeparator := false
	for _, character := range name {
		valid := character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '.' ||
			character == '_' || character == '-'
		if valid {
			result.WriteRune(character)
			lastSeparator = false
		} else if !lastSeparator {
			result.WriteByte('-')
			lastSeparator = true
		}
	}
	name = strings.Trim(result.String(), "._-")
	if name == "" {
		name = "job"
	}
	if len(name) > 128 {
		name = strings.TrimRight(name[:128], "._-")
	}

	return name
}

type sharedListOptions struct {
	limit      int
	phase      string
	pageToken  string
	jsonOutput bool
}

func newSharedListCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	options := &sharedListOptions{}
	command := &cobra.Command{
		Use:   listCommandName,
		Short: "List shared jobs",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			if options.limit < 1 || options.limit > 200 {
				return usageError(errors.New("--limit must be between 1 and 200"))
			}
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				page, err := sharedContext.client.ListJobs(
					command.Context(), options.limit, options.phase, options.pageToken,
				)
				if err != nil {
					return err
				}
				return writeSharedJobPage(command, page, options.jsonOutput)
			})
		},
	}
	command.Flags().IntVar(&options.limit, "limit", defaultSharedListLimit, "maximum jobs in this page")
	command.Flags().StringVar(&options.phase, "phase", "", "include only jobs in this shared phase")
	command.Flags().StringVar(&options.pageToken, "page-token", "", "continue from an earlier page")
	command.Flags().BoolVar(&options.jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func writeSharedJobPage(command *cobra.Command, page controlclient.JobPage, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(command, page)
	}
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "ID\tNAME\tPHASE\tOUTCOME\tTARGET\tSUBMITTED"); err != nil {
		return err
	}
	for _, job := range page.Items {
		if _, err := fmt.Fprintf(
			writer, "%s\t%s\t%s\t%s\t%s\t%s\n",
			job.Metadata.ID, redactField(command, "name", job.Metadata.Name),
			job.Status.Phase, job.Status.Outcome, job.Spec.Placement.Target,
			job.Metadata.CreatedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return err
		}
	}
	if page.NextPageToken != "" {
		if _, err := fmt.Fprintf(writer, "NEXT_PAGE_TOKEN\t%s\n", page.NextPageToken); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func newSharedStatusCommand(dependencies dependencies, root *rootOptions, shared *sharedOptions) *cobra.Command {
	return newSharedInspectCommand(
		"status", "Show concise shared job status", false, dependencies, root, shared,
	)
}

func newSharedShowCommand(dependencies dependencies, root *rootOptions, shared *sharedOptions) *cobra.Command {
	return newSharedInspectCommand("show", "Show a shared job", true, dependencies, root, shared)
}

func newSharedInspectCommand(
	name,
	short string,
	detailed bool,
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   name + " JOB",
		Short: short,
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				job, err := sharedContext.client.GetJob(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				if jsonOutput || detailed {
					return writeJSON(command, job)
				}
				_, err = fmt.Fprintf(
					command.OutOrStdout(), "%s\t%s\t%s\t%s\t%s\n",
					job.Metadata.ID, redactField(command, "name", job.Metadata.Name),
					job.Status.Phase, job.Status.Outcome, job.Status.ObservationConfidence,
				)
				return err
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

type sharedWaitOptions struct {
	pollInterval time.Duration
	timeout      time.Duration
	jsonOutput   bool
}

func newSharedWaitCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	options := &sharedWaitOptions{}
	command := &cobra.Command{
		Use:   "wait JOB",
		Short: "Wait for a shared job to become terminal",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			if options.pollInterval <= 0 || options.timeout < 0 {
				return usageError(errors.New("--poll-interval must be positive and --timeout must not be negative"))
			}
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				return runSharedWait(command, sharedContext, arguments[0], options)
			})
		},
	}
	command.Flags().DurationVar(&options.pollInterval, "poll-interval", time.Second, "status polling interval")
	command.Flags().DurationVar(&options.timeout, "timeout", 0, "maximum wait time; zero is unlimited")
	command.Flags().BoolVar(&options.jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func runSharedWait(
	command *cobra.Command,
	shared sharedContext,
	jobID string,
	options *sharedWaitOptions,
) error {
	ctx := command.Context()
	var cancel context.CancelFunc
	if options.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, options.timeout)
		defer cancel()
	}
	for {
		job, err := shared.client.GetJob(ctx, jobID)
		if err != nil {
			return err
		}
		if job.Status.Phase == "terminal" {
			return finishSharedWait(command, job, options.jsonOutput)
		}
		if err := waitForSharedPoll(ctx, options.pollInterval); err != nil {
			return err
		}
	}
}

func finishSharedWait(command *cobra.Command, job controlclient.Job, jsonOutput bool) error {
	if jsonOutput {
		if err := writeJSON(command, job); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintf(
		command.OutOrStdout(), "%s\t%s\n", job.Metadata.ID, job.Status.Outcome,
	); err != nil {
		return err
	}
	if job.Status.Outcome != "success" {
		return sharedOutcomeError{outcome: job.Status.Outcome}
	}

	return nil
}

func waitForSharedPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}

		return fmt.Errorf("wait for shared job: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

type sharedOutcomeError struct{ outcome string }

func (err sharedOutcomeError) Error() string {
	return "shared job completed with outcome " + err.outcome
}
func (sharedOutcomeError) Silent() bool { return true }

func newSharedCancelCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "cancel JOB",
		Short: "Request cancellation of a shared job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				id, err := newSharedOperationID()
				if err != nil {
					return err
				}
				job, err := sharedContext.client.CancelJob(
					command.Context(), arguments[0], "cancel-"+id,
				)
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, job)
				}
				_, err = fmt.Fprintf(
					command.OutOrStdout(), "%s\t%s\t%s\n",
					job.Metadata.ID, job.Status.Phase, job.Status.DesiredState,
				)
				return err
			})
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}

func newSharedTargetCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "target",
		Short: "Inspect shared placement targets",
		Args:  usageArgs(cobra.NoArgs),
	}
	command.AddCommand(
		newSharedTargetListCommand(dependencies, root, shared),
		newSharedTargetShowCommand(dependencies, root, shared),
		newSharedTargetStateCommand(dependencies, root, shared),
	)

	return command
}

func newSharedTargetStateCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var revision int64
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "state TARGET STATE",
		Short: "Change whether a target accepts new assignments",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE: func(command *cobra.Command, arguments []string) error {
			if revision < 1 {
				return usageError(errors.New("--revision must be positive"))
			}
			state := arguments[1]
			if !slices.Contains([]string{"active", "draining", "disabled", "retired"}, state) {
				return usageError(errors.New("STATE must be active, draining, disabled, or retired"))
			}
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				operationID, err := newSharedOperationID()
				if err != nil {
					return err
				}
				target, err := sharedContext.client.UpdateTargetState(
					command.Context(), arguments[0], state, revision, "target-state-"+operationID,
				)
				if err != nil {
					return err
				}
				if jsonOutput {
					return writeJSON(command, target)
				}
				_, err = fmt.Fprintf(
					command.OutOrStdout(), "%s\t%s\t%d\n",
					target.Metadata.Name, target.Status.State, target.Metadata.Revision,
				)
				return err
			})
		},
	}
	command.Flags().Int64Var(&revision, "revision", 0, "current target revision from target show")
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")
	_ = command.MarkFlagRequired("revision") //nolint:errcheck // The flag is declared immediately above.

	return command
}

func newSharedTargetListCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var listJSON bool
	list := &cobra.Command{
		Use:   listCommandName,
		Short: "List shared placement targets",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				targets, err := sharedContext.client.ListTargets(command.Context())
				if err != nil {
					return err
				}
				return writeSharedTargetList(command, targets, listJSON)
			})
		},
	}
	list.Flags().BoolVar(&listJSON, "json", false, "emit versioned JSON")

	return list
}

func writeSharedTargetList(command *cobra.Command, targets []controlclient.Target, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(command, struct {
			Targets []controlclient.Target `json:"targets"`
		}{Targets: targets})
	}
	writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "NAME\tKIND\tBACKEND\tRUNTIMES\tSTATE"); err != nil {
		return err
	}
	for _, target := range targets {
		if _, err := fmt.Fprintf(
			writer, "%s\t%s\t%s\t%s\t%s\n", target.Metadata.Name,
			target.Spec.Kind, target.Spec.ExecutionBackend,
			strings.Join(target.Spec.Runtimes, ","), target.Status.State,
		); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func newSharedTargetShowCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	var showJSON bool
	show := &cobra.Command{
		Use:   "show TARGET",
		Short: "Show a shared placement target",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				target, err := sharedContext.client.GetTarget(command.Context(), arguments[0])
				if err != nil {
					return err
				}
				return writeSharedTarget(command, target, showJSON)
			})
		},
	}
	show.Flags().BoolVar(&showJSON, "json", false, "emit versioned JSON")

	return show
}

func writeSharedTarget(command *cobra.Command, target controlclient.Target, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(command, target)
	}
	partitions := make([]string, 0, len(target.Spec.Partitions))
	for _, partition := range target.Spec.Partitions {
		name := partition.Name
		if partition.IsDefault {
			name += " (default)"
		}
		partitions = append(partitions, name)
	}
	sort.Strings(partitions)
	_, err := fmt.Fprintf(
		command.OutOrStdout(), "%s\t%s\t%s\t%s\t%s\n",
		target.Metadata.Name, target.Spec.Kind, target.Spec.ExecutionBackend,
		target.Status.State, strings.Join(partitions, ","),
	)

	return err
}

func newSharedOperationsCommand(root *rootOptions) *cobra.Command {
	var jsonOutput bool
	command := &cobra.Command{
		Use:   "operations",
		Short: "List local durable shared-submission operations",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			stateDir, err := config.StateDir(root.stateDir)
			if err != nil {
				return err
			}
			operations, err := listSharedOperations(stateDir)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(command, struct {
					Operations []sharedOperationSummary `json:"operations"`
				}{Operations: operations})
			}
			writer := tabwriter.NewWriter(command.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err = fmt.Fprintln(writer, "ID\tPROFILE\tNAMESPACE\tSTATUS\tJOB\tCREATED"); err != nil {
				return err
			}
			for _, operation := range operations {
				if _, err = fmt.Fprintf(
					writer, "%s\t%s\t%s\t%s\t%s\t%s\n",
					operation.ID, operation.Profile, operation.Namespace, operation.Status,
					operation.JobID, operation.CreatedAt.UTC().Format(time.RFC3339),
				); err != nil {
					return err
				}
			}

			return writer.Flush()
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit versioned JSON")

	return command
}
