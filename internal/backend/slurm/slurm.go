// Package slurm provides the bounded Slurm CLI adapter used by the Jobman
// agent on a cluster submit host.
package slurm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

const (
	stateCancelled      = "cancelled" //nolint:misspell // Frozen v1alpha1 wire value.
	stateTimedOut       = "timed_out"
	slurmStateCancelled = "CANCELLED" //nolint:misspell // Native Slurm state.
)

// Runner is the test seam for exact-argument Slurm CLI invocations.
type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

// ExecRunner invokes Slurm commands directly without a shell.
type ExecRunner struct{}

// Run executes one command and returns its combined output.
func (ExecRunner) Run(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...) // #nosec G204 -- command names are fixed Slurm tools.

	return command.CombinedOutput()
}

// Adapter translates portable execution intent into Slurm CLI calls.
type Adapter struct {
	runner Runner
}

// New returns an adapter backed by runner.
func New(runner Runner) (*Adapter, error) {
	if runner == nil {
		return nil, errors.New("create Slurm adapter: runner is required")
	}

	return &Adapter{runner: runner}, nil
}

// SubmitRequest contains one immutable NFS execution bundle and approved
// portable scheduling intent.
type SubmitRequest struct {
	JobName    string
	Partition  string
	ScriptPath string
	StdoutPath string
	StderrPath string
	RunnerPath string
	RunnerArgs []string
	Resources  *protocol.Resources
}

// ArraySubmitRequest contains one immutable NFS runner bundle shared by a
// contiguous set of independently tracked array tasks.
type ArraySubmitRequest struct {
	SubmitRequest
	TaskCount   int
	MaxParallel int
}

// Submission is the durable identity returned by sbatch --parsable.
type Submission struct {
	JobID   string
	Cluster string
}

// Observation is one normalized scheduler snapshot.
type Observation struct {
	JobID    string
	Cluster  string
	State    string
	Reason   string
	Terminal bool
	Result   *protocol.ProcessResult
}

// ValidateResources verifies that portable resource intent has an exact,
// supported Slurm translation before an assignment is accepted.
func ValidateResources(resources *protocol.Resources) error {
	_, err := resourceArguments(resources)

	return err
}

// Probe checks that the required submission, query, accounting, and cancel
// clients are callable for the enrolled user.
func (adapter *Adapter) Probe(ctx context.Context) error {
	for _, command := range []string{"sbatch", "squeue", "sacct", "scancel"} {
		if _, err := adapter.runner.Run(ctx, command, "--version"); err != nil {
			return fmt.Errorf("probe Slurm command %s: %w", command, err)
		}
	}

	return nil
}

// Submit invokes sbatch once and parses its machine-readable identity.
func (adapter *Adapter) Submit(ctx context.Context, request SubmitRequest) (Submission, error) {
	if err := validateSubmitRequest(request); err != nil {
		return Submission{}, err
	}
	resourceArguments, err := resourceArguments(request.Resources)
	if err != nil {
		return Submission{}, err
	}
	arguments := []string{
		"--parsable", "--job-name=" + request.JobName,
		"--output=" + request.StdoutPath, "--error=" + request.StderrPath,
	}
	if request.Partition != "" {
		arguments = append(arguments, "--partition="+request.Partition)
	}
	arguments = append(arguments, resourceArguments...)
	arguments = append(arguments, request.ScriptPath, request.RunnerPath)
	arguments = append(arguments, request.RunnerArgs...)
	output, err := adapter.runner.Run(ctx, "sbatch", arguments...)
	if err != nil {
		return Submission{}, fmt.Errorf("submit Slurm job: %w", err)
	}

	return parseSubmission(output)
}

// SubmitArray invokes one sbatch array allocation. Task indices are always the
// contiguous range 0..TaskCount-1; the runner reads SLURM_ARRAY_TASK_ID from
// the scheduler environment and maps it through an immutable manifest.
func (adapter *Adapter) SubmitArray(
	ctx context.Context,
	request ArraySubmitRequest,
) (Submission, error) {
	if request.TaskCount < 1 || request.TaskCount > 10_000 ||
		request.MaxParallel < 1 || request.MaxParallel > request.TaskCount {
		return Submission{}, errors.New("submit Slurm array: invalid task or concurrency bound")
	}
	if err := validateSubmitRequest(request.SubmitRequest); err != nil {
		return Submission{}, err
	}
	resourceArguments, err := resourceArguments(request.Resources)
	if err != nil {
		return Submission{}, err
	}
	arguments := []string{
		"--parsable", "--job-name=" + request.JobName,
		"--output=" + request.StdoutPath, "--error=" + request.StderrPath,
		"--array=0-" + strconv.Itoa(request.TaskCount-1) + "%" + strconv.Itoa(request.MaxParallel),
	}
	if request.Partition != "" {
		arguments = append(arguments, "--partition="+request.Partition)
	}
	arguments = append(arguments, resourceArguments...)
	arguments = append(arguments, request.ScriptPath, request.RunnerPath)
	arguments = append(arguments, request.RunnerArgs...)
	output, err := adapter.runner.Run(ctx, "sbatch", arguments...)
	if err != nil {
		return Submission{}, fmt.Errorf("submit Slurm array: %w", err)
	}

	return parseSubmission(output)
}

// ArrayTaskID returns the native scheduler identity for one independently
// observable child.
func ArrayTaskID(jobID string, index int) (string, error) {
	if !validToken(jobID) || index < 0 || index >= 10_000 {
		return "", errors.New("slurm array task identity is invalid")
	}

	return jobID + "_" + strconv.Itoa(index), nil
}

// ObserveArrayTask observes one child using its native task identity.
func (adapter *Adapter) ObserveArrayTask(
	ctx context.Context,
	jobID string,
	index int,
) (Observation, error) {
	taskID, err := ArrayTaskID(jobID, index)
	if err != nil {
		return Observation{}, err
	}

	return adapter.Observe(ctx, taskID)
}

// CancelArrayTask requests cancellation of one child without canceling its
// siblings.
func (adapter *Adapter) CancelArrayTask(ctx context.Context, jobID string, index int) error {
	taskID, err := ArrayTaskID(jobID, index)
	if err != nil {
		return err
	}

	return adapter.Cancel(ctx, taskID)
}

// Observe queries the live queue first and uses accounting for jobs that have
// already left the queue.
func (adapter *Adapter) Observe(ctx context.Context, jobID string) (Observation, error) {
	if !validToken(jobID) {
		return Observation{}, errors.New("observe Slurm job: invalid job ID")
	}
	output, err := adapter.runner.Run(
		ctx, "squeue", "--jobs="+jobID, "--noheader", "--format=%i|%T|%r",
	)
	if err != nil {
		return Observation{}, fmt.Errorf("query Slurm queue: %w", err)
	}
	if strings.TrimSpace(string(output)) != "" {
		return parseQueueObservation(output, jobID)
	}
	output, err = adapter.runner.Run(
		ctx, "sacct", "--jobs="+jobID, "--noheader", "--parsable2", "--allocations",
		"--format=JobIDRaw,State,ExitCode,Reason,Cluster",
	)
	if err != nil {
		return Observation{}, fmt.Errorf("query Slurm accounting: %w", err)
	}

	return parseAccountingObservation(output, jobID)
}

// FindByName locates a possibly submitted job after an ambiguous sbatch
// outcome. It fails closed when the tag resolves to zero or multiple jobs.
func (adapter *Adapter) FindByName(
	ctx context.Context,
	jobName, executionUser string,
	since time.Time,
) (Submission, error) {
	if !validToken(jobName) || !validToken(executionUser) || since.IsZero() {
		return Submission{}, errors.New("find Slurm job: invalid lookup")
	}
	output, err := adapter.runner.Run(
		ctx, "sacct", "--name="+jobName, "--user="+executionUser,
		"--starttime="+since.UTC().Format("2006-01-02T15:04:05"), "--noheader",
		"--parsable2", "--allocations", "--format=JobIDRaw,Cluster",
	)
	if err != nil {
		return Submission{}, fmt.Errorf("find Slurm job: %w", err)
	}
	var matches []Submission
	for _, line := range nonemptyLines(output) {
		fields := strings.Split(line, "|")
		if len(fields) < 2 || !validToken(fields[0]) {
			return Submission{}, errors.New("find Slurm job: malformed accounting response")
		}
		matches = append(matches, Submission{JobID: fields[0], Cluster: fields[1]})
	}
	if len(matches) != 1 {
		return Submission{}, fmt.Errorf("find Slurm job: expected one match, found %d", len(matches))
	}

	return matches[0], nil
}

// Cancel requests cancellation of one known scheduler allocation.
func (adapter *Adapter) Cancel(ctx context.Context, jobID string) error {
	if !validToken(jobID) {
		return errors.New("cancel Slurm job: invalid job ID")
	}
	if _, err := adapter.runner.Run(ctx, "scancel", jobID); err != nil {
		return fmt.Errorf("cancel Slurm job: %w", err)
	}

	return nil
}

func validateSubmitRequest(request SubmitRequest) error {
	for name, value := range map[string]string{
		"job name": request.JobName, "script path": request.ScriptPath,
		"stdout path": request.StdoutPath, "stderr path": request.StderrPath,
		"runner path": request.RunnerPath,
	} {
		if value == "" || strings.ContainsRune(value, 0) {
			return fmt.Errorf("submit Slurm job: invalid %s", name)
		}
	}
	if request.Partition != "" && !validToken(request.Partition) {
		return errors.New("submit Slurm job: invalid partition")
	}
	for _, argument := range request.RunnerArgs {
		if strings.ContainsRune(argument, 0) {
			return errors.New("submit Slurm job: invalid runner argument")
		}
	}

	return nil
}

func resourceArguments(resources *protocol.Resources) ([]string, error) {
	if resources == nil {
		return nil, nil
	}
	if resources.TemporaryStorage != "" {
		return nil, errors.New("submit Slurm job: temporary storage is not supported")
	}
	var arguments []string
	if resources.CPU > 0 {
		arguments = append(arguments, "--cpus-per-task="+strconv.Itoa(resources.CPU))
	}
	if resources.Memory != "" {
		memoryMiB, err := quantityMiB(resources.Memory)
		if err != nil {
			return nil, fmt.Errorf("submit Slurm job: %w", err)
		}
		arguments = append(arguments, "--mem="+strconv.FormatUint(memoryMiB, 10)+"M")
	}
	if resources.GPU > 0 {
		arguments = append(arguments, "--gpus="+strconv.Itoa(resources.GPU))
	}
	if resources.Nodes > 0 {
		arguments = append(arguments, "--nodes="+strconv.Itoa(resources.Nodes))
	}
	if resources.Tasks > 0 {
		arguments = append(arguments, "--ntasks="+strconv.Itoa(resources.Tasks))
	}
	if resources.WallTime != "" {
		duration, err := time.ParseDuration(resources.WallTime)
		if err != nil || duration <= 0 {
			return nil, errors.New("invalid wall time")
		}
		minutes := int64(math.Ceil(duration.Minutes()))
		arguments = append(arguments, "--time="+strconv.FormatInt(minutes, 10))
	}

	return arguments, nil
}

func quantityMiB(quantity string) (uint64, error) {
	suffixes := []struct {
		name       string
		multiplier uint64
	}{
		{"TiB", 1 << 40},
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"TB", 1_000_000_000_000},
		{"GB", 1_000_000_000},
		{"MB", 1_000_000},
		{"KB", 1_000},
		{"B", 1},
	}
	for _, suffix := range suffixes {
		if !strings.HasSuffix(quantity, suffix.name) {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSuffix(quantity, suffix.name), 10, 64)
		if err != nil || value == 0 || value > math.MaxUint64/suffix.multiplier {
			return 0, errors.New("invalid memory quantity")
		}
		bytes := value * suffix.multiplier

		return (bytes + (1 << 20) - 1) / (1 << 20), nil
	}

	return 0, errors.New("invalid memory quantity")
}

func parseSubmission(output []byte) (Submission, error) {
	fields := strings.Split(strings.TrimSpace(string(output)), ";")
	if len(fields) < 1 || len(fields) > 2 || !validToken(fields[0]) {
		return Submission{}, errors.New("submit Slurm job: malformed sbatch response")
	}
	submission := Submission{JobID: fields[0]}
	if len(fields) == 2 {
		if fields[1] != "" && !validToken(fields[1]) {
			return Submission{}, errors.New("submit Slurm job: malformed cluster name")
		}
		submission.Cluster = fields[1]
	}

	return submission, nil
}

func parseQueueObservation(output []byte, expectedJobID string) (Observation, error) {
	lines := nonemptyLines(output)
	if len(lines) != 1 {
		return Observation{}, errors.New("query Slurm queue: ambiguous response")
	}
	fields := strings.Split(lines[0], "|")
	if len(fields) < 3 || fields[0] != expectedJobID {
		return Observation{}, errors.New("query Slurm queue: malformed response")
	}
	state, terminal := normalizeState(fields[1])
	if terminal {
		return Observation{}, errors.New("query Slurm queue: unexpected terminal state")
	}

	return Observation{JobID: fields[0], State: state, Reason: fields[2]}, nil
}

func parseAccountingObservation(output []byte, expectedJobID string) (Observation, error) {
	lines := nonemptyLines(output)
	if len(lines) != 1 {
		return Observation{}, errors.New("query Slurm accounting: job is absent or ambiguous")
	}
	fields := strings.Split(lines[0], "|")
	if len(fields) < 5 || fields[0] != expectedJobID {
		return Observation{}, errors.New("query Slurm accounting: malformed response")
	}
	state, terminal := normalizeState(fields[1])
	observation := Observation{
		JobID: fields[0], State: state, Reason: fields[3], Cluster: fields[4], Terminal: terminal,
	}
	if terminal {
		result := schedulerResult(state, fields[2])
		observation.Result = &result
	}

	return observation, nil
}

//nolint:cyclop // Slurm's native states require an explicit normalization table.
func normalizeState(value string) (string, bool) {
	state := strings.ToUpper(strings.TrimSuffix(strings.TrimSpace(value), "+"))
	if fields := strings.Fields(state); len(fields) != 0 {
		state = fields[0]
	}
	switch state {
	case "PENDING", "CONFIGURING", "REQUEUED", "RESIZING":
		return "queued", false
	case "RUNNING":
		return "running", false
	case "SUSPENDED", "STOPPED":
		return "suspended", false
	case "COMPLETING", "STAGE_OUT":
		return "completing", false
	case "COMPLETED":
		return "completed", true
	case slurmStateCancelled, "CANCELLED_BY_USER": //nolint:misspell // Native Slurm state.
		return stateCancelled, true
	case "TIMEOUT":
		return stateTimedOut, true
	case "PREEMPTED":
		return "preempted", true
	case "NODE_FAIL":
		return "node_failed", true
	case "OUT_OF_MEMORY":
		return "out_of_memory", true
	case "BOOT_FAIL":
		return "boot_failed", true
	case "DEADLINE":
		return "deadline", true
	case "FAILED", "REVOKED", "SPECIAL_EXIT":
		return "failed", true
	default:
		return "unknown", false
	}
}

func schedulerResult(state, exitStatus string) protocol.ProcessResult {
	code, signal, valid := parseExitStatus(exitStatus)
	if state == "completed" && valid && code == 0 && signal == 0 {
		return protocol.ProcessResult{Outcome: "success", ExitCode: &code}
	}
	result := protocol.ProcessResult{Outcome: "failure", FailureCode: "slurm_" + state}
	switch state {
	case stateCancelled:
		result.Outcome = stateCancelled
	case stateTimedOut:
		result.Outcome = stateTimedOut
	case "lost":
		result.Outcome = "lost"
	}
	if valid {
		result.ExitCode = &code
		if signal != 0 {
			result.Signal = "signal_" + strconv.Itoa(signal)
		}
	}

	return result
}

func parseExitStatus(value string) (code, signal int, valid bool) {
	fields := strings.Split(value, ":")
	if len(fields) != 2 {
		return 0, 0, false
	}
	code, codeErr := strconv.Atoi(fields[0])
	signal, signalErr := strconv.Atoi(fields[1])
	if codeErr != nil || signalErr != nil || code < 0 || code > 255 || signal < 0 || signal > 255 {
		return 0, 0, false
	}

	return code, signal, true
}

func nonemptyLines(output []byte) []string {
	var result []string
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}

	return result
}

func validToken(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\x00\r\n|")
}
