package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/faultinject"
	"github.com/ryancswallace/jobman/internal/liveinput"
	"github.com/ryancswallace/jobman/internal/logstore"
	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/platform"
	"github.com/ryancswallace/jobman/internal/store"
	"github.com/ryancswallace/jobman/internal/supervisor"
)

const (
	defaultClaimWindow = 10 * time.Second
	defaultStopGrace   = 5 * time.Second
)

// Service is one short-lived client over a local metadata store.
type Service struct {
	store                *store.Store
	stateDir             string
	executable           string
	launch               func(context.Context, supervisor.LaunchOptions) (supervisor.Acknowledgement, error)
	sendInput            func(context.Context, string, string, io.Reader, bool) (liveinput.Result, error)
	processAlive         func(platform.ProcessIdentity) (bool, error)
	processPause         func(platform.ProcessIdentity) error
	processResume        func(platform.ProcessIdentity) error
	processTerminate     func(platform.ProcessIdentity, bool) error
	pauseResumeSupported func() bool
	now                  func() time.Time
	random               io.Reader
	ids                  *model.UUIDv7Generator
	retention            config.Retention
	knownPools           map[string]struct{}
}

// Open constructs a production application backend rooted at stateDir.
func Open(ctx context.Context, stateDir string) (Backend, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate Jobman executable: %w", err)
	}
	ids, err := model.NewUUIDv7Generator(time.Now, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("construct identifier source: %w", err)
	}
	database, err := store.Open(ctx, store.Options{
		StateDir:      stateDir,
		JobmanVersion: buildinfo.Version,
		Now:           time.Now,
		EventIDs:      ids,
	})
	if err != nil {
		return nil, err
	}

	return &Service{
		store:      database,
		stateDir:   stateDir,
		executable: executable,
		launch:     supervisor.Launch,
		now:        time.Now,
		random:     rand.Reader,
		ids:        ids,
	}, nil
}

// Close releases the client store connection.
func (service *Service) Close() error {
	return service.store.Close()
}

// Doctor verifies the store and performs only explicitly requested,
// conservative repair work. Recovery uses persisted job specifications and is
// independent of the current configuration files.
func (service *Service) Doctor(ctx context.Context, request DoctorRequest) (DoctorReport, error) {
	report := DoctorReport{}
	if request.BackupPath != "" {
		if err := service.store.Backup(ctx, request.BackupPath); err != nil {
			return report, err
		}
		report.BackupPath = request.BackupPath
	}
	health, err := service.store.CheckHealth(ctx, request.Repair)
	report.Store = health
	if err != nil {
		return report, err
	}
	if !request.Repair {
		return report, nil
	}
	if err := service.reconcileActiveJobs(ctx, store.ListJobsOptions{}); err != nil {
		return report, fmt.Errorf("reconcile stale lifecycle state: %w", err)
	}
	report.StaleOwnershipReconciled = true
	if err := supervisor.RecoverNotifications(ctx, service.store); err != nil {
		return report, fmt.Errorf("recover notifications: %w", err)
	}
	report.NotificationsRecovered = true

	return report, nil
}

// ConfigureInvocation installs non-durable policy needed by one command.
func (service *Service) ConfigureInvocation(configuration config.Config) {
	service.retention = configuration.Retention
	service.knownPools = make(map[string]struct{}, len(configuration.Concurrency.Pools))
	for name := range configuration.Concurrency.Pools {
		service.knownPools[name] = struct{}{}
	}
}

// ApplyConfig synchronizes global and named-pool capacities. Reapplying an
// unchanged effective configuration is idempotent and does not advance durable
// revisions.
func (service *Service) ApplyConfig(ctx context.Context, configuration config.Config) error {
	service.ConfigureInvocation(configuration)
	var global *uint64
	if value, finite := configuration.Concurrency.MaxActiveSlots.Value(); finite {
		converted := uint64(value)
		global = &converted
	}
	pools := make(map[string]*uint64, len(configuration.Concurrency.Pools))
	for name, limit := range configuration.Concurrency.Pools {
		var capacity *uint64
		if value, finite := limit.Value(); finite {
			converted := uint64(value)
			capacity = &converted
		}
		pools[name] = capacity
	}
	if err := service.store.SynchronizeConcurrencyLimits(ctx, global, pools, service.now().UTC()); err != nil {
		return fmt.Errorf("synchronize concurrency limits: %w", err)
	}

	return nil
}

// Submit validates and durably transfers one job to a detached supervisor.
func (service *Service) Submit(
	ctx context.Context,
	request SubmitRequest,
) (model.JobState, error) {
	executionPolicy, edges, err := service.prepareExecutionPolicy(ctx, request)
	if err != nil {
		return model.JobState{}, err
	}
	stopPolicy := request.StopPolicy
	if !request.StopPolicySet {
		stopPolicy = model.StopPolicy{GracePeriod: defaultStopGrace, ForceAfterGrace: true}
	}
	specification, err := model.NewJobSpec(model.JobSpecInput{
		Executable:             request.Executable,
		Arguments:              request.Arguments,
		WorkingDirectory:       request.WorkingDirectory,
		Environment:            request.Environment,
		UnsetEnvironment:       request.UnsetEnvironment,
		EnvironmentInheritance: model.EnvironmentInheritSubmission,
		Name:                   request.Name,
		StopPolicy:             stopPolicy,
		StdinPolicy:            request.StdinPolicy,
		ExecutionPolicy:        executionPolicy,
	})
	if err != nil {
		return model.JobState{}, fmt.Errorf("validate job specification: %w", err)
	}
	jobID, err := service.ids.NewJobID()
	if err != nil {
		return model.JobState{}, fmt.Errorf("generate job ID: %w", err)
	}
	credential := make([]byte, 32)
	if _, readErr := io.ReadFull(service.random, credential); readErr != nil {
		return model.JobState{}, fmt.Errorf("generate supervisor credential: %w", readErr)
	}
	hash, err := model.NewCredentialHash(credential)
	if err != nil {
		return model.JobState{}, err
	}
	submittedAt := service.now().UTC()
	for index := range edges {
		edges[index].JobID = jobID
	}
	if _, submitErr := service.store.SubmitWithDependencies(
		ctx,
		jobID,
		specification,
		hash,
		submittedAt,
		submittedAt.Add(defaultClaimWindow),
		edges,
	); submitErr != nil {
		return model.JobState{}, translateStoreError("submit job", submitErr)
	}
	faultinject.Hit("job-insert-committed")

	if _, launchErr := service.launch(ctx, supervisor.LaunchOptions{
		Store:      service.store,
		Executable: service.executable,
		StateDir:   service.stateDir,
		JobID:      jobID,
		Credential: credential,
	}); launchErr != nil {
		return service.reconcileFailedLaunch(ctx, jobID, launchErr)
	}

	loadCtx, cancelLoad := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancelLoad()
	job, err := service.store.GetJob(loadCtx, jobID)
	if err != nil {
		return model.JobState{}, translateStoreError("load submitted job", err)
	}

	return job, nil
}

func (service *Service) prepareExecutionPolicy(
	ctx context.Context,
	request SubmitRequest,
) (model.ExecutionPolicy, []store.Dependency, error) {
	executionPolicy := request.ExecutionPolicy
	if executionPolicy.Concurrency.Slots == 0 {
		executionPolicy.Concurrency.Slots = 1
	}
	if executionPolicy.Concurrency.Pool != "" && service.knownPools != nil {
		if _, known := service.knownPools[executionPolicy.Concurrency.Pool]; !known {
			return model.ExecutionPolicy{}, nil, fmt.Errorf(
				"validate concurrency admission: pool %q is not configured: %w",
				executionPolicy.Concurrency.Pool,
				ErrConflict,
			)
		}
	}
	if err := service.store.ValidateAdmissionRequest(
		ctx,
		executionPolicy.Concurrency.Pool,
		executionPolicy.Concurrency.Slots,
	); err != nil {
		return model.ExecutionPolicy{}, nil, fmt.Errorf(
			"validate concurrency admission: %w",
			errors.Join(ErrConflict, err),
		)
	}
	edges, dependencies, err := service.resolveDependencies(ctx, request.Dependencies)
	if err != nil {
		return model.ExecutionPolicy{}, nil, err
	}
	executionPolicy.Dependencies = dependencies

	return executionPolicy, edges, nil
}

func (service *Service) resolveDependencies(
	ctx context.Context,
	requests []DependencyRequest,
) ([]store.Dependency, []model.DependencyRequirement, error) {
	edges := make([]store.Dependency, 0, len(requests))
	dependencies := make([]model.DependencyRequirement, 0, len(requests))
	seenDependencies := make(map[model.JobID]string, len(requests))
	for _, dependency := range requests {
		resolved, resolveErr := service.store.ResolveJob(ctx, dependency.Selector)
		if resolveErr != nil {
			return nil, nil, translateStoreError("resolve dependency", resolveErr)
		}
		predicate := store.DependencyPredicate(dependency.Predicate)
		if !predicate.Valid() {
			return nil, nil, fmt.Errorf("validate dependency predicate %q: %w", dependency.Predicate, ErrConflict)
		}
		if previous, duplicate := seenDependencies[resolved.ID]; duplicate {
			if previous == dependency.Predicate {
				continue
			}

			return nil, nil, fmt.Errorf(
				"dependency %s has contradictory predicates %q and %q: %w",
				resolved.ID,
				previous,
				dependency.Predicate,
				ErrConflict,
			)
		}
		seenDependencies[resolved.ID] = dependency.Predicate
		edges = append(edges, store.Dependency{DependsOn: resolved.ID, Predicate: predicate})
		dependencies = append(dependencies, model.DependencyRequirement{
			JobID: resolved.ID, Predicate: dependency.Predicate,
		})
	}

	return edges, dependencies, nil
}

// List returns jobs newest first.
func (service *Service) List(ctx context.Context) ([]model.JobState, error) {
	jobs, err := service.store.ListJobs(ctx, store.ListJobsOptions{Limit: store.MaximumListLimit})
	if err != nil {
		return nil, translateStoreError("list jobs", err)
	}
	changed, err := service.reconcileListedJobs(ctx, jobs)
	if err != nil {
		return nil, err
	}
	if changed {
		jobs, err = service.store.ListJobs(ctx, store.ListJobsOptions{Limit: store.MaximumListLimit})
		if err != nil {
			return nil, translateStoreError("reload reconciled jobs", err)
		}
	}

	return jobs, nil
}

func (service *Service) reconcileActiveJobs(ctx context.Context, options store.ListJobsOptions) error {
	return service.reconcileActiveJobsAcrossPages(ctx, options, store.MaximumListLimit)
}

func (service *Service) reconcileActiveJobsAcrossPages(
	ctx context.Context,
	options store.ListJobsOptions,
	pageLimit int,
) error {
	options.Phase = ""
	options.Outcome = ""
	options.Active = true
	options.Completed = false
	options.Cursor = nil
	options.Limit = pageLimit
	for {
		jobs, err := service.store.ListJobs(ctx, options)
		if err != nil {
			return translateStoreError("list active jobs for reconciliation", err)
		}
		if _, err := service.reconcileListedJobs(ctx, jobs); err != nil {
			return err
		}
		if len(jobs) < options.Limit {
			return nil
		}
		last := jobs[len(jobs)-1]
		options.Cursor = &store.JobListCursor{SubmittedAt: last.SubmittedAt, ID: last.ID}
	}
}

func (service *Service) listAllJobs(ctx context.Context, options store.ListJobsOptions) ([]model.JobState, error) {
	return service.listJobsAcrossPages(ctx, options, store.MaximumListLimit)
}

func (service *Service) listJobsAcrossPages(
	ctx context.Context,
	options store.ListJobsOptions,
	pageLimit int,
) ([]model.JobState, error) {
	options.Limit = pageLimit
	jobs := make([]model.JobState, 0, pageLimit)
	for {
		page, err := service.store.ListJobs(ctx, options)
		if err != nil {
			return nil, translateStoreError("list jobs across pages", err)
		}
		jobs = append(jobs, page...)
		if len(page) < options.Limit {
			return jobs, nil
		}
		last := page[len(page)-1]
		options.Cursor = &store.JobListCursor{SubmittedAt: last.SubmittedAt, ID: last.ID}
	}
}

// ListJobs applies the v1 bounded list filters after lifecycle reconciliation.
func (service *Service) ListJobs(ctx context.Context, request ListRequest) ([]ListedJob, error) {
	if err := validateListRequest(request); err != nil {
		return nil, err
	}
	options := store.ListJobsOptions{
		Phase: request.Phase, Outcome: request.Outcome, Name: request.Name, Group: request.Group,
		SubmittedAfter: request.SubmittedAfter, SubmittedBefore: request.SubmittedBefore,
		Active: request.Active, Completed: request.Completed, Limit: request.Limit,
	}
	if err := service.reconcileActiveJobs(ctx, options); err != nil {
		return nil, err
	}
	jobs, err := service.store.ListJobs(ctx, options)
	if err != nil {
		return nil, translateStoreError("list jobs", err)
	}
	changed, err := service.reconcileListedJobs(ctx, jobs)
	if err != nil {
		return nil, err
	}
	if changed {
		jobs, err = service.store.ListJobs(ctx, options)
		if err != nil {
			return nil, translateStoreError("reload reconciled jobs", err)
		}
	}
	result := make([]ListedJob, 0, request.Limit)
	for _, job := range jobs {
		listed := ListedJob{Job: job}
		if request.ShowRuns {
			listed.Runs, err = service.store.ListRuns(ctx, job.ID)
			if err != nil {
				return nil, translateStoreError("list job runs", err)
			}
		}
		result = append(result, listed)
		if len(result) == request.Limit {
			break
		}
	}

	return result, nil
}

func (service *Service) reconcileListedJobs(ctx context.Context, jobs []model.JobState) (bool, error) {
	changed := false
	for _, job := range jobs {
		if reconciled, err := service.reconcileExpiredSubmission(ctx, job); err != nil {
			return false, err
		} else if reconciled {
			changed = true
			continue
		}
		if job.Phase == model.JobPhaseCompleted || !job.SupervisorID.Valid() {
			continue
		}
		runs, err := service.store.ListRuns(ctx, job.ID)
		if err != nil {
			return false, translateStoreError("load jobs for stale-owner reconciliation", err)
		}
		if reconciled, err := service.reconcileStaleOwnership(ctx, job, runs); err != nil {
			return false, err
		} else if reconciled {
			changed = true
		}
	}

	return changed, nil
}

func validateListRequest(request ListRequest) error {
	if request.Limit < 1 || request.Limit > store.MaximumListLimit {
		return fmt.Errorf("list jobs: limit must be between 1 and %d", store.MaximumListLimit)
	}
	if request.Active && request.Completed {
		return fmt.Errorf("list jobs: active and completed filters are mutually exclusive: %w", ErrConflict)
	}
	if request.Phase != "" && !request.Phase.Valid() {
		return fmt.Errorf("list jobs: invalid phase %q: %w", request.Phase, ErrConflict)
	}
	if request.Outcome != "" && !request.Outcome.Valid() {
		return fmt.Errorf("list jobs: invalid outcome %q: %w", request.Outcome, ErrConflict)
	}

	return nil
}

func jobMatchesListRequest(job model.JobState, request ListRequest) bool {
	policy := job.Spec.ExecutionPolicy()

	return matchesOptional(request.Phase, job.Phase) &&
		matchesOptional(request.Outcome, job.Outcome) &&
		matchesOptional(request.Name, job.Spec.Name()) &&
		matchesGroup(request.Group, policy.Groups) &&
		matchesCompletion(request.Active, request.Completed, job.Phase) &&
		matchesTimeBounds(request.SubmittedAfter, request.SubmittedBefore, job.SubmittedAt)
}

func matchesOptional[T comparable](filter, value T) bool {
	var zero T

	return filter == zero || filter == value
}

func matchesGroup(group string, groups []string) bool {
	return group == "" || slices.Contains(groups, group)
}

func matchesCompletion(active, completed bool, phase model.JobPhase) bool {
	if active {
		return phase != model.JobPhaseCompleted
	}
	if completed {
		return phase == model.JobPhaseCompleted
	}

	return true
}

func matchesTimeBounds(after, before, submitted time.Time) bool {
	return (after.IsZero() || submitted.After(after)) &&
		(before.IsZero() || submitted.Before(before))
}

// Inspect resolves one selector and returns a transactionally consistent job
// snapshot and ordered run history.
func (service *Service) Inspect(ctx context.Context, selector string) (JobDetails, error) {
	job, runs, err := service.store.GetJobWithRuns(ctx, selector)
	if err != nil {
		return JobDetails{}, translateStoreError("inspect job", err)
	}
	if reconciled, reconcileErr := service.reconcileExpiredSubmission(ctx, job); reconcileErr != nil {
		return JobDetails{}, reconcileErr
	} else if reconciled {
		job, runs, err = service.store.GetJobWithRuns(ctx, selector)
		if err != nil {
			return JobDetails{}, translateStoreError("reload reconciled job", err)
		}
	}
	if reconciled, reconcileErr := service.reconcileStaleOwnership(ctx, job, runs); reconcileErr != nil {
		return JobDetails{}, reconcileErr
	} else if reconciled {
		job, runs, err = service.store.GetJobWithRuns(ctx, selector)
		if err != nil {
			return JobDetails{}, translateStoreError("reload reconciled ownership", err)
		}
	}

	runtimeState, err := service.store.GetRuntime(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load job runtime", err)
	}
	dependencies, err := service.store.ListDependencies(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load job dependencies", err)
	}
	waitEvaluations, err := service.store.ListWaitEvaluations(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load job wait evaluations", err)
	}
	admission, admitted, err := service.store.GetAdmission(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load job admission", err)
	}
	deliveries, err := service.store.ListNotificationDeliveries(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load notification deliveries", err)
	}
	attempts, err := service.store.ListNotificationAttempts(ctx, job.ID)
	if err != nil {
		return JobDetails{}, translateStoreError("load notification attempts", err)
	}
	var admissionState *store.Admission
	if admitted {
		admissionState = &admission
	}

	return JobDetails{
		Job: job, Runs: runs, Runtime: runtimeState,
		Dependencies: dependencies, WaitEvaluations: waitEvaluations,
		Admission: admissionState, NotificationDeliveries: deliveries,
		NotificationAttempts: attempts,
	}, nil
}

func (service *Service) reconcileFailedLaunch(
	ctx context.Context,
	jobID model.JobID,
	launchErr error,
) (model.JobState, error) {
	job, getErr := service.store.GetJob(ctx, jobID)
	if getErr == nil && job.SupervisorID.Valid() && job.Phase != model.JobPhaseSubmitting {
		return job, nil
	}
	if getErr == nil {
		_, reconcileErr := service.reconcileExpiredSubmission(ctx, job)
		if reconcileErr != nil {
			return model.JobState{}, errors.Join(
				fmt.Errorf("launch job supervisor: %w", launchErr),
				reconcileErr,
			)
		}
	}

	return model.JobState{}, errors.Join(
		fmt.Errorf("launch job supervisor: %w", launchErr),
		getErr,
	)
}

func (service *Service) reconcileExpiredSubmission(
	ctx context.Context,
	job model.JobState,
) (bool, error) {
	if job.Phase != model.JobPhaseSubmitting || job.ClaimDeadline == nil ||
		service.now().UTC().Before(*job.ClaimDeadline) {
		return false, nil
	}
	_, err := service.store.MarkSubmissionFailed(
		ctx,
		job.ID,
		"supervisor_claim_expired",
		service.now().UTC(),
	)
	if err == nil {
		return true, nil
	}
	if model.IsConflict(err) || errors.Is(err, store.ErrConflict) {
		// A concurrently claiming supervisor or another client won the compare-
		// and-swap. Reloading by the caller yields the authoritative result.
		return true, nil
	}

	return false, translateStoreError("reconcile expired submission", err)
}

func (service *Service) reconcileStaleOwnership(
	ctx context.Context,
	job model.JobState,
	runs []model.RunState,
) (bool, error) {
	if job.Phase == model.JobPhaseCompleted || !job.SupervisorID.Valid() {
		return false, nil
	}
	owner, err := service.store.GetSupervisor(ctx, job.SupervisorID)
	if err != nil {
		return false, translateStoreError("load job supervisor", err)
	}
	if service.now().UTC().Before(owner.LeaseExpiresAt) {
		return false, nil
	}

	identity := platform.ProcessIdentity{
		PID:      owner.Process.PID,
		Creation: owner.Process.CreationID,
		Boot:     owner.Process.BootID,
		Tree:     owner.Process.TreeID,
	}
	alive, aliveErr := service.targetAlive(identity)
	if aliveErr != nil && !errors.Is(aliveErr, platform.ErrIdentityMismatch) {
		return false, fmt.Errorf("reconcile stale supervisor identity: %w", aliveErr)
	}
	if alive {
		return false, nil
	}

	activeRun := findRun(runs, job.ActiveRunID)
	var logs *model.LogMetadata
	if activeRun != nil {
		recovered := recoverLogMetadata(service.stateDir, job.ID, *activeRun)
		logs = &recovered
	}
	_, err = service.store.MarkOwnershipLost(
		ctx,
		job.ID,
		logs,
		"supervisor_lease_expired",
		service.now().UTC(),
	)
	if err == nil || model.IsConflict(err) || errors.Is(err, store.ErrConflict) {
		return true, nil
	}

	return false, translateStoreError("record lost job ownership", err)
}

func findRun(runs []model.RunState, id model.RunID) *model.RunState {
	for index := range runs {
		if runs[index].ID == id {
			return &runs[index]
		}
	}

	return nil
}

func recoverLogMetadata(
	stateDir string,
	jobID model.JobID,
	run model.RunState,
) model.LogMetadata {
	logs := run.Logs
	logs.Integrity = model.LogIntegrityPartial
	logs.RecordingHealth = model.RecordingDegraded
	logs.DiagnosticCode = "supervisor_lease_expired"
	if stdout, err := os.Stat(logs.StdoutPath); err == nil {
		logs.StdoutSize = stdout.Size()
	}
	if stderr, err := os.Stat(logs.StderrPath); err == nil {
		logs.StderrSize = stderr.Size()
	}
	reader, err := logstore.OpenRun(stateDir, jobID.String(), run.Number)
	if err != nil {
		return logs
	}
	status, err := reader.ScanIndex(nil)
	if err != nil {
		if errors.Is(err, logstore.ErrCorruptIndex) {
			logs.Integrity = model.LogIntegrityCorrupt
		}

		return logs
	}
	if !status.TornTail && status.UnindexedStdoutBytes == 0 && status.UnindexedStderrBytes == 0 {
		logs.Integrity = model.LogIntegrityValid
	}

	return logs
}

// ReadLogs reads the latest run using raw stream or observed combined order.
func (service *Service) ReadLogs(
	ctx context.Context,
	selector string,
	stream LogStream,
) ([]byte, error) {
	return service.ReadRunLogs(ctx, selector, stream, 0)
}

// ReadRunLogs reads a selected run; zero selects the latest run.
func (service *Service) ReadRunLogs(
	ctx context.Context,
	selector string,
	stream LogStream,
	runNumber uint64,
) ([]byte, error) {
	details, err := service.Inspect(ctx, selector)
	if err != nil {
		return nil, err
	}
	if len(details.Runs) == 0 {
		return nil, fmt.Errorf("read job logs: %w", ErrConflict)
	}
	run := details.Runs[len(details.Runs)-1]
	if runNumber != 0 {
		found := false
		for _, candidate := range details.Runs {
			if candidate.Number == runNumber {
				run = candidate
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("read job logs: run %d: %w", runNumber, ErrNotFound)
		}
	}
	if !run.Logs.Available() {
		return nil, fmt.Errorf(
			"read job logs: run %d was pruned at %s: %w",
			run.Number,
			run.Logs.PrunedAt.UTC().Format(time.RFC3339Nano),
			ErrNotFound,
		)
	}
	reader, err := logstore.OpenRun(service.stateDir, details.Job.ID.String(), run.Number)
	if err != nil {
		return nil, fmt.Errorf("open job logs: %w", err)
	}

	var output bytes.Buffer
	switch stream {
	case LogStdout:
		_, err = reader.CopyStream(&output, logstore.Stdout)
	case LogStderr:
		_, err = reader.CopyStream(&output, logstore.Stderr)
	case LogBoth:
		_, _, err = reader.CopyCombined(&output)
	default:
		return nil, fmt.Errorf("read job logs: invalid stream %q", stream)
	}
	if err != nil {
		return nil, fmt.Errorf("read job logs: %w", err)
	}

	return output.Bytes(), nil
}

// Cancel durably records cancellation before signaling the verified tree.
func (service *Service) Cancel(ctx context.Context, selector string) (model.JobState, error) {
	job, runs, err := service.store.GetJobWithRuns(ctx, selector)
	if err != nil {
		return model.JobState{}, translateStoreError("resolve job", err)
	}
	job, err = service.reconcileBeforeCancellation(ctx, job, runs)
	if err != nil {
		return model.JobState{}, err
	}
	if job.Phase == model.JobPhaseCompleted {
		return job, nil
	}
	result, err := service.store.RequestCancellation(ctx, job.ID, service.now().UTC())
	if err != nil {
		return model.JobState{}, translateStoreError("request cancellation", err)
	}
	faultinject.Hit("cancellation-intent-committed")

	stopped, err := service.stopCancelledTarget(ctx, job, result)
	faultinject.Hit("cancellation-signal-sent")

	return stopped, err
}

func (service *Service) reconcileBeforeCancellation(
	ctx context.Context,
	job model.JobState,
	runs []model.RunState,
) (model.JobState, error) {
	reconciled, err := service.reconcileStaleOwnership(ctx, job, runs)
	if err != nil || !reconciled {
		return job, err
	}
	reloaded, err := service.store.GetJob(ctx, job.ID)
	if err != nil {
		return model.JobState{}, translateStoreError("reload reconciled job", err)
	}

	return reloaded, nil
}

func (service *Service) stopCancelledTarget(
	ctx context.Context,
	prior model.JobState,
	result model.TransitionResult,
) (model.JobState, error) {
	// A retried cancellation may observe a completed run that retains its
	// historical process identity. Only an active stop intent permits signaling.
	if result.Run == nil || result.Run.Process == nil || result.Run.Phase != model.RunPhaseStopping {
		return result.Job, nil
	}

	identity := platform.ProcessIdentity{
		PID:      result.Run.Process.PID,
		Creation: result.Run.Process.CreationID,
		Boot:     result.Run.Process.BootID,
		Tree:     result.Run.Process.TreeID,
	}
	if result.Run.Phase == model.RunPhaseStopping && prior.Phase == model.JobPhasePaused {
		if resumeErr := service.resumeTarget(identity); resumeErr != nil &&
			!errors.Is(resumeErr, platform.ErrUnsupported) {
			return result.Job, fmt.Errorf("resume paused target before cancellation: %w", resumeErr)
		}
	}
	if terminateErr := service.terminateTarget(identity, false); terminateErr != nil {
		return result.Job, fmt.Errorf("request graceful target termination: %w", terminateErr)
	}
	if !result.Job.Spec.StopPolicy().ForceAfterGrace {
		return result.Job, nil
	}
	if waitErr := waitForExitWithAlive(
		ctx, identity, result.Job.Spec.StopPolicy().GracePeriod, service.targetAlive,
	); waitErr != nil {
		return result.Job, waitErr
	}
	alive, err := service.targetAlive(identity)
	if err != nil && !errors.Is(err, platform.ErrIdentityMismatch) {
		return result.Job, fmt.Errorf("recheck target before forced termination: %w", err)
	}
	if alive {
		if forceErr := service.terminateTarget(identity, true); forceErr != nil {
			return result.Job, fmt.Errorf("force target termination: %w", forceErr)
		}
	}

	return result.Job, nil
}

// Pause durably records pause intent before applying a platform process-tree
// suspension effect.
func (service *Service) Pause(ctx context.Context, selector string) (model.JobState, error) {
	job, err := service.store.ResolveJob(ctx, selector)
	if err != nil {
		return model.JobState{}, translateStoreError("resolve job", err)
	}
	if job.ActiveRunID != "" {
		run, runErr := service.store.GetRun(ctx, job.ActiveRunID)
		if runErr != nil {
			return model.JobState{}, translateStoreError("load active run", runErr)
		}
		if run.Process != nil && !service.targetPauseResumeSupported() {
			return job, fmt.Errorf("pause active target: %w", platform.ErrUnsupported)
		}
	}
	result, err := service.store.Pause(ctx, job.ID, service.now().UTC())
	if err != nil {
		return model.JobState{}, translateStoreError("pause job", err)
	}
	if result.Run == nil || result.Run.Process == nil {
		return result.Job, nil
	}
	identity := platform.ProcessIdentity{
		PID: result.Run.Process.PID, Creation: result.Run.Process.CreationID, Boot: result.Run.Process.BootID,
		Tree: result.Run.Process.TreeID,
	}
	if err := service.pauseTarget(identity); err != nil {
		rollback, rollbackErr := service.store.Resume(ctx, job.ID, service.now().UTC())
		if rollbackErr == nil {
			result.Job = rollback.Job
		}

		return result.Job, errors.Join(fmt.Errorf("pause target: %w", err), rollbackErr)
	}

	return result.Job, nil
}

// Resume restores a paused scheduler phase before continuing a suspended tree.
func (service *Service) Resume(ctx context.Context, selector string) (model.JobState, error) {
	job, err := service.store.ResolveJob(ctx, selector)
	if err != nil {
		return model.JobState{}, translateStoreError("resolve job", err)
	}
	if job.ActiveRunID != "" && !service.targetPauseResumeSupported() {
		return job, fmt.Errorf("resume active target: %w", platform.ErrUnsupported)
	}
	result, err := service.store.Resume(ctx, job.ID, service.now().UTC())
	if err != nil {
		return model.JobState{}, translateStoreError("resume job", err)
	}
	if result.Run == nil || result.Run.Process == nil {
		return result.Job, nil
	}
	identity := platform.ProcessIdentity{
		PID: result.Run.Process.PID, Creation: result.Run.Process.CreationID, Boot: result.Run.Process.BootID,
		Tree: result.Run.Process.TreeID,
	}
	if err := service.resumeTarget(identity); err != nil {
		rollback, rollbackErr := service.store.Pause(ctx, job.ID, service.now().UTC())
		if rollbackErr == nil {
			result.Job = rollback.Job
		}

		return result.Job, errors.Join(fmt.Errorf("resume target: %w", err), rollbackErr)
	}

	return result.Job, nil
}

// Wait blocks until the selected job reaches a terminal snapshot.
func (service *Service) Wait(ctx context.Context, selector string) (model.JobState, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, runs, err := service.store.GetJobWithRuns(ctx, selector)
		if err != nil {
			return model.JobState{}, translateStoreError("wait for job", err)
		}
		job, err = service.reconcileWaitSnapshot(ctx, job, runs)
		if err != nil {
			return model.JobState{}, err
		}
		if job.Phase == model.JobPhaseCompleted {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return model.JobState{}, fmt.Errorf("wait for job: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (service *Service) reconcileWaitSnapshot(
	ctx context.Context,
	job model.JobState,
	runs []model.RunState,
) (model.JobState, error) {
	reconciled, err := service.reconcileExpiredSubmission(ctx, job)
	if err != nil {
		return model.JobState{}, err
	}
	operation := "reload reconciled submission"
	if !reconciled {
		reconciled, err = service.reconcileStaleOwnership(ctx, job, runs)
		if err != nil {
			return model.JobState{}, err
		}
		operation = "reload reconciled ownership"
	}
	if !reconciled {
		return job, nil
	}
	reloaded, err := service.store.GetJob(ctx, job.ID)
	if err != nil {
		return model.JobState{}, translateStoreError(operation, err)
	}

	return reloaded, nil
}

// Rerun clones a prior immutable specification and submits it with resolved
// dependency IDs unchanged.
func (service *Service) Rerun(
	ctx context.Context,
	selector string,
	overrides RerunRequest,
) (model.JobState, error) {
	details, err := service.Inspect(ctx, selector)
	if err != nil {
		return model.JobState{}, err
	}
	specification := details.Job.Spec
	name := specification.Name()
	if overrides.Name != "" {
		name = overrides.Name
	}
	executionPolicy := specification.ExecutionPolicy()
	dependencies := make([]DependencyRequest, len(executionPolicy.Dependencies))
	for index, dependency := range executionPolicy.Dependencies {
		dependencies[index] = DependencyRequest{
			Selector: dependency.JobID.String(), Predicate: dependency.Predicate,
		}
	}

	return service.Submit(ctx, SubmitRequest{
		Name:             name,
		Executable:       specification.Executable(),
		Arguments:        specification.Arguments(),
		WorkingDirectory: specification.WorkingDirectory(),
		Environment:      specification.Environment(),
		UnsetEnvironment: specification.UnsetEnvironment(),
		StdinPolicy:      specification.StdinPolicy(),
		StopPolicy:       specification.StopPolicy(),
		StopPolicySet:    true,
		ExecutionPolicy:  executionPolicy,
		Dependencies:     dependencies,
	})
}

// SendInput writes bounded binary data through the private local supervisor
// endpoint and durably records an accepted EOF request.
//
//nolint:gocognit,cyclop // Streaming keeps partial-delivery, run-identity, read, send, and EOF failures distinct.
func (service *Service) SendInput(
	ctx context.Context,
	selector string,
	source io.Reader,
	sendEOF bool,
) (liveinput.Result, error) {
	if source == nil {
		return liveinput.Result{}, errors.New("send live input: source is nil")
	}
	job, err := service.store.ResolveJob(ctx, selector)
	if err != nil {
		return liveinput.Result{}, translateStoreError("resolve input job", err)
	}
	runtimeState, runID, err := service.waitForInputTarget(ctx, job.ID)
	if err != nil {
		return liveinput.Result{}, err
	}
	if sendEOF && runtimeState.InputEOFRequested {
		return liveinput.Result{}, fmt.Errorf("send live-input EOF: %w", ErrConflict)
	}
	result := liveinput.Result{}
	buffer := make([]byte, 64*1024)
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			part, sendErr := service.sendLiveInput(
				ctx,
				runtimeState.InputEndpoint,
				runID.String(),
				bytes.NewReader(buffer[:count]),
				false,
			)
			result.Delivered += part.Delivered
			if sendErr != nil {
				return result, fmt.Errorf("send live input after %d bytes: %w", result.Delivered, sendErr)
			}
			current, getErr := service.store.GetJob(ctx, job.ID)
			if getErr != nil {
				return result, translateStoreError("verify live-input run", getErr)
			}
			if current.ActiveRunID != runID {
				return result, fmt.Errorf("send live input: active run changed after %d bytes: %w", result.Delivered, ErrConflict)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, fmt.Errorf("read live input after %d bytes: %w", result.Delivered, readErr)
		}
	}
	if sendEOF {
		// Persist EOF against the expected active run before touching the pipe.
		// The supervisor also observes this durable intent, closing the crash
		// window between an accepted request and transport delivery.
		if err := service.store.RecordInputEOF(ctx, job.ID, runID, service.now().UTC()); err != nil {
			return result, fmt.Errorf("record live-input EOF: %w", err)
		}
		result.EOF = true
		part, sendErr := service.sendLiveInput(
			ctx,
			runtimeState.InputEndpoint,
			runID.String(),
			bytes.NewReader(nil),
			true,
		)
		if sendErr == nil {
			result.EOF = part.EOF
		}
		// The durable intent is authoritative. A transport race or client-side
		// disconnect cannot revoke it; the supervisor polling loop applies it.
	}

	return result, nil
}

func (service *Service) sendLiveInput(
	ctx context.Context,
	endpoint string,
	runID string,
	source io.Reader,
	sendEOF bool,
) (liveinput.Result, error) {
	if service.sendInput != nil {
		return service.sendInput(ctx, endpoint, runID, source, sendEOF)
	}

	return liveinput.Send(ctx, endpoint, runID, source, sendEOF)
}

func (service *Service) waitForInputTarget(
	ctx context.Context,
	jobID model.JobID,
) (store.JobRuntime, model.RunID, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := service.store.GetJob(ctx, jobID)
		if err != nil {
			return store.JobRuntime{}, "", translateStoreError("load live-input job", err)
		}
		runtimeState, err := service.store.GetRuntime(ctx, jobID)
		if err != nil {
			return store.JobRuntime{}, "", translateStoreError("load live-input endpoint", err)
		}
		if job.ActiveRunID != "" && runtimeState.InputEndpoint != "" {
			run, runErr := service.store.GetRun(ctx, job.ActiveRunID)
			if runErr != nil {
				return store.JobRuntime{}, "", translateStoreError("load live-input run", runErr)
			}
			if run.Phase == model.RunPhaseRunning || run.Phase == model.RunPhasePaused {
				return runtimeState, job.ActiveRunID, nil
			}
		}
		if job.Phase == model.JobPhaseCompleted || job.Spec.StdinPolicy() != model.StdinLive {
			return store.JobRuntime{}, "", fmt.Errorf("send live input: %w", ErrConflict)
		}
		select {
		case <-ctx.Done():
			return store.JobRuntime{}, "", fmt.Errorf("wait for live-input target: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// FollowLogs streams one selected run until its capture is finalized.
//
//nolint:gocognit,cyclop // Selection, initial-run waiting, pruning, and three stream modes are one CLI operation.
func (service *Service) FollowLogs(
	ctx context.Context,
	selector string,
	stream LogStream,
	runNumber uint64,
	destination io.Writer,
) error {
	var details JobDetails
	var err error
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		details, err = service.Inspect(ctx, selector)
		if err != nil {
			return err
		}
		if len(details.Runs) > 0 {
			break
		}
		if details.Job.Phase == model.JobPhaseCompleted {
			return fmt.Errorf("follow job logs: job completed without a run: %w", ErrConflict)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("follow job logs: %w", ctx.Err())
		case <-ticker.C:
		}
	}
	run := details.Runs[len(details.Runs)-1]
	if runNumber != 0 {
		found := false
		for _, candidate := range details.Runs {
			if candidate.Number == runNumber {
				run = candidate
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("follow job logs: run %d: %w", runNumber, ErrNotFound)
		}
	}
	if !run.Logs.Available() {
		return fmt.Errorf(
			"follow job logs: run %d was pruned at %s: %w",
			run.Number,
			run.Logs.PrunedAt.UTC().Format(time.RFC3339Nano),
			ErrNotFound,
		)
	}
	reader, err := logstore.OpenRun(service.stateDir, details.Job.ID.String(), run.Number)
	if err != nil {
		return fmt.Errorf("open followed logs: %w", err)
	}
	options := logstore.FollowOptions{Complete: func(checkCtx context.Context) (bool, error) {
		current, getErr := service.store.GetRun(checkCtx, run.ID)
		return current.Phase == model.RunPhaseCompleted, getErr
	}}
	switch stream {
	case LogStdout:
		_, err = reader.FollowStream(ctx, destination, logstore.Stdout, options)
	case LogStderr:
		_, err = reader.FollowStream(ctx, destination, logstore.Stderr, options)
	case LogBoth:
		_, err = reader.FollowCombined(ctx, destination, options)
	default:
		return fmt.Errorf("follow job logs: invalid stream %q", stream)
	}
	if err != nil {
		return fmt.Errorf("follow job logs: %w", err)
	}

	return nil
}

// Clean safely removes completed log sets selected explicitly, by effective
// retention policy, or by an all-completed request. Unscoped age cleanup
// retains tombstone metadata; explicitly selected and all-completed cleanup
// also prune eligible history.
//
//nolint:gocognit,cyclop,maintidx // Cleanup keeps policy planning, state rechecks, deletion, and tombstoning together.
func (service *Service) Clean(ctx context.Context, request CleanRequest) (CleanResult, error) {
	var jobs []model.JobState
	if request.Selector != "" {
		job, err := service.store.ResolveJob(ctx, request.Selector)
		if err != nil {
			return CleanResult{}, translateStoreError("resolve cleanup job", err)
		}
		if err := validateSelectedCleanupJob(job); err != nil {
			return CleanResult{}, err
		}
		jobs = []model.JobState{job}
	} else {
		var err error
		jobs, err = service.listAllJobs(ctx, store.ListJobsOptions{Phase: model.JobPhaseCompleted})
		if err != nil {
			return CleanResult{}, err
		}
	}
	type retainedRun struct {
		job       model.JobState
		run       model.RunState
		candidate logstore.RetentionCandidate
	}
	all := make([]retainedRun, 0)
	for _, job := range jobs {
		runs, err := service.store.ListRuns(ctx, job.ID)
		if err != nil {
			return CleanResult{}, translateStoreError("list cleanup runs", err)
		}
		for _, run := range runs {
			if run.Phase == model.RunPhaseCompleted && !run.Logs.Available() {
				if finalizeErr := logstore.FinalizeCleanupRun(
					service.stateDir,
					job.ID.String(),
					run.Number,
				); finalizeErr != nil {
					return CleanResult{}, fmt.Errorf(
						"finalize prior cleanup %s/%d: %w",
						job.ID,
						run.Number,
						finalizeErr,
					)
				}
			}
			if run.Phase != model.RunPhaseCompleted || run.CompletedAt == nil ||
				job.Phase != model.JobPhaseCompleted || !run.Logs.Available() {
				continue
			}
			retainedBytes := uint64(run.Logs.StdoutSize) + uint64(run.Logs.StderrSize) //nolint:gosec // Persisted sizes are nonnegative.
			all = append(all, retainedRun{job: job, run: run, candidate: logstore.RetentionCandidate{
				JobID: job.ID.String(), RunNumber: run.Number, CompletedAt: run.CompletedAt.UTC(), Bytes: retainedBytes,
			}})
		}
	}

	selected := make(map[string]struct{}, len(all))
	now := service.now().UTC()
	switch {
	case request.All:
		for _, item := range all {
			selected[cleanupCandidateKey(item.candidate.JobID, item.candidate.RunNumber)] = struct{}{}
		}
	case request.UsePolicy && request.Selector == "":
		candidates := make([]logstore.RetentionCandidate, len(all))
		for index, item := range all {
			candidates[index] = item.candidate
		}
		planned, err := logstore.PlanRetention(now, candidates, retentionPlanPolicy(service.retention))
		if err != nil {
			return CleanResult{}, fmt.Errorf("plan log retention: %w", err)
		}
		for _, candidate := range planned {
			selected[cleanupCandidateKey(candidate.JobID, candidate.RunNumber)] = struct{}{}
		}
		for _, item := range all {
			policy := item.job.Spec.ExecutionPolicy()
			if !policy.LogRetentionUnlimited && !item.candidate.CompletedAt.After(now.Add(-policy.LogRetentionMaxAge)) {
				selected[cleanupCandidateKey(item.candidate.JobID, item.candidate.RunNumber)] = struct{}{}
			}
		}
		if maximumAge, finite := service.retention.CompletedMetadataMaxAge.Value(); finite {
			metadataCutoff := now.Add(-maximumAge)
			for _, item := range all {
				if item.job.CompletedAt != nil && !item.job.CompletedAt.After(metadataCutoff) {
					selected[cleanupCandidateKey(item.candidate.JobID, item.candidate.RunNumber)] = struct{}{}
				}
			}
		}
	default:
		cutoff := now.Add(-request.OlderThan)
		for _, item := range all {
			if !item.candidate.CompletedAt.After(cutoff) {
				selected[cleanupCandidateKey(item.candidate.JobID, item.candidate.RunNumber)] = struct{}{}
			}
		}
	}
	sort.Slice(all, func(left, right int) bool {
		return all[left].candidate.CompletedAt.Before(all[right].candidate.CompletedAt)
	})
	result := CleanResult{}
	for _, item := range all {
		if _, chosen := selected[cleanupCandidateKey(item.candidate.JobID, item.candidate.RunNumber)]; !chosen {
			continue
		}
		if request.DryRun {
			result.Runs++
			result.Bytes += item.candidate.Bytes
			continue
		}
		job := item.job
		run := item.run
		cleaned, cleanErr := logstore.CleanupRun(
			ctx,
			service.stateDir,
			job.ID.String(),
			run.Number,
			func(checkCtx context.Context) (bool, error) {
				currentRun, getErr := service.store.GetRun(checkCtx, run.ID)
				if getErr != nil {
					return false, getErr
				}
				currentJob, getErr := service.store.GetJob(checkCtx, job.ID)
				return currentRun.Phase == model.RunPhaseCompleted && currentRun.Logs.Available() &&
					currentJob.Phase == model.JobPhaseCompleted, getErr
			},
		)
		if cleanErr != nil {
			return result, fmt.Errorf("clean run %s/%d: %w", job.ID, run.Number, cleanErr)
		}
		faultinject.Hit("cleanup-files-removed-before-metadata")
		if markErr := service.store.MarkRunLogsPruned(
			ctx,
			run.ID,
			service.now().UTC(),
			cleaned.Files,
			cleaned.Bytes,
		); markErr != nil {
			return result, fmt.Errorf("record cleaned run %s/%d: %w", job.ID, run.Number, markErr)
		}
		if finalizeErr := logstore.FinalizeCleanupRun(service.stateDir, job.ID.String(), run.Number); finalizeErr != nil {
			return result, fmt.Errorf("finalize cleaned run %s/%d: %w", job.ID, run.Number, finalizeErr)
		}
		result.Runs++
		result.Files += cleaned.Files
		result.Bytes += cleaned.Bytes
	}
	metadataCutoff := time.Time{}
	if request.Selector != "" {
		metadataCutoff = now.Add(-request.OlderThan)
	}
	if request.All {
		metadataCutoff = now
	} else if request.UsePolicy && request.Selector == "" {
		if maximumAge, finite := service.retention.CompletedMetadataMaxAge.Value(); finite {
			metadataCutoff = now.Add(-maximumAge)
		}
	}
	if !metadataCutoff.IsZero() {
		for _, job := range jobs {
			if job.Phase != model.JobPhaseCompleted || job.CompletedAt == nil ||
				job.CompletedAt.After(metadataCutoff) {
				continue
			}
			pruned, pruneErr := service.store.PruneCompletedJobMetadata(
				ctx,
				job.ID,
				metadataCutoff,
				request.DryRun,
			)
			if pruneErr != nil {
				return result, fmt.Errorf("clean job metadata %s: %w", job.ID, pruneErr)
			}
			if pruned {
				result.Jobs++
			}
		}
	}

	return result, nil
}

func validateSelectedCleanupJob(job model.JobState) error {
	if job.Phase == model.JobPhaseCompleted {
		return nil
	}

	return fmt.Errorf(
		"clean job %s: job is not completed (phase %s): %w",
		job.ID,
		job.Phase,
		ErrConflict,
	)
}

func cleanupCandidateKey(jobID string, runNumber uint64) string {
	return fmt.Sprintf("%s/%d", jobID, runNumber)
}

func retentionPlanPolicy(configuration config.Retention) logstore.RetentionPolicy {
	maximumAge := logstore.UnlimitedRetentionAge()
	if value, finite := configuration.CompletedLogMaxAge.Value(); finite {
		maximumAge = logstore.RetentionAgeLimit{Maximum: value}
	}
	return logstore.RetentionPolicy{
		MaxAge:         maximumAge,
		MaxJobs:        integerRetentionLimit(configuration.MaxJobs),
		MaxRunsPerJob:  integerRetentionLimit(configuration.MaxRunsPerJob),
		MaxBytesPerJob: byteRetentionLimit(configuration.MaxLogBytesPerJob),
		MaxTotalBytes:  byteRetentionLimit(configuration.MaxTotalLogBytes),
	}
}

func integerRetentionLimit(value config.IntegerLimit) logstore.RetentionLimit {
	if !value.IsSet() || value.IsUnlimited() {
		return logstore.UnlimitedRetentionLimit()
	}
	maximum, _ := value.Value()
	return logstore.RetentionLimit{Maximum: maximum}
}

func byteRetentionLimit(value config.ByteLimit) logstore.RetentionLimit {
	if !value.IsSet() || value.IsUnlimited() {
		return logstore.UnlimitedRetentionLimit()
	}
	maximum, _ := value.Value()
	return logstore.RetentionLimit{Maximum: maximum}
}

func (service *Service) targetAlive(identity platform.ProcessIdentity) (bool, error) {
	if service.processAlive != nil {
		return service.processAlive(identity)
	}

	return platform.Alive(identity)
}

func (service *Service) pauseTarget(identity platform.ProcessIdentity) error {
	if service.processPause != nil {
		return service.processPause(identity)
	}

	return platform.Pause(identity)
}

func (service *Service) resumeTarget(identity platform.ProcessIdentity) error {
	if service.processResume != nil {
		return service.processResume(identity)
	}

	return platform.Resume(identity)
}

func (service *Service) terminateTarget(identity platform.ProcessIdentity, force bool) error {
	if service.processTerminate != nil {
		return service.processTerminate(identity, force)
	}

	return platform.Terminate(identity, force)
}

func (service *Service) targetPauseResumeSupported() bool {
	if service.pauseResumeSupported != nil {
		return service.pauseResumeSupported()
	}

	return platform.PauseResumeSupported()
}

func waitForExit(ctx context.Context, identity platform.ProcessIdentity, grace time.Duration) error {
	return waitForExitWithAlive(ctx, identity, grace, platform.Alive)
}

func waitForExitWithAlive(
	ctx context.Context,
	identity platform.ProcessIdentity,
	grace time.Duration,
	alive func(platform.ProcessIdentity) (bool, error),
) error {
	if grace <= 0 {
		return nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for target termination: %w", ctx.Err())
		case <-timer.C:
			return nil
		case <-ticker.C:
			running, err := alive(identity)
			if errors.Is(err, platform.ErrIdentityMismatch) || (!running && err == nil) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("wait for target termination: %w", err)
			}
		}
	}
}

func translateStoreError(operation string, err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%s: %w", operation, ErrNotFound)
	case errors.Is(err, store.ErrAmbiguous):
		return fmt.Errorf("%s: %w", operation, ErrAmbiguous)
	case errors.Is(err, store.ErrConflict), model.IsConflict(err):
		return fmt.Errorf("%s: %w", operation, ErrConflict)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

var _ Backend = (*Service)(nil)
