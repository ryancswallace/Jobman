//go:build linux

package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/policy"
)

// A probe can finish after a client pauses the waiting job, while the
// scheduler still holds the preceding waiting snapshot. That transition must
// not kill the supervisor and eventually turn an otherwise valid job "lost".
func TestAwaitRunnablePauseDuringWaitEvaluation(t *testing.T) {
	directory := t.TempDir()
	probeStarted := filepath.Join(directory, "probe-started")
	probeRelease := filepath.Join(directory, "probe-release")
	configuration := model.DefaultExecutionPolicy()
	configuration.WaitConditions = []model.WaitCondition{{
		Kind:         model.WaitProbe,
		PollInterval: 10 * time.Millisecond,
		Probe: policy.ProbeSpec{
			Executable: "/bin/sh",
			Arguments: []string{
				"-c",
				`printf 'ready' > "$1"; while [ ! -e "$2" ]; do sleep 0.01; done`,
				"probe", probeStarted, probeRelease,
			},
			Timeout: 5 * time.Second, OutputLimit: 32, FatalOnError: true,
		},
	}}
	fixture := submitSupervisorFixtureWithPolicy(t, true, false, configuration)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)
	job := claimCoverageFixture(t, database, fixture, time.Minute)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type outcome struct {
		result schedulerResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := awaitRunnable(ctx, ctx, database, job.ID, fixedJitterSource(0))
		done <- outcome{result: result, err: err}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(probeStarted); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect probe marker: %v", err)
		}
		select {
		case got := <-done:
			t.Fatalf("scheduler exited before probe could be paused: %+v", got)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wait for probe start: %v", ctx.Err())
		}
	}

	if _, err := database.Pause(ctx, job.ID, time.Now().UTC()); err != nil {
		t.Fatalf("pause waiting job during probe: %v", err)
	}
	if err := os.WriteFile(probeRelease, []byte("continue"), 0o600); err != nil {
		t.Fatalf("release wait probe: %v", err)
	}

	// Wait for the probe's completed evaluation, not an arbitrary sleep: the
	// scheduler is required to observe the pause before admitting the job.
	for {
		evaluations, err := database.ListWaitEvaluations(ctx, job.ID)
		if err != nil {
			t.Fatalf("read probe evaluations: %v", err)
		}
		if len(evaluations) > 0 && evaluations[0].SatisfiedAt != nil {
			break
		}
		select {
		case got := <-done:
			t.Fatalf("scheduler exited on a concurrent pause: %+v", got)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wait for completed probe evaluation: %v", ctx.Err())
		}
	}
	paused, err := database.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("read paused job: %v", err)
	}
	if paused.Phase != model.JobPhasePaused {
		t.Fatalf("phase after ready probe = %q, want paused", paused.Phase)
	}
	// Allow a scheduler tick while the persisted phase is still paused.
	// Without the conflict recovery, the scheduler exits on its stale
	// waiting -> queued transition and the owner eventually becomes lost.
	pausedTick := time.NewTimer(250 * time.Millisecond)
	defer pausedTick.Stop()
	select {
	case got := <-done:
		t.Fatalf("scheduler exited instead of respecting pause: %+v", got)
	case <-pausedTick.C:
	case <-ctx.Done():
		t.Fatalf("wait while paused: %v", ctx.Err())
	}

	if _, err := database.Resume(ctx, job.ID, time.Now().UTC()); err != nil {
		t.Fatalf("resume waiting job: %v", err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.terminal || got.result.job.Phase != model.JobPhaseStarting {
			t.Fatalf("scheduler after resume = (%+v, %v), want runnable", got.result, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for resumed scheduling: %v", ctx.Err())
	}
}


func TestQueueSatisfiedWaitingJobRequiresAChangedRevision(t *testing.T) {
	fixture := submitSupervisorFixture(t, true)
	database := openSupervisorStore(t, fixture.stateDir)
	defer closeSupervisorStore(t, database)

	job := claimCoverageFixture(t, database, fixture, time.Minute)
	now := time.Now().UTC()
	waiting, err := database.MoveJob(t.Context(), job.ID, model.JobPhaseWaiting, now, "prerequisites")
	if err != nil {
		t.Fatalf("move job to waiting: %v", err)
	}
	if _, err := database.Pause(t.Context(), job.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("pause waiting job: %v", err)
	}

	// The scheduler's preceding waiting snapshot is now stale. It should
	// reread the paused state rather than terminating the supervisor.
	observed, retry, err := queueSatisfiedWaitingJob(t.Context(), database, waiting.Job, now.Add(2*time.Millisecond))
	if err != nil || !retry || observed.Phase != model.JobPhasePaused {
		t.Fatalf("queue stale waiting snapshot = (%+v, retry=%t, %v)", observed, retry, err)
	}

	// An unchanged invalid phase is a real conflict, not permission to loop.
	observed, retry, err = queueSatisfiedWaitingJob(t.Context(), database, observed, now.Add(3*time.Millisecond))
	if !model.IsConflict(err) || retry {
		t.Fatalf("queue unchanged paused snapshot = (%+v, retry=%t, %v)", observed, retry, err)
	}
	if _, err := database.Resume(t.Context(), job.ID, now.Add(4*time.Millisecond)); err != nil {
		t.Fatalf("resume waiting job: %v", err)
	}
	reloaded, err := database.GetJob(t.Context(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	queued, retry, err := queueSatisfiedWaitingJob(t.Context(), database, reloaded, now.Add(5*time.Millisecond))
	if err != nil || retry || queued.Phase != model.JobPhaseQueued {
		t.Fatalf("queue resumed waiting job = (%+v, retry=%t, %v)", queued, retry, err)
	}
}
