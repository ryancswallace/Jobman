package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/internal/model"
)

type cancellationEventIDs func() (model.EventID, error)

func (source cancellationEventIDs) NewEventID() (model.EventID, error) { return source() }

func TestCancellationRecomputesAfterConcurrentProcessStart(t *testing.T) {
	t.Parallel()
	database, jobID, now := claimedRuntimeFixture(t, 0x3c100)
	runID := mustRunID(t, 0x3c102)
	logs := testLogs(database, jobID, model.LogIntegrityPending, model.RecordingHealthy)
	if _, err := database.ReserveRun(t.Context(), jobID, runID, 1, logs, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	other := openTestStoreAt(t, database.StateDir(), newSequentialEventIDs(0x3c200))
	source := database.eventIDs
	interleaved := false
	database.eventIDs = cancellationEventIDs(func() (model.EventID, error) {
		if !interleaved {
			interleaved = true
			if _, err := other.MarkProcessStarted(t.Context(), jobID, runID, "/bin/true",
				testProcessIdentity(8123, "concurrent-start"), now.Add(3*time.Second)); err != nil {
				return "", err
			}
		}
		return source.NewEventID()
	})
	// Intent can precede publication of the running snapshot, even after the
	// child has become ready. Retrying must preserve that original timestamp.
	requestedAt := now.Add(2500 * time.Millisecond)
	result, err := database.RequestCancellation(t.Context(), jobID, requestedAt)
	if err != nil {
		t.Fatalf("cancel during process startup: %v", err)
	}
	if result.Run == nil || result.Run.Process == nil || result.Run.Phase != model.RunPhaseStopping ||
		len(result.Effects) != 1 || result.Effects[0].Type != model.EffectStopTarget {
		t.Fatalf("cancellation did not recompute its stop effect: %+v", result)
	}
	if result.Job.Cancellation == nil || !result.Job.Cancellation.RequestedAt.Equal(requestedAt) ||
		result.Run.StopRequestedAt == nil || !result.Run.StopRequestedAt.Equal(requestedAt) {
		t.Fatal("cancellation changed the original request time")
	}
	assertJobSnapshot(t, database, result.Job)
	assertRunSnapshot(t, database, *result.Run)
	assertCancellationEventCount(t, database, jobID, 2)
}

func TestCancellationRetryObservesConcurrentTerminalState(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"accepted", "cancelled", "success"} { //nolint:misspell // Persisted outcome.
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			database, jobID, runID, logs, now := runningRuntimeFixture(t, 0x3c300)
			other := openTestStoreAt(t, database.StateDir(), newSequentialEventIDs(0x3c400))
			source := database.eventIDs
			interleaved := false
			database.eventIDs = cancellationEventIDs(func() (model.EventID, error) {
				if interleaved {
					return source.NewEventID()
				}
				interleaved = true
				if outcome != "success" {
					if _, err := other.RequestCancellation(t.Context(), jobID, now.Add(4*time.Second)); err != nil {
						return "", err
					}
				}
				if outcome == "accepted" {
					return source.NewEventID()
				}
				code := 0
				exit := &model.ExitInfo{ExitCode: &code, ObservedAt: now.Add(5 * time.Second)}
				runOutcome := model.RunOutcomeSuccess
				if outcome != "success" {
					runOutcome = model.RunOutcomeCancelled
					exit.ExitCode = nil
					exit.PlatformReason = "terminated"
				}
				if _, err := other.FinalizeRun(t.Context(), jobID, runID, runOutcome, exit, logs, now.Add(5*time.Second)); err != nil {
					return "", err
				}
				return source.NewEventID()
			})
			result, err := database.RequestCancellation(t.Context(), jobID, now.Add(6*time.Second))
			if outcome == "success" {
				if !model.IsConflict(err) {
					t.Fatalf("cancel completed job error = %v, want model conflict", err)
				}
				assertCancellationEventCount(t, database, jobID, 0)
				return
			}
			if err != nil || len(result.Events) != 0 || len(result.Effects) != 0 {
				t.Fatalf("accepted cancellation must be unchanged: events=%d effects=%d error=%v", len(result.Events), len(result.Effects), err)
			}
			if result.Job.Cancellation == nil || !result.Job.Cancellation.RequestedAt.Equal(now.Add(4*time.Second)) {
				t.Fatal("retry replaced the accepted cancellation")
			}
			assertCancellationEventCount(t, database, jobID, 2)
		})
	}
}

func TestCancellationRetryIsBoundedAndContextAware(t *testing.T) {
	t.Parallel()
	for _, cancelContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhaustion", true: "context"}[cancelContext], func(t *testing.T) {
			t.Parallel()
			database, jobID, _, _, now := runningRuntimeFixture(t, 0x3c500)
			other := openTestStoreAt(t, database.StateDir(), newSequentialEventIDs(0x3c600))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := database.eventIDs
			calls := 0
			database.eventIDs = cancellationEventIDs(func() (model.EventID, error) {
				calls++
				if calls > 32 {
					t.Fatal("cancellation retried without a bound")
				}
				at := now.Add(time.Duration(4+calls) * time.Second)
				var err error
				if calls%2 == 1 {
					_, err = other.Pause(t.Context(), jobID, at)
				} else {
					_, err = other.Resume(t.Context(), jobID, at)
				}
				if err != nil {
					return "", err
				}
				if cancelContext {
					cancel()
				}
				return source.NewEventID()
			})
			_, err := database.RequestCancellation(ctx, jobID, now.Add(4*time.Second))
			if cancelContext {
				if !errors.Is(err, context.Canceled) || calls > 2 {
					t.Fatalf("context cancellation error=%v event calls=%d", err, calls)
				}
			} else {
				var conflict *RevisionConflictError
				if !errors.As(err, &conflict) || calls <= 2 {
					t.Fatalf("exhausted cancellation error=%v event calls=%d", err, calls)
				}
			}
			assertCancellationEventCount(t, database, jobID, 0)
		})
	}
}

func assertCancellationEventCount(t *testing.T, database *Store, jobID model.JobID, want int) {
	t.Helper()
	var count int
	if err := database.db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM state_events WHERE job_id = ? AND event_type = ?`,
		jobID.String(), string(model.EventCancellation)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("persisted cancellation events=%d, want %d", count, want)
	}
}
