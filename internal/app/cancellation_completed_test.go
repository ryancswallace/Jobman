package app

import (
	"testing"

	"github.com/ryancswallace/jobman/internal/model"
	"github.com/ryancswallace/jobman/internal/platform"
)

func TestCancellationDoesNotSignalConcurrentlyCompletedRun(t *testing.T) {
	t.Parallel()
	service := &Service{
		processTerminate: func(platform.ProcessIdentity, bool) error {
			t.Fatal("cancellation signaled a completed target")
			return nil
		},
		processResume: func(platform.ProcessIdentity) error {
			t.Fatal("cancellation resumed a completed target")
			return nil
		},
		processAlive: func(platform.ProcessIdentity) (bool, error) {
			t.Fatal("cancellation probed a completed target")
			return false, nil
		},
	}
	// A retried cancellation can observe completion after the caller's initial
	// active snapshot. The completed run retains its historical process identity.
	result := model.TransitionResult{
		Job: model.JobState{Phase: model.JobPhaseCompleted, Outcome: model.JobOutcomeCancelled},
		Run: &model.RunState{Phase: model.RunPhaseCompleted, Process: &model.ProcessIdentity{PID: 123}},
	}
	job, err := service.stopCancelledTarget(t.Context(), model.JobState{Phase: model.JobPhasePaused}, result)
	if err != nil || job.Phase != model.JobPhaseCompleted || job.Outcome != model.JobOutcomeCancelled {
		t.Fatalf("completed cancellation: phase=%s outcome=%s error=%v", job.Phase, job.Outcome, err)
	}
}
