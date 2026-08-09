package failureclass

import (
	"testing"

	"github.com/ryancswallace/jobman/internal/model"
)

func TestRunClassificationUsesExactFacts(t *testing.T) {
	t.Parallel()

	exitCode := 2
	for _, test := range []struct {
		name      string
		run       model.RunState
		want      string
		confirmed bool
	}{
		{name: "success", run: model.RunState{Outcome: model.RunOutcomeSuccess}},
		{name: "diagnostic", run: model.RunState{
			Outcome: model.RunOutcomeStartFailed, LastDiagnosticCode: string(model.DiagnosticTargetExecutableNotFound),
		}, want: "executable_not_found", confirmed: true},
		{name: "signal", run: model.RunState{
			Outcome: model.RunOutcomeFailure, Exit: &model.ExitInfo{Signal: "TERM"},
		}, want: "signal_termination", confirmed: true},
		{name: "exit", run: model.RunState{
			Outcome: model.RunOutcomeFailure, Exit: &model.ExitInfo{ExitCode: &exitCode},
		}, want: "nonzero_exit"},
		{name: "timeout", run: model.RunState{Outcome: model.RunOutcomeTimedOut}, want: "timeout", confirmed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := Run(test.run)
			if got.Class != test.want || got.Confirmed != test.confirmed {
				t.Fatalf("Run() = %#v, want class %q confirmed=%t", got, test.want, test.confirmed)
			}
		})
	}
}

func TestJobClassificationRequiresNoRuns(t *testing.T) {
	t.Parallel()

	job := model.JobState{Outcome: model.JobOutcomeSubmissionFailed, LastDiagnosticCode: string(model.DiagnosticTargetPermissionDenied)}
	if got := Job(job, 0); got.Class != "permission_denied" || !got.Confirmed {
		t.Fatalf("Job() = %#v", got)
	}
	if got := Job(job, 1); got != (Classification{}) {
		t.Fatalf("Job(with run) = %#v", got)
	}
}

func TestClassificationCoversEveryStableFallback(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		job  model.JobState
		want Classification
	}{
		{name: "empty", job: model.JobState{}},
		{name: "success", job: model.JobState{Outcome: model.JobOutcomeSuccess}},
		{
			name: "submission fallback", job: model.JobState{Outcome: model.JobOutcomeSubmissionFailed},
			want: Classification{Class: "submission_failed", Confirmed: true},
		},
		{
			name: "timeout", job: model.JobState{Outcome: model.JobOutcomeTimedOut},
			want: Classification{Class: "job_timeout", Confirmed: true},
		},
		{
			name: "canceled", job: model.JobState{Outcome: model.JobOutcomeCancelled},
			want: Classification{Class: "user_cancellation", Confirmed: true},
		},
		{
			name: "lost", job: model.JobState{Outcome: model.JobOutcomeLost},
			want: Classification{Class: "ownership_lost", Confirmed: true},
		},
		{
			name: "other", job: model.JobState{Outcome: model.JobOutcomeFailure},
			want: Classification{Class: "job_failure_without_run"},
		},
	} {
		t.Run("job "+test.name, func(t *testing.T) {
			t.Parallel()
			if got := Job(test.job, 0); got != test.want {
				t.Fatalf("Job() = %#v, want %#v", got, test.want)
			}
		})
	}

	for _, test := range []struct {
		name string
		run  model.RunState
		want Classification
	}{
		{name: "empty", run: model.RunState{}},
		{
			name: "canceled", run: model.RunState{Outcome: model.RunOutcomeCancelled},
			want: Classification{Class: "user_cancellation", Confirmed: true},
		},
		{
			name: "start failed", run: model.RunState{Outcome: model.RunOutcomeStartFailed},
			want: Classification{Class: "target_start_failed", Confirmed: true},
		},
		{
			name: "lost", run: model.RunState{Outcome: model.RunOutcomeLost},
			want: Classification{Class: "ownership_lost", Confirmed: true},
		},
		{
			name: "failure without exit", run: model.RunState{Outcome: model.RunOutcomeFailure},
			want: Classification{Class: "target_failure"},
		},
		{name: "unknown", run: model.RunState{Outcome: model.RunOutcome("unknown")}},
	} {
		t.Run("run "+test.name, func(t *testing.T) {
			t.Parallel()
			if got := Run(test.run); got != test.want {
				t.Fatalf("Run() = %#v, want %#v", got, test.want)
			}
		})
	}

	for code, want := range map[model.DiagnosticCode]string{
		model.DiagnosticTargetWorkingDirectoryMissing: "working_directory_missing",
		model.DiagnosticTargetStartFailed:             "target_start_failed",
		model.DiagnosticJobTimeout:                    "job_timeout",
		model.DiagnosticRunTimeout:                    "run_timeout",
		model.DiagnosticSupervisorClaimExpired:        "supervisor_claim_expired",
		model.DiagnosticSupervisorLeaseExpired:        "ownership_lost",
		model.DiagnosticWaitEvaluationError:           "wait_evaluation_error",
		model.DiagnosticLogCaptureDegraded:            "log_recording_degraded",
		model.DiagnosticLogFlushFailed:                "log_recording_degraded",
		model.DiagnosticLogSyncFailed:                 "log_recording_degraded",
	} {
		if got := FromDiagnostic(string(code), "fallback"); got != want {
			t.Errorf("FromDiagnostic(%q) = %q, want %q", code, got, want)
		}
	}
	if got := FromDiagnostic("unknown", "fallback"); got != "fallback" {
		t.Errorf("FromDiagnostic(unknown) = %q", got)
	}
}
