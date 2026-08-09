// Package failureclass maps durable lifecycle facts to stable failure classes.
// It never inspects command text, paths, environment values, or log content.
package failureclass

import "github.com/ryancswallace/jobman/internal/model"

// Classification is a stable failure class and whether the class follows
// conclusively from a durable condition rather than a generic outcome.
type Classification struct {
	Class     string
	Confirmed bool
}

// Job classifies an abnormal job that has no run to classify.
func Job(job model.JobState, runCount int) Classification {
	if runCount != 0 || job.Outcome == "" || job.Outcome == model.JobOutcomeSuccess {
		return Classification{}
	}
	switch job.Outcome {
	case model.JobOutcomeSubmissionFailed:
		return Classification{Class: FromDiagnostic(job.LastDiagnosticCode, "submission_failed"), Confirmed: true}
	case model.JobOutcomeTimedOut:
		return Classification{Class: "job_timeout", Confirmed: true}
	case model.JobOutcomeCancelled:
		return Classification{Class: "user_cancellation", Confirmed: true}
	case model.JobOutcomeLost:
		return Classification{Class: FromDiagnostic(job.LastDiagnosticCode, "ownership_lost"), Confirmed: true}
	default:
		return Classification{Class: "job_failure_without_run"}
	}
}

// Run classifies an abnormal completed run from exact persisted facts.
func Run(run model.RunState) Classification {
	if run.Outcome == "" || run.Outcome == model.RunOutcomeSuccess {
		return Classification{}
	}
	if exact := FromDiagnostic(run.LastDiagnosticCode, ""); exact != "" {
		return Classification{Class: exact, Confirmed: true}
	}
	switch run.Outcome {
	case model.RunOutcomeTimedOut:
		return Classification{Class: "timeout", Confirmed: true}
	case model.RunOutcomeCancelled:
		return Classification{Class: "user_cancellation", Confirmed: true}
	case model.RunOutcomeStartFailed:
		return Classification{Class: "target_start_failed", Confirmed: true}
	case model.RunOutcomeLost:
		return Classification{Class: "ownership_lost", Confirmed: true}
	case model.RunOutcomeFailure:
		if run.Exit != nil && run.Exit.Signal != "" {
			return Classification{Class: "signal_termination", Confirmed: true}
		}
		if run.Exit != nil && run.Exit.ExitCode != nil && *run.Exit.ExitCode != 0 {
			return Classification{Class: "nonzero_exit"}
		}

		return Classification{Class: "target_failure"}
	default:
		return Classification{}
	}
}

// FromDiagnostic maps known exact diagnostic codes to stable classes and uses
// fallback for an unknown or absent code.
func FromDiagnostic(code, fallback string) string {
	switch model.DiagnosticCode(code) {
	case model.DiagnosticTargetExecutableNotFound:
		return "executable_not_found"
	case model.DiagnosticTargetWorkingDirectoryMissing:
		return "working_directory_missing"
	case model.DiagnosticTargetPermissionDenied:
		return "permission_denied"
	case model.DiagnosticTargetStartFailed:
		return "target_start_failed"
	case model.DiagnosticJobTimeout:
		return "job_timeout"
	case model.DiagnosticRunTimeout:
		return "run_timeout"
	case model.DiagnosticSupervisorClaimExpired:
		return "supervisor_claim_expired"
	case model.DiagnosticSupervisorLeaseExpired:
		return "ownership_lost"
	case model.DiagnosticWaitEvaluationError:
		return "wait_evaluation_error"
	case model.DiagnosticLogCaptureDegraded, model.DiagnosticLogFlushFailed, model.DiagnosticLogSyncFailed:
		return "log_recording_degraded"
	default:
		return fallback
	}
}
