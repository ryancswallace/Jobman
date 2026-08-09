package model

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

const diagnosticRecordSchemaVersion = 1

const diagnosticOperationDeliver = "deliver"

// DiagnosticCode is a stable, secret-free observed-condition identifier.
type DiagnosticCode string

// Stable core diagnostic codes used by current production paths.
const (
	DiagnosticTargetStartFailed             DiagnosticCode = "target_start_failed"
	DiagnosticTargetExecutableNotFound      DiagnosticCode = "target_executable_not_found"
	DiagnosticTargetWorkingDirectoryMissing DiagnosticCode = "target_working_directory_missing"
	DiagnosticTargetPermissionDenied        DiagnosticCode = "target_permission_denied"
	DiagnosticProcessStartPublishFailed     DiagnosticCode = "process_start_publish_failed"
	DiagnosticProcessWaitFailed             DiagnosticCode = "process_wait_failed"
	DiagnosticJobTimeout                    DiagnosticCode = "job_timeout"
	DiagnosticRunTimeout                    DiagnosticCode = "run_timeout"
	DiagnosticSupervisorClaimExpired        DiagnosticCode = "supervisor_claim_expired"
	DiagnosticSupervisorLeaseExpired        DiagnosticCode = "supervisor_lease_expired"
	DiagnosticLiveInputUnavailable          DiagnosticCode = "live_input_unavailable"
	DiagnosticWaitEvaluationError           DiagnosticCode = "wait_evaluation_error"
	DiagnosticLogCaptureDegraded            DiagnosticCode = "log_capture_degraded"
	DiagnosticLogFlushFailed                DiagnosticCode = "log_flush_failed"
	DiagnosticLogSyncFailed                 DiagnosticCode = "log_sync_failed"
	DiagnosticNotificationInvalid           DiagnosticCode = "notification_invalid"
	DiagnosticNotificationCanceled          DiagnosticCode = "notification_canceled"
	DiagnosticNotificationTimeout           DiagnosticCode = "notification_timeout"
	DiagnosticNotificationTransport         DiagnosticCode = "notification_transport"
	DiagnosticNotificationRejected          DiagnosticCode = "notification_rejected"
	DiagnosticNotificationInternal          DiagnosticCode = "notification_internal"
)

// DiagnosticOrigin identifies the subsystem that directly observed a
// condition. It does not identify a speculative root cause.
type DiagnosticOrigin string

// Supported diagnostic origins.
const (
	DiagnosticOriginTarget       DiagnosticOrigin = "target"
	DiagnosticOriginJobman       DiagnosticOrigin = "jobman"
	DiagnosticOriginPlatform     DiagnosticOrigin = "platform"
	DiagnosticOriginPolicy       DiagnosticOrigin = "policy"
	DiagnosticOriginDependency   DiagnosticOrigin = "dependency"
	DiagnosticOriginStorage      DiagnosticOrigin = "storage"
	DiagnosticOriginLogging      DiagnosticOrigin = "logging"
	DiagnosticOriginNotification DiagnosticOrigin = "notification"
	DiagnosticOriginUnknown      DiagnosticOrigin = "unknown"
)

// DiagnosticCategory groups exact observations without parsing their codes.
type DiagnosticCategory string

// Supported diagnostic categories.
const (
	DiagnosticCategoryStart        DiagnosticCategory = "start"
	DiagnosticCategoryTimeout      DiagnosticCategory = "timeout"
	DiagnosticCategoryOwnership    DiagnosticCategory = "ownership"
	DiagnosticCategoryInput        DiagnosticCategory = "input"
	DiagnosticCategoryWait         DiagnosticCategory = "wait"
	DiagnosticCategoryLogging      DiagnosticCategory = "logging"
	DiagnosticCategoryNotification DiagnosticCategory = "notification"
	DiagnosticCategoryUnknown      DiagnosticCategory = "unknown"
)

// DiagnosticRetryClass states whether the observed condition itself is likely
// to change. It is not authority to create a run.
type DiagnosticRetryClass string

// Supported observed-condition retry classes.
const (
	DiagnosticRetryImmediate   DiagnosticRetryClass = "immediate"
	DiagnosticRetryAfterDelay  DiagnosticRetryClass = "after_delay"
	DiagnosticRetryAfterChange DiagnosticRetryClass = "after_change"
	DiagnosticRetryNever       DiagnosticRetryClass = "never"
	DiagnosticRetryUnknown     DiagnosticRetryClass = "unknown"
)

// DiagnosticRecord is the versioned, allowlisted event detail retained for an
// observed abnormal condition.
type DiagnosticRecord struct {
	SchemaVersion int                  `json:"schema_version"`
	Code          DiagnosticCode       `json:"code"`
	Origin        DiagnosticOrigin     `json:"origin"`
	Operation     string               `json:"operation"`
	Category      DiagnosticCategory   `json:"category"`
	RetryClass    DiagnosticRetryClass `json:"retry_class"`
	Attributes    map[string]string    `json:"attributes"`
}

type diagnosticDescriptor struct {
	origin     DiagnosticOrigin
	operation  string
	category   DiagnosticCategory
	retryClass DiagnosticRetryClass
	attributes []string
}

var diagnosticCatalog = map[DiagnosticCode]diagnosticDescriptor{
	DiagnosticTargetStartFailed:             {DiagnosticOriginTarget, "start", DiagnosticCategoryStart, DiagnosticRetryUnknown, nil},
	DiagnosticTargetExecutableNotFound:      {DiagnosticOriginTarget, "resolve_executable", DiagnosticCategoryStart, DiagnosticRetryAfterChange, nil},
	DiagnosticTargetWorkingDirectoryMissing: {DiagnosticOriginTarget, "open_working_directory", DiagnosticCategoryStart, DiagnosticRetryAfterChange, nil},
	DiagnosticTargetPermissionDenied:        {DiagnosticOriginTarget, "start", DiagnosticCategoryStart, DiagnosticRetryAfterChange, nil},
	DiagnosticProcessStartPublishFailed:     {DiagnosticOriginJobman, "publish_process_identity", DiagnosticCategoryStart, DiagnosticRetryUnknown, nil},
	DiagnosticProcessWaitFailed:             {DiagnosticOriginPlatform, "wait_process", DiagnosticCategoryOwnership, DiagnosticRetryUnknown, nil},
	DiagnosticJobTimeout:                    {DiagnosticOriginPolicy, "job_timeout", DiagnosticCategoryTimeout, DiagnosticRetryAfterChange, nil},
	DiagnosticRunTimeout:                    {DiagnosticOriginPolicy, "run_timeout", DiagnosticCategoryTimeout, DiagnosticRetryAfterDelay, nil},
	DiagnosticSupervisorClaimExpired:        {DiagnosticOriginJobman, "claim_supervisor", DiagnosticCategoryOwnership, DiagnosticRetryImmediate, nil},
	DiagnosticSupervisorLeaseExpired:        {DiagnosticOriginJobman, "renew_supervisor_lease", DiagnosticCategoryOwnership, DiagnosticRetryUnknown, nil},
	DiagnosticLiveInputUnavailable:          {DiagnosticOriginJobman, "open_live_input", DiagnosticCategoryInput, DiagnosticRetryUnknown, nil},
	DiagnosticWaitEvaluationError:           {DiagnosticOriginPolicy, "evaluate_wait", DiagnosticCategoryWait, DiagnosticRetryAfterDelay, nil},
	DiagnosticLogCaptureDegraded:            {DiagnosticOriginLogging, "capture_output", DiagnosticCategoryLogging, DiagnosticRetryUnknown, nil},
	DiagnosticLogFlushFailed:                {DiagnosticOriginLogging, "flush_output", DiagnosticCategoryLogging, DiagnosticRetryUnknown, nil},
	DiagnosticLogSyncFailed:                 {DiagnosticOriginLogging, "sync_output", DiagnosticCategoryLogging, DiagnosticRetryUnknown, nil},
	DiagnosticNotificationInvalid:           {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryAfterChange, nil},
	DiagnosticNotificationCanceled:          {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryNever, nil},
	DiagnosticNotificationTimeout:           {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryAfterDelay, nil},
	DiagnosticNotificationTransport:         {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryAfterDelay, nil},
	DiagnosticNotificationRejected:          {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryAfterChange, nil},
	DiagnosticNotificationInternal:          {DiagnosticOriginNotification, diagnosticOperationDeliver, DiagnosticCategoryNotification, DiagnosticRetryUnknown, nil},
}

// NewDiagnosticRecord returns a safe structured record. Unknown legacy codes
// remain representable with unknown metadata.
func NewDiagnosticRecord(code DiagnosticCode, attributes map[string]string) (DiagnosticRecord, error) {
	if !validDiagnosticCode(code) {
		return DiagnosticRecord{}, fmt.Errorf("invalid diagnostic code %q", code)
	}
	descriptor, known := diagnosticCatalog[code]
	if !known {
		descriptor = diagnosticDescriptor{
			origin: DiagnosticOriginUnknown, operation: "unknown",
			category: DiagnosticCategoryUnknown, retryClass: DiagnosticRetryUnknown,
		}
	}
	allowed := make(map[string]struct{}, len(descriptor.attributes))
	for _, name := range descriptor.attributes {
		allowed[name] = struct{}{}
	}
	for name, value := range attributes {
		if _, ok := allowed[name]; !ok {
			return DiagnosticRecord{}, fmt.Errorf("diagnostic code %q does not allow attribute %q", code, name)
		}
		if !validDiagnosticAttribute(name, value) {
			return DiagnosticRecord{}, fmt.Errorf("diagnostic code %q has invalid attribute %q", code, name)
		}
	}
	record := DiagnosticRecord{
		SchemaVersion: diagnosticRecordSchemaVersion,
		Code:          code, Origin: descriptor.origin, Operation: descriptor.operation,
		Category: descriptor.category, RetryClass: descriptor.retryClass,
		Attributes: maps.Clone(attributes),
	}
	if record.Attributes == nil {
		record.Attributes = map[string]string{}
	}

	return record, nil
}

// ValidDiagnosticCodes returns the current catalog in stable order.
func ValidDiagnosticCodes() []DiagnosticCode {
	codes := slices.Collect(maps.Keys(diagnosticCatalog))
	slices.Sort(codes)

	return codes
}

func validDiagnosticCode(code DiagnosticCode) bool {
	value := string(code)
	if value == "" || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' {
			continue
		}
		return false
	}

	return true
}

func validDiagnosticAttribute(name, value string) bool {
	return validDiagnosticCode(DiagnosticCode(name)) && len(value) <= 256 &&
		!strings.ContainsAny(value, "\r\n\x00")
}
