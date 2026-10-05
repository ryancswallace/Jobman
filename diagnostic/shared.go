package diagnostic

import (
	"context"
	"time"
)

// Shared snapshot and collector versions are independent of the agent protocol.
const (
	SharedSnapshotKind          = "jobman.shared_diagnostic_snapshot"
	SharedSnapshotVersion       = 1
	SharedCollectorVersion      = "1.0.0"
	SharedSourceControl         = "control"
	SharedMaximumBytes          = 2 * 1024 * 1024
	SharedMaximumItems          = 1024
	SharedMaximumRuns           = 32
	SharedMaximumTailBytes      = 64 * 1024
	SharedProfileMetadata       = "metadata"
	SharedProfileIncludeLogTail = "include_log_tail"
)

// SharedSource is the immutable authority for a shared snapshot. DeploymentID
// is the operator-configured Dashboard source UUID; ControlInstanceID is the
// instance identity returned by that Control, not its URL or namespace name.
type SharedSource struct {
	Kind              string `json:"kind"`
	DeploymentID      string `json:"deployment_id"`
	ControlInstanceID string `json:"control_instance_id"`
	NamespaceID       string `json:"namespace_id"`
	ControlVersion    string `json:"control_version"`
	ContractVersion   string `json:"contract_version"`
}

// SharedRun retains real Control run and execution identities. ExecutionID is
// absent when no execution was assigned or historical import lacks it. Number
// is the durable run number, never a selected-page index.
type SharedRun struct {
	ID          string `json:"id"`
	Number      uint64 `json:"number,string"`
	ExecutionID string `json:"execution_id,omitempty"`
}

// SharedProvenance is sealed as part of schema-2 evidence identity. Job ID,
// revision and selected run numbers remain in Evidence.Subject; Runs adds the
// exact UUID and execution joins that the local-store schema cannot express.
type SharedProvenance struct {
	Source  SharedSource         `json:"source"`
	Runs    []SharedRun          `json:"runs"`
	Logs    []SharedLogReference `json:"logs"`
	Profile string               `json:"profile"`
}

// SharedSelection pins the requested authority and job. ExpectedJobRevision
// and RunID are optional constraints; a reader must return an explicit conflict
// if a requested revision is no longer available, never relabel current data.
type SharedSelection struct {
	DeploymentID        string `json:"deployment_id"`
	ControlInstanceID   string `json:"control_instance_id"`
	NamespaceID         string `json:"namespace_id"`
	JobID               string `json:"job_id"`
	ExpectedJobRevision uint64 `json:"expected_job_revision,string,omitempty"`
	RunID               string `json:"run_id,omitempty"`
}

// SharedJob describes the transactionally selected metadata. Missing execution
// observations are represented by omissions, not inferred from UpdatedAt.
type SharedJob struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision,string"`
	Phase    string `json:"phase"`
	Outcome  string `json:"outcome,omitempty"`
}

// SharedLogReference is an opaque, source-qualified reference to the bounded
// manifest captured with metadata. It contains no filesystem path or URL.
// Readers must authenticate and authorize every read, validate the manifest's
// immutable chunk checksums, and reject changes to this reference's identity.
type SharedLogReference struct {
	ID               string `json:"id"`
	RunID            string `json:"run_id"`
	ExecutionID      string `json:"execution_id"`
	Stream           string `json:"stream"`
	ManifestRevision uint64 `json:"manifest_revision,string"`
	Bytes            uint64 `json:"bytes,string"`
	Complete         bool   `json:"complete"`
}

// SharedSnapshot is factual metadata captured in one read transaction by
// Control. Items use the existing fact registry or additive control.* codes;
// their source EntityID must identify this job, a selected run, or an event.
// Readers must bound their database queries before constructing this value.
// JobmanVersion and Platform describe the collecting core library/build host,
// not a fabricated agent or SQLite store. Items and omissions are mandatory.
type SharedSnapshot struct {
	Kind             string               `json:"kind"`
	SchemaVersion    int                  `json:"schema_version"`
	CapturedAt       time.Time            `json:"captured_at"`
	Source           SharedSource         `json:"source"`
	JobmanVersion    string               `json:"jobman_version"`
	Platform         string               `json:"platform"`
	Job              SharedJob            `json:"job"`
	Runs             []SharedRun          `json:"runs"`
	Metadata         MetadataConsistency  `json:"metadata"`
	Items            []Item               `json:"items"`
	Logs             []SharedLogReference `json:"logs"`
	Omissions        []Omission           `json:"omissions"`
	RedactionNotices []RedactionNotice    `json:"redaction_notices"`
}

// SnapshotReader performs an authorized, bounded, transactional Control read.
// Implementations must return errors for denied/unavailable authority and must
// not return an empty snapshot to disguise failure. It does not read log bytes.
type SnapshotReader interface {
	ReadSnapshot(context.Context, SharedSelection) (SharedSnapshot, error)
}

// SharedLogRequest pins the exact metadata snapshot and selected stream.
type SharedLogRequest struct {
	Selection SharedSelection    `json:"selection"`
	Reference SharedLogReference `json:"reference"`
	MaxBytes  uint64             `json:"max_bytes,string"`
}

// SharedLogTail contains original byte offsets and pre-redaction bytes from
// the referenced manifest. Data must cover [ByteStart, ByteEnd) exactly, with
// ByteEnd equal to Reference.Bytes. Complete immutable manifests are stable;
// an unfinished stream is a point-in-time snapshot even if chunks are immutable.
type SharedLogTail struct {
	Reference  SharedLogReference `json:"reference"`
	Data       []byte             `json:"data"`
	ByteStart  uint64             `json:"byte_start,string"`
	ByteEnd    uint64             `json:"byte_end,string"`
	CapturedAt time.Time          `json:"captured_at"`
}

// LogReader obtains bounded bytes only after the metadata transaction ends.
// Every error, including revoked access, aborts collection. Known absence must
// be an omission in the snapshot, never an authorization error hidden as one.
type LogReader interface {
	ReadLogTail(context.Context, SharedLogRequest) (SharedLogTail, error)
}

// SharedCollectionRequest defaults to metadata-only evidence. IncludeLogTail
// requires a value-aware Sanitizer. LogBytes defaults to and cannot exceed
// 64 KiB per stream; the entire sealed document is bounded to 2 MiB.
type SharedCollectionRequest struct {
	Selection      SharedSelection
	IncludeLogTail bool
	LogBytes       uint64
}

// SharedCollector collects facts without analysis, inference, process launch,
// local database substitution, filesystem traversal or model/provider calls.
type SharedCollector struct {
	Snapshots SnapshotReader
	Logs      LogReader
	Sanitizer Sanitizer
}
