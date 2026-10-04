// Package diagnostic defines Jobman's public, model-independent diagnostic
// evidence contract.
//
// The package deliberately imports only the Go standard library. Consumers
// such as jobman-diagnose can therefore decode evidence without importing
// Jobman's internal application, persistence, or process-management packages.
package diagnostic

import (
	"encoding/json"
	"time"
)

const (
	// Kind identifies the diagnostic evidence document type.
	Kind = "jobman.diagnostic_evidence"
	// SchemaVersion is the stable local-store schema and the default for Seal.
	SchemaVersion = 1
	// SharedSchemaVersion identifies evidence collected from Jobman Control.
	SharedSchemaVersion = 2
	// CollectorVersion identifies the schema-1 core evidence collector semantics.
	CollectorVersion = "1.0.0"
)

// Evidence is a bounded, immutable snapshot of factual Jobman observations.
// EvidenceID seals its semantic content but deliberately excludes CapturedAt.
type Evidence struct {
	Kind             string            `json:"kind"`
	SchemaVersion    int               `json:"schema_version"`
	EvidenceID       string            `json:"evidence_id"`
	CapturedAt       time.Time         `json:"captured_at"`
	Source           Source            `json:"source"`
	Subject          Subject           `json:"subject"`
	Consistency      Consistency       `json:"consistency"`
	Items            []Item            `json:"items"`
	Artifacts        []Artifact        `json:"artifacts"`
	Omissions        []Omission        `json:"omissions"`
	RedactionNotices []RedactionNotice `json:"redaction_notices"`
	Limits           Limits            `json:"limits"`
	Shared           *SharedProvenance `json:"shared,omitempty"`
}

// Source identifies the Jobman build and platform that collected evidence.
type Source struct {
	JobmanVersion      string   `json:"jobman_version"`
	CollectorVersion   string   `json:"collector_version"`
	StoreSchemaVersion int      `json:"store_schema_version,omitempty"`
	Platform           string   `json:"platform"`
	Capabilities       []string `json:"capabilities"`
}

// Subject identifies the job snapshot and runs represented by a bundle.
type Subject struct {
	JobID        string   `json:"job_id"`
	JobRevision  uint64   `json:"job_revision"`
	SelectedRuns []uint64 `json:"selected_runs"`
	Phase        string   `json:"phase"`
	Outcome      string   `json:"outcome,omitempty"`
}

// Consistency describes the relationship between transactional metadata and
// separately captured filesystem artifacts.
type Consistency struct {
	Metadata                   MetadataConsistency `json:"metadata"`
	Artifacts                  ArtifactConsistency `json:"artifacts"`
	ActiveStateMayHaveAdvanced bool                `json:"active_state_may_have_advanced"`
}

// MetadataConsistency identifies the metadata snapshot guarantee.
type MetadataConsistency string

// Supported metadata consistency values.
const (
	MetadataTransactionalSnapshot MetadataConsistency = "transactional_snapshot"
)

// ArtifactConsistency identifies the completeness of artifact snapshots.
type ArtifactConsistency string

// Supported artifact consistency values.
const (
	ArtifactsNotCollected ArtifactConsistency = "not_collected"
	ArtifactsStable       ArtifactConsistency = "stable"
	ArtifactsPointInTime  ArtifactConsistency = "point_in_time"
	ArtifactsMixed        ArtifactConsistency = "mixed"
)

// Item is one typed factual observation. Value is canonical JSON whose type is
// defined by Code's registry entry.
type Item struct {
	ID         string          `json:"id"`
	Code       string          `json:"code"`
	Value      json.RawMessage `json:"value"`
	ObservedAt *time.Time      `json:"observed_at,omitempty"`
	Source     ItemSource      `json:"source"`
	Quality    Quality         `json:"quality"`
	Disclosure DisclosureClass `json:"disclosure"`
}

// ItemSource attributes an item to a durable snapshot, event, artifact, or
// exact core derivation.
type ItemSource struct {
	Kind       string `json:"kind"`
	EntityID   string `json:"entity_id,omitempty"`
	Revision   uint64 `json:"revision,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	ByteStart  uint64 `json:"byte_start,omitempty"`
	ByteEnd    uint64 `json:"byte_end,omitempty"`
}

// Quality states how directly Jobman established an observation.
type Quality string

// Supported evidence qualities.
const (
	QualityObserved     Quality = "observed"
	QualityConfirmed    Quality = "confirmed"
	QualityDerivedExact Quality = "derived_exact"
	QualityPointInTime  Quality = "point_in_time"
	QualityUnknown      Quality = "unknown"
)

// DisclosureClass controls which evidence can be projected to a provider.
type DisclosureClass string

// Supported disclosure classes.
const (
	DisclosureMetadata        DisclosureClass = "metadata"
	DisclosureCommand         DisclosureClass = "command"
	DisclosurePath            DisclosureClass = "path"
	DisclosureEnvironmentName DisclosureClass = "environment_name"
	DisclosureLogContent      DisclosureClass = "log_content"
	DisclosureSensitive       DisclosureClass = "sensitive"
	DisclosureLocalOnly       DisclosureClass = "local_only"
)

// Artifact contains bounded bytes selected from a separately stored source.
// Data is encoded as base64 by encoding/json, preserving arbitrary bytes.
type Artifact struct {
	ID            string          `json:"id"`
	Role          string          `json:"role"`
	Run           uint64          `json:"run"`
	Stream        string          `json:"stream,omitempty"`
	MediaType     string          `json:"media_type"`
	Data          []byte          `json:"data"`
	OriginalBytes uint64          `json:"original_bytes"`
	ByteStart     uint64          `json:"byte_start"`
	ByteEnd       uint64          `json:"byte_end"`
	SelectedBytes uint64          `json:"selected_bytes"`
	ContentBytes  uint64          `json:"content_bytes"`
	Digest        string          `json:"digest"`
	Truncated     bool            `json:"truncated"`
	CapturedAt    time.Time       `json:"captured_at"`
	Quality       Quality         `json:"quality"`
	Disclosure    DisclosureClass `json:"disclosure"`
}

// Omission explains why requested or useful evidence is absent.
type Omission struct {
	Code    string   `json:"code"`
	Affects []string `json:"affects"`
}

// RedactionNotice records a sanitized field without retaining its original
// value or the matching pattern.
type RedactionNotice struct {
	Code    string   `json:"code"`
	Affects []string `json:"affects"`
	Count   uint64   `json:"count"`
}

// Limits records the actual bounded size of the sealed bundle.
type Limits struct {
	ItemCount     uint64 `json:"item_count"`
	ArtifactCount uint64 `json:"artifact_count"`
	EncodedBytes  uint64 `json:"encoded_bytes"`
	ArtifactBytes uint64 `json:"artifact_bytes"`
	LogBytes      uint64 `json:"log_bytes"`
}

// EvidenceRequest contains core collection choices and no inference options.
type EvidenceRequest struct {
	Selector string
	Run      int64
	AllRuns  bool
	// IncludeCommand explicitly requests direct command specifications,
	// including ordered argument vectors. Environment values are never part of
	// command evidence.
	IncludeCommand bool
	// IncludePaths explicitly requests filesystem paths such as the working
	// directory and each run's resolved executable.
	IncludePaths bool
	// IncludeEnvironmentNames explicitly requests environment variable names
	// and inheritance/set/unset/secret-reference roles, never values or secret
	// reference identifiers.
	IncludeEnvironmentNames bool
	// IncludeSystem explicitly requests bounded point-in-time host capacity,
	// cgroup, and container context. It never collects system logs, hostnames,
	// cgroup paths, process lists, or environment values.
	IncludeSystem bool
	Logs          LogMode
	LogBytes      uint64
	Similar       uint64
}

// LogMode selects bounded target-log evidence.
type LogMode string

// Supported log collection modes.
const (
	LogsMetadata LogMode = "metadata"
	LogsTail     LogMode = "tail"
	LogsNone     LogMode = "none"
)

// Sanitizer removes configured sensitive values before evidence is sealed.
// The returned boolean reports whether the bytes changed.
type Sanitizer interface {
	Sanitize(field string, value []byte) ([]byte, bool)
}

// ValueRedactionReporter lets a sanitizer state whether it has a configured
// value-aware policy suitable for protecting unstructured artifacts before a
// user explicitly discloses them. Field-name-only redaction is insufficient
// for target log bytes.
type ValueRedactionReporter interface {
	ValueRedactionConfigured() bool
}
