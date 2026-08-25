// Package controlclient implements the bounded pre-release Jobman Control API client.
package controlclient

import (
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

const apiVersion = "jobman.control/v1alpha1"

// Job is the current shared job snapshot returned by Jobman Control.
type Job struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   JobMetadata `json:"metadata"`
	Spec       JobSpec     `json:"spec"`
	Status     JobStatus   `json:"status"`
}

// JobMetadata identifies a namespace-scoped shared job.
type JobMetadata struct {
	ID        string            `json:"id"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
	Revision  int64             `json:"revision"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// JobSpec describes the immutable workload binding and resolved placement.
type JobSpec struct {
	WorkloadDigest string       `json:"workloadDigest"`
	Placement      JobPlacement `json:"placement"`
}

// JobPlacement is the target generation selected by Jobman Control.
type JobPlacement struct {
	Target             string `json:"target"`
	Partition          string `json:"partition,omitempty"`
	TargetID           string `json:"targetId,omitempty"`
	TargetGenerationID string `json:"targetGenerationId,omitempty"`
	ExecutionBackend   string `json:"executionBackend,omitempty"`
}

// JobStatus contains the current shared lifecycle projection.
type JobStatus struct {
	Phase                 string           `json:"phase"`
	DesiredState          string           `json:"desiredState"`
	Outcome               string           `json:"outcome,omitempty"`
	ObservationConfidence string           `json:"observationConfidence,omitempty"`
	ConfidenceUpdatedAt   *time.Time       `json:"confidenceUpdatedAt,omitempty"`
	NativeID              string           `json:"nativeId,omitempty"`
	Scheduler             *SchedulerStatus `json:"scheduler,omitempty"`
}

// SchedulerStatus is the latest normalized scheduler evidence projected by
// Jobman Control.
type SchedulerStatus struct {
	Backend    string    `json:"backend"`
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	Cluster    string    `json:"cluster,omitempty"`
	ObservedAt time.Time `json:"observedAt"`
}

// JobPage is one newest-first page of shared jobs.
type JobPage struct {
	APIVersion    string `json:"apiVersion"`
	Kind          string `json:"kind"`
	Items         []Job  `json:"items"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

// Collection is one aggregate plus independently observable child jobs.
type Collection struct {
	APIVersion string             `json:"apiVersion"`
	Kind       string             `json:"kind"`
	Metadata   CollectionMetadata `json:"metadata"`
	Spec       CollectionSpec     `json:"spec"`
	Status     CollectionStatus   `json:"status"`
	Items      []CollectionItem   `json:"items"`
}

// CollectionMetadata identifies a namespace-scoped collection.
type CollectionMetadata struct {
	ID        string            `json:"id"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
	Revision  int64             `json:"revision"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// CollectionSpec is durable dispatch and failure policy.
type CollectionSpec struct {
	MaxActive     int    `json:"maxActive"`
	FailurePolicy string `json:"failurePolicy"`
	ArrayPolicy   string `json:"arrayPolicy"`
}

// CollectionStatus is the current aggregate projection.
type CollectionStatus struct {
	Phase     string `json:"phase"`
	Outcome   string `json:"outcome,omitempty"`
	ArrayMode string `json:"arrayMode"`
	Total     int    `json:"total"`
	Active    int    `json:"active"`
	Terminal  int    `json:"terminal"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Canceled  int    `json:"cancelled"` //nolint:misspell // Frozen v1alpha1 wire spelling.
}

// CollectionItem binds one stable index/name to a child job.
type CollectionItem struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
	Job   Job    `json:"job"`
}

// Graph is one immutable dependency graph with independently observable jobs.
type Graph struct {
	APIVersion string        `json:"apiVersion"`
	Kind       string        `json:"kind"`
	Metadata   GraphMetadata `json:"metadata"`
	Spec       GraphSpec     `json:"spec"`
	Status     GraphStatus   `json:"status"`
	Items      []GraphItem   `json:"items"`
}

// GraphMetadata identifies a namespace-scoped graph.
type GraphMetadata struct {
	ID        string            `json:"id"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
	Revision  int64             `json:"revision"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// GraphSpec contains immutable graph scheduling policy.
type GraphSpec struct {
	MaxActive         int    `json:"maxActive"`
	UnsatisfiedPolicy string `json:"unsatisfiedPolicy"`
}

// GraphStatus is the current aggregate node projection.
type GraphStatus struct {
	Phase     string `json:"phase"`
	Outcome   string `json:"outcome,omitempty"`
	Total     int    `json:"total"`
	Waiting   int    `json:"waiting"`
	Active    int    `json:"active"`
	Terminal  int    `json:"terminal"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Canceled  int    `json:"cancelled"` //nolint:misspell // Frozen v1alpha1 wire spelling.
	Skipped   int    `json:"skipped"`
	Blocked   int    `json:"blocked"`
}

// GraphDependency reports one upstream predicate and its current satisfaction.
type GraphDependency struct {
	From      string   `json:"from"`
	Predicate string   `json:"predicate"`
	Outcomes  []string `json:"outcomes,omitempty"`
	Satisfied bool     `json:"satisfied"`
}

// GraphItem binds one stable node identity to a child job.
type GraphItem struct {
	Index        int               `json:"index"`
	Name         string            `json:"name"`
	Disposition  string            `json:"disposition,omitempty"`
	Dependencies []GraphDependency `json:"dependencies,omitempty"`
	Job          Job               `json:"job"`
}

// Target is one namespace-visible placement target.
type Target struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   TargetMetadata `json:"metadata"`
	Spec       TargetSpec     `json:"spec"`
	Status     TargetStatus   `json:"status"`
}

// TargetMetadata binds a target name to its current immutable generation.
type TargetMetadata struct {
	ID           string    `json:"id"`
	GenerationID string    `json:"generationId"`
	Generation   int64     `json:"generation"`
	Namespace    string    `json:"namespace"`
	Name         string    `json:"name"`
	Revision     int64     `json:"revision"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// TargetSpec contains administrator-approved target capabilities.
type TargetSpec struct {
	Kind             string                   `json:"kind"`
	ExecutionBackend string                   `json:"executionBackend"`
	ControlTransport string                   `json:"controlTransport"`
	Runtimes         []string                 `json:"runtimes"`
	OperatingSystems []string                 `json:"operatingSystems,omitempty"`
	Architectures    []string                 `json:"architectures,omitempty"`
	Capabilities     []string                 `json:"capabilities,omitempty"`
	Partitions       []Partition              `json:"partitions,omitempty"`
	LogStore         *ArtifactStoreReference  `json:"logStore,omitempty"`
	ArtifactStores   []ArtifactStoreReference `json:"artifactStores,omitempty"`
	Provider         TargetProvider           `json:"provider"`
}

// TargetProvider identifies on-premises or AWS ParallelCluster placement.
type TargetProvider struct {
	Kind        string `json:"kind"`
	Region      string `json:"region,omitempty"`
	ClusterName string `json:"clusterName,omitempty"`
}

// ArtifactStoreReference identifies a logical mapping generation without a
// physical host path.
type ArtifactStoreReference struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// Partition is one configured scheduler placement beneath a target.
type Partition struct {
	Name      string `json:"name"`
	IsDefault bool   `json:"isDefault"`
}

// TargetStatus reports whether new work may be placed on a target.
type TargetStatus struct {
	State string `json:"state"`
}

type targetPage struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Items      []Target `json:"items"`
}

// LogManifest is the authorized logical object manifest for one shared job.
type LogManifest struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Namespace  string      `json:"namespace"`
	JobID      string      `json:"jobId"`
	Items      []LogStream `json:"items"`
}

// LogStream describes one execution's stdout or stderr object sequence.
type LogStream struct {
	ExecutionID string     `json:"executionId"`
	RunNumber   int        `json:"runNumber"`
	Stream      string     `json:"stream"`
	State       string     `json:"state"`
	ByteLength  int64      `json:"byteLength"`
	Truncated   bool       `json:"truncated"`
	Chunks      []LogChunk `json:"chunks"`
}

// LogChunk references one immutable object without exposing a physical path.
type LogChunk struct {
	Sequence     int64     `json:"sequence"`
	StoreName    string    `json:"storeName"`
	StoreVersion int64     `json:"storeVersion"`
	ObjectKey    string    `json:"objectKey"`
	ByteOffset   int64     `json:"byteOffset"`
	ByteLength   int64     `json:"byteLength"`
	Checksum     string    `json:"checksum"`
	CapturedAt   time.Time `json:"capturedAt"`
}

// ArtifactManifest is the authorized immutable output manifest for one shared
// job. Artifact bytes remain in the configured local or NFS store.
type ArtifactManifest struct {
	APIVersion string              `json:"apiVersion"`
	Kind       string              `json:"kind"`
	Namespace  string              `json:"namespace"`
	JobID      string              `json:"jobId"`
	Items      []PublishedArtifact `json:"items"`
}

// PublishedArtifact references one immutable output object.
type PublishedArtifact struct {
	ExecutionID  string    `json:"executionId"`
	RunNumber    int       `json:"runNumber"`
	Name         string    `json:"name"`
	StoreName    string    `json:"storeName"`
	StoreVersion int64     `json:"storeVersion"`
	ObjectKey    string    `json:"objectKey"`
	ByteLength   int64     `json:"byteLength"`
	Checksum     string    `json:"checksum"`
	PublishedAt  time.Time `json:"publishedAt"`
}

// CompletedHistoryImportRequest is the control-service migration envelope for
// one quiescent terminal standalone job.
type CompletedHistoryImportRequest struct {
	APIVersion string                      `json:"apiVersion"`
	Kind       string                      `json:"kind"`
	Metadata   protocol.JobRequestMetadata `json:"metadata"`
	Spec       CompletedHistoryImportSpec  `json:"spec"`
}

// CompletedHistoryImportSpec contains terminal outcome and source provenance.
type CompletedHistoryImportSpec struct {
	Outcome     string                   `json:"outcome"`
	CompletedAt time.Time                `json:"completedAt"`
	Source      CompletedHistorySource   `json:"source"`
	Workload    protocol.WorkloadBinding `json:"workload"`
	Placement   protocol.Placement       `json:"placement"`
}

// CompletedHistorySource identifies the immutable standalone source record.
type CompletedHistorySource struct {
	Store  string `json:"store"`
	Schema int    `json:"schema"`
	JobID  string `json:"jobId"`
}

type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}
