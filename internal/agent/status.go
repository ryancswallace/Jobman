package agent

import (
	"fmt"
	"time"

	"github.com/ryancswallace/jobman/internal/buildinfo"
)

// Status is a secret-free description of an enrolled agent installation.
type Status struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   StatusMetadata `json:"metadata"`
	Status     StatusState    `json:"status"`
}

// StatusMetadata identifies the enrolled agent and immutable target generation.
type StatusMetadata struct {
	AgentID            string `json:"agentId"`
	TargetGenerationID string `json:"targetGenerationId"`
}

// StatusState reports non-secret local version and credential-expiry metadata.
type StatusState struct {
	AgentVersion         string    `json:"agentVersion"`
	ServerURL            string    `json:"serverUrl"`
	SessionExpiresAt     time.Time `json:"sessionExpiresAt"`
	CertificateExpiresAt time.Time `json:"certificateExpiresAt"`
}

func loadStatus(stateDirectory string) (Status, error) {
	credentials, err := loadCredentials(stateDirectory)
	if err != nil {
		return Status{}, fmt.Errorf("read agent status: %w", err)
	}

	return Status{
		APIVersion: "jobman.agent/v1alpha1", Kind: "AgentStatus",
		Metadata: StatusMetadata{
			AgentID: credentials.Metadata.AgentID, TargetGenerationID: credentials.Metadata.TargetGenerationID,
		},
		Status: StatusState{
			AgentVersion: buildinfo.Version, ServerURL: credentials.Metadata.ServerURL,
			SessionExpiresAt:     credentials.Metadata.SessionExpiresAt,
			CertificateExpiresAt: credentials.Metadata.CertificateExpiresAt,
		},
	}, nil
}
