package jobman

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/diagnostic"
	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/config"
)

func TestShowEvidenceJSONUsesDedicatedVerifiedEnvelope(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	backend.evidence = testEvidence(t, nil)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"show", "evidence", "--run", "-1", "--logs", "tail", "--log-bytes", "8KiB",
		"--similar", "2", "--command", "--paths", "--environment-names", "--system", "--json", testJobID,
	})
	if err != nil {
		t.Fatalf("show evidence error = %v", err)
	}
	if backend.evidenceRequest == nil || backend.evidenceRequest.Run != -1 ||
		backend.evidenceRequest.Logs != diagnostic.LogsTail || backend.evidenceRequest.LogBytes != 8<<10 ||
		backend.evidenceRequest.Similar != 2 || !backend.evidenceRequest.IncludeCommand ||
		!backend.evidenceRequest.IncludePaths || !backend.evidenceRequest.IncludeEnvironmentNames ||
		!backend.evidenceRequest.IncludeSystem {
		t.Fatalf("evidence request = %#v", backend.evidenceRequest)
	}
	var envelope struct {
		SchemaVersion int `json:"schema_version"`
		Data          struct {
			Evidence diagnostic.Evidence `json:"evidence"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout)
	}
	if envelope.SchemaVersion != 1 {
		t.Fatalf("envelope schema = %d", envelope.SchemaVersion)
	}
	if err := diagnostic.Verify(envelope.Data.Evidence); err != nil {
		t.Fatalf("Verify(output) error = %v", err)
	}
}

func TestShowEvidenceAppliesConfiguredRedactionBeforeEncoding(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	backend.evidence = testEvidence(t, []byte("private-123\n"))
	configuration := filepath.Join(t.TempDir(), "jobman.yaml")
	if err := os.WriteFile(configuration, []byte("redaction:\n  patterns: ['private-[0-9]+']\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{
		"--config", configuration, "show", "evidence", "--json", testJobID,
	})
	if err != nil {
		t.Fatalf("show evidence error = %v", err)
	}
	if strings.Contains(stdout, "private-123") || !strings.Contains(stdout, "W1JFREFDVEVEXQ") {
		t.Fatalf("redacted evidence output = %s", stdout)
	}
	var envelope struct {
		Data struct {
			Evidence diagnostic.Evidence `json:"evidence"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatal(err)
	}
	if err := diagnostic.Verify(envelope.Data.Evidence); err != nil {
		t.Fatalf("Verify(redacted output) error = %v", err)
	}
}

func TestShowEvidenceHumanSummaryAndFlagValidation(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t)
	backend.evidence = testEvidence(t, nil)
	stdout, err := executeCommand(t, dependenciesFor(backend), []string{"show", "evidence", testJobID})
	if err != nil {
		t.Fatalf("show evidence error = %v", err)
	}
	if !strings.Contains(stdout, backend.evidence.EvidenceID) || !strings.Contains(stdout, "Evidence schema:") {
		t.Fatalf("human evidence summary = %q", stdout)
	}
	for _, arguments := range [][]string{
		{"show", "evidence", "--run", "0", testJobID},
		{"show", "evidence", "--logs", "everything", testJobID},
	} {
		if _, err := executeCommand(t, dependenciesFor(newFakeBackend(t)), arguments); err == nil {
			t.Fatalf("arguments %v error = nil", arguments)
		}
	}
}

func TestShowEvidencePresentationAndBackendErrors(t *testing.T) {
	t.Parallel()

	mode := (*evidenceLogModeValue)(nil)
	if mode.String() != "" {
		t.Fatalf("nil evidence log mode = %q", mode.String())
	}
	if got := newEvidenceLogModeValue(new(diagnostic.LogMode)).Type(); got != "log-mode" {
		t.Fatalf("evidence log mode type = %q", got)
	}
	redactor, err := config.NewRedactor(config.RedactionConfig{Patterns: []string{"secret"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sanitizer := commandEvidenceSanitizer{redactor: redactor}
	if !sanitizer.ValueRedactionConfigured() {
		t.Fatal("configured sanitizer did not report value protection")
	}

	valid := testEvidence(t, nil)
	invalid := valid
	invalid.Subject.Phase = "changed"
	command := &cobra.Command{}
	command.SetOut(&bytes.Buffer{})
	if err := writeEvidenceJSON(command, invalid); err == nil {
		t.Fatal("writeEvidenceJSON(invalid) error = nil")
	}
	command.SetOut(&commandFailWriter{})
	if err := writeEvidenceJSON(command, valid); err == nil {
		t.Fatal("writeEvidenceJSON(failing writer) error = nil")
	}
	if err := writeEvidenceSummary(command, valid); err == nil {
		t.Fatal("writeEvidenceSummary(failing writer) error = nil")
	}

	backend := newFakeBackend(t)
	backend.operationErr = errors.New("evidence failed")
	if _, err := executeCommand(t, dependenciesFor(backend), []string{"show", "evidence", testJobID}); !errors.Is(err, backend.operationErr) {
		t.Fatalf("show evidence backend error = %v", err)
	}
	wrapped := backendWithoutDiagnostics{Backend: newFakeBackend(t)}
	if _, err := executeCommand(t, dependenciesFor(wrapped), []string{"show", "evidence", testJobID}); err == nil {
		t.Fatal("show evidence without diagnostic backend error = nil")
	}
}

func TestExactRunSelectorAndUnavailableLogFormatting(t *testing.T) {
	t.Parallel()

	command := &cobra.Command{Use: "run"}
	command.Flags().Bool("json", false, "")
	if err := exactRunSelectorArgs(command, []string{"job", "1", "--json"}); err != nil {
		t.Fatalf("exactRunSelectorArgs(flag) error = %v", err)
	}
	if err := exactRunSelectorArgs(command, []string{"job", "1", "extra"}); err == nil {
		t.Fatal("exactRunSelectorArgs(extra) error = nil")
	}
	if got := formatLogAvailability(false, nil); got != "unavailable" {
		t.Fatalf("formatLogAvailability(false, nil) = %q", got)
	}
}

type backendWithoutDiagnostics struct{ app.Backend }

func testEvidence(t *testing.T, artifactData []byte) diagnostic.Evidence {
	t.Helper()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	value, err := diagnostic.JSONValue("completed")
	if err != nil {
		t.Fatal(err)
	}
	evidence := diagnostic.Evidence{
		CapturedAt: now,
		Source: diagnostic.Source{
			JobmanVersion: "test", CollectorVersion: diagnostic.CollectorVersion,
			StoreSchemaVersion: 7, Platform: "test", Capabilities: []string{},
		},
		Subject: diagnostic.Subject{
			JobID: testJobID, JobRevision: 1, SelectedRuns: []uint64{1}, Phase: "completed",
		},
		Consistency: diagnostic.Consistency{
			Metadata: diagnostic.MetadataTransactionalSnapshot, Artifacts: diagnostic.ArtifactsNotCollected,
		},
		Items: []diagnostic.Item{{
			ID: "ev:job:phase", Code: diagnostic.CodeJobPhase, Value: value,
			Source:  diagnostic.ItemSource{Kind: "job_snapshot", EntityID: testJobID, Revision: 1},
			Quality: diagnostic.QualityObserved, Disclosure: diagnostic.DisclosureMetadata,
		}},
		Artifacts: []diagnostic.Artifact{}, Omissions: []diagnostic.Omission{},
		RedactionNotices: []diagnostic.RedactionNotice{},
	}
	if artifactData != nil {
		evidence.Consistency.Artifacts = diagnostic.ArtifactsStable
		evidence.Artifacts = []diagnostic.Artifact{{
			ID: "artifact:run:00000000000000000001:stderr", Role: diagnostic.ArtifactRoleLogTail,
			Run: 1, Stream: "stderr", MediaType: "application/octet-stream",
			Data: bytes.Clone(artifactData), OriginalBytes: uint64(len(artifactData)),
			ByteEnd: uint64(len(artifactData)), CapturedAt: now, Quality: diagnostic.QualityObserved,
			Disclosure: diagnostic.DisclosureLogContent,
		}}
	}
	sealed, err := diagnostic.Seal(evidence)
	if err != nil {
		t.Fatal(err)
	}

	return sealed
}
