package diagnostic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
)

const sharedLogsDomain = "logs"

// Collect reads one authorized metadata snapshot, optionally captures bounded
// tails outside its transaction, then seals the exact schema-2 evidence. Errors
// never yield partial evidence. Readers must implement current authorization;
// the collector cannot infer access from an earlier authenticated request.
func (collector SharedCollector) Collect(ctx context.Context, request SharedCollectionRequest) (Evidence, error) {
	if err := validateSharedCollectionRequest(request, collector.Snapshots); err != nil {
		return Evidence{}, err
	}
	if err := ctx.Err(); err != nil {
		return Evidence{}, err
	}
	snapshot, err := collector.Snapshots.ReadSnapshot(ctx, request.Selection)
	if err != nil {
		return Evidence{}, fmt.Errorf("collect shared snapshot: %w", err)
	}
	if err := ValidateSharedSnapshot(snapshot); err != nil {
		return Evidence{}, err
	}
	if err := matchSharedSelection(request.Selection, snapshot); err != nil {
		return Evidence{}, err
	}
	evidence := sharedEvidence(snapshot, request.IncludeLogTail)
	if err := collectSharedItems(&evidence, snapshot.Items); err != nil {
		return Evidence{}, err
	}
	if request.IncludeLogTail {
		if err := collector.collectSharedLogs(ctx, &evidence, request); err != nil {
			return Evidence{}, err
		}
	} else {
		evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionLogContentNotRequested, Affects: []string{sharedLogsDomain}})
	}
	if err := ctx.Err(); err != nil {
		return Evidence{}, err
	}
	return sealSharedWithinBudget(evidence)
}

func validateSharedCollectionRequest(request SharedCollectionRequest, reader SnapshotReader) error {
	if reader == nil {
		return errors.New("collect shared evidence: snapshot reader is nil")
	}
	if err := validateSharedSelection(request.Selection); err != nil {
		return err
	}
	if request.LogBytes > SharedMaximumTailBytes || (!request.IncludeLogTail && request.LogBytes != 0) {
		return errors.New("collect shared evidence: invalid log budget")
	}
	return nil
}

func sharedEvidence(snapshot SharedSnapshot, includeLogs bool) Evidence {
	profile := SharedProfileMetadata
	if includeLogs {
		profile = SharedProfileIncludeLogTail
	}
	runs := make([]uint64, len(snapshot.Runs))
	for index, run := range snapshot.Runs {
		runs[index] = run.Number
	}
	evidence := Evidence{
		SchemaVersion: SharedSchemaVersion,
		CapturedAt:    snapshot.CapturedAt,
		Source: Source{
			JobmanVersion: snapshot.JobmanVersion, CollectorVersion: SharedCollectorVersion,
			Platform: snapshot.Platform, Capabilities: []string{"shared_snapshot_v1"},
		},
		Subject: Subject{
			JobID: snapshot.Job.ID, JobRevision: snapshot.Job.Revision,
			SelectedRuns: runs, Phase: snapshot.Job.Phase, Outcome: snapshot.Job.Outcome,
		},
		Consistency: Consistency{
			Metadata: snapshot.Metadata, Artifacts: ArtifactsNotCollected,
			ActiveStateMayHaveAdvanced: snapshot.Job.Phase != "terminal",
		},
		Omissions:        cloneSharedOmissions(snapshot.Omissions),
		RedactionNotices: slices.Clone(snapshot.RedactionNotices),
		Shared: &SharedProvenance{
			Source: snapshot.Source, Runs: slices.Clone(snapshot.Runs), Logs: slices.Clone(snapshot.Logs), Profile: profile,
		},
	}
	for _, run := range snapshot.Runs {
		if run.ExecutionID == "" {
			evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionSharedExecutionIdentity, Affects: []string{run.ID}})
		}
	}
	return evidence
}

func collectSharedItems(evidence *Evidence, items []Item) error {
	for _, item := range items {
		if item.Disclosure != DisclosureMetadata {
			evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionSharedDisclosure, Affects: []string{item.ID}})
			continue
		}
		if !sharedFactAllowed(item.Code) {
			evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionSharedUnsupportedFact, Affects: []string{item.ID}})
			continue
		}
		if err := validateSharedItem(item); err != nil {
			return err
		}
		item.Value = bytes.Clone(item.Value)
		evidence.Items = append(evidence.Items, item)
	}
	return nil
}

func (collector SharedCollector) collectSharedLogs(ctx context.Context, evidence *Evidence, request SharedCollectionRequest) error {
	reporter, configured := collector.Sanitizer.(ValueRedactionReporter)
	if !configured || !reporter.ValueRedactionConfigured() {
		evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionConfiguredRedactionMissing, Affects: []string{sharedLogsDomain}})
		return nil
	}
	if len(evidence.Shared.Logs) == 0 {
		evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionLogsUnavailable, Affects: []string{sharedLogsDomain}})
		return nil
	}
	if collector.Logs == nil {
		return errors.New("collect shared evidence: log reader is nil")
	}
	limit := request.LogBytes
	if limit == 0 {
		limit = SharedMaximumTailBytes
	}
	selection := request.Selection
	selection.ExpectedJobRevision = evidence.Subject.JobRevision
	for _, ref := range evidence.Shared.Logs {
		if err := ctx.Err(); err != nil {
			return err
		}
		logRequest := SharedLogRequest{Selection: selection, Reference: ref, MaxBytes: limit}
		tail, err := collector.Logs.ReadLogTail(ctx, logRequest)
		if err != nil {
			return fmt.Errorf("collect shared log tail: %w", err)
		}
		if err := validateSharedTail(tail, logRequest); err != nil {
			return err
		}
		if err := collector.addSharedTail(evidence, tail); err != nil {
			return err
		}
	}
	return nil
}

func (collector SharedCollector) addSharedTail(evidence *Evidence, tail SharedLogTail) error {
	data, redacted := collector.Sanitizer.Sanitize("shared.log."+tail.Reference.Stream, bytes.Clone(tail.Data))
	redacted = redacted || !bytes.Equal(data, tail.Data)
	if len(data) > SharedMaximumTailBytes {
		return errors.New("collect shared evidence: sanitized log exceeds stream budget")
	}
	var runNumber uint64
	for _, run := range evidence.Shared.Runs {
		if run.ID == tail.Reference.RunID {
			runNumber = run.Number
			break
		}
	}
	evidence.Artifacts = append(evidence.Artifacts, Artifact{
		ID: tail.Reference.ID, Role: ArtifactRoleLogTail, Run: runNumber, Stream: tail.Reference.Stream,
		MediaType: "application/octet-stream", Data: bytes.Clone(data), OriginalBytes: tail.Reference.Bytes,
		ByteStart: tail.ByteStart, ByteEnd: tail.ByteEnd, Truncated: tail.ByteStart > 0,
		CapturedAt: tail.CapturedAt, Quality: sharedLogQuality(tail.Reference), Disclosure: DisclosureLogContent,
	})
	if redacted {
		evidence.RedactionNotices = append(evidence.RedactionNotices, RedactionNotice{
			Code: RedactionConfiguredPattern, Affects: []string{tail.Reference.ID}, Count: 1,
		})
	}
	return nil
}

func sealSharedWithinBudget(evidence Evidence) (Evidence, error) {
	// The encoded budget includes base64 expansion. Remove whole artifacts
	// deterministically, keeping original manifest references and explicit gaps.
	for {
		evidence.Consistency.Artifacts = sharedArtifactConsistency(evidence.Artifacts)
		evidence.Omissions = mergeSharedOmissions(evidence.Omissions)
		sealed, err := Seal(evidence)
		if err == nil {
			return sealed, nil
		}
		if len(evidence.Artifacts) == 0 {
			return Evidence{}, err
		}
		if !errors.Is(err, errSharedEvidenceBounds) {
			return Evidence{}, err
		}
		last := evidence.Artifacts[len(evidence.Artifacts)-1]
		evidence.Artifacts = evidence.Artifacts[:len(evidence.Artifacts)-1]
		evidence.Omissions = append(evidence.Omissions, Omission{Code: OmissionLogBudgetExceeded, Affects: []string{last.ID}})
	}
}

func sharedArtifactConsistency(artifacts []Artifact) ArtifactConsistency {
	if len(artifacts) == 0 {
		return ArtifactsNotCollected
	}
	stable, pointInTime := false, false
	for _, artifact := range artifacts {
		stable = stable || artifact.Quality == QualityConfirmed
		pointInTime = pointInTime || artifact.Quality == QualityPointInTime
	}
	if stable && pointInTime {
		return ArtifactsMixed
	}
	if pointInTime {
		return ArtifactsPointInTime
	}
	return ArtifactsStable
}

func cloneSharedOmissions(values []Omission) []Omission {
	cloned := slices.Clone(values)
	for index := range cloned {
		cloned[index].Affects = slices.Clone(cloned[index].Affects)
	}
	return cloned
}

func mergeSharedOmissions(values []Omission) []Omission {
	byCode := make(map[string][]string, len(values))
	for _, omission := range values {
		byCode[omission.Code] = append(byCode[omission.Code], omission.Affects...)
	}
	merged := make([]Omission, 0, len(byCode))
	for code, affects := range byCode {
		slices.Sort(affects)
		merged = append(merged, Omission{Code: code, Affects: slices.Compact(affects)})
	}
	return merged
}
