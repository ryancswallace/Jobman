package diagnostic

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

var errSharedEvidenceBounds = errors.New("validate evidence: shared evidence bounds exceeded")

// DecodeSharedSnapshot bounds and validates a snapshot from an authenticated
// Control transport. It rejects duplicate keys, trailing values and unknown
// envelope fields. This is validation, not source authentication/authorization.
func DecodeSharedSnapshot(reader io.Reader) (SharedSnapshot, error) {
	if reader == nil {
		return SharedSnapshot{}, errors.New("decode shared snapshot: reader is nil")
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, SharedMaximumBytes+1))
	if err != nil {
		return SharedSnapshot{}, fmt.Errorf("decode shared snapshot: %w", err)
	}
	if len(encoded) > SharedMaximumBytes {
		return SharedSnapshot{}, errors.New("decode shared snapshot: byte limit exceeded")
	}
	if _, err := canonicalJSON(encoded, defaultMaximumJSONDepth); err != nil {
		return SharedSnapshot{}, fmt.Errorf("decode shared snapshot: %w", err)
	}
	var snapshot SharedSnapshot
	if err := decodeKnownValue(encoded, &snapshot); err != nil {
		return SharedSnapshot{}, fmt.Errorf("decode shared snapshot: %w", err)
	}
	if err := ValidateSharedSnapshot(snapshot); err != nil {
		return SharedSnapshot{}, err
	}

	return snapshot, nil
}

// ValidateSharedSnapshot checks bounds, identity joins, metadata consistency and
// factual structure before collection. It does not authenticate the provider.
func ValidateSharedSnapshot(snapshot SharedSnapshot) error {
	if err := validateSharedSnapshotHeader(snapshot); err != nil {
		return err
	}
	if err := validateSharedSource(snapshot.Source); err != nil {
		return err
	}
	if err := validateSharedReferences(snapshot.Runs, snapshot.Logs); err != nil {
		return err
	}
	if err := validateItems(snapshot.Items); err != nil {
		return err
	}
	if err := validateOmissions(snapshot.Omissions); err != nil {
		return err
	}
	if err := validateRedactions(snapshot.RedactionNotices); err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > SharedMaximumBytes {
		return errors.New("validate shared snapshot: encoded byte limit exceeded")
	}
	return nil
}

func validateSharedSnapshotHeader(snapshot SharedSnapshot) error {
	if snapshot.Kind != SharedSnapshotKind || snapshot.SchemaVersion != SharedSnapshotVersion {
		return errors.New("validate shared snapshot: unsupported kind or version")
	}
	if snapshot.Metadata != MetadataTransactionalSnapshot || snapshot.JobmanVersion == "" || snapshot.Platform == "" {
		return errors.New("validate shared snapshot: incomplete collection source")
	}
	if err := validateUTC("shared snapshot captured_at", snapshot.CapturedAt); err != nil {
		return err
	}
	if !validSharedUUID(snapshot.Job.ID) || snapshot.Job.Revision == 0 ||
		!sharedToken(snapshot.Job.Phase, false) || !sharedToken(snapshot.Job.Outcome, true) {
		return errors.New("validate shared snapshot: invalid job")
	}
	if sharedSnapshotCollectionsAbsent(snapshot) || len(snapshot.Items) > SharedMaximumItems {
		return errors.New("validate shared snapshot: absent or excessive collections")
	}
	return nil
}

func sharedSnapshotCollectionsAbsent(snapshot SharedSnapshot) bool {
	return snapshot.Items == nil || snapshot.Logs == nil || snapshot.Runs == nil ||
		snapshot.Omissions == nil || snapshot.RedactionNotices == nil
}

func validateEvidenceProvenance(value Evidence) error {
	if value.SchemaVersion == SchemaVersion {
		if value.Source.StoreSchemaVersion < 1 || value.Shared != nil {
			return errors.New("validate evidence: invalid local-store provenance")
		}
		return nil
	}
	if value.Source.StoreSchemaVersion != 0 || value.Shared == nil {
		return errors.New("validate evidence: shared evidence must have shared provenance and no local store")
	}
	if err := validateSharedEvidenceIdentity(value); err != nil {
		return err
	}
	for _, item := range value.Items {
		if err := validateSharedItem(item); err != nil {
			return err
		}
		if err := validateSharedFactJoin(item, value); err != nil {
			return err
		}
	}
	return validateSharedArtifacts(value)
}

func validateSharedEvidenceIdentity(value Evidence) error {
	if err := validateSharedSource(value.Shared.Source); err != nil {
		return err
	}
	if err := validateSharedReferences(value.Shared.Runs, value.Shared.Logs); err != nil {
		return err
	}
	if !validSharedUUID(value.Subject.JobID) || !sharedProfileValid(value.Shared.Profile) ||
		!sharedToken(value.Subject.Phase, false) || !sharedToken(value.Subject.Outcome, true) {
		return errors.New("validate evidence: invalid shared subject or profile")
	}
	runNumbers := make([]uint64, len(value.Shared.Runs))
	for index, run := range value.Shared.Runs {
		runNumbers[index] = run.Number
	}
	if !slices.Equal(runNumbers, value.Subject.SelectedRuns) {
		return errors.New("validate evidence: selected runs do not match shared identities")
	}
	// Encode adds a newline, included in the public decoder's input budget.
	if len(value.Items) > SharedMaximumItems || value.Limits.EncodedBytes >= SharedMaximumBytes {
		return errSharedEvidenceBounds
	}
	return validateSharedCitationIDs(value.Items, value.Shared.Logs)
}

func validateSharedCitationIDs(items []Item, logs []SharedLogReference) error {
	ids := make(map[string]bool, len(items))
	for _, item := range items {
		ids[item.ID] = true
	}
	for _, ref := range logs {
		if ids[ref.ID] {
			return errors.New("validate evidence: shared fact and log reference reuse a citation ID")
		}
	}
	return nil
}

func validateSharedSource(source SharedSource) error {
	if source.Kind != SharedSourceControl || !validSharedUUID(source.DeploymentID) ||
		!validSharedUUID(source.ControlInstanceID) || !validSharedUUID(source.NamespaceID) ||
		!validIdentifier(source.ControlVersion) || !validIdentifier(source.ContractVersion) {
		return errors.New("validate shared source: incomplete Control provenance")
	}
	return nil
}

func validateSharedReferences(runs []SharedRun, logs []SharedLogReference) error {
	if runs == nil || logs == nil || len(runs) > SharedMaximumRuns || len(logs) > 2*SharedMaximumRuns {
		return errors.New("validate shared references: absent or excessive collections")
	}
	runIDs := make(map[string]SharedRun, len(runs))
	var previous uint64
	for _, run := range runs {
		_, duplicate := runIDs[run.ID]
		if !validSharedUUID(run.ID) || run.Number <= previous || duplicate ||
			(run.ExecutionID != "" && !validSharedUUID(run.ExecutionID)) {
			return errors.New("validate shared references: invalid or duplicate run identity")
		}
		previous = run.Number
		runIDs[run.ID] = run
	}
	return validateSharedLogReferences(logs, runIDs)
}

func validateSharedLogReferences(logs []SharedLogReference, runs map[string]SharedRun) error {
	previous := ""
	streams := make(map[string]bool, len(logs))
	for _, log := range logs {
		run, exists := runs[log.RunID]
		key := log.RunID + ":" + log.Stream
		if !validIdentifier(log.ID) || log.ID <= previous || !exists || streams[key] ||
			!validSharedUUID(log.ExecutionID) || log.ExecutionID != run.ExecutionID ||
			(log.Stream != "stdout" && log.Stream != "stderr") || log.ManifestRevision == 0 {
			return errors.New("validate shared references: invalid log identity or run join")
		}
		previous = log.ID
		streams[key] = true
	}
	return nil
}

func validateSharedArtifacts(value Evidence) error {
	if value.Shared.Profile == SharedProfileMetadata && len(value.Artifacts) != 0 {
		return errors.New("validate evidence: metadata profile contains artifacts")
	}
	if value.Consistency.Artifacts != sharedArtifactConsistency(value.Artifacts) {
		return errors.New("validate evidence: artifact consistency does not match collected tails")
	}
	refs := make(map[string]SharedLogReference, len(value.Shared.Logs))
	for _, ref := range value.Shared.Logs {
		refs[ref.ID] = ref
	}
	runs := make(map[string]uint64, len(value.Shared.Runs))
	for _, run := range value.Shared.Runs {
		runs[run.ID] = run.Number
	}
	for _, artifact := range value.Artifacts {
		ref, exists := refs[artifact.ID]
		if !exists {
			return errors.New("validate evidence: shared artifact has no sealed manifest")
		}
		if err := validateSharedArtifact(artifact, ref, runs[ref.RunID]); err != nil {
			return err
		}
	}
	return nil
}

func validateSharedArtifact(artifact Artifact, ref SharedLogReference, runNumber uint64) error {
	if artifact.Role != ArtifactRoleLogTail || artifact.Disclosure != DisclosureLogContent ||
		artifact.Run != runNumber || artifact.Stream != ref.Stream || artifact.OriginalBytes != ref.Bytes ||
		artifact.ByteEnd != ref.Bytes || artifact.SelectedBytes > SharedMaximumTailBytes ||
		len(artifact.Data) > SharedMaximumTailBytes || artifact.Truncated != (artifact.ByteStart > 0) {
		return errors.New("validate evidence: shared artifact does not match its sealed manifest")
	}
	if artifact.Quality != sharedLogQuality(ref) {
		return errors.New("validate evidence: shared artifact consistency does not match manifest")
	}
	return nil
}

func validSharedUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16 && compact != strings.Repeat("0", 32) && value == strings.ToLower(value)
}

func sharedProfileValid(profile string) bool {
	return profile == SharedProfileMetadata || profile == SharedProfileIncludeLogTail
}

func matchSharedSelection(selection SharedSelection, snapshot SharedSnapshot) error {
	if selection.DeploymentID != snapshot.Source.DeploymentID || selection.ControlInstanceID != snapshot.Source.ControlInstanceID ||
		selection.NamespaceID != snapshot.Source.NamespaceID || selection.JobID != snapshot.Job.ID ||
		(selection.ExpectedJobRevision != 0 && selection.ExpectedJobRevision != snapshot.Job.Revision) {
		return errors.New("collect shared evidence: snapshot does not match requested authority and revision")
	}
	if selection.RunID != "" && (len(snapshot.Runs) != 1 || snapshot.Runs[0].ID != selection.RunID) {
		return errors.New("collect shared evidence: snapshot does not match requested run")
	}
	return nil
}

func validateSharedTail(tail SharedLogTail, request SharedLogRequest) error {
	expectedBytes := min(request.MaxBytes, request.Reference.Bytes)
	if tail.Reference != request.Reference || tail.ByteEnd != request.Reference.Bytes || tail.ByteStart > tail.ByteEnd ||
		tail.ByteEnd-tail.ByteStart != uint64(len(tail.Data)) || uint64(len(tail.Data)) != expectedBytes {
		return errors.New("collect shared evidence: log range does not match requested manifest")
	}
	return validateUTC("shared log captured_at", tail.CapturedAt)
}

func validateSharedSelection(selection SharedSelection) error {
	if !validSharedUUID(selection.DeploymentID) || !validSharedUUID(selection.ControlInstanceID) ||
		!validSharedUUID(selection.NamespaceID) || !validSharedUUID(selection.JobID) ||
		(selection.RunID != "" && !validSharedUUID(selection.RunID)) {
		return errors.New("collect shared evidence: invalid source-qualified selection")
	}
	return nil
}

func sharedLogQuality(ref SharedLogReference) Quality {
	if ref.Complete {
		return QualityConfirmed
	}
	return QualityPointInTime
}

// Ensure ordinary JSON round trips do not silently reinterpret very large
// integers in provider facts; use JSONValue or canonical raw JSON throughout.
func canonicalSharedValue(raw json.RawMessage) (json.RawMessage, error) {
	canonical, err := canonicalJSON(raw, defaultMaximumJSONDepth)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.New("validate shared fact: noncanonical value")
	}
	return canonical, nil
}
