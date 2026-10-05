package diagnostic

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const (
	defaultMaximumDecodeBytes = 2 * 1024 * 1024
	defaultMaximumJSONDepth   = 32
	maximumIdentifierBytes    = 256
	maximumCodeBytes          = 160
)

// DecodeLimits bounds untrusted evidence decoding. Zero values select safe
// defaults.
type DecodeLimits struct {
	MaxBytes int64
	MaxDepth int
}

// JSONValue returns canonical JSON suitable for an Item value.
func JSONValue(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode evidence value: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return nil, fmt.Errorf("canonicalize evidence value: %w", err)
	}

	return canonical, nil
}

// Seal normalizes, validates, sizes, and hashes an evidence bundle.
func Seal(value Evidence) (Evidence, error) {
	value.Kind = Kind
	if value.SchemaVersion == 0 {
		value.SchemaVersion = SchemaVersion
	}
	value.EvidenceID = ""
	value = normalize(value)
	for index := range value.Artifacts {
		digest := sha256.Sum256(value.Artifacts[index].Data)
		value.Artifacts[index].Digest = "sha256:" + hex.EncodeToString(digest[:])
		if value.Artifacts[index].ByteEnd >= value.Artifacts[index].ByteStart {
			value.Artifacts[index].SelectedBytes = value.Artifacts[index].ByteEnd - value.Artifacts[index].ByteStart
		}
		value.Artifacts[index].ContentBytes = uint64(len(value.Artifacts[index].Data))
	}
	value.Limits = measuredLimits(value)
	// EncodedBytes includes the final fixed-width evidence ID and the field that
	// reports the size itself. Iteration converges once its decimal width stops
	// changing.
	value.EvidenceID = "sha256:" + strings.Repeat("0", sha256.Size*2)
	for range 8 {
		encoded, err := json.Marshal(value)
		if err != nil {
			return Evidence{}, fmt.Errorf("measure evidence: %w", err)
		}
		measured := uint64(len(encoded))
		if value.Limits.EncodedBytes == measured {
			break
		}
		value.Limits.EncodedBytes = measured
	}
	if err := validateEvidence(value, true); err != nil {
		return Evidence{}, err
	}
	digest, err := semanticDigest(value)
	if err != nil {
		return Evidence{}, err
	}
	value.EvidenceID = digest
	if err := Validate(value); err != nil {
		return Evidence{}, err
	}

	return value, nil
}

// Verify checks a sealed bundle, including its semantic digest and measured
// limits.
func Verify(value Evidence) error {
	if err := Validate(value); err != nil {
		return err
	}
	want, err := semanticDigest(value)
	if err != nil {
		return err
	}
	if value.EvidenceID != want {
		return errors.New("verify evidence: evidence ID does not match semantic content")
	}

	return nil
}

// Validate checks the structural and semantic invariants of a sealed bundle.
func Validate(value Evidence) error { return validateEvidence(value, false) }

// Encode writes one verified evidence JSON value followed by a newline.
func Encode(destination io.Writer, value Evidence) error {
	if destination == nil {
		return errors.New("encode evidence: destination is nil")
	}
	if err := Verify(value); err != nil {
		return err
	}
	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode evidence: %w", err)
	}

	return nil
}

// Decode reads, bounds, decodes, and verifies one evidence JSON value.
func Decode(source io.Reader, limits DecodeLimits) (Evidence, error) {
	if source == nil {
		return Evidence{}, errors.New("decode evidence: source is nil")
	}
	maximumBytes := limits.MaxBytes
	if maximumBytes == 0 {
		maximumBytes = defaultMaximumDecodeBytes
	}
	if maximumBytes < 1 {
		return Evidence{}, errors.New("decode evidence: maximum bytes must be positive")
	}
	maximumDepth := limits.MaxDepth
	if maximumDepth == 0 {
		maximumDepth = defaultMaximumJSONDepth
	}
	if maximumDepth < 1 {
		return Evidence{}, errors.New("decode evidence: maximum depth must be positive")
	}
	encoded, err := io.ReadAll(io.LimitReader(source, maximumBytes+1))
	if err != nil {
		return Evidence{}, fmt.Errorf("decode evidence: read: %w", err)
	}
	if int64(len(encoded)) > maximumBytes {
		return Evidence{}, fmt.Errorf("decode evidence: input exceeds %d bytes", maximumBytes)
	}
	if _, err := canonicalJSON(encoded, maximumDepth); err != nil {
		return Evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	var header struct {
		Kind          string `json:"kind"`
		SchemaVersion int    `json:"schema_version"`
	}
	if err := json.Unmarshal(encoded, &header); err != nil {
		return Evidence{}, fmt.Errorf("decode evidence header: %w", err)
	}
	if header.Kind != Kind {
		return Evidence{}, fmt.Errorf("decode evidence: unsupported kind %q", header.Kind)
	}
	if header.SchemaVersion != SchemaVersion && header.SchemaVersion != SharedSchemaVersion {
		return Evidence{}, fmt.Errorf("decode evidence: unsupported schema version %d", header.SchemaVersion)
	}
	var value Evidence
	if err := json.Unmarshal(encoded, &value); err != nil {
		return Evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	if err := Verify(value); err != nil {
		return Evidence{}, err
	}

	return value, nil
}

//nolint:gocognit,cyclop // Contract validation intentionally reports each independent invariant explicitly.
func validateEvidence(value Evidence, allowPlaceholderID bool) error {
	if value.Kind != Kind {
		return fmt.Errorf("validate evidence: kind is %q", value.Kind)
	}
	if value.SchemaVersion != SchemaVersion && value.SchemaVersion != SharedSchemaVersion {
		return fmt.Errorf("validate evidence: unsupported schema version %d", value.SchemaVersion)
	}
	if allowPlaceholderID {
		if value.EvidenceID != "sha256:"+strings.Repeat("0", sha256.Size*2) {
			return errors.New("validate evidence: invalid placeholder evidence ID")
		}
	} else if !validDigest(value.EvidenceID) {
		return errors.New("validate evidence: invalid evidence ID")
	}
	if err := validateUTC("captured_at", value.CapturedAt); err != nil {
		return err
	}
	if value.Source.JobmanVersion == "" || value.Source.CollectorVersion == "" || value.Source.Platform == "" {
		return errors.New("validate evidence: incomplete source")
	}
	if err := validateEvidenceProvenance(value); err != nil {
		return err
	}
	if !sortedUniqueStrings(value.Source.Capabilities) {
		return errors.New("validate evidence: capabilities must be sorted and unique")
	}
	if !validIdentifier(value.Subject.JobID) || value.Subject.JobRevision == 0 || value.Subject.Phase == "" {
		return errors.New("validate evidence: incomplete subject")
	}
	if !slices.IsSorted(value.Subject.SelectedRuns) || hasDuplicateRuns(value.Subject.SelectedRuns) {
		return errors.New("validate evidence: selected runs must be sorted and unique")
	}
	if value.Consistency.Metadata != MetadataTransactionalSnapshot {
		return errors.New("validate evidence: unsupported metadata consistency")
	}
	if !validArtifactConsistency(value.Consistency.Artifacts) {
		return errors.New("validate evidence: unsupported artifact consistency")
	}
	if value.Items == nil || value.Artifacts == nil || value.Omissions == nil || value.RedactionNotices == nil {
		return errors.New("validate evidence: collections must be present")
	}
	if err := validateItems(value.Items); err != nil {
		return err
	}
	if err := validateArtifacts(value.Artifacts); err != nil {
		return err
	}
	if err := validateOmissions(value.Omissions); err != nil {
		return err
	}
	if err := validateRedactions(value.RedactionNotices); err != nil {
		return err
	}
	measured := measuredLimits(value)
	measured.EncodedBytes = value.Limits.EncodedBytes
	if measured != value.Limits {
		return errors.New("validate evidence: measured limits do not match content")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("validate evidence: encode: %w", err)
	}
	if uint64(len(encoded)) != value.Limits.EncodedBytes {
		return errors.New("validate evidence: encoded byte limit does not match content")
	}

	return nil
}

//nolint:gocognit,cyclop // Item validation keeps all untrusted-value checks at one boundary.
func validateItems(items []Item) error {
	prior := ""
	for _, item := range items {
		if !validIdentifier(item.ID) || item.ID <= prior {
			return errors.New("validate evidence: item IDs must be sorted and unique")
		}
		prior = item.ID
		if !validCode(item.Code) || len(item.Value) == 0 {
			return fmt.Errorf("validate evidence: invalid item %q", item.ID)
		}
		canonical, err := canonicalJSON(item.Value, defaultMaximumJSONDepth)
		if err != nil || !bytes.Equal(canonical, item.Value) {
			return fmt.Errorf("validate evidence: item %q value is not canonical JSON", item.ID)
		}
		if item.ObservedAt != nil {
			if err := validateUTC("item observed_at", *item.ObservedAt); err != nil {
				return err
			}
		}
		if item.Source.Kind == "" || !validQuality(item.Quality) || !validDisclosure(item.Disclosure) {
			return fmt.Errorf("validate evidence: incomplete item %q", item.ID)
		}
		if item.Source.ByteEnd < item.Source.ByteStart ||
			(item.Source.ArtifactID == "" && (item.Source.ByteStart != 0 || item.Source.ByteEnd != 0)) {
			return fmt.Errorf("validate evidence: invalid item source %q", item.ID)
		}
	}

	return validateKnownItems(items)
}

func validateKnownItems(items []Item) error {
	if err := validateContextItems(items); err != nil {
		return err
	}
	fingerprints := make(map[string]struct{})
	similar := make([]SimilarFailure, 0)
	seenSimilarRuns := make(map[string]struct{})
	for _, item := range items {
		switch item.Code {
		case CodeResourceObservation:
			if err := validateResourceItem(item); err != nil {
				return err
			}
		case CodeFailureFingerprint:
			fingerprint, err := validateFingerprintItem(item)
			if err != nil {
				return err
			}
			fingerprints[fingerprint.Value] = struct{}{}
		case CodeSimilarFailure:
			failure, err := validateSimilarItem(item)
			if err != nil {
				return err
			}
			key := failure.JobID + "\x00" + failure.RunID
			if _, duplicate := seenSimilarRuns[key]; duplicate {
				return fmt.Errorf("validate evidence: duplicate similar failure %q", item.ID)
			}
			seenSimilarRuns[key] = struct{}{}
			similar = append(similar, failure)
		}
	}
	for _, failure := range similar {
		if _, matches := fingerprints[failure.Fingerprint.Value]; !matches {
			return errors.New("validate evidence: similar failure does not match a bundled fingerprint")
		}
	}

	return nil
}

func validateContextItems(items []Item) error {
	for _, item := range items {
		switch item.Code {
		case CodeTargetCommand, CodeWaitCommand, CodeNotifierCommand:
			if err := validateCommandItem(item); err != nil {
				return err
			}
		case CodeTargetWorkingDirectory, CodeTargetStdinPath, CodeWaitPath,
			CodeNotifierWorkingDirectory, CodeRunResolvedExecutable:
			if err := validatePathItem(item); err != nil {
				return err
			}
		case CodeTargetEnvironmentNames, CodeWaitEnvironmentNames, CodeNotifierEnvironmentNames:
			if err := validateEnvironmentNamesItem(item); err != nil {
				return err
			}
		case CodeExecutionPolicy:
			if err := validateExecutionPolicyItem(item); err != nil {
				return err
			}
		case CodeSystemContext:
			if err := validateSystemContextItem(item); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateCommandItem(item Item) error {
	var command Command
	if err := decodeKnownValue(item.Value, &command); err != nil || command.Validate() != nil ||
		item.Quality != QualityObserved || item.Disclosure != DisclosureCommand {
		return fmt.Errorf("validate evidence: invalid command context %q", item.ID)
	}

	return nil
}

func validatePathItem(item Item) error {
	var path string
	if err := decodeKnownValue(item.Value, &path); err != nil || path == "" ||
		len(path) > 4096 || strings.ContainsRune(path, '\x00') ||
		item.Quality != QualityObserved || item.Disclosure != DisclosurePath {
		return fmt.Errorf("validate evidence: invalid path context %q", item.ID)
	}

	return nil
}

func validateEnvironmentNamesItem(item Item) error {
	var environment EnvironmentNames
	if err := decodeKnownValue(item.Value, &environment); err != nil || environment.Validate() != nil ||
		item.Quality != QualityObserved || item.Disclosure != DisclosureEnvironmentName {
		return fmt.Errorf("validate evidence: invalid environment-name context %q", item.ID)
	}

	return nil
}

func validateExecutionPolicyItem(item Item) error {
	var policy ExecutionPolicy
	if err := decodeKnownValue(item.Value, &policy); err != nil || policy.Validate() != nil ||
		item.Quality != QualityObserved || item.Disclosure != DisclosureMetadata {
		return fmt.Errorf("validate evidence: invalid execution policy %q", item.ID)
	}

	return nil
}

func validateResourceItem(item Item) error {
	var observation ResourceObservation
	if err := decodeKnownValue(item.Value, &observation); err != nil || observation.Validate() != nil ||
		item.Quality != QualityObserved || item.Disclosure != DisclosureMetadata {
		return fmt.Errorf("validate evidence: invalid resource observation %q", item.ID)
	}

	return nil
}

func validateSystemContextItem(item Item) error {
	var context SystemContext
	if err := decodeKnownValue(item.Value, &context); err != nil || context.Validate() != nil ||
		item.Quality != QualityPointInTime || item.Disclosure != DisclosureMetadata {
		return fmt.Errorf("validate evidence: invalid system context %q", item.ID)
	}

	return nil
}

func validateFingerprintItem(item Item) (FailureFingerprint, error) {
	var fingerprint FailureFingerprint
	if err := decodeKnownValue(item.Value, &fingerprint); err != nil || fingerprint.Validate() != nil ||
		item.Quality != QualityDerivedExact || item.Disclosure != DisclosureLocalOnly {
		return FailureFingerprint{}, fmt.Errorf("validate evidence: invalid failure fingerprint %q", item.ID)
	}

	return fingerprint, nil
}

func validateSimilarItem(item Item) (SimilarFailure, error) {
	var failure SimilarFailure
	if err := decodeKnownValue(item.Value, &failure); err != nil || failure.Validate() != nil ||
		item.Quality != QualityDerivedExact || item.Disclosure != DisclosureLocalOnly {
		return SimilarFailure{}, fmt.Errorf("validate evidence: invalid similar failure %q", item.ID)
	}

	return failure, nil
}

func decodeKnownValue(encoded json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("known evidence value contains trailing JSON")
	}

	return nil
}

//nolint:cyclop // Artifact validation enumerates independent integrity invariants.
func validateArtifacts(artifacts []Artifact) error {
	prior := ""
	for _, artifact := range artifacts {
		if !validIdentifier(artifact.ID) || artifact.ID <= prior {
			return errors.New("validate evidence: artifact IDs must be sorted and unique")
		}
		prior = artifact.ID
		if artifact.Role == "" || artifact.Run == 0 || artifact.MediaType == "" ||
			!validDigest(artifact.Digest) || !validQuality(artifact.Quality) || !validDisclosure(artifact.Disclosure) {
			return fmt.Errorf("validate evidence: invalid artifact %q", artifact.ID)
		}
		if err := validateUTC("artifact captured_at", artifact.CapturedAt); err != nil {
			return err
		}
		if artifact.ByteEnd < artifact.ByteStart || artifact.ByteEnd > artifact.OriginalBytes ||
			artifact.SelectedBytes != artifact.ByteEnd-artifact.ByteStart ||
			artifact.ContentBytes != uint64(len(artifact.Data)) {
			return fmt.Errorf("validate evidence: invalid artifact range %q", artifact.ID)
		}
		digest := sha256.Sum256(artifact.Data)
		if artifact.Digest != "sha256:"+hex.EncodeToString(digest[:]) {
			return fmt.Errorf("validate evidence: artifact %q digest mismatch", artifact.ID)
		}
	}

	return nil
}

func validateOmissions(values []Omission) error {
	prior := ""
	for _, value := range values {
		key := value.Code + "\x00" + strings.Join(value.Affects, "\x00")
		if !validCode(value.Code) || key <= prior || len(value.Affects) == 0 || !sortedUniqueStrings(value.Affects) {
			return errors.New("validate evidence: omissions must be valid, sorted, and unique")
		}
		prior = key
	}

	return nil
}

func validateRedactions(values []RedactionNotice) error {
	prior := ""
	for _, value := range values {
		key := value.Code + "\x00" + strings.Join(value.Affects, "\x00")
		if !validCode(value.Code) || key <= prior || value.Count == 0 ||
			len(value.Affects) == 0 || !sortedUniqueStrings(value.Affects) {
			return errors.New("validate evidence: redaction notices must be valid, sorted, and unique")
		}
		prior = key
	}

	return nil
}

func normalize(value Evidence) Evidence {
	value.CapturedAt = normalizeTime(value.CapturedAt)
	slices.Sort(value.Source.Capabilities)
	value.Source.Capabilities = slices.Compact(value.Source.Capabilities)
	slices.Sort(value.Subject.SelectedRuns)
	value.Subject.SelectedRuns = slices.Compact(value.Subject.SelectedRuns)
	for index := range value.Items {
		if value.Items[index].ObservedAt != nil {
			timestamp := normalizeTime(*value.Items[index].ObservedAt)
			value.Items[index].ObservedAt = &timestamp
		}
		canonical, err := canonicalJSON(value.Items[index].Value, defaultMaximumJSONDepth)
		if err == nil {
			value.Items[index].Value = canonical
		}
	}
	for index := range value.Artifacts {
		value.Artifacts[index].CapturedAt = normalizeTime(value.Artifacts[index].CapturedAt)
	}
	slices.SortFunc(value.Items, func(left, right Item) int { return strings.Compare(left.ID, right.ID) })
	slices.SortFunc(value.Artifacts, func(left, right Artifact) int { return strings.Compare(left.ID, right.ID) })
	for index := range value.Omissions {
		slices.Sort(value.Omissions[index].Affects)
		value.Omissions[index].Affects = slices.Compact(value.Omissions[index].Affects)
	}
	slices.SortFunc(value.Omissions, func(left, right Omission) int {
		return strings.Compare(left.Code+"\x00"+strings.Join(left.Affects, "\x00"), right.Code+"\x00"+strings.Join(right.Affects, "\x00"))
	})
	for index := range value.RedactionNotices {
		slices.Sort(value.RedactionNotices[index].Affects)
		value.RedactionNotices[index].Affects = slices.Compact(value.RedactionNotices[index].Affects)
	}
	slices.SortFunc(value.RedactionNotices, func(left, right RedactionNotice) int {
		return strings.Compare(left.Code+"\x00"+strings.Join(left.Affects, "\x00"), right.Code+"\x00"+strings.Join(right.Affects, "\x00"))
	})
	if value.Items == nil {
		value.Items = []Item{}
	}
	if value.Artifacts == nil {
		value.Artifacts = []Artifact{}
	}
	if value.Omissions == nil {
		value.Omissions = []Omission{}
	}
	if value.RedactionNotices == nil {
		value.RedactionNotices = []RedactionNotice{}
	}

	return value
}

func semanticDigest(value Evidence) (string, error) {
	semanticLimits := value.Limits
	// EncodedBytes is presentation metadata and varies with CapturedAt, which is
	// deliberately excluded from the stable semantic identity.
	semanticLimits.EncodedBytes = 0
	semanticArtifacts := slices.Clone(value.Artifacts)
	for index := range semanticArtifacts {
		semanticArtifacts[index].CapturedAt = time.Time{}
	}
	projection := struct {
		Kind             string            `json:"kind"`
		SchemaVersion    int               `json:"schema_version"`
		Source           Source            `json:"source"`
		Subject          Subject           `json:"subject"`
		Consistency      Consistency       `json:"consistency"`
		Items            []Item            `json:"items"`
		Artifacts        []Artifact        `json:"artifacts"`
		Omissions        []Omission        `json:"omissions"`
		RedactionNotices []RedactionNotice `json:"redaction_notices"`
		Limits           Limits            `json:"limits"`
		Shared           *SharedProvenance `json:"shared,omitempty"`
	}{
		Kind: value.Kind, SchemaVersion: value.SchemaVersion, Source: value.Source,
		Subject: value.Subject, Consistency: value.Consistency, Items: value.Items,
		Artifacts: semanticArtifacts, Omissions: value.Omissions,
		RedactionNotices: value.RedactionNotices, Limits: semanticLimits, Shared: value.Shared,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("hash evidence: %w", err)
	}
	digest := sha256.Sum256(encoded)

	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func measuredLimits(value Evidence) Limits {
	limits := Limits{ItemCount: uint64(len(value.Items)), ArtifactCount: uint64(len(value.Artifacts))}
	for _, artifact := range value.Artifacts {
		limits.ArtifactBytes += uint64(len(artifact.Data))
		if artifact.Disclosure == DisclosureLogContent {
			limits.LogBytes += uint64(len(artifact.Data))
		}
	}

	return limits
}

func validDigest(value string) bool {
	encoded, found := strings.CutPrefix(value, "sha256:")
	if !found || len(encoded) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(encoded)

	return err == nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > maximumIdentifierBytes {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}

	return true
}

func validCode(value string) bool {
	if value == "" || len(value) > maximumCodeBytes || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			character == '_' || character == '.' || character == '-' {
			continue
		}
		return false
	}

	return true
}

func validQuality(value Quality) bool {
	return value == QualityObserved || value == QualityConfirmed || value == QualityDerivedExact ||
		value == QualityPointInTime || value == QualityUnknown
}

func validDisclosure(value DisclosureClass) bool {
	return value == DisclosureMetadata || value == DisclosureCommand || value == DisclosurePath ||
		value == DisclosureEnvironmentName || value == DisclosureLogContent || value == DisclosureSensitive ||
		value == DisclosureLocalOnly
}

func validArtifactConsistency(value ArtifactConsistency) bool {
	return value == ArtifactsNotCollected || value == ArtifactsStable || value == ArtifactsPointInTime ||
		value == ArtifactsMixed
}

func validateUTC(field string, value time.Time) error {
	if value.IsZero() || value.Location() != time.UTC {
		return fmt.Errorf("validate evidence: %s must be a nonzero UTC timestamp", field)
	}

	return nil
}

func normalizeTime(value time.Time) time.Time { return value.UTC().Round(0) }

func sortedUniqueStrings(values []string) bool {
	return slices.IsSorted(values) && !hasDuplicateStrings(values)
}

func hasDuplicateStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return true
		}
	}

	return false
}

func hasDuplicateRuns(values []uint64) bool {
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return true
		}
	}

	return false
}
