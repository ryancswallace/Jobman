package diagnostic

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestResourceObservationValidationMatrix(t *testing.T) {
	t.Parallel()

	validCPU := ResourceObservation{
		Metric: ResourceCPUUserTime, Value: 1, Unit: ResourceUnitNanoseconds,
		Scope: ResourceScopeProcess, Source: ResourceSourceProcessState,
		Completeness: ResourceCompleteAtExit,
	}
	validMemory := ResourceObservation{
		Metric: ResourcePeakRSS, Value: 1, Unit: ResourceUnitBytes,
		Scope: ResourceScopeTree, Source: ResourceSourceWaitRusage,
		Completeness: ResourcePartial,
	}
	if err := validCPU.Validate(); err != nil {
		t.Fatalf("Validate(valid CPU) error = %v", err)
	}
	if err := validMemory.Validate(); err != nil {
		t.Fatalf("Validate(valid memory) error = %v", err)
	}

	tests := map[string]func(*ResourceObservation){
		"scope":        func(value *ResourceObservation) { value.Scope = "host" },
		"source":       func(value *ResourceObservation) { value.Source = "guess" },
		"completeness": func(value *ResourceObservation) { value.Completeness = "maybe" },
		"CPU unit":     func(value *ResourceObservation) { value.Unit = ResourceUnitBytes },
		"memory unit": func(value *ResourceObservation) {
			value.Metric = ResourcePeakRSS
			value.Unit = ResourceUnitNanoseconds
		},
		"metric": func(value *ResourceObservation) { value.Metric = "gpu_time" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := validCPU
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatalf("Validate(%#v) error = nil", value)
			}
		})
	}
}

func TestFailureFingerprintAndSimilarityValidationMatrix(t *testing.T) {
	t.Parallel()

	fingerprint := FailureFingerprint{
		Algorithm:          FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: FingerprintInputSchemaVersion,
		Value:              strings.Repeat("a", 64), Scope: FingerprintScopeStoreLocal,
	}
	if err := fingerprint.Validate(); err != nil {
		t.Fatalf("Validate(valid fingerprint) error = %v", err)
	}
	for name, mutate := range map[string]func(*FailureFingerprint){
		"algorithm": func(value *FailureFingerprint) { value.Algorithm = "sha256" },
		"version":   func(value *FailureFingerprint) { value.InputSchemaVersion++ },
		"scope":     func(value *FailureFingerprint) { value.Scope = "global" },
		"length":    func(value *FailureFingerprint) { value.Value = "abc" },
		"hex":       func(value *FailureFingerprint) { value.Value = strings.Repeat("A", 64) },
	} {
		t.Run("fingerprint "+name, func(t *testing.T) {
			t.Parallel()
			value := fingerprint
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatalf("Validate(%#v) error = nil", value)
			}
		})
	}

	valid := SimilarFailure{
		JobID: "job-1", RunID: "run-1", RunNumber: 1,
		CompletedAt: time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
		Outcome:     "failure", FailureClass: "nonzero_exit", Fingerprint: fingerprint,
	}
	for _, outcome := range []string{"failure", "timed_out", "cancelled", "start_failed", "lost"} { //nolint:misspell // Stable Jobman outcome spelling.
		value := valid
		value.Outcome = outcome
		if err := value.Validate(); err != nil {
			t.Errorf("Validate(outcome %q) error = %v", outcome, err)
		}
	}
	for name, mutate := range map[string]func(*SimilarFailure){
		"job":          func(value *SimilarFailure) { value.JobID = "" },
		"run":          func(value *SimilarFailure) { value.RunID = "bad run" },
		"number":       func(value *SimilarFailure) { value.RunNumber = 0 },
		"time":         func(value *SimilarFailure) { value.CompletedAt = time.Time{} },
		"time zone":    func(value *SimilarFailure) { value.CompletedAt = value.CompletedAt.In(time.FixedZone("test", 60)) },
		"outcome":      func(value *SimilarFailure) { value.Outcome = "success" },
		"empty class":  func(value *SimilarFailure) { value.FailureClass = "" },
		"class prefix": func(value *SimilarFailure) { value.FailureClass = "1bad" },
		"class rune":   func(value *SimilarFailure) { value.FailureClass = "bad-class" },
		"fingerprint":  func(value *SimilarFailure) { value.Fingerprint.Value = "bad" },
	} {
		t.Run("similar "+name, func(t *testing.T) {
			t.Parallel()
			value := valid
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatalf("Validate(%#v) error = nil", value)
			}
		})
	}
}

func TestValidateRejectsMalformedEvidenceEnvelope(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Evidence){
		"kind":                 func(value *Evidence) { value.Kind = "other" },
		"schema":               func(value *Evidence) { value.SchemaVersion = 2 },
		"evidence ID":          func(value *Evidence) { value.EvidenceID = "sha256:bad" },
		"capture time":         func(value *Evidence) { value.CapturedAt = time.Time{} },
		"source":               func(value *Evidence) { value.Source.JobmanVersion = "" },
		"capability order":     func(value *Evidence) { value.Source.Capabilities = []string{"z", "a"} },
		"capability duplicate": func(value *Evidence) { value.Source.Capabilities = []string{"a", "a"} },
		"subject ID":           func(value *Evidence) { value.Subject.JobID = "bad job" },
		"subject revision":     func(value *Evidence) { value.Subject.JobRevision = 0 },
		"subject phase":        func(value *Evidence) { value.Subject.Phase = "" },
		"run order":            func(value *Evidence) { value.Subject.SelectedRuns = []uint64{2, 1} },
		"run duplicate":        func(value *Evidence) { value.Subject.SelectedRuns = []uint64{1, 1} },
		"metadata consistency": func(value *Evidence) { value.Consistency.Metadata = "eventual" },
		"artifact consistency": func(value *Evidence) { value.Consistency.Artifacts = "unknown" },
		"nil items":            func(value *Evidence) { value.Items = nil },
		"nil artifacts":        func(value *Evidence) { value.Artifacts = nil },
		"nil omissions":        func(value *Evidence) { value.Omissions = nil },
		"nil redactions":       func(value *Evidence) { value.RedactionNotices = nil },
		"measured limits":      func(value *Evidence) { value.Limits.ItemCount++ },
		"encoded size":         func(value *Evidence) { value.Limits.EncodedBytes++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := minimalEvidence(t, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
			mutate(&value)
			if err := Validate(value); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestValidateRejectsMalformedItemsArtifactsAndNotices(t *testing.T) {
	t.Parallel()

	itemTests := map[string]func(*Item){
		"ID":           func(value *Item) { value.ID = "bad id" },
		"code":         func(value *Item) { value.Code = "Bad" },
		"empty value":  func(value *Item) { value.Value = nil },
		"noncanonical": func(value *Item) { value.Value = []byte(`{ "b": 2, "a": 1 }`) },
		"observed time": func(value *Item) {
			local := fixtureTime.In(time.FixedZone("test", 60))
			value.ObservedAt = &local
		},
		"source":         func(value *Item) { value.Source.Kind = "" },
		"quality":        func(value *Item) { value.Quality = "estimated" },
		"disclosure":     func(value *Item) { value.Disclosure = "public" },
		"reversed range": func(value *Item) { value.Source.ByteStart, value.Source.ByteEnd = 2, 1 },
		"unbound range":  func(value *Item) { value.Source.ByteEnd = 1 },
	}
	for name, mutate := range itemTests {
		t.Run("item "+name, func(t *testing.T) {
			t.Parallel()
			value := minimalEvidence(t, fixtureTime)
			mutate(&value.Items[0])
			if err := Validate(value); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}

	artifactBase := activeRunFixture(t)
	artifactTests := map[string]func(*Artifact){
		"ID":            func(value *Artifact) { value.ID = "bad id" },
		"role":          func(value *Artifact) { value.Role = "" },
		"run":           func(value *Artifact) { value.Run = 0 },
		"media":         func(value *Artifact) { value.MediaType = "" },
		"digest":        func(value *Artifact) { value.Digest = "bad" },
		"quality":       func(value *Artifact) { value.Quality = "estimated" },
		"disclosure":    func(value *Artifact) { value.Disclosure = "public" },
		"captured time": func(value *Artifact) { value.CapturedAt = time.Time{} },
		"reversed range": func(value *Artifact) {
			value.ByteStart, value.ByteEnd = value.ByteEnd, value.ByteStart
		},
		"past original": func(value *Artifact) { value.ByteEnd = value.OriginalBytes + 1 },
		"selected bytes": func(value *Artifact) {
			value.SelectedBytes++
		},
		"content bytes": func(value *Artifact) { value.ContentBytes++ },
		"content digest": func(value *Artifact) {
			value.Data = append([]byte(nil), value.Data...)
			value.Data[0] ^= 1
		},
	}
	for name, mutate := range artifactTests {
		t.Run("artifact "+name, func(t *testing.T) {
			t.Parallel()
			value := artifactBase
			value.Artifacts = append([]Artifact(nil), artifactBase.Artifacts...)
			mutate(&value.Artifacts[0])
			if err := Validate(value); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}

	noticeBase, err := Seal(Evidence{
		CapturedAt:  fixtureTime,
		Source:      Source{JobmanVersion: "test", CollectorVersion: CollectorVersion, StoreSchemaVersion: 8, Platform: "linux"},
		Subject:     Subject{JobID: "job-1", JobRevision: 1, Phase: "completed"},
		Consistency: Consistency{Metadata: MetadataTransactionalSnapshot, Artifacts: ArtifactsNotCollected},
		Items:       []Item{}, Artifacts: []Artifact{},
		Omissions:        []Omission{{Code: "logs_unavailable", Affects: []string{"stderr"}}},
		RedactionNotices: []RedactionNotice{{Code: "configured_value", Affects: []string{"stderr"}, Count: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Evidence){
		"omission code":     func(value *Evidence) { value.Omissions[0].Code = "Bad" },
		"omission affects":  func(value *Evidence) { value.Omissions[0].Affects = nil },
		"redaction code":    func(value *Evidence) { value.RedactionNotices[0].Code = "Bad" },
		"redaction affects": func(value *Evidence) { value.RedactionNotices[0].Affects = nil },
		"redaction count":   func(value *Evidence) { value.RedactionNotices[0].Count = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := noticeBase
			value.Omissions = append([]Omission(nil), noticeBase.Omissions...)
			value.RedactionNotices = append([]RedactionNotice(nil), noticeBase.RedactionNotices...)
			mutate(&value)
			if err := Validate(value); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestCommandContextItemValidation(t *testing.T) {
	t.Parallel()

	base := minimalEvidence(t, fixtureTime)
	command, err := JSONValue(Command{Executable: "/usr/bin/false", Arguments: []string{"--example", ""}})
	if err != nil {
		t.Fatal(err)
	}
	base.Items = append(base.Items, Item{
		ID: "ev:job:target:command", Code: CodeTargetCommand, Value: command,
		Source:  ItemSource{Kind: "job_snapshot", EntityID: base.Subject.JobID, Revision: 1},
		Quality: QualityObserved, Disclosure: DisclosureCommand,
	})
	if _, err := Seal(base); err != nil {
		t.Fatalf("Seal(valid target command) error = %v", err)
	}
	for name, mutate := range map[string]func(*Item){
		"empty":      func(item *Item) { item.Value = json.RawMessage(`{"executable":"","arguments":[]}`) },
		"quality":    func(item *Item) { item.Quality = QualityConfirmed },
		"disclosure": func(item *Item) { item.Disclosure = DisclosureMetadata },
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			value.Items = slices.Clone(base.Items)
			mutate(&value.Items[len(value.Items)-1])
			if _, err := Seal(value); err == nil {
				t.Fatal("Seal(invalid target command) error = nil")
			}
		})
	}
}

func TestCodecRejectsInvalidBoundariesAndIO(t *testing.T) {
	t.Parallel()

	valid := minimalEvidence(t, fixtureTime)
	if _, err := JSONValue(duplicateKeyMarshaler{}); err == nil {
		t.Error("JSONValue(duplicate object key) error = nil")
	}
	if err := Encode(nil, valid); err == nil {
		t.Error("Encode(nil) error = nil")
	}
	if err := Encode(&bytes.Buffer{}, Evidence{}); err == nil {
		t.Error("Encode(unsealed evidence) error = nil")
	}
	if err := Encode(errorWriter{}, valid); err == nil {
		t.Error("Encode(failing writer) error = nil")
	}
	wrongDigest := valid
	wrongDigest.EvidenceID = "sha256:" + strings.Repeat("0", 64)
	if err := Verify(wrongDigest); err == nil {
		t.Error("Verify(wrong semantic digest) error = nil")
	}
	if err := validateEvidence(valid, true); err == nil {
		t.Error("validateEvidence(non-placeholder ID) error = nil")
	}
	if _, err := Decode(nil, DecodeLimits{}); err == nil {
		t.Error("Decode(nil) error = nil")
	}
	if _, err := Decode(bytes.NewReader(nil), DecodeLimits{MaxBytes: -1}); err == nil {
		t.Error("Decode(negative bytes) error = nil")
	}
	if _, err := Decode(bytes.NewReader(nil), DecodeLimits{MaxDepth: -1}); err == nil {
		t.Error("Decode(negative depth) error = nil")
	}
	if _, err := Decode(errorReader{}, DecodeLimits{}); err == nil {
		t.Error("Decode(failing reader) error = nil")
	}
	if _, err := Decode(strings.NewReader(`[[[0]]]`), DecodeLimits{MaxDepth: 1}); err == nil {
		t.Error("Decode(excessive depth) error = nil")
	}
	if _, err := Decode(strings.NewReader(`[]`), DecodeLimits{}); err == nil {
		t.Error("Decode(non-object header) error = nil")
	}
	if _, err := Decode(strings.NewReader(`{"kind":"other","schema_version":1}`), DecodeLimits{}); err == nil {
		t.Error("Decode(other kind) error = nil")
	}
	if _, err := Decode(strings.NewReader(
		`{"kind":"jobman.diagnostic_evidence","schema_version":1,"subject":{"job_revision":"bad"}}`,
	), DecodeLimits{}); err == nil {
		t.Error("Decode(typed field mismatch) error = nil")
	}
	if _, err := Decode(strings.NewReader(
		`{"kind":"jobman.diagnostic_evidence","schema_version":1}`,
	), DecodeLimits{}); err == nil {
		t.Error("Decode(unsealed evidence) error = nil")
	}
	var decoded int
	if err := decodeKnownValue([]byte(`1 2`), &decoded); err == nil {
		t.Error("decodeKnownValue(trailing JSON) error = nil")
	}
	normalized := normalize(Evidence{RedactionNotices: []RedactionNotice{
		{Code: "z", Affects: []string{"z"}, Count: 1},
		{Code: "a", Affects: []string{"a"}, Count: 1},
	}})
	if normalized.RedactionNotices[0].Code != "a" {
		t.Errorf("normalize(redactions) = %#v", normalized.RedactionNotices)
	}
	if validCode("bad/code") {
		t.Error("validCode(invalid separator) = true")
	}
}

func TestCanonicalJSONCompoundAndFailureCases(t *testing.T) {
	t.Parallel()

	canonical, err := canonicalJSON([]byte(`{"z":[true,null,1],"a":{"b":"x"}}`), 4)
	if err != nil {
		t.Fatalf("canonicalJSON(valid) error = %v", err)
	}
	if string(canonical) != `{"a":{"b":"x"},"z":[true,null,1]}` {
		t.Fatalf("canonical JSON = %s", canonical)
	}
	for name, input := range map[string]string{
		"duplicate":           `{"a":1,"a":2}`,
		"trailing":            `{} []`,
		"unterminated object": `{"a":1`,
		"unterminated array":  `[1`,
		"too deep":            `[[0]]`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			depth := 4
			if name == "too deep" {
				depth = 0
			}
			if _, err := canonicalJSON([]byte(input), depth); err == nil {
				t.Fatal("canonicalJSON() error = nil")
			}
		})
	}
}

func TestKnownItemCrossReferencesAndNormalization(t *testing.T) {
	t.Parallel()

	base := similarHistoryFixture(t)
	find := func(items []Item, code string) int {
		for index, item := range items {
			if item.Code == code {
				return index
			}
		}
		return -1
	}
	similarIndex := find(base.Items, CodeSimilarFailure)
	fingerprintIndex := find(base.Items, CodeFailureFingerprint)
	if similarIndex < 0 || fingerprintIndex < 0 {
		t.Fatal("similar-history fixture lacks typed facts")
	}

	duplicate := base
	duplicate.Items = append([]Item(nil), base.Items...)
	copyItem := duplicate.Items[similarIndex]
	copyItem.ID += ":duplicate"
	duplicate.Items = append(duplicate.Items, copyItem)
	slices.SortFunc(duplicate.Items, func(left, right Item) int { return strings.Compare(left.ID, right.ID) })
	if err := Validate(duplicate); err == nil {
		t.Fatal("Validate(duplicate similar run) error = nil")
	}

	mismatch := base
	mismatch.Items = append([]Item(nil), base.Items...)
	var failure SimilarFailure
	if err := json.Unmarshal(mismatch.Items[similarIndex].Value, &failure); err != nil {
		t.Fatal(err)
	}
	failure.Fingerprint.Value = strings.Repeat("b", 64)
	encoded, err := JSONValue(failure)
	if err != nil {
		t.Fatal(err)
	}
	mismatch.Items[similarIndex].Value = encoded
	if validationErr := Validate(mismatch); validationErr == nil {
		t.Fatal("Validate(mismatched similar fingerprint) error = nil")
	}

	unknown := failedExitFixture(t)
	resourceIndex := find(unknown.Items, CodeResourceObservation)
	unknown.Items = append([]Item(nil), unknown.Items...)
	unknown.Items[resourceIndex].Value = []byte(`{"completeness":"complete_at_exit","extra":true,"metric":"cpu_user_time","scope":"process","source":"process_state","unit":"nanoseconds","value":1}`)
	if validationErr := Validate(unknown); validationErr == nil {
		t.Fatal("Validate(resource with unknown field) error = nil")
	}
	wrongQuality := failedExitFixture(t)
	wrongQuality.Items = append([]Item(nil), wrongQuality.Items...)
	wrongQuality.Items[find(wrongQuality.Items, CodeResourceObservation)].Quality = QualityConfirmed
	if validationErr := Validate(wrongQuality); validationErr == nil {
		t.Fatal("Validate(resource with wrong quality) error = nil")
	}

	normalized := minimalEvidence(t, fixtureTime)
	normalized.EvidenceID = ""
	normalized.Items = nil
	normalized.Artifacts = nil
	normalized.Omissions = nil
	normalized.RedactionNotices = nil
	normalized, err = Seal(normalized)
	if err != nil {
		t.Fatalf("Seal(nil collections) error = %v", err)
	}
	if normalized.Items == nil || normalized.Artifacts == nil || normalized.Omissions == nil || normalized.RedactionNotices == nil {
		t.Fatal("Seal() did not normalize nil collections")
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type duplicateKeyMarshaler struct{}

func (duplicateKeyMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`{"a":1,"a":2}`), nil
}
