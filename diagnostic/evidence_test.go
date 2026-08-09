package diagnostic

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSealEncodeDecodeEvidence(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 30, 0, 123, time.UTC)
	value, err := JSONValue(map[string]any{"exit": 2, "signal": ""})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := Seal(Evidence{
		CapturedAt: now,
		Source: Source{
			JobmanVersion: "1.0.0", CollectorVersion: CollectorVersion,
			StoreSchemaVersion: 7, Platform: "linux", Capabilities: []string{"signals", "logs"},
		},
		Subject: Subject{
			JobID: "01980f4c-7b2a-7a6f-8c10-0123456789ab", JobRevision: 3,
			SelectedRuns: []uint64{2, 1}, Phase: "completed", Outcome: "failure",
		},
		Consistency: Consistency{Metadata: MetadataTransactionalSnapshot, Artifacts: ArtifactsStable},
		Items: []Item{{
			ID: "ev:run:1:exit", Code: CodeRunExitCode, Value: value,
			Source:  ItemSource{Kind: "run_snapshot", EntityID: "run-1", Revision: 2},
			Quality: QualityObserved, Disclosure: DisclosureMetadata,
		}},
		Artifacts: []Artifact{{
			ID: "artifact:run:1:stderr", Role: ArtifactRoleLogTail, Run: 1, Stream: "stderr",
			MediaType: "application/octet-stream", Data: []byte("failure\n"), OriginalBytes: 8,
			ByteStart: 0, ByteEnd: 8, CapturedAt: now, Quality: QualityObserved,
			Disclosure: DisclosureLogContent,
		}},
		Omissions:        []Omission{{Code: OmissionSimilarNotRequested, Affects: []string{"similar"}}},
		RedactionNotices: []RedactionNotice{},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if evidence.Subject.SelectedRuns[0] != 1 || evidence.Source.Capabilities[0] != "logs" {
		t.Fatalf("Seal() did not normalize evidence: %#v", evidence)
	}
	var encoded bytes.Buffer
	if encodeErr := Encode(&encoded, evidence); encodeErr != nil {
		t.Fatalf("Encode() error = %v", encodeErr)
	}
	decoded, err := Decode(&encoded, DecodeLimits{})
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.EvidenceID != evidence.EvidenceID || decoded.Limits.LogBytes != 8 {
		t.Fatalf("Decode() = %#v, want ID %q and 8 log bytes", decoded, evidence.EvidenceID)
	}
}

func TestEvidenceDigestExcludesCaptureTime(t *testing.T) {
	first := minimalEvidence(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	second := minimalEvidence(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if first.EvidenceID != second.EvidenceID {
		t.Fatalf("capture time changed evidence ID: %q != %q", first.EvidenceID, second.EvidenceID)
	}
	first.Artifacts = []Artifact{{
		ID: "artifact:run:1:stderr", Role: ArtifactRoleLogTail, Run: 1, Stream: "stderr",
		MediaType: "application/octet-stream", Data: []byte("failure\n"), OriginalBytes: 8,
		ByteStart: 0, ByteEnd: 8, CapturedAt: first.CapturedAt, Quality: QualityObserved,
		Disclosure: DisclosureLogContent,
	}}
	second.Artifacts = []Artifact{{
		ID: "artifact:run:1:stderr", Role: ArtifactRoleLogTail, Run: 1, Stream: "stderr",
		MediaType: "application/octet-stream", Data: []byte("failure\n"), OriginalBytes: 8,
		ByteStart: 0, ByteEnd: 8, CapturedAt: second.CapturedAt, Quality: QualityObserved,
		Disclosure: DisclosureLogContent,
	}}
	var err error
	first, err = Seal(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err = Seal(second)
	if err != nil {
		t.Fatal(err)
	}
	if first.EvidenceID != second.EvidenceID {
		t.Fatalf("artifact capture time changed evidence ID: %q != %q", first.EvidenceID, second.EvidenceID)
	}
}

func TestDecodeRejectsUntrustedJSON(t *testing.T) {
	valid := minimalEvidence(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"oversized":       append(encoded, bytes.Repeat([]byte(" "), 64)...),
		"trailing":        append(encoded, []byte(`{}`)...),
		"duplicate field": []byte(`{"kind":"jobman.diagnostic_evidence","kind":"duplicate"}`),
		"newer schema":    []byte(`{"kind":"jobman.diagnostic_evidence","schema_version":2}`),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			limits := DecodeLimits{}
			if name == "oversized" {
				limits.MaxBytes = int64(len(encoded))
			}
			if _, err := Decode(bytes.NewReader(input), limits); err == nil {
				t.Fatal("Decode() error = nil")
			}
		})
	}
}

func TestVerifyRejectsMutation(t *testing.T) {
	evidence := minimalEvidence(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	evidence.Subject.Outcome = "success"
	if err := Verify(evidence); err == nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestSealRejectsInvalidKnownFactSemantics(t *testing.T) {
	t.Parallel()

	base := minimalEvidence(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	base.EvidenceID = ""
	fingerprint := FailureFingerprint{
		Algorithm: FingerprintAlgorithmHMACSHA256, InputSchemaVersion: FingerprintInputSchemaVersion,
		Value: strings.Repeat("a", 64), Scope: FingerprintScopeStoreLocal,
	}
	fingerprintValue, err := JSONValue(fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	similarValue, err := JSONValue(SimilarFailure{
		JobID: "job-2", RunID: "run-2", RunNumber: 1,
		CompletedAt: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		Outcome:     "success", FailureClass: "nonzero_exit", Fingerprint: fingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]Item{
		"wrong fingerprint disclosure": {
			ID: "ev:run:1:fingerprint", Code: CodeFailureFingerprint, Value: fingerprintValue,
			Source: ItemSource{Kind: "fixture"}, Quality: QualityDerivedExact, Disclosure: DisclosureMetadata,
		},
		"successful similar failure": {
			ID: "ev:similar:1", Code: CodeSimilarFailure, Value: similarValue,
			Source: ItemSource{Kind: "fixture"}, Quality: QualityDerivedExact, Disclosure: DisclosureLocalOnly,
		},
	}
	for name, item := range tests {
		t.Run(name, func(t *testing.T) {
			evidence := base
			evidence.Items = append(slices.Clone(base.Items), item)
			if _, sealErr := Seal(evidence); sealErr == nil {
				t.Fatal("Seal() error = nil")
			}
		})
	}
}

func TestJSONValueRejectsUnsupportedValue(t *testing.T) {
	if _, err := JSONValue(make(chan int)); err == nil {
		t.Fatal("JSONValue() error = nil")
	}
}

func minimalEvidence(tb testing.TB, capturedAt time.Time) Evidence {
	tb.Helper()
	value, err := JSONValue("completed")
	if err != nil {
		tb.Fatal(err)
	}
	evidence, err := Seal(Evidence{
		CapturedAt: capturedAt,
		Source: Source{
			JobmanVersion: "1.0.0", CollectorVersion: CollectorVersion,
			StoreSchemaVersion: 7, Platform: "linux", Capabilities: []string{},
		},
		Subject:     Subject{JobID: "job-1", JobRevision: 1, Phase: "completed", SelectedRuns: []uint64{}},
		Consistency: Consistency{Metadata: MetadataTransactionalSnapshot, Artifacts: ArtifactsNotCollected},
		Items: []Item{{
			ID: "ev:job:phase", Code: CodeJobPhase, Value: value,
			Source:  ItemSource{Kind: "job_snapshot", EntityID: "job-1", Revision: 1},
			Quality: QualityObserved, Disclosure: DisclosureMetadata,
		}},
		Artifacts: []Artifact{}, Omissions: []Omission{}, RedactionNotices: []RedactionNotice{},
	})
	if err != nil {
		tb.Fatal(err)
	}

	return evidence
}

func FuzzDecodeEvidence(f *testing.F) {
	seed := minimalEvidence(f, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	encoded, err := json.Marshal(seed)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Fuzz(func(_ *testing.T, input []byte) {
		if _, decodeErr := Decode(bytes.NewReader(input), DecodeLimits{MaxBytes: 64 * 1024, MaxDepth: 32}); decodeErr != nil {
			return
		}
	})
}
