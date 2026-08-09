package diagnostic

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const fixtureSecretCanary = "JOBMAN_DIAG_SECRET_CANARY_7f84d1" // #nosec G101 -- Deliberate non-secret fixture sentinel.

var fixtureTime = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

func TestEvidenceFixtures(t *testing.T) {
	builders := map[string]func(*testing.T) Evidence{
		"active-run-v1.json":           activeRunFixture,
		"additive-fact-v1.json":        additiveFactFixture,
		"failed-exit-v1.json":          failedExitFixture,
		"log-budget-boundary-v1.json":  logBudgetBoundaryFixture,
		"notification-failure-v1.json": notificationFailureFixture,
		"pruned-logs-v1.json":          prunedLogsFixture,
		"secret-canary-v1.json":        secretCanaryFixture,
		"similar-history-v1.json":      similarHistoryFixture,
		"start-failure-v1.json":        startFailureFixture,
		"timeout-v1.json":              timeoutFixture,
	}
	if os.Getenv("UPDATE_DIAGNOSTIC_FIXTURES") == "1" {
		updateEvidenceFixtures(t, builders)
	}

	manifest := readFixtureManifest(t)
	if manifest.JobmanRelease != "unreleased" || manifest.EvidenceSchema != SchemaVersion {
		t.Fatalf("fixture manifest origin = %#v", manifest)
	}
	if len(manifest.Fixtures) != len(builders) {
		t.Fatalf("fixture manifest entries = %d, want %d", len(manifest.Fixtures), len(builders))
	}
	for _, entry := range manifest.Fixtures {
		builder, ok := builders[entry.File]
		if !ok {
			t.Errorf("manifest contains unexpected fixture %q", entry.File)
			continue
		}
		encoded := readFixture(t, entry.File)
		digest := sha256.Sum256(encoded)
		if got := hex.EncodeToString(digest[:]); got != entry.SHA256 {
			t.Errorf("%s SHA-256 = %s, want %s", entry.File, got, entry.SHA256)
		}
		decoded, err := Decode(bytes.NewReader(encoded), DecodeLimits{})
		if err != nil {
			t.Errorf("Decode(%s) error = %v", entry.File, err)
			continue
		}
		if decoded.EvidenceID != entry.EvidenceID {
			t.Errorf("%s evidence ID = %s, want %s", entry.File, decoded.EvidenceID, entry.EvidenceID)
		}
		if want := builder(t); decoded.EvidenceID != want.EvidenceID {
			t.Errorf("%s no longer matches its constructor", entry.File)
		}
		delete(builders, entry.File)
	}
	if len(builders) != 0 {
		t.Fatalf("fixtures absent from manifest: %v", slices.Sorted(maps.Keys(builders)))
	}
	if bytes.Contains(readFixture(t, "secret-canary-v1.json"), []byte(fixtureSecretCanary)) {
		t.Fatal("secret canary appears in the published fixture")
	}
}

func TestInvalidEvidenceFixtures(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"duplicate-key.json", "unsupported-schema-v2.json"} {
		if _, err := Decode(bytes.NewReader(readFixture(t, name)), DecodeLimits{}); err == nil {
			t.Errorf("Decode(%s) error = nil", name)
		}
	}
}

type fixtureManifest struct {
	JobmanRelease  string                 `json:"jobman_release"`
	EvidenceSchema int                    `json:"evidence_schema"`
	Fixtures       []fixtureManifestEntry `json:"fixtures"`
}

type fixtureManifestEntry struct {
	File       string `json:"file"`
	EvidenceID string `json:"evidence_id"`
	SHA256     string `json:"sha256"`
}

func updateEvidenceFixtures(t *testing.T, builders map[string]func(*testing.T) Evidence) {
	t.Helper()

	names := slices.Sorted(maps.Keys(builders))
	manifest := fixtureManifest{JobmanRelease: "unreleased", EvidenceSchema: SchemaVersion}
	for _, name := range names {
		var encoded bytes.Buffer
		evidence := builders[name](t)
		if err := Encode(&encoded, evidence); err != nil {
			t.Fatalf("Encode(%s) error = %v", name, err)
		}
		path := filepath.Join("testdata", name)
		// #nosec G306 -- canonical fixtures are intentionally public repository data.
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		digest := sha256.Sum256(encoded.Bytes())
		manifest.Fixtures = append(manifest.Fixtures, fixtureManifestEntry{
			File: name, EvidenceID: evidence.EvidenceID, SHA256: hex.EncodeToString(digest[:]),
		})
	}
	encodedManifest, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encodedManifest = append(encodedManifest, '\n')
	// #nosec G306 -- the origin manifest is intentionally public repository data.
	if err := os.WriteFile(filepath.Join("testdata", "manifest.json"), encodedManifest, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFixtureManifest(t *testing.T) fixtureManifest {
	t.Helper()

	var manifest fixtureManifest
	if err := json.Unmarshal(readFixture(t, "manifest.json"), &manifest); err != nil {
		t.Fatalf("decode fixture manifest: %v", err)
	}

	return manifest
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	encoded, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}

	return encoded
}

func fixtureBase(phase, outcome string, runs []uint64, artifacts ArtifactConsistency) Evidence {
	return Evidence{
		CapturedAt: fixtureTime,
		Source: Source{
			JobmanVersion: "1.4.0-dev", CollectorVersion: CollectorVersion,
			StoreSchemaVersion: 8, Platform: "linux",
			Capabilities: []string{
				"diagnostic_records_v1", "failure_fingerprints_v1", "log_metadata", "log_tail",
				"resource_observations_v1", "similar_history_v1", "transactional_snapshot",
			},
		},
		Subject: Subject{
			JobID: "01980f4c-7b2a-7a6f-8c10-0123456789ab", JobRevision: 9,
			SelectedRuns: runs, Phase: phase, Outcome: outcome,
		},
		Consistency: Consistency{Metadata: MetadataTransactionalSnapshot, Artifacts: artifacts},
		Items:       []Item{}, Artifacts: []Artifact{}, Omissions: []Omission{}, RedactionNotices: []RedactionNotice{},
	}
}

func fixtureItem(t *testing.T, id, code string, value any, quality Quality) Item {
	t.Helper()

	return fixtureItemWithDisclosure(t, id, code, value, quality, DisclosureMetadata)
}

func fixtureItemWithDisclosure(
	t *testing.T,
	id string,
	code string,
	value any,
	quality Quality,
	disclosure DisclosureClass,
) Item {
	t.Helper()

	encoded, err := JSONValue(value)
	if err != nil {
		t.Fatal(err)
	}

	return Item{
		ID: id, Code: code, Value: encoded,
		Source:  ItemSource{Kind: "fixture_snapshot", EntityID: "fixture", Revision: 9},
		Quality: quality, Disclosure: disclosure,
	}
}

func fixtureFailureFingerprint() FailureFingerprint {
	return FailureFingerprint{
		Algorithm: FingerprintAlgorithmHMACSHA256, InputSchemaVersion: FingerprintInputSchemaVersion,
		Value: strings.Repeat("a", 64), Scope: FingerprintScopeStoreLocal,
	}
}

func fixtureCPUObservation() ResourceObservation {
	return ResourceObservation{
		Metric: ResourceCPUUserTime, Value: 12500000, Unit: ResourceUnitNanoseconds,
		Scope: ResourceScopeProcess, Source: ResourceSourceProcessState, Completeness: ResourceCompleteAtExit,
	}
}

func sealFixture(t *testing.T, evidence Evidence) Evidence {
	t.Helper()

	sealed, err := Seal(evidence)
	if err != nil {
		t.Fatal(err)
	}

	return sealed
}

func failedExitFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := fixtureBase("completed", "failure", []uint64{1}, ArtifactsNotCollected)
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:outcome", CodeJobOutcome, "failure", QualityObserved),
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "completed", QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:exit:code", CodeRunExitCode, 2, QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:failure_class", CodeFailureClass,
			map[string]string{"class": "nonzero_exit", "scope": "run"}, QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:outcome", CodeRunOutcome, "failure", QualityObserved),
		fixtureItemWithDisclosure(t, "ev:run:00000000000000000001:failure:fingerprint",
			CodeFailureFingerprint, fixtureFailureFingerprint(), QualityDerivedExact, DisclosureLocalOnly),
		fixtureItem(t, "ev:run:00000000000000000001:resource:cpu_user_time",
			CodeResourceObservation, fixtureCPUObservation(), QualityObserved),
	)
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionLogContentNotRequested, Affects: []string{"run:1:stderr", "run:1:stdout"}},
		Omission{Code: OmissionSimilarNotRequested, Affects: []string{"similar_failures"}},
	)

	return sealFixture(t, evidence)
}

func startFailureFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := fixtureBase("completed", "submission_failed", []uint64{1}, ArtifactsNotCollected)
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:outcome", CodeJobOutcome, "submission_failed", QualityObserved),
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "completed", QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:failure_class", CodeFailureClass,
			map[string]string{"class": "executable_not_found", "scope": "run"}, QualityConfirmed),
		fixtureItem(t, "ev:run:00000000000000000001:outcome", CodeRunOutcome, "start_failed", QualityObserved),
		fixtureItemWithDisclosure(t, "ev:run:00000000000000000001:failure:fingerprint",
			CodeFailureFingerprint, fixtureFailureFingerprint(), QualityDerivedExact, DisclosureLocalOnly),
	)
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionResourceNotApplicable, Affects: []string{"run:1:resource_observations"}},
		Omission{Code: OmissionSimilarNotRequested, Affects: []string{"similar_failures"}},
	)

	return sealFixture(t, evidence)
}

func timeoutFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := fixtureBase("completed", "timed_out", []uint64{2}, ArtifactsNotCollected)
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:outcome", CodeJobOutcome, "timed_out", QualityObserved),
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "completed", QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000002:failure_class", CodeFailureClass,
			map[string]string{"class": "run_timeout", "scope": "run"}, QualityConfirmed),
		fixtureItem(t, "ev:run:00000000000000000002:timeout_scope", CodeRunTimeoutScope, "run", QualityConfirmed),
		fixtureItemWithDisclosure(t, "ev:run:00000000000000000002:failure:fingerprint",
			CodeFailureFingerprint, fixtureFailureFingerprint(), QualityDerivedExact, DisclosureLocalOnly),
		fixtureItem(t, "ev:run:00000000000000000002:resource:cpu_user_time",
			CodeResourceObservation, fixtureCPUObservation(), QualityObserved),
	)
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionSimilarNotRequested, Affects: []string{"similar_failures"}},
	)

	return sealFixture(t, evidence)
}

func activeRunFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := fixtureBase("active", "", []uint64{3}, ArtifactsPointInTime)
	evidence.Consistency.ActiveStateMayHaveAdvanced = true
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "active", QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000003:phase", CodeRunPhase, "active", QualityPointInTime),
	)
	evidence.Artifacts = append(evidence.Artifacts, Artifact{
		ID: "artifact:run:00000000000000000003:stderr", Role: ArtifactRoleLogTail, Run: 3,
		Stream: "stderr", MediaType: "application/octet-stream", Data: []byte("still running\n"),
		OriginalBytes: 14, ByteStart: 0, ByteEnd: 14, CapturedAt: fixtureTime,
		Quality: QualityPointInTime, Disclosure: DisclosureLogContent,
	})
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionActiveStateMayHaveAdvanced, Affects: []string{"job_state", "run_state"}},
		Omission{Code: OmissionResourceUnavailable, Affects: []string{"run:3:resource_observations"}},
		Omission{Code: OmissionSimilarNotRequested, Affects: []string{"similar_failures"}},
	)

	return sealFixture(t, evidence)
}

func prunedLogsFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := failedExitFixture(t)
	evidence.EvidenceID = ""
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionLogsPruned, Affects: []string{"run:1:stderr", "run:1:stdout"}},
	)

	return sealFixture(t, evidence)
}

func notificationFailureFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := fixtureBase("completed", "success", []uint64{1}, ArtifactsNotCollected)
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:outcome", CodeJobOutcome, "success", QualityObserved),
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "completed", QualityObserved),
		fixtureItem(t, "ev:notification:delivery:000000:status", CodeNotificationStatus,
			map[string]any{"attempt_count": 3, "event_type": "job_succeeded", "max_attempts": 3, "status": "failed"},
			QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:outcome", CodeRunOutcome, "success", QualityObserved),
		fixtureItem(t, "ev:run:00000000000000000001:resource:cpu_user_time",
			CodeResourceObservation, fixtureCPUObservation(), QualityObserved),
	)
	evidence.Omissions = append(evidence.Omissions,
		Omission{Code: OmissionSimilarNotRequested, Affects: []string{"similar_failures"}},
	)

	return sealFixture(t, evidence)
}

func additiveFactFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := failedExitFixture(t)
	evidence.EvidenceID = ""
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:zz:future-safe-fact", "jobman.fixture.additive_fact",
			map[string]any{"introduced_by": "newer_collector", "safe_to_ignore": true}, QualityDerivedExact),
	)

	return sealFixture(t, evidence)
}

func secretCanaryFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := failedExitFixture(t)
	evidence.EvidenceID = ""
	evidence.Consistency.Artifacts = ArtifactsStable
	evidence.Artifacts = append(evidence.Artifacts, Artifact{
		ID: "artifact:run:00000000000000000001:stderr", Role: ArtifactRoleLogTail, Run: 1,
		Stream: "stderr", MediaType: "application/octet-stream", Data: []byte("token=[REDACTED]\n"),
		OriginalBytes: uint64(len("token=" + fixtureSecretCanary + "\n")), ByteStart: 0,
		ByteEnd: uint64(len("token=" + fixtureSecretCanary + "\n")), CapturedAt: fixtureTime,
		Quality: QualityObserved, Disclosure: DisclosureLogContent,
	})
	evidence.RedactionNotices = append(evidence.RedactionNotices, RedactionNotice{
		Code: RedactionConfiguredPattern, Affects: []string{"artifact:run:00000000000000000001:stderr"}, Count: 1,
	})

	return sealFixture(t, evidence)
}

func similarHistoryFixture(t *testing.T) Evidence {
	t.Helper()

	evidence := failedExitFixture(t)
	evidence.EvidenceID = ""
	evidence.Omissions = slices.DeleteFunc(evidence.Omissions, func(omission Omission) bool {
		return omission.Code == OmissionSimilarNotRequested
	})
	for index, laterSucceeded := range []bool{false, true} {
		failure := SimilarFailure{
			JobID: "01980f4c-7b2a-7a6f-8c10-1123456789ab",
			RunID: fmt.Sprintf("01980f4c-7b2a-7a6f-8c10-2123456789a%d", index), RunNumber: uint64(index + 1),
			CompletedAt: fixtureTime.Add(time.Duration(index+1) * time.Minute),
			Outcome:     "failure", FailureClass: "nonzero_exit", Fingerprint: fixtureFailureFingerprint(),
			LaterSucceeded: laterSucceeded,
		}
		evidence.Items = append(evidence.Items, fixtureItemWithDisclosure(
			t, fmt.Sprintf("ev:similar:%06d", index), CodeSimilarFailure, failure,
			QualityDerivedExact, DisclosureLocalOnly,
		))
	}

	return sealFixture(t, evidence)
}

func logBudgetBoundaryFixture(t *testing.T) Evidence {
	t.Helper()

	const maximumCollectorLogBytes = 1024 * 1024
	evidence := fixtureBase("completed", "failure", []uint64{1}, ArtifactsStable)
	evidence.Items = append(evidence.Items,
		fixtureItem(t, "ev:job:outcome", CodeJobOutcome, "failure", QualityObserved),
		fixtureItem(t, "ev:job:phase", CodeJobPhase, "completed", QualityObserved),
	)
	evidence.Artifacts = append(evidence.Artifacts, Artifact{
		ID: "artifact:run:00000000000000000001:stderr", Role: ArtifactRoleLogTail, Run: 1,
		Stream: "stderr", MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), maximumCollectorLogBytes),
		OriginalBytes: maximumCollectorLogBytes, ByteStart: 0, ByteEnd: maximumCollectorLogBytes,
		CapturedAt: fixtureTime, Quality: QualityObserved, Disclosure: DisclosureLogContent,
	})

	return sealFixture(t, evidence)
}
