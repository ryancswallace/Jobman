package diagnostic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	sharedDeployment = "01990000-0000-7000-8000-000000000001"
	sharedInstance   = "01990000-0000-7000-8000-000000000002"
	sharedNamespace  = "01990000-0000-7000-8000-000000000003"
	sharedJob        = "01990000-0000-7000-8000-000000000004"
	sharedRun        = "01990000-0000-7000-8000-000000000005"
	sharedExecution  = "01990000-0000-7000-8000-000000000006"
	sharedOther      = "01990000-0000-7000-8000-000000000007"
)

type sharedSnapshotFunc func(context.Context, SharedSelection) (SharedSnapshot, error)

func (reader sharedSnapshotFunc) ReadSnapshot(ctx context.Context, selection SharedSelection) (SharedSnapshot, error) {
	return reader(ctx, selection)
}

type sharedLogFunc func(context.Context, SharedLogRequest) (SharedLogTail, error)

func (reader sharedLogFunc) ReadLogTail(ctx context.Context, request SharedLogRequest) (SharedLogTail, error) {
	return reader(ctx, request)
}

type sharedTestSanitizer struct{}

func (sharedTestSanitizer) Sanitize(_ string, value []byte) ([]byte, bool) {
	cleaned := bytes.ReplaceAll(value, []byte("SECRET"), []byte("[redacted]"))
	return cleaned, !bytes.Equal(cleaned, value)
}

func (sharedTestSanitizer) ValueRedactionConfigured() bool { return true }

func sharedFixtureSnapshot(t *testing.T) SharedSnapshot {
	t.Helper()
	return SharedSnapshot{
		Kind: SharedSnapshotKind, SchemaVersion: SharedSnapshotVersion,
		CapturedAt: fixtureTime,
		Source: SharedSource{
			Kind: SharedSourceControl, DeploymentID: sharedDeployment, ControlInstanceID: sharedInstance,
			NamespaceID: sharedNamespace, ControlVersion: "0.2.0", ContractVersion: "jobman.control/v1alpha2",
		},
		JobmanVersion: "1.9.0-dev", Platform: "linux/arm64",
		Job:      SharedJob{ID: sharedJob, Revision: 12, Phase: "terminal", Outcome: "failure"},
		Runs:     []SharedRun{{ID: sharedRun, Number: 3, ExecutionID: sharedExecution}},
		Metadata: MetadataTransactionalSnapshot,
		Items: []Item{
			sharedFixtureItem(t, "fact:job:outcome", CodeJobOutcome, "failure", sharedJob),
			sharedFixtureItem(t, "fact:run:exit", CodeRunExitCode, 17, sharedRun),
		},
		Logs: []SharedLogReference{{
			ID: "artifact:run:3:stderr", RunID: sharedRun, ExecutionID: sharedExecution,
			Stream: "stderr", ManifestRevision: 9, Bytes: 18, Complete: true,
		}},
		Omissions: []Omission{}, RedactionNotices: []RedactionNotice{},
	}
}

func sharedFixtureItem(t *testing.T, id, code string, value any, entity string) Item {
	t.Helper()
	encoded, err := JSONValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return Item{
		ID: id, Code: code, Value: encoded, Source: ItemSource{Kind: "control_snapshot", EntityID: entity},
		Quality: QualityObserved, Disclosure: DisclosureMetadata,
	}
}

func sharedFixtureRequest() SharedCollectionRequest {
	return SharedCollectionRequest{Selection: SharedSelection{
		DeploymentID: sharedDeployment, ControlInstanceID: sharedInstance,
		NamespaceID: sharedNamespace, JobID: sharedJob, ExpectedJobRevision: 12, RunID: sharedRun,
	}}
}

func sharedFixtureCollector(snapshot SharedSnapshot) SharedCollector {
	return SharedCollector{
		Snapshots: sharedSnapshotFunc(func(context.Context, SharedSelection) (SharedSnapshot, error) { return snapshot, nil }),
		Logs: sharedLogFunc(func(_ context.Context, request SharedLogRequest) (SharedLogTail, error) {
			data := []byte("prefix SECRET tail")
			if request.MaxBytes < uint64(len(data)) {
				data = data[uint64(len(data))-request.MaxBytes:]
			}
			return SharedLogTail{
				Reference: request.Reference, Data: data, ByteStart: request.Reference.Bytes - uint64(len(data)),
				ByteEnd: request.Reference.Bytes, CapturedAt: snapshot.CapturedAt,
			}, nil
		}),
		Sanitizer: sharedTestSanitizer{},
	}
}

func TestSharedMetadataDefaultAndSchemaOneCompatibility(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	command := sharedFixtureItem(t, "fact:secret:command", CodeTargetCommand,
		Command{Executable: "SECRET", Arguments: []string{"ARGUMENT_SECRET"}}, sharedJob)
	command.Disclosure = DisclosureCommand
	unknown := sharedFixtureItem(t, "fact:secret:unknown", "control.unknown", "SECRET", sharedJob)
	snapshot.Items = append(snapshot.Items, command, unknown)
	collector := sharedFixtureCollector(snapshot)
	collector.Logs = sharedLogFunc(func(context.Context, SharedLogRequest) (SharedLogTail, error) {
		t.Fatal("metadata-only collection read log bytes")
		return SharedLogTail{}, nil
	})
	evidence, err := collector.Collect(t.Context(), sharedFixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("SECRET")) || bytes.Contains(encoded, []byte("store_schema_version")) {
		t.Fatal("shared evidence disclosed commands or fabricated local-store provenance")
	}
	if evidence.SchemaVersion != 2 || evidence.Shared.Profile != SharedProfileMetadata || len(evidence.Items) != 2 ||
		len(evidence.Artifacts) != 0 || len(evidence.Omissions) != 3 || evidence.Subject.SelectedRuns[0] != 3 {
		t.Fatalf("unexpected shared metadata evidence: %#v", evidence)
	}
	if decoded, decodeErr := Decode(bytes.NewReader(encoded), DecodeLimits{}); decodeErr != nil || decoded.EvidenceID != evidence.EvidenceID {
		t.Fatalf("shared round trip: %v", decodeErr)
	}
	legacy, err := Decode(bytes.NewReader(readFixture(t, "failed-exit-v1.json")), DecodeLimits{})
	if err != nil || legacy.SchemaVersion != 1 || legacy.Shared != nil {
		t.Fatalf("legacy decode changed: %v", err)
	}
	if sealed, err := Seal(legacy); err != nil || sealed.EvidenceID != legacy.EvidenceID {
		t.Fatalf("schema-1 semantic identity changed: %v", err)
	}
}

func TestSharedCollectionRejectsSourceSubstitution(t *testing.T) {
	for name, mutate := range map[string]func(*SharedSnapshot){
		"deployment": func(s *SharedSnapshot) { s.Source.DeploymentID = sharedOther },
		"instance":   func(s *SharedSnapshot) { s.Source.ControlInstanceID = sharedOther },
		"namespace":  func(s *SharedSnapshot) { s.Source.NamespaceID = sharedOther },
		"job":        func(s *SharedSnapshot) { s.Job.ID = sharedOther },
		"revision":   func(s *SharedSnapshot) { s.Job.Revision++ },
		"run":        func(s *SharedSnapshot) { s.Runs[0].ID = sharedOther; s.Logs[0].RunID = sharedOther },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := sharedFixtureSnapshot(t)
			mutate(&snapshot)
			if _, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest()); err == nil {
				t.Fatal("accepted substituted authority, subject or revision")
			}
		})
	}
}

func TestSharedLogRedactionRangeAndConsistency(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	snapshot.Logs[0].Complete = false
	request := sharedFixtureRequest()
	request.IncludeLogTail = true
	request.LogBytes = 11
	evidence, err := sharedFixtureCollector(snapshot).Collect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Artifacts) != 1 || evidence.Consistency.Artifacts != ArtifactsPointInTime {
		t.Fatalf("log consistency = %#v", evidence.Consistency)
	}
	artifact := evidence.Artifacts[0]
	if string(artifact.Data) != "[redacted] tail" || artifact.ByteStart != 7 || artifact.ByteEnd != 18 ||
		artifact.SelectedBytes != 11 || artifact.ContentBytes != 15 || !artifact.Truncated ||
		artifact.Run != 3 || len(evidence.RedactionNotices) != 1 {
		t.Fatalf("incorrect redaction/offsets: %#v", artifact)
	}
	if err := Verify(evidence); err != nil {
		t.Fatal(err)
	}
	artifact.Data[0] = 'x'
	if err := Verify(evidence); err == nil {
		t.Fatal("accepted mutated citation bytes")
	}
}

func TestSharedIdentityIncludesProvenanceProfileAndManifest(t *testing.T) {
	base, err := sharedFixtureCollector(sharedFixtureSnapshot(t)).Collect(t.Context(), sharedFixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Evidence){
		"deployment":     func(e *Evidence) { e.Shared.Source.DeploymentID = sharedOther },
		"instance":       func(e *Evidence) { e.Shared.Source.ControlInstanceID = sharedOther },
		"namespace":      func(e *Evidence) { e.Shared.Source.NamespaceID = sharedOther },
		"source version": func(e *Evidence) { e.Shared.Source.ControlVersion = "0.3.0" },
		"profile":        func(e *Evidence) { e.Shared.Profile = SharedProfileIncludeLogTail },
		"manifest":       func(e *Evidence) { e.Shared.Logs[0].ManifestRevision++ },
		"execution": func(e *Evidence) {
			e.Shared.Runs[0].ExecutionID = sharedOther
			e.Shared.Logs[0].ExecutionID = sharedOther
		},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(base)
			if err != nil {
				t.Fatal(err)
			}
			var changed Evidence
			if decodeErr := json.Unmarshal(encoded, &changed); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			mutate(&changed)
			if verifyErr := Verify(changed); verifyErr == nil {
				t.Fatal("accepted modified sealed provenance")
			}
			sealed, err := Seal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if sealed.EvidenceID == base.EvidenceID {
				t.Fatal("provenance absent from semantic identity")
			}
		})
	}
}

func TestSharedLogErrorsAbortAndMissingRedactionOmits(t *testing.T) {
	request := sharedFixtureRequest()
	request.IncludeLogTail = true
	snapshot := sharedFixtureSnapshot(t)
	collector := sharedFixtureCollector(snapshot)
	revoked := errors.New("authorization revoked")
	collector.Logs = sharedLogFunc(func(context.Context, SharedLogRequest) (SharedLogTail, error) { return SharedLogTail{}, revoked })
	if _, err := collector.Collect(t.Context(), request); !errors.Is(err, revoked) {
		t.Fatalf("access failure = %v", err)
	}
	collector.Sanitizer = nil
	evidence, err := collector.Collect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Artifacts) != 0 || len(evidence.Omissions) != 1 || evidence.Omissions[0].Code != OmissionConfiguredRedactionMissing {
		t.Fatalf("unsafe log fallback: %#v", evidence)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := collector.Collect(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestSharedLogRejectsChangedManifestAndIncompleteRanges(t *testing.T) {
	for name, mutate := range map[string]func(*SharedLogTail){
		"revision":   func(tail *SharedLogTail) { tail.Reference.ManifestRevision++ },
		"run":        func(tail *SharedLogTail) { tail.Reference.RunID = sharedOther },
		"short tail": func(tail *SharedLogTail) { tail.Data = tail.Data[1:]; tail.ByteStart++ },
		"extra tail": func(tail *SharedLogTail) { tail.Data = append(tail.Data, 'x') },
		"wrong end":  func(tail *SharedLogTail) { tail.ByteEnd-- },
	} {
		t.Run(name, func(t *testing.T) {
			collector := sharedFixtureCollector(sharedFixtureSnapshot(t))
			reader := collector.Logs
			collector.Logs = sharedLogFunc(func(ctx context.Context, request SharedLogRequest) (SharedLogTail, error) {
				tail, err := reader.ReadLogTail(ctx, request)
				mutate(&tail)
				return tail, err
			})
			request := sharedFixtureRequest()
			request.IncludeLogTail = true
			if _, err := collector.Collect(t.Context(), request); err == nil {
				t.Fatal("accepted invalid log source/range")
			}
		})
	}
}

func TestSharedSnapshotDecodeAndLargeRevision(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	snapshot.Job.Revision = math.MaxUint64
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"revision":"18446744073709551615"`)) {
		t.Fatal("revision not encoded as decimal string")
	}
	decoded, err := DecodeSharedSnapshot(bytes.NewReader(encoded))
	if err != nil || decoded.Job.Revision != math.MaxUint64 {
		t.Fatalf("large revision: %v", err)
	}
	for name, encoded := range map[string][]byte{
		"duplicate": []byte(`{"kind":"x","kind":"y"}`),
		"trailing":  append(bytes.Clone(encoded), []byte(`{}`)...),
		"unknown":   bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":"value","kind":`), 1),
		"oversized": bytes.Repeat([]byte(" "), SharedMaximumBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSharedSnapshot(bytes.NewReader(encoded)); err == nil {
				t.Fatal("accepted invalid snapshot JSON")
			}
		})
	}
}

func TestSharedBundleBudgetAndRunJoins(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	snapshot.Runs = []SharedRun{}
	snapshot.Logs = []SharedLogReference{}
	snapshot.Items = snapshot.Items[:1]
	for number := uint64(1); number <= SharedMaximumRuns; number++ {
		id := fmt.Sprintf("01990000-0000-7000-8000-%012d", number+100)
		snapshot.Runs = append(snapshot.Runs, SharedRun{ID: id, Number: number, ExecutionID: sharedExecution})
		for _, stream := range []string{"stderr", "stdout"} {
			snapshot.Logs = append(snapshot.Logs, SharedLogReference{
				ID: fmt.Sprintf("artifact:%03d:%s", number, stream), RunID: id, ExecutionID: sharedExecution,
				Stream: stream, ManifestRevision: 1, Bytes: SharedMaximumTailBytes, Complete: true,
			})
		}
	}
	collector := sharedFixtureCollector(snapshot)
	collector.Logs = sharedLogFunc(func(_ context.Context, request SharedLogRequest) (SharedLogTail, error) {
		return SharedLogTail{
			Reference: request.Reference, Data: bytes.Repeat([]byte("x"), SharedMaximumTailBytes),
			ByteStart: 0, ByteEnd: request.Reference.Bytes, CapturedAt: fixtureTime,
		}, nil
	})
	request := sharedFixtureRequest()
	request.Selection.RunID = ""
	request.IncludeLogTail = true
	evidence, err := collector.Collect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Limits.EncodedBytes > SharedMaximumBytes || len(evidence.Artifacts) == 0 || len(evidence.Artifacts) == len(snapshot.Logs) {
		t.Fatalf("bundle budget not enforced: %#v", evidence.Limits)
	}
	if !slices.ContainsFunc(evidence.Omissions, func(o Omission) bool { return o.Code == OmissionLogBudgetExceeded }) {
		t.Fatal("truncated artifacts have no omission")
	}
}

func TestSharedInvalidProvenanceAndDisclosure(t *testing.T) {
	for name, mutate := range map[string]func(*SharedSnapshot){
		"missing source":    func(s *SharedSnapshot) { s.Source.ControlInstanceID = "" },
		"wrong source kind": func(s *SharedSnapshot) { s.Source.Kind = "local" },
		"duplicate run":     func(s *SharedSnapshot) { s.Runs = append(s.Runs, s.Runs[0]) },
		"zero run":          func(s *SharedSnapshot) { s.Runs[0].Number = 0 },
		"log execution":     func(s *SharedSnapshot) { s.Logs[0].ExecutionID = sharedOther },
		"foreign fact":      func(s *SharedSnapshot) { s.Items[0].Source.EntityID = sharedOther },
		"unsafe token":      func(s *SharedSnapshot) { s.Items[0].Value = []byte(`"/secret/path"`) },
		"wrong value":       func(s *SharedSnapshot) { s.Items[1].Value = []byte(`"not-an-exit"`) },
		"too many items":    func(s *SharedSnapshot) { s.Items = make([]Item, SharedMaximumItems+1) },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := sharedFixtureSnapshot(t)
			mutate(&snapshot)
			if _, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest()); err == nil {
				t.Fatal("accepted invalid shared evidence")
			}
		})
	}
}

func TestSharedEvidenceFixture(t *testing.T) {
	evidence, err := sharedFixtureCollector(sharedFixtureSnapshot(t)).Collect(t.Context(), sharedFixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if encodeErr := Encode(&encoded, evidence); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	const path = "testdata/shared-control-failure-v2.json"
	if os.Getenv("UPDATE_DIAGNOSTIC_FIXTURES") == "1" {
		if writeErr := os.WriteFile(path, encoded.Bytes(), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	fixture, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fixture, encoded.Bytes()) {
		t.Fatal("shared fixture bytes changed; review contract before regenerating")
	}
	if _, err := Decode(strings.NewReader(string(fixture)), DecodeLimits{}); err != nil {
		t.Fatal(err)
	}
}

func TestSharedTypedFactsPreserveObservationsAndRejectInjectedContext(t *testing.T) {
	for name, codeAndValue := range map[string]struct {
		code   string
		value  any
		entity string
	}{
		"scheduler": {CodeSharedSchedulerObservation, SharedSchedulerObservation{
			State: "FUTURE_STATE", Reason: "DependencyNeverSatisfied", ObservedAt: fixtureTime,
		}, sharedRun},
		"dependency": {CodeSharedDependencyObservation, SharedDependencyObservation{
			JobID: sharedOther, Predicate: "success", ObservedOutcome: "failure", Satisfied: false, Disposition: "blocked",
		}, sharedJob},
		"event": {CodeSharedLifecycleEvent, SharedLifecycleEvent{
			ID: sharedOther, Type: "process_finished", RunID: sharedRun, ExecutionID: sharedExecution,
			Phase: "terminal", Outcome: "failure", ObservedAt: fixtureTime, RecordedAt: fixtureTime.Add(time.Second),
		}, sharedOther},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := sharedFixtureSnapshot(t)
			fact := sharedFixtureItem(t, "fact:typed", codeAndValue.code, codeAndValue.value, codeAndValue.entity)
			snapshot.Items = append(snapshot.Items, fact)
			evidence, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest())
			if err != nil {
				t.Fatal(err)
			}
			if len(evidence.Items) != 3 || !bytes.Equal(evidence.Items[2].Value, fact.Value) {
				t.Fatal("collector altered the source's factual observation")
			}
			fact.Value = bytes.Replace(fact.Value, []byte("{"), []byte(`{"environment":{"TOKEN":"SECRET"},`), 1)
			var canonicalErr error
			fact.Value, canonicalErr = canonicalJSON(fact.Value, 32)
			if canonicalErr != nil {
				t.Fatal(canonicalErr)
			}
			snapshot.Items[2] = fact
			if _, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest()); err == nil {
				t.Fatal("accepted arbitrary environment context in typed observation")
			}
		})
	}
}

func TestSharedMissingExecutionAndEmptyHistoryRemainExplicit(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	snapshot.Logs = []SharedLogReference{}
	snapshot.Runs[0].ExecutionID = ""
	evidence, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Shared.Runs[0].ExecutionID != "" || !slices.ContainsFunc(evidence.Omissions, func(value Omission) bool {
		return value.Code == OmissionSharedExecutionIdentity
	}) {
		t.Fatal("missing imported execution was invented or unreported")
	}
	snapshot.Runs = []SharedRun{}
	snapshot.Items = []Item{}
	snapshot.Job.Phase = "accepted"
	snapshot.Job.Outcome = ""
	request := sharedFixtureRequest()
	request.Selection.RunID = ""
	evidence, err = sharedFixtureCollector(snapshot).Collect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Shared.Runs) != 0 || !evidence.Consistency.ActiveStateMayHaveAdvanced {
		t.Fatal("invented a run or stable active state")
	}
}

func TestSharedInvalidRequestsNeverReadSources(t *testing.T) {
	collector := SharedCollector{Snapshots: sharedSnapshotFunc(func(context.Context, SharedSelection) (SharedSnapshot, error) {
		t.Fatal("invalid request reached source reader")
		return SharedSnapshot{}, nil
	})}
	for name, mutate := range map[string]func(*SharedCollectionRequest){
		"unqualified source": func(r *SharedCollectionRequest) { r.Selection.NamespaceID = "research" },
		"invalid run":        func(r *SharedCollectionRequest) { r.Selection.RunID = "latest" },
		"excessive logs":     func(r *SharedCollectionRequest) { r.IncludeLogTail = true; r.LogBytes = SharedMaximumTailBytes + 1 },
		"implicit logs":      func(r *SharedCollectionRequest) { r.LogBytes = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			request := sharedFixtureRequest()
			mutate(&request)
			if _, err := collector.Collect(t.Context(), request); err == nil {
				t.Fatal("accepted invalid request")
			}
		})
	}
	if _, err := (SharedCollector{}).Collect(t.Context(), sharedFixtureRequest()); err == nil {
		t.Fatal("accepted missing reader")
	}
}

func TestSharedScalarFactsDoNotCoerceMissingObservations(t *testing.T) {
	for name, test := range map[string]struct {
		code  string
		value any
		valid bool
	}{
		"revision":             {CodeJobRevision, uint64(12), true},
		"bytes":                {CodeLogStderrBytes, uint64(18), true},
		"available":            {CodeLogAvailable, true, true},
		"completed":            {CodeRunCompletedAt, fixtureTime, true},
		"missing exit":         {CodeRunExitCode, nil, false},
		"missing availability": {CodeLogAvailable, nil, false},
		"negative bytes":       {CodeLogStdoutBytes, -1, false},
		"invalid time":         {CodeRunStartedAt, "not-a-timestamp", false},
		"zero time":            {CodeRunStartedAt, time.Time{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := sharedFixtureSnapshot(t)
			entity := sharedRun
			if test.code == CodeJobRevision {
				entity = sharedJob
			}
			snapshot.Items = []Item{sharedFixtureItem(t, "fact:selected", test.code, test.value, entity)}
			_, err := sharedFixtureCollector(snapshot).Collect(t.Context(), sharedFixtureRequest())
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v, error=%v", test.valid, err)
			}
		})
	}
}

func TestSharedCancellationAfterMetadataAndNoLogReferences(t *testing.T) {
	snapshot := sharedFixtureSnapshot(t)
	ctx, cancel := context.WithCancel(t.Context())
	collector := sharedFixtureCollector(snapshot)
	collector.Snapshots = sharedSnapshotFunc(func(context.Context, SharedSelection) (SharedSnapshot, error) {
		cancel()
		return snapshot, nil
	})
	if _, err := collector.Collect(ctx, sharedFixtureRequest()); !errors.Is(err, context.Canceled) {
		t.Fatalf("late cancellation: %v", err)
	}
	snapshot.Logs = []SharedLogReference{}
	request := sharedFixtureRequest()
	request.IncludeLogTail = true
	evidence, err := sharedFixtureCollector(snapshot).Collect(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(evidence.Omissions, func(value Omission) bool { return value.Code == OmissionLogsUnavailable }) {
		t.Fatal("missing log availability is not explicit")
	}
}
