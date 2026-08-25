package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type conformanceManifest struct {
	Contract string            `json:"contract"`
	Cases    []conformanceCase `json:"cases"`
}

type conformanceCase struct {
	File           string `json:"file"`
	Kind           string `json:"kind"`
	Valid          bool   `json:"valid"`
	Digest         string `json:"digest,omitempty"`
	WorkloadDigest string `json:"workloadDigest,omitempty"`
}

func TestConformanceFixtures(t *testing.T) {
	t.Parallel()
	root := filepath.Join("testdata", "conformance")
	encoded, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest conformanceManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.Contract != V1Alpha1 {
		t.Fatalf("contract = %q", manifest.Contract)
	}
	if len(manifest.Cases) == 0 {
		t.Fatal("manifest has no cases")
	}
	for _, test := range manifest.Cases {
		t.Run(test.File, func(t *testing.T) {
			t.Parallel()
			contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(test.File)))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			switch test.Kind {
			case WorkloadKind:
				sealed, decodeErr := DecodeWorkload(strings.NewReader(string(contents)), DecodeLimits{})
				assertFixtureResult(t, test, sealed.Digest, "", decodeErr)
			case JobRequestKind:
				sealed, decodeErr := DecodeJobRequest(strings.NewReader(string(contents)), DecodeLimits{})
				assertFixtureResult(t, test, sealed.RequestDigest, sealed.WorkloadDigest, decodeErr)
			case EffectiveExecutionKind:
				sealed, decodeErr := DecodeEffectiveExecution(strings.NewReader(string(contents)), DecodeLimits{})
				assertFixtureResult(t, test, sealed.Digest, sealed.Document.Spec.Workload.Digest, decodeErr)
			case AgentAssignmentKind:
				sealed, decodeErr := DecodeAgentAssignment(strings.NewReader(string(contents)), DecodeLimits{})
				assertFixtureResult(
					t, test, sealed.EffectiveExecutionDigest,
					sealed.Document.Spec.EffectiveExecution.Spec.Workload.Digest, decodeErr,
				)
			default:
				t.Fatalf("unsupported fixture kind %q", test.Kind)
			}
		})
	}
}

func assertFixtureResult(t *testing.T, test conformanceCase, digestValue, workloadDigest string, err error) {
	t.Helper()
	if !test.Valid {
		if err == nil {
			t.Fatal("fixture unexpectedly passed")
		}

		return
	}
	if err != nil {
		t.Fatalf("fixture failed: %v", err)
	}
	if test.Digest == "" {
		t.Fatalf("manifest digest is missing; computed %q", digestValue)
	}
	if test.Digest != digestValue {
		t.Fatalf("digest = %q, want %q", digestValue, test.Digest)
	}
	if test.WorkloadDigest != workloadDigest {
		t.Fatalf("workload digest = %q, want %q", workloadDigest, test.WorkloadDigest)
	}
}
