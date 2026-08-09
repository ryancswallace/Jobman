package model

import (
	"strings"
	"testing"
)

func TestDiagnosticCatalog(t *testing.T) {
	codes := ValidDiagnosticCodes()
	if len(codes) == 0 {
		t.Fatal("ValidDiagnosticCodes() is empty")
	}
	for index, code := range codes {
		if index > 0 && codes[index-1] >= code {
			t.Fatalf("catalog is not sorted and unique: %q then %q", codes[index-1], code)
		}
		record, err := NewDiagnosticRecord(code, nil)
		if err != nil {
			t.Fatalf("NewDiagnosticRecord(%q) error = %v", code, err)
		}
		if record.SchemaVersion != 1 || record.Code != code || record.Operation == "" || record.Attributes == nil {
			t.Fatalf("NewDiagnosticRecord(%q) = %#v", code, record)
		}
	}
}

func TestDiagnosticRecordPreservesUnknownLegacyCode(t *testing.T) {
	record, err := NewDiagnosticRecord("legacy_failure", nil)
	if err != nil {
		t.Fatal(err)
	}
	if record.Origin != DiagnosticOriginUnknown || record.Category != DiagnosticCategoryUnknown {
		t.Fatalf("record = %#v", record)
	}
}

func TestDiagnosticRecordRejectsUnsafeAttributes(t *testing.T) {
	if _, err := NewDiagnosticRecord(DiagnosticTargetStartFailed, map[string]string{"path": "/secret"}); err == nil {
		t.Fatal("NewDiagnosticRecord() accepted unallowlisted path")
	}
}

func TestDiagnosticAttributeBoundary(t *testing.T) {
	t.Parallel()

	if !validDiagnosticAttribute("attempt", "safe value") {
		t.Fatal("validDiagnosticAttribute(valid) = false")
	}
	for _, test := range [][2]string{
		{"Bad", "value"},
		{"attempt", strings.Repeat("x", 257)},
		{"attempt", "line\nbreak"},
	} {
		if validDiagnosticAttribute(test[0], test[1]) {
			t.Errorf("validDiagnosticAttribute(%q, %q) = true", test[0], test[1])
		}
	}
}

func TestLegacyDetailEncodingOmitsInvalidStructuredRecord(t *testing.T) {
	encoded := diagnosticDetails("other-client")
	if string(encoded) != `{"diagnostic_code":"other-client"}` {
		t.Fatalf("diagnosticDetails() = %s", encoded)
	}
	retry := retryDetails(RunDisposition{Reason: "next_run"}, "other-client")
	if string(retry) != `{"reason":"next_run","diagnostic_code":"other-client"}` {
		t.Fatalf("retryDetails() = %s", retry)
	}
}
