// Package fingerprint builds opaque, store-local failure group identifiers
// from typed factual inputs.
package fingerprint

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ryancswallace/jobman/diagnostic"
)

const keyBytes = 32

// Input contains only core facts. ExecutableIdentity is HMACed separately and
// never appears in the returned value or persisted input record.
type Input struct {
	ExecutableIdentity string
	Outcome            string
	FailureClass       string
	DiagnosticCode     string
	ExitCode           *int
	Signal             string
	PlatformReason     string
	TimeoutScope       string
	PolicyDisposition  string
}

// Build returns an opaque fingerprint when enough facts exist. Successful
// outcomes and missing executable/class inputs intentionally return
// available=false rather than creating weak groups.
func Build(key []byte, input Input) (value diagnostic.FailureFingerprint, available bool, err error) {
	if len(key) != keyBytes {
		return diagnostic.FailureFingerprint{}, false, fmt.Errorf("build failure fingerprint: key must contain %d bytes", keyBytes)
	}
	if input.Outcome == "success" || input.Outcome == "" || input.ExecutableIdentity == "" || input.FailureClass == "" {
		return diagnostic.FailureFingerprint{}, false, nil
	}
	if validationErr := validateInput(input); validationErr != nil {
		return diagnostic.FailureFingerprint{}, false, validationErr
	}
	executableMAC := hmac.New(sha256.New, key)
	_, _ = executableMAC.Write([]byte("jobman.failure.executable.v1\x00"))
	_, _ = executableMAC.Write([]byte(input.ExecutableIdentity))
	exitCode := ""
	if input.ExitCode != nil {
		exitCode = strconv.Itoa(*input.ExitCode)
	}
	projection := struct {
		SchemaVersion     int    `json:"schema_version"`
		ExecutableDigest  string `json:"executable_digest"`
		Outcome           string `json:"outcome"`
		FailureClass      string `json:"failure_class"`
		DiagnosticCode    string `json:"diagnostic_code"`
		ExitCode          string `json:"exit_code"`
		Signal            string `json:"signal"`
		PlatformReason    string `json:"platform_reason"`
		TimeoutScope      string `json:"timeout_scope"`
		PolicyDisposition string `json:"policy_disposition"`
	}{
		SchemaVersion:     diagnostic.FingerprintInputSchemaVersion,
		ExecutableDigest:  hex.EncodeToString(executableMAC.Sum(nil)),
		Outcome:           input.Outcome,
		FailureClass:      input.FailureClass,
		DiagnosticCode:    input.DiagnosticCode,
		ExitCode:          exitCode,
		Signal:            input.Signal,
		PlatformReason:    input.PlatformReason,
		TimeoutScope:      input.TimeoutScope,
		PolicyDisposition: input.PolicyDisposition,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return diagnostic.FailureFingerprint{}, false, fmt.Errorf("build failure fingerprint: encode inputs: %w", err)
	}
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte("jobman.failure.fingerprint.v1\x00"))
	_, _ = digest.Write(encoded)
	value = diagnostic.FailureFingerprint{
		Algorithm:          diagnostic.FingerprintAlgorithmHMACSHA256,
		InputSchemaVersion: diagnostic.FingerprintInputSchemaVersion,
		Value:              hex.EncodeToString(digest.Sum(nil)),
		Scope:              diagnostic.FingerprintScopeStoreLocal,
	}
	if err := value.Validate(); err != nil {
		return diagnostic.FailureFingerprint{}, false, err
	}

	return value, true, nil
}

func validateInput(input Input) error {
	values := []string{
		input.Outcome, input.FailureClass, input.DiagnosticCode, input.Signal,
		input.PlatformReason, input.TimeoutScope, input.PolicyDisposition,
	}
	for _, value := range values {
		if len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("build failure fingerprint: input is outside the safe factual boundary")
		}
	}
	if input.ExitCode != nil && *input.ExitCode < 0 {
		return errors.New("build failure fingerprint: exit code must not be negative")
	}

	return nil
}
