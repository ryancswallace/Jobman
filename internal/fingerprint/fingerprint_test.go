package fingerprint

import (
	"bytes"
	"strings"
	"testing"
)

func TestBuildIsKeyedDeterministicAndSensitiveToFacts(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x41}, keyBytes)
	exitCode := 2
	input := Input{
		ExecutableIdentity: "/private/target", Outcome: "failure", FailureClass: "nonzero_exit",
		ExitCode: &exitCode, PolicyDisposition: "non_retryable_failure",
	}
	first, available, err := Build(key, input)
	if err != nil || !available {
		t.Fatalf("Build() = %#v, %v, %v", first, available, err)
	}
	second, _, err := Build(key, input)
	if err != nil || first != second {
		t.Fatalf("second Build() = %#v, %v", second, err)
	}
	changedExecutable := input
	changedExecutable.ExecutableIdentity = "/private/other"
	other, _, err := Build(key, changedExecutable)
	if err != nil || other == first {
		t.Fatalf("Build(changed executable) = %#v, %v", other, err)
	}
	otherKey, _, err := Build(bytes.Repeat([]byte{0x42}, keyBytes), input)
	if err != nil || otherKey == first {
		t.Fatalf("Build(changed key) = %#v, %v", otherKey, err)
	}
	if strings.Contains(first.Value, input.ExecutableIdentity) {
		t.Fatal("fingerprint exposes executable identity")
	}
}

func TestBuildReturnsUnavailableForInsufficientFacts(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x41}, keyBytes)
	for name, input := range map[string]Input{
		"success":            {ExecutableIdentity: "target", Outcome: "success", FailureClass: "none"},
		"missing executable": {Outcome: "failure", FailureClass: "nonzero_exit"},
		"missing class":      {ExecutableIdentity: "target", Outcome: "failure"},
	} {
		t.Run(name, func(t *testing.T) {
			if value, available, err := Build(key, input); err != nil || available || value.Value != "" {
				t.Fatalf("Build() = %#v, %v, %v", value, available, err)
			}
		})
	}
	if _, _, err := Build([]byte("short"), Input{}); err == nil {
		t.Fatal("Build(short key) error = nil")
	}
}

func TestBuildRejectsUnsafeFactualInputs(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x41}, keyBytes)
	exitCode := 2
	base := Input{ExecutableIdentity: "target", Outcome: "failure", FailureClass: "nonzero_exit"}
	for name, mutate := range map[string]func(*Input){
		"oversized":     func(value *Input) { value.PlatformReason = strings.Repeat("x", 257) },
		"newline":       func(value *Input) { value.Signal = "TERM\nsecret" },
		"negative exit": func(value *Input) { negative := -1; value.ExitCode = &negative },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			input := base
			mutate(&input)
			if _, _, err := Build(key, input); err == nil {
				t.Fatal("Build() error = nil")
			}
		})
	}
	base.ExitCode = &exitCode
	if _, available, err := Build(key, Input{ExecutableIdentity: "target", FailureClass: "class"}); err != nil || available {
		t.Fatalf("Build(missing outcome) = available:%t error:%v", available, err)
	}
}
