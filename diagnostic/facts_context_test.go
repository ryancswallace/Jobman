package diagnostic

import (
	"strings"
	"testing"
)

func TestExecutionContextFactValidationBoundaries(t *testing.T) {
	t.Parallel()

	if err := (Command{Executable: "/bin/tool", Arguments: []string{"", "value"}}).Validate(); err != nil {
		t.Fatalf("Command.Validate(valid) error = %v", err)
	}
	for name, value := range map[string]Command{
		"empty executable": {Arguments: []string{}},
		"excess arguments": {Executable: "tool", Arguments: make([]string, maximumCommandArguments+1)},
		"NUL argument":     {Executable: "tool", Arguments: []string{"bad\x00argument"}},
		"oversized vector": {Executable: "tool", Arguments: []string{strings.Repeat("x", maximumCommandBytes)}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if value.Validate() == nil {
				t.Fatal("Command.Validate(invalid) error = nil")
			}
		})
	}

	validEnvironment := EnvironmentNames{
		Inheritance: "submission", Set: []string{"MODE"}, Unset: []string{"DEBUG"}, Secret: []string{"TOKEN"},
	}
	if err := validEnvironment.Validate(); err != nil {
		t.Fatalf("EnvironmentNames.Validate(valid) error = %v", err)
	}
	tooMany := make([]string, maximumEnvironmentNames+1)
	for index := range tooMany {
		tooMany[index] = "NAME"
	}
	for name, value := range map[string]EnvironmentNames{
		"invalid inheritance": {Inheritance: "bad\x00policy"},
		"too many names":      {Set: tooMany},
		"empty name":          {Set: []string{""}},
		"invalid name":        {Secret: []string{"bad\x00name"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if value.Validate() == nil {
				t.Fatal("EnvironmentNames.Validate(invalid) error = nil")
			}
		})
	}

	validPolicy := ExecutionPolicy{
		StdinPolicy: "null", StopGracePeriod: "0s", RunTimeout: "0s", JobTimeout: "0s",
		WaitMode: "all", ConcurrencySlots: 1, LogCapture: "both", LogRetention: "24h0m0s",
	}
	if err := validPolicy.Validate(); err != nil {
		t.Fatalf("ExecutionPolicy.Validate(valid) error = %v", err)
	}
	missing := validPolicy
	missing.WaitMode = ""
	if missing.Validate() == nil {
		t.Fatal("ExecutionPolicy.Validate(missing field) error = nil")
	}
	containsNUL := validPolicy
	containsNUL.Tags = []string{"bad\x00tag"}
	if containsNUL.Validate() == nil {
		t.Fatal("ExecutionPolicy.Validate(NUL) error = nil")
	}
	oversized := validPolicy
	oversized.Tags = []string{strings.Repeat("x", maximumExecutionPolicyBytes)}
	if oversized.Validate() == nil {
		t.Fatal("ExecutionPolicy.Validate(oversized) error = nil")
	}
}

func TestExecutionContextItemValidationRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	path, err := JSONValue("")
	if err != nil {
		t.Fatal(err)
	}
	environment, err := JSONValue(EnvironmentNames{Set: []string{""}})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := JSONValue(ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for name, item := range map[string]Item{
		"path": {
			ID: "ev:job:target:working_directory", Code: CodeTargetWorkingDirectory, Value: path,
			Quality: QualityObserved, Disclosure: DisclosurePath,
		},
		"environment names": {
			ID: "ev:job:target:environment_names", Code: CodeTargetEnvironmentNames, Value: environment,
			Quality: QualityObserved, Disclosure: DisclosureEnvironmentName,
		},
		"execution policy": {
			ID: "ev:job:policy", Code: CodeExecutionPolicy, Value: policy,
			Quality: QualityObserved, Disclosure: DisclosureMetadata,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if validateContextItems([]Item{item}) == nil {
				t.Fatal("validateContextItems(invalid) error = nil")
			}
		})
	}
}
