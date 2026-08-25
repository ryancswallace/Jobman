package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

func normalizeWorkload(value *Workload) error {
	normalizeMetadata(&value.Metadata)
	normalizeCommand(&value.Spec.Command)
	normalizeOptionalEnvironment(&value.Spec)
	if value.Spec.Resources != nil && *value.Spec.Resources == (Resources{}) {
		value.Spec.Resources = nil
	}
	normalizeRuntime(&value.Spec.Runtime)
	normalizeArtifacts(&value.Spec)
	normalizePolicy(&value.Spec.Policy)
	normalizeRequirements(&value.Spec)
	if err := normalizeExtensions(value.Spec.Extensions); err != nil {
		return err
	}
	if len(value.Spec.Extensions) == 0 {
		value.Spec.Extensions = nil
	}

	return nil
}

func normalizeWorkloadBinding(value *WorkloadBinding) error {
	sealed, err := SealWorkload(value.Document)
	if err != nil {
		return err
	}
	if value.Digest != "" && value.Digest != sealed.Digest {
		return errors.New("workload digest does not match document")
	}
	value.Document = sealed.Document
	value.Digest = sealed.Digest

	return nil
}

func normalizeMetadata(value *WorkloadMetadata) {
	if len(value.Labels) == 0 {
		value.Labels = nil
	}
	if len(value.Annotations) == 0 {
		value.Annotations = nil
	}
}

func normalizeCommand(value *Command) {
	if len(value.Args) == 0 {
		value.Args = nil
	}
}

func normalizeOptionalEnvironment(value *WorkloadSpec) {
	if value.WorkingDirectory == "" {
		value.WorkingDirectory = "workspace:/"
	}
	if value.Environment != nil {
		normalizeEnvironment(value.Environment)
		if emptyEnvironment(*value.Environment) {
			value.Environment = nil
		}
	}
}

func normalizeRuntime(value *Runtime) {
	if value.Kind == "" {
		value.Kind = "native"
	}
	if value.Container == nil {
		return
	}
	if value.Container.PullPolicy == "" {
		value.Container.PullPolicy = "if-not-present"
	}
	if value.Container.Network == "" {
		value.Container.Network = "restricted"
	}
}

func normalizeArtifacts(value *WorkloadSpec) {
	if value.Artifacts == nil {
		return
	}
	sort.Slice(value.Artifacts.Inputs, func(left, right int) bool {
		return value.Artifacts.Inputs[left].Name < value.Artifacts.Inputs[right].Name
	})
	sort.Slice(value.Artifacts.Outputs, func(left, right int) bool {
		return value.Artifacts.Outputs[left].Name < value.Artifacts.Outputs[right].Name
	})
	if len(value.Artifacts.Inputs) == 0 {
		value.Artifacts.Inputs = nil
	}
	if len(value.Artifacts.Outputs) == 0 {
		value.Artifacts.Outputs = nil
	}
	if len(value.Artifacts.Inputs) == 0 && len(value.Artifacts.Outputs) == 0 {
		value.Artifacts = nil
	}
}

func normalizePolicy(value *ExecutionPolicy) {
	if value.Retry.MaxRuns == 0 {
		value.Retry.MaxRuns = 1
	}
	if value.DuplicateRisk == "" {
		value.DuplicateRisk = "reject"
	}
}

func normalizeRequirements(value *WorkloadSpec) {
	if value.Requirements == nil {
		return
	}
	sort.Strings(value.Requirements.OperatingSystems)
	sort.Strings(value.Requirements.Architectures)
	sort.Strings(value.Requirements.Capabilities)
	if emptyRequirements(*value.Requirements) {
		value.Requirements = nil
	}
}

func normalizeEnvironment(value *Environment) {
	if len(value.Values) == 0 {
		value.Values = nil
	}
	sort.Slice(value.Secrets, func(left, right int) bool {
		return value.Secrets[left].Name < value.Secrets[right].Name
	})
	if len(value.Secrets) == 0 {
		value.Secrets = nil
	}
}

func normalizeExtensions(extensions map[string]json.RawMessage) error {
	for name, raw := range extensions {
		canonical, err := canonicalJSON(raw, defaultMaximumJSONDepth)
		if err != nil {
			return fmt.Errorf("extension %q: %w", name, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(canonical))
		decoder.UseNumber()
		var object map[string]any
		if err := decoder.Decode(&object); err != nil || object == nil {
			return fmt.Errorf("extension %q must be a JSON object", name)
		}
		extensions[name] = canonical
	}

	return nil
}

func emptyEnvironment(value Environment) bool {
	return value.Profile == "" && len(value.Values) == 0 && len(value.Secrets) == 0
}

func emptyRequirements(value Requirements) bool {
	return len(value.OperatingSystems) == 0 &&
		len(value.Architectures) == 0 &&
		len(value.Capabilities) == 0
}
