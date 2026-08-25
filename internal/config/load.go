package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	maxConfigBytes = 1 << 20
	maxYAMLDepth   = 64
	maxYAMLNodes   = 10000
)

const builtInYAML = `
schema_version: 1
trusted_project_roots: []
job_specs: {}
wait_conditions: {}
secrets: {}
concurrency:
  max_active_slots: unlimited
  pools: {}
retention:
  completed_metadata_max_age: unlimited
  completed_log_max_age: 30d
  max_jobs: unlimited
  max_runs_per_job: unlimited
  max_log_bytes_per_job: unlimited
  max_total_log_bytes: unlimited
notifiers: {}
profiles: {}
shared:
  current_profile: ""
  profiles: {}
redaction:
  names: []
  patterns: []
`

// SourceKind identifies a configuration precedence layer.
type SourceKind string

const (
	// SourceBuiltIn contains Jobman's immutable defaults.
	SourceBuiltIn SourceKind = "built-in"
	// SourceSystem is a machine-wide configuration file.
	SourceSystem SourceKind = "system"
	// SourceUser is the current user's configuration file.
	SourceUser SourceKind = "user"
	// SourceProject is an explicitly trusted project configuration file.
	SourceProject SourceKind = "project"
	// SourceExplicit is a file explicitly selected by the caller.
	SourceExplicit SourceKind = "explicit"
	// SourceEnvironment contains documented JOBMAN_ overrides.
	SourceEnvironment SourceKind = "environment"
	// SourceFlags contains typed command-line overrides encoded as YAML.
	SourceFlags SourceKind = "flags"
)

// Source supplies one low-to-high-precedence YAML layer. When Data is nil,
// Path is read. Missing optional files are skipped.
type Source struct {
	Kind     SourceKind
	Name     string
	Path     string
	Data     []byte
	Optional bool
}

// SourceInfo is non-sensitive provenance for one loaded source.
type SourceInfo struct {
	Kind SourceKind `json:"kind"`
	Name string     `json:"name"`
	Path string     `json:"path,omitempty"`
}

// Precedence returns the source's normative low-to-high precedence rank.
func (kind SourceKind) Precedence() (int, bool) {
	switch kind {
	case SourceBuiltIn:
		return 0, true
	case SourceSystem:
		return 10, true
	case SourceUser:
		return 20, true
	case SourceProject:
		return 30, true
	case SourceExplicit:
		return 40, true
	case SourceEnvironment:
		return 50, true
	case SourceFlags:
		return 60, true
	default:
		return 0, false
	}
}

// Loaded is a validated effective configuration plus leaf-level provenance.
type Loaded struct {
	Config  Config                `json:"config"`
	Sources []SourceInfo          `json:"sources"`
	Origins map[string]SourceInfo `json:"origins"`
}

// FileSource creates a required file-backed configuration source.
func FileSource(kind SourceKind, path string) Source {
	return Source{Kind: kind, Name: string(kind), Path: path}
}

// OptionalFileSource creates a file-backed source skipped when it does not exist.
func OptionalFileSource(kind SourceKind, path string) Source {
	return Source{Kind: kind, Name: string(kind), Path: path, Optional: true}
}

// BytesSource creates an in-memory source and defensively copies the input.
func BytesSource(kind SourceKind, name string, data []byte) Source {
	copyOfData := make([]byte, len(data))
	copy(copyOfData, data)

	return Source{Kind: kind, Name: name, Data: copyOfData}
}

// Default returns the validated built-in configuration.
func Default() Config {
	loaded, err := Load()
	if err != nil {
		panic(fmt.Sprintf("invalid built-in configuration: %v", err))
	}

	return loaded.Config
}

// Parse strictly parses one complete explicit YAML configuration over defaults.
func Parse(data []byte) (Config, error) {
	loaded, err := Load(BytesSource(SourceExplicit, "configuration", data))
	if err != nil {
		return Config{}, err
	}

	return loaded.Config, nil
}

// Load recursively merges sources in low-to-high precedence order, strictly
// decodes the effective result, applies field defaults, and validates references.
func Load(sources ...Source) (Loaded, error) {
	root, defaultsErr := decodeYAML([]byte(builtInYAML), "built-in defaults")
	if defaultsErr != nil {
		return Loaded{}, fmt.Errorf("decode built-in defaults: %w", defaultsErr)
	}

	loaded := Loaded{
		Sources: []SourceInfo{{Kind: SourceBuiltIn, Name: "built-in defaults"}},
		Origins: make(map[string]SourceInfo),
	}
	recordOrigins(root.Content[0], nil, loaded.Sources[0], loaded.Origins)

	lastPrecedence := 0
	for _, source := range sources {
		if sourceErr := validateSource(source); sourceErr != nil {
			return Loaded{}, sourceErr
		}
		precedence, _ := source.Kind.Precedence()
		if precedence < lastPrecedence {
			return Loaded{}, fmt.Errorf("configuration source %s is out of precedence order", sourceLabel(source))
		}
		lastPrecedence = precedence
		data, skipped, sourceErr := readSource(source)
		if sourceErr != nil {
			return Loaded{}, sourceErr
		}
		if skipped {
			continue
		}
		label := sourceLabel(source)
		node, err := decodeYAML(data, label)
		if err != nil {
			return Loaded{}, invalidError(fmt.Errorf("decode %s: %w", label, err))
		}
		if err := validateSourcePolicy(source, node.Content[0]); err != nil {
			return Loaded{}, invalidError(fmt.Errorf("validate %s: %w", label, err))
		}
		if err := validateSourceSchema(node.Content[0]); err != nil {
			return Loaded{}, invalidError(fmt.Errorf("validate %s: %w", label, err))
		}

		info := SourceInfo{Kind: source.Kind, Name: source.Name, Path: source.Path}
		if info.Name == "" {
			info.Name = string(source.Kind)
		}
		mergeMapping(root.Content[0], node.Content[0], nil, info, loaded.Origins)
		loaded.Sources = append(loaded.Sources, info)
	}

	configuration, err := decodeConfig(root)
	if err != nil {
		return Loaded{}, invalidError(err)
	}
	normalize(&configuration)
	if err := configuration.Validate(); err != nil {
		return Loaded{}, invalidError(fmt.Errorf("validate effective configuration: %w", err))
	}
	loaded.Config = configuration

	return loaded, nil
}

func validateSource(source Source) error {
	switch source.Kind {
	case SourceSystem, SourceUser, SourceProject, SourceExplicit, SourceEnvironment, SourceFlags:
	default:
		return fmt.Errorf("configuration source kind %q is invalid", source.Kind)
	}
	if source.Data == nil && source.Path == "" {
		return fmt.Errorf("configuration source %q has neither data nor a path", sourceLabel(source))
	}
	if source.Data != nil && source.Path != "" {
		return fmt.Errorf("configuration source %q has both data and a path", sourceLabel(source))
	}

	return nil
}

func readSource(source Source) (data []byte, skipped bool, returnedErr error) {
	if source.Data != nil {
		if len(source.Data) > maxConfigBytes {
			return nil, false, invalidError(
				fmt.Errorf("read %s: configuration exceeds %d bytes", sourceLabel(source), maxConfigBytes),
			)
		}

		return append([]byte(nil), source.Data...), false, nil
	}

	file, err := os.Open(source.Path)
	if err != nil {
		if source.Optional && errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, fmt.Errorf("open %s: %w", sourceLabel(source), err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && returnedErr == nil {
			data = nil
			returnedErr = fmt.Errorf("close %s: %w", sourceLabel(source), closeErr)
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat %s: %w", sourceLabel(source), err)
	}
	if !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("read %s: path is not a regular file", sourceLabel(source))
	}
	reader := io.LimitReader(file, maxConfigBytes+1)
	data, err = io.ReadAll(reader)
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", sourceLabel(source), err)
	}
	if len(data) > maxConfigBytes {
		return nil, false, invalidError(
			fmt.Errorf("read %s: configuration exceeds %d bytes", sourceLabel(source), maxConfigBytes),
		)
	}

	return data, false, nil
}

func decodeYAML(data []byte, label string) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	err := decoder.Decode(&document)
	if errors.Is(err, io.EOF) {
		document = emptyDocument()
	} else if err != nil {
		return nil, err
	}

	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple YAML documents are not allowed")
		}

		return nil, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s must contain one YAML mapping", label)
	}
	counter := 0
	if err := validateYAMLNode(document.Content[0], 0, &counter); err != nil {
		return nil, err
	}

	return &document, nil
}

// Validation necessarily walks and classifies every YAML node and mapping key.
//
//nolint:gocognit // The branches enforce separate structural security limits.
func validateYAMLNode(node *yaml.Node, depth int, counter *int) error {
	*counter++
	if *counter > maxYAMLNodes {
		return fmt.Errorf("YAML contains more than %d nodes", maxYAMLNodes)
	}
	if depth > maxYAMLDepth {
		return fmt.Errorf("YAML nesting exceeds %d levels", maxYAMLDepth)
	}
	if node.Kind == yaml.AliasNode || node.Anchor != "" {
		return errors.New("YAML aliases and anchors are not supported")
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != yamlTagString || key.Value == "" {
				return errors.New("YAML mapping keys must be nonempty strings")
			}
			if key.Value == "<<" {
				return errors.New("YAML merge keys are not supported")
			}
			if _, duplicate := seen[key.Value]; duplicate {
				return fmt.Errorf("duplicate YAML key %q at line %d", key.Value, key.Line)
			}
			seen[key.Value] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := validateYAMLNode(child, depth+1, counter); err != nil {
			return err
		}
	}

	return nil
}

func decodeConfig(document *yaml.Node) (Config, error) {
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		return Config{}, fmt.Errorf("encode effective configuration: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return Config{}, fmt.Errorf("encode effective configuration: %w", err)
	}

	decoder := yaml.NewDecoder(&encoded)
	decoder.KnownFields(true)
	var configuration Config
	if err := decoder.Decode(&configuration); err != nil {
		return Config{}, fmt.Errorf("decode effective configuration: %w", err)
	}

	return configuration, nil
}

func validateSourceSchema(mapping *yaml.Node) error {
	value := mappingValue(mapping, "schema_version")
	if value == nil {
		return nil
	}
	if value.Kind != yaml.ScalarNode || value.Tag != yamlTagInteger || value.Value != "1" {
		return fmt.Errorf("schema_version must be %d", SchemaVersion)
	}

	return nil
}

func validateSourcePolicy(source Source, mapping *yaml.Node) error {
	if source.Kind != SourceSystem && source.Kind != SourceProject && source.Kind != SourceEnvironment {
		return nil
	}
	if mappingValue(mapping, "trusted_project_roots") != nil {
		return fmt.Errorf("%s configuration cannot set trusted_project_roots", source.Kind)
	}
	if source.Kind == SourceProject && mappingValue(mapping, "shared") != nil {
		return errors.New("project configuration cannot set shared control profiles")
	}

	return nil
}

func mergeMapping(destination, overlay *yaml.Node, path []string, source SourceInfo, origins map[string]SourceInfo) {
	for index := 0; index < len(overlay.Content); index += 2 {
		key := overlay.Content[index]
		value := overlay.Content[index+1]
		childPath := appendPath(path, key.Value)
		destinationIndex := mappingIndex(destination, key.Value)
		if destinationIndex >= 0 && destination.Content[destinationIndex+1].Kind == yaml.MappingNode && value.Kind == yaml.MappingNode {
			mergeMapping(destination.Content[destinationIndex+1], value, childPath, source, origins)
			continue
		}

		copyValue := cloneYAMLNode(value)
		removeOrigins(childPath, origins)
		if destinationIndex >= 0 {
			destination.Content[destinationIndex+1] = copyValue
		} else {
			destination.Content = append(destination.Content, cloneYAMLNode(key), copyValue)
		}
		recordOrigins(copyValue, childPath, source, origins)
	}
}

func removeOrigins(path []string, origins map[string]SourceInfo) {
	prefix := strings.Join(path, ".")
	for originPath := range origins {
		if originPath == prefix || strings.HasPrefix(originPath, prefix+".") {
			delete(origins, originPath)
		}
	}
}

func recordOrigins(node *yaml.Node, path []string, source SourceInfo, origins map[string]SourceInfo) {
	if node.Kind != yaml.MappingNode {
		origins[strings.Join(path, ".")] = source
		return
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		recordOrigins(node.Content[index+1], appendPath(path, key.Value), source, origins)
	}
}

func mappingValue(mapping *yaml.Node, key string) *yaml.Node {
	index := mappingIndex(mapping, key)
	if index < 0 {
		return nil
	}

	return mapping.Content[index+1]
}

func mappingIndex(mapping *yaml.Node, key string) int {
	for index := 0; index < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return index
		}
	}

	return -1
}

func cloneYAMLNode(node *yaml.Node) *yaml.Node {
	clone := *node
	clone.Alias = nil
	clone.Content = make([]*yaml.Node, len(node.Content))
	for index, child := range node.Content {
		clone.Content[index] = cloneYAMLNode(child)
	}

	return &clone
}

func appendPath(path []string, item string) []string {
	result := make([]string, len(path)+1)
	copy(result, path)
	result[len(path)] = item

	return result
}

func emptyDocument() yaml.Node {
	return yaml.Node{
		Kind: yaml.DocumentNode,
		Content: []*yaml.Node{{
			Kind: yaml.MappingNode,
			Tag:  yamlTagMap,
		}},
	}
}

func sourceLabel(source Source) string {
	if source.Path != "" {
		return fmt.Sprintf("%s configuration %q", source.Kind, source.Path)
	}
	if source.Name != "" {
		return fmt.Sprintf("%s configuration %q", source.Kind, source.Name)
	}

	return string(source.Kind) + " configuration"
}
