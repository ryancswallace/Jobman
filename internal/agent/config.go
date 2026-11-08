package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.yaml.in/yaml/v3"
)

const (
	agentConfigEnvironment = "JOBMAN_AGENT_CONFIG"
	agentSchemaVersionKey  = "schema_version"
	configString           = "string"
	configInteger          = "integer"
	configBoolean          = "boolean"
	defaultAWSExecutable   = "aws"
	agentRunCommand        = "run"
	agentApptainer         = "apptainer"
)

type agentConfigField struct {
	kind     string
	fallback any
}

// The catalog is shared by strict decoding, defaults, flag mapping, and schema
// output. It contains references only; enrollment credentials remain runtime state.
var agentConfigFields = map[string]agentConfigField{
	"state_dir": {configString, ""}, "poll_interval": {configString, "2s"},
	"artifact_store": {configString, ""}, "artifact_store_version": {configInteger, int64(1)},
	"artifact_root": {configString, ""}, "artifact_s3_bucket": {configString, ""},
	"artifact_s3_prefix": {configString, ""}, "artifact_s3_region": {configString, ""},
	"artifact_s3_expected_owner": {configString, ""}, "aws_cli": {configString, defaultAWSExecutable},
	"container_engine": {configString, ""}, "container_runtime": {configString, ""},
	"container_host_network": {configBoolean, false}, "max_log_bytes": {configInteger, int64(64 * 1024 * 1024)},
	"max_artifact_bytes": {configInteger, int64(1024 * 1024 * 1024)},
	"slurm_root":         {configString, ""}, "slurm_runner": {configString, ""}, "slurm": {configBoolean, false},
	"server": {configString, ""}, "target_generation": {configString, ""},
	"token_file": {configString, "-"}, "server_ca": {configString, ""},
	"agent_binary": {configString, ""}, "unit_dir": {configString, ""},
	"host": {configString, ""}, "ssh": {configString, "ssh"},
	"remote_binary":    {configString, ".local/bin/jobman-agent"},
	"remote_state_dir": {configString, ".local/state/jobman-agent"}, "expected_version": {configString, ""},
	"log_level": {configString, "info"},
}

type agentConfiguration struct {
	Values  map[string]any    `json:"config"`
	Origins map[string]string `json:"origins"`
}

func defaultAgentConfiguration() agentConfiguration {
	result := agentConfiguration{Values: map[string]any{agentSchemaVersionKey: 1}, Origins: map[string]string{agentSchemaVersionKey: "built-in"}}
	for name, field := range agentConfigFields {
		result.Values[name] = field.fallback
		result.Origins[name] = "built-in"
	}
	return result
}

func loadAgentConfiguration(path string) (agentConfiguration, error) {
	result := defaultAgentConfiguration()
	if path == "" {
		return result, nil
	}
	information, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect agent configuration: %w", err)
	}
	if !information.Mode().IsRegular() || runtime.GOOS != "windows" && information.Mode().Perm()&0o022 != 0 {
		return result, errors.New("agent configuration must be a regular file and not group/world writable")
	}
	// #nosec G304 -- only an explicitly selected deployment configuration is read.
	file, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("open agent configuration: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 1024*1024+1))
	if err = errors.Join(readErr, file.Close()); err != nil {
		return result, fmt.Errorf("read agent configuration: %w", err)
	}
	if len(data) > 1024*1024 {
		return result, errors.New("agent configuration exceeds 1 MiB")
	}
	if err := decodeAgentConfiguration(data, path, &result); err != nil {
		return result, err
	}
	return result, validateAgentConfiguration(result.Values)
}

//nolint:cyclop // Strict scalar parsing rejects duplicate, ambiguous, and unsupported YAML forms.
func decodeAgentConfiguration(data []byte, path string, result *agentConfiguration) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return errors.New("agent configuration must be one valid YAML mapping")
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("agent configuration must contain exactly one document")
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode || document.Content[0].Anchor != "" {
		return errors.New("agent configuration must be one unanchored mapping")
	}
	mapping := document.Content[0]
	seen := make(map[string]bool)
	for index := 0; index < len(mapping.Content); index += 2 {
		key, value := mapping.Content[index], mapping.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Anchor != "" || seen[key.Value] || value.Kind != yaml.ScalarNode || value.Anchor != "" {
			return errors.New("agent configuration requires unique string keys and unanchored scalar values")
		}
		seen[key.Value] = true
		if key.Value == agentSchemaVersionKey {
			if value.Tag != "!!int" || value.Value != "1" {
				return errors.New("agent configuration schema_version must be 1")
			}
			result.Origins[key.Value] = path
			continue
		}
		field, known := agentConfigFields[key.Value]
		if !known {
			return fmt.Errorf("unknown agent configuration field %q", key.Value)
		}
		decoded, err := decodeAgentScalar(value, field.kind)
		if err != nil {
			return fmt.Errorf("invalid agent configuration field %s: %w", key.Value, err)
		}
		result.Values[key.Value], result.Origins[key.Value] = decoded, path
	}
	if !seen[agentSchemaVersionKey] {
		return errors.New("agent configuration schema_version is required")
	}
	return nil
}

func decodeAgentScalar(node *yaml.Node, kind string) (any, error) {
	switch kind {
	case configBoolean:
		if node.Tag == "!!bool" && (node.Value == "true" || node.Value == "false") {
			return node.Value == "true", nil
		}
	case configInteger:
		if node.Tag == "!!int" && strings.Trim(node.Value, "0123456789") == "" {
			value, err := strconv.ParseInt(node.Value, 10, 64)
			if err == nil {
				return value, nil
			}
		}
	case configString:
		if node.Tag == "!!str" && len(node.Value) <= 4096 && !strings.ContainsFunc(node.Value, unicode.IsControl) {
			return node.Value, nil
		}
	}
	return nil, errors.New("value has an invalid scalar type or range")
}

func agentString(values map[string]any, key string) string {
	value, ok := values[key].(string)
	if !ok {
		return ""
	}
	return value
}

//nolint:cyclop,gocognit // The catalog's independent deployment boundaries are checked without external I/O.
func validateAgentConfiguration(values map[string]any) error {
	interval, err := time.ParseDuration(agentString(values, "poll_interval"))
	if err != nil || interval < 250*time.Millisecond || interval > time.Minute {
		return errors.New("agent poll_interval must be between 250ms and 1m")
	}
	for _, key := range []string{"artifact_store_version", "max_log_bytes", "max_artifact_bytes"} {
		value, ok := values[key].(int64)
		if !ok || value < 1 {
			return fmt.Errorf("agent %s must be positive", key)
		}
	}
	for _, key := range []string{"state_dir", "artifact_root", "slurm_root", "slurm_runner", "agent_binary", "unit_dir", "server_ca"} {
		path := agentString(values, key)
		if path != "" && (!filepath.IsAbs(path) || filepath.Clean(path) != path) {
			return fmt.Errorf("agent %s must be a clean absolute path", key)
		}
	}
	token := agentString(values, "token_file")
	if token != "-" && (!filepath.IsAbs(token) || filepath.Clean(token) != token) {
		return errors.New("agent token_file must be an absolute file reference or - for stdin")
	}
	server := agentString(values, "server")
	if server != "" {
		parsed, parseErr := url.Parse(server)
		if parseErr != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return errors.New("agent server must be an HTTPS origin without credentials")
		}
	}
	if (agentString(values, "slurm_root") == "") != (agentString(values, "slurm_runner") == "") {
		return errors.New("agent slurm_root and slurm_runner must be configured together")
	}
	engine := agentString(values, "container_engine")
	if engine != "" && engine != "docker" && engine != "podman" && engine != agentApptainer {
		return errors.New("agent container_engine must be docker, podman, or apptainer")
	}
	hostNetwork, validBoolean := values["container_host_network"].(bool)
	if !validBoolean {
		return errors.New("agent container_host_network must be boolean")
	}
	if engine == "" && (agentString(values, "container_runtime") != "" || hostNetwork) {
		return errors.New("agent container_engine is required for container policy")
	}
	root, bucket := agentString(values, "artifact_root"), agentString(values, "artifact_s3_bucket")
	if agentString(values, "artifact_store") != "" || root != "" || bucket != "" {
		if agentString(values, "artifact_store") == "" || (root == "") == (bucket == "") {
			return errors.New("agent artifact_store requires exactly one filesystem root or S3 bucket")
		}
	}
	switch agentString(values, "log_level") {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("agent log_level must be debug, info, warn, or error")
	}
	return nil
}

func registerAgentConfiguration(flags *flag.FlagSet) *string {
	return flags.String("config", "", "read a strict agent YAML file (or JOBMAN_AGENT_CONFIG)")
}

func agentRunFlag(name string) bool {
	switch name {
	case "state-dir", "poll-interval", "artifact-store", "artifact-store-version", "artifact-root",
		"artifact-s3-bucket", "artifact-s3-prefix", "artifact-s3-region", "artifact-s3-expected-owner", "aws-cli",
		"container-engine", "container-runtime", "container-host-network", "max-log-bytes", "max-artifact-bytes",
		"slurm-root", "slurm-runner", "log-level":
		return true
	default:
		return false
	}
}

func agentRunOverrides(flags *flag.FlagSet) []string {
	var overrides []string
	flags.Visit(func(field *flag.Flag) {
		if agentRunFlag(field.Name) {
			overrides = append(overrides, "--"+field.Name+"="+field.Value.String())
		}
	})
	return overrides
}

func applyAgentConfiguration(flags *flag.FlagSet, selected string) (string, error) {
	path := selected
	if path == "" {
		path = os.Getenv(agentConfigEnvironment)
		if path != "" && (!filepath.IsAbs(path) || filepath.Clean(path) != path) {
			return "", errors.New("JOBMAN_AGENT_CONFIG must be a clean absolute path")
		}
	}
	if path == "" {
		return "", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve agent configuration: %w", err)
	}
	configuration, err := loadAgentConfiguration(absolute)
	if err != nil {
		return "", err
	}
	explicit := make(map[string]bool)
	flags.Visit(func(field *flag.Flag) { explicit[field.Name] = true })
	for name, value := range configuration.Values {
		flagName := strings.ReplaceAll(name, "_", "-")
		if !explicit[flagName] && flags.Lookup(flagName) != nil {
			if err = flags.Set(flagName, fmt.Sprint(value)); err != nil {
				return "", fmt.Errorf("apply agent configuration field %s: %w", name, err)
			}
		}
	}
	return absolute, nil
}

func agentConfigurationSchema() map[string]any {
	properties := map[string]any{agentSchemaVersionKey: map[string]any{"const": 1}}
	for name, field := range agentConfigFields {
		property := map[string]any{"type": field.kind, "default": field.fallback}
		if field.kind == configInteger {
			property["minimum"] = 1
		}
		if field.kind == configString {
			property["maxLength"] = 4096
		}
		properties[name] = property
	}
	return map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Jobman agent configuration schema 1",
		"description": "Validate semantic constraints with jobman-agent config validate PATH. No credentials are resolved.",
		"type":        "object", "additionalProperties": false, "required": []string{agentSchemaVersionKey}, "properties": properties,
	}
}

func executeAgentConfig(args []string, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "schema" {
		return json.NewEncoder(stdout).Encode(agentConfigurationSchema())
	}
	if len(args) != 2 || (args[0] != "validate" && args[0] != "show") {
		return errors.New("usage: jobman-agent config schema | validate PATH | show PATH")
	}
	configuration, err := loadAgentConfiguration(args[1])
	if err != nil {
		return err
	}
	if args[0] == "show" {
		return json.NewEncoder(stdout).Encode(configuration)
	}
	_, err = fmt.Fprintln(stdout, "valid")
	return err
}
