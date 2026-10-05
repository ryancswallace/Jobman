package artifact

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
)

const (
	logReaderPolicyFilename     = ".jobman-log-reader.json"
	maximumLogReaderPolicyBytes = 1024
	maximumLogACLBytes          = 4096
	maximumSharedLogChunkBytes  = 256 * 1024
)

// This is operator-owned store configuration, never a workload field. The
// numeric NFSv4 principal must match ReaderUID; name/domain guessing is unsafe.
type logReaderPolicy struct {
	SchemaVersion int    `json:"schema_version"`
	StoreName     string `json:"store_name"`
	StoreVersion  int64  `json:"store_version"`
	ReaderUID     uint32 `json:"reader_uid"`
}

func decodeLogReaderPolicy(data []byte, name string, version int64) (*logReaderPolicy, error) {
	count, err := validateLogPolicyFields(data)
	if err != nil {
		return nil, err
	}
	var policy logReaderPolicy
	strict := json.NewDecoder(bytes.NewReader(data))
	strict.DisallowUnknownFields()
	if err = strict.Decode(&policy); err != nil || count != 4 || policy.SchemaVersion != 1 ||
		policy.StoreName != name || policy.StoreVersion != version || policy.ReaderUID == 0 || policy.ReaderUID == ^uint32(0) {
		return nil, errors.New("log reader policy has invalid identity, version or reader")
	}
	return &policy, nil
}

func validateLogPolicyFields(data []byte) (int, error) {
	if len(data) > maximumLogReaderPolicyBytes {
		return 0, errors.New("log reader policy exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return 0, errors.New("log reader policy must be an object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		key, tokenErr := decoder.Token()
		field, ok := key.(string)
		if tokenErr != nil || !ok || fields[field] != nil {
			return 0, errors.New("log reader policy contains invalid or duplicate fields")
		}
		switch field {
		case "schema_version", "store_name", "store_version", "reader_uid":
		default:
			return 0, errors.New("log reader policy contains an unknown field")
		}
		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return 0, errors.New("log reader policy contains invalid JSON")
		}
		fields[field] = raw
	}
	if closing, closeErr := decoder.Token(); closeErr != nil || closing != json.Delim('}') {
		return 0, errors.New("log reader policy is incomplete")
	}
	if _, trailingErr := decoder.Token(); !errors.Is(trailingErr, io.EOF) {
		return 0, errors.New("log reader policy has trailing data")
	}
	return len(fields), nil
}

var logNamespacePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$`)

func validLogUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16 && decoded[6]>>4 >= 1 && decoded[6]>>4 <= 5 && decoded[8]>>6 == 2 && value == strings.ToLower(value)
}

// sharedLogPrefix identifies only the producer's existing canonical namespace,
// job, execution and stream hierarchy. Other paths keep private directory modes.
func sharedLogPrefix(parts []string) int {
	if len(parts) < 2 || parts[0] != "namespaces" || !logNamespacePattern.MatchString(parts[1]) {
		return 0
	}
	if len(parts) < 4 || parts[2] != "jobs" || !validLogUUID(parts[3]) {
		return 2
	}
	if len(parts) < 6 || parts[4] != "executions" || !validLogUUID(parts[5]) {
		return 4
	}
	if len(parts) < 8 || parts[6] != "logs" || (parts[7] != "stdout" && parts[7] != "stderr") {
		return 6
	}
	return 8
}

func isSharedLogKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) != 9 || sharedLogPrefix(parts) != 8 || !strings.HasSuffix(parts[8], ".chunk") {
		return false
	}
	sequence := strings.TrimSuffix(parts[8], ".chunk")
	number, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil || number < 1 || len(sequence) < 8 {
		return false
	}
	canonical := strconv.FormatInt(number, 10)
	if len(canonical) < 8 {
		canonical = strings.Repeat("0", 8-len(canonical)) + canonical
	}
	return sequence == canonical
}
