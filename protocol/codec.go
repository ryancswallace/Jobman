package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	defaultMaximumDecodeBytes = 2 * 1024 * 1024
	defaultMaximumJSONDepth   = 32
)

// DecodeWorkload reads, bounds, normalizes, validates, and seals one workload.
func DecodeWorkload(source io.Reader, limits DecodeLimits) (SealedWorkload, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedWorkload{}, fmt.Errorf("decode workload: %w", err)
	}
	var value Workload
	if err := strictUnmarshal(encoded, &value); err != nil {
		return SealedWorkload{}, fmt.Errorf("decode workload: %w", err)
	}

	return SealWorkload(value)
}

// SealWorkload returns normalized canonical JSON and its SHA-256 digest.
func SealWorkload(value Workload) (SealedWorkload, error) {
	cloned, cloneErr := cloneJSON(value)
	if cloneErr != nil {
		return SealedWorkload{}, fmt.Errorf("seal workload: clone: %w", cloneErr)
	}
	if normalizeErr := normalizeWorkload(&cloned); normalizeErr != nil {
		return SealedWorkload{}, fmt.Errorf("seal workload: %w", normalizeErr)
	}
	if validationErr := ValidateWorkload(cloned); validationErr != nil {
		return SealedWorkload{}, validationErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedWorkload{}, fmt.Errorf("seal workload: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedWorkload{}, fmt.Errorf("seal workload: canonicalize: %w", err)
	}

	return SealedWorkload{
		Document:      cloned,
		CanonicalJSON: canonical,
		Digest:        digest(canonical),
	}, nil
}

// DecodeJobRequest reads, bounds, normalizes, validates, and seals one job
// request and its embedded workload.
func DecodeJobRequest(source io.Reader, limits DecodeLimits) (SealedJobRequest, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedJobRequest{}, fmt.Errorf("decode job request: %w", err)
	}
	var value JobRequest
	if err := strictUnmarshal(encoded, &value); err != nil {
		return SealedJobRequest{}, fmt.Errorf("decode job request: %w", err)
	}

	return SealJobRequest(value)
}

// SealJobRequest normalizes and seals a request. An empty workload digest is
// populated; a non-empty digest must match the embedded canonical workload.
func SealJobRequest(value JobRequest) (SealedJobRequest, error) {
	cloned, cloneErr := cloneJSON(value)
	if cloneErr != nil {
		return SealedJobRequest{}, fmt.Errorf("seal job request: clone: %w", cloneErr)
	}
	sealedWorkload, err := SealWorkload(cloned.Spec.Workload.Document)
	if err != nil {
		return SealedJobRequest{}, fmt.Errorf("seal job request: workload: %w", err)
	}
	if cloned.Spec.Workload.Digest != "" && cloned.Spec.Workload.Digest != sealedWorkload.Digest {
		return SealedJobRequest{}, errors.New("seal job request: workload digest does not match document")
	}
	cloned.Spec.Workload = WorkloadBinding{
		Digest:   sealedWorkload.Digest,
		Document: sealedWorkload.Document,
	}
	if len(cloned.Metadata.Labels) == 0 {
		cloned.Metadata.Labels = nil
	}
	if validationErr := ValidateJobRequest(cloned); validationErr != nil {
		return SealedJobRequest{}, validationErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedJobRequest{}, fmt.Errorf("seal job request: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedJobRequest{}, fmt.Errorf("seal job request: canonicalize: %w", err)
	}

	return SealedJobRequest{
		Document:       cloned,
		CanonicalJSON:  canonical,
		RequestDigest:  digest(canonical),
		WorkloadDigest: sealedWorkload.Digest,
	}, nil
}

// DecodeCollectionRequest reads, bounds, normalizes, validates, and seals one
// collection and all of its explicit child workloads.
func DecodeCollectionRequest(source io.Reader, limits DecodeLimits) (SealedCollectionRequest, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedCollectionRequest{}, fmt.Errorf("decode collection request: %w", err)
	}
	var value CollectionRequest
	if err := strictUnmarshal(encoded, &value); err != nil {
		return SealedCollectionRequest{}, fmt.Errorf("decode collection request: %w", err)
	}

	return SealCollectionRequest(value)
}

// SealCollectionRequest seals every child workload while retaining the
// caller's deterministic item order.
func SealCollectionRequest(value CollectionRequest) (SealedCollectionRequest, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedCollectionRequest{}, fmt.Errorf("seal collection request: clone: %w", err)
	}
	if len(cloned.Metadata.Labels) == 0 {
		cloned.Metadata.Labels = nil
	}
	if cloned.Spec.MaxActive == 0 {
		cloned.Spec.MaxActive = 1
	}
	if cloned.Spec.FailurePolicy == "" {
		cloned.Spec.FailurePolicy = "continue"
	}
	if cloned.Spec.ArrayPolicy == "" {
		cloned.Spec.ArrayPolicy = "prefer"
	}
	digests := make([]string, len(cloned.Spec.Items))
	for index := range cloned.Spec.Items {
		sealed, sealErr := SealWorkload(cloned.Spec.Items[index].Workload.Document)
		if sealErr != nil {
			return SealedCollectionRequest{}, fmt.Errorf(
				"seal collection request: item %q: %w", cloned.Spec.Items[index].Name, sealErr,
			)
		}
		if supplied := cloned.Spec.Items[index].Workload.Digest; supplied != "" && supplied != sealed.Digest {
			return SealedCollectionRequest{}, fmt.Errorf(
				"seal collection request: item %q workload digest does not match document",
				cloned.Spec.Items[index].Name,
			)
		}
		cloned.Spec.Items[index].Workload = WorkloadBinding{
			Digest: sealed.Digest, Document: sealed.Document,
		}
		digests[index] = sealed.Digest
	}
	if validateErr := ValidateCollectionRequest(cloned); validateErr != nil {
		return SealedCollectionRequest{}, validateErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedCollectionRequest{}, fmt.Errorf("seal collection request: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedCollectionRequest{}, fmt.Errorf("seal collection request: canonicalize: %w", err)
	}

	return SealedCollectionRequest{
		Document: cloned, CanonicalJSON: canonical,
		RequestDigest: digest(canonical), WorkloadDigests: digests,
	}, nil
}

// DecodeGraphRequest reads, bounds, normalizes, validates, and seals one
// immutable dependency graph.
func DecodeGraphRequest(source io.Reader, limits DecodeLimits) (SealedGraphRequest, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedGraphRequest{}, fmt.Errorf("decode graph request: %w", err)
	}
	var value GraphRequest
	if err := strictUnmarshal(encoded, &value); err != nil {
		return SealedGraphRequest{}, fmt.Errorf("decode graph request: %w", err)
	}

	return SealGraphRequest(value)
}

// SealGraphRequest seals every node and canonicalizes semantically unordered
// edge and selected-outcome lists before cycle validation.
func SealGraphRequest(value GraphRequest) (SealedGraphRequest, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedGraphRequest{}, fmt.Errorf("seal graph request: clone: %w", err)
	}
	if len(cloned.Metadata.Labels) == 0 {
		cloned.Metadata.Labels = nil
	}
	if cloned.Spec.MaxActive == 0 {
		cloned.Spec.MaxActive = 1
	}
	if cloned.Spec.UnsatisfiedPolicy == "" {
		cloned.Spec.UnsatisfiedPolicy = "skip"
	}
	digests := make([]string, len(cloned.Spec.Nodes))
	for index := range cloned.Spec.Nodes {
		sealed, sealErr := SealWorkload(cloned.Spec.Nodes[index].Workload.Document)
		if sealErr != nil {
			return SealedGraphRequest{}, fmt.Errorf(
				"seal graph request: node %q: %w", cloned.Spec.Nodes[index].Name, sealErr,
			)
		}
		if supplied := cloned.Spec.Nodes[index].Workload.Digest; supplied != "" && supplied != sealed.Digest {
			return SealedGraphRequest{}, fmt.Errorf(
				"seal graph request: node %q workload digest does not match document",
				cloned.Spec.Nodes[index].Name,
			)
		}
		cloned.Spec.Nodes[index].Workload = WorkloadBinding{Digest: sealed.Digest, Document: sealed.Document}
		digests[index] = sealed.Digest
	}
	for index := range cloned.Spec.Edges {
		sort.Strings(cloned.Spec.Edges[index].Outcomes)
		if len(cloned.Spec.Edges[index].Outcomes) == 0 {
			cloned.Spec.Edges[index].Outcomes = nil
		}
	}
	sort.Slice(cloned.Spec.Edges, func(left, right int) bool {
		if cloned.Spec.Edges[left].From != cloned.Spec.Edges[right].From {
			return cloned.Spec.Edges[left].From < cloned.Spec.Edges[right].From
		}

		return cloned.Spec.Edges[left].To < cloned.Spec.Edges[right].To
	})
	if validateErr := ValidateGraphRequest(cloned); validateErr != nil {
		return SealedGraphRequest{}, validateErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedGraphRequest{}, fmt.Errorf("seal graph request: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedGraphRequest{}, fmt.Errorf("seal graph request: canonicalize: %w", err)
	}

	return SealedGraphRequest{
		Document: cloned, CanonicalJSON: canonical,
		RequestDigest: digest(canonical), WorkloadDigests: digests,
	}, nil
}

// DecodeEffectiveExecution reads, bounds, normalizes, validates, and seals one
// server-generated effective execution.
func DecodeEffectiveExecution(source io.Reader, limits DecodeLimits) (SealedEffectiveExecution, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("decode effective execution: %w", err)
	}
	var value EffectiveExecution
	if err = strictUnmarshal(encoded, &value); err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("decode effective execution: %w", err)
	}

	return SealEffectiveExecution(value)
}

// SealEffectiveExecution returns normalized canonical JSON and a digest. A
// missing workload digest is populated and a supplied digest must match.
func SealEffectiveExecution(value EffectiveExecution) (SealedEffectiveExecution, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("seal effective execution: clone: %w", err)
	}
	if err = normalizeWorkloadBinding(&cloned.Spec.Workload); err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("seal effective execution: workload: %w", err)
	}
	sort.Slice(cloned.Spec.ArtifactStores, func(left, right int) bool {
		return cloned.Spec.ArtifactStores[left].Name < cloned.Spec.ArtifactStores[right].Name
	})
	if validationErr := ValidateEffectiveExecution(cloned); validationErr != nil {
		return SealedEffectiveExecution{}, validationErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("seal effective execution: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedEffectiveExecution{}, fmt.Errorf("seal effective execution: canonicalize: %w", err)
	}

	return SealedEffectiveExecution{Document: cloned, CanonicalJSON: canonical, Digest: digest(canonical)}, nil
}

// DecodeAgentAssignment reads, bounds, normalizes, and validates one
// redeliverable assignment envelope.
func DecodeAgentAssignment(source io.Reader, limits DecodeLimits) (SealedAgentAssignment, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("decode agent assignment: %w", err)
	}
	var value AgentAssignment
	if err = strictUnmarshal(encoded, &value); err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("decode agent assignment: %w", err)
	}

	return SealAgentAssignment(value)
}

// SealAgentAssignment seals the embedded effective execution and verifies its
// digest. It does not confer execution acceptance.
func SealAgentAssignment(value AgentAssignment) (SealedAgentAssignment, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("seal agent assignment: clone: %w", err)
	}
	sealedExecution, err := SealEffectiveExecution(cloned.Spec.EffectiveExecution)
	if err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("seal agent assignment: effective execution: %w", err)
	}
	if cloned.Spec.EffectiveExecutionDigest != "" && cloned.Spec.EffectiveExecutionDigest != sealedExecution.Digest {
		return SealedAgentAssignment{}, errors.New("seal agent assignment: effective execution digest does not match document")
	}
	cloned.Spec.EffectiveExecution = sealedExecution.Document
	cloned.Spec.EffectiveExecutionDigest = sealedExecution.Digest
	if validationErr := ValidateAgentAssignment(cloned); validationErr != nil {
		return SealedAgentAssignment{}, validationErr
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("seal agent assignment: encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return SealedAgentAssignment{}, fmt.Errorf("seal agent assignment: canonicalize: %w", err)
	}

	return SealedAgentAssignment{
		Document:                 cloned,
		CanonicalJSON:            canonical,
		EffectiveExecutionDigest: sealedExecution.Digest,
	}, nil
}

func readCanonical(source io.Reader, limits DecodeLimits) ([]byte, error) {
	if source == nil {
		return nil, errors.New("source is nil")
	}
	maximumBytes := limits.MaxBytes
	if maximumBytes == 0 {
		maximumBytes = defaultMaximumDecodeBytes
	}
	if maximumBytes < 1 {
		return nil, errors.New("maximum bytes must be positive")
	}
	maximumDepth := limits.MaxDepth
	if maximumDepth == 0 {
		maximumDepth = defaultMaximumJSONDepth
	}
	if maximumDepth < 1 {
		return nil, errors.New("maximum depth must be positive")
	}
	encoded, err := io.ReadAll(io.LimitReader(source, maximumBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	if int64(len(encoded)) > maximumBytes {
		return nil, fmt.Errorf("input exceeds %d bytes", maximumBytes)
	}
	canonical, err := canonicalJSON(encoded, maximumDepth)
	if err != nil {
		return nil, err
	}

	return canonical, nil
}

func strictUnmarshal(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}

		return err
	}

	return nil
}

func cloneJSON[T any](value T) (T, error) {
	var cloned T
	encoded, err := json.Marshal(value)
	if err != nil {
		return cloned, err
	}
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return cloned, err
	}

	return cloned, nil
}

// canonicalJSON rejects duplicate keys, excessive nesting, trailing values,
// and invalid numbers before returning encoding/json's stable representation.
func canonicalJSON(encoded []byte, maximumDepth int) ([]byte, error) {
	if !utf8.Valid(encoded) {
		return nil, errors.New("JSON input is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder, 0, maximumDepth)
	if err != nil {
		return nil, err
	}
	if _, trailingErr := decoder.Token(); !errors.Is(trailingErr, io.EOF) {
		if trailingErr == nil {
			trailingErr = errors.New("trailing JSON value")
		}

		return nil, trailingErr
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return canonical, nil
}

//nolint:gocognit,cyclop // Recursive token validation handles each JSON compound kind explicitly.
func decodeJSONValue(decoder *json.Decoder, depth, maximumDepth int) (any, error) {
	if depth > maximumDepth {
		return nil, fmt.Errorf("JSON nesting exceeds %d", maximumDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if number, isNumber := token.(json.Number); isNumber {
		canonical, canonicalErr := normalizeJSONNumber(number.String())
		if canonicalErr != nil {
			return nil, canonicalErr
		}

		return canonical, nil
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delimiter {
	case '{':
		value := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, duplicate := value[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object key %q", key)
			}
			child, err := decodeJSONValue(decoder, depth+1, maximumDepth)
			if err != nil {
				return nil, err
			}
			value[key] = child
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}

		return value, nil
	case '[':
		value := make([]any, 0)
		for decoder.More() {
			child, err := decodeJSONValue(decoder, depth+1, maximumDepth)
			if err != nil {
				return nil, err
			}
			value = append(value, child)
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}

		return value, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

// normalizeJSONNumber emits one exact scientific-decimal representation. It
// does not round through binary floating point, so implementations in other
// languages can reproduce the same bytes for arbitrarily precise JSON input.
func normalizeJSONNumber(value string) (json.Number, error) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = strings.TrimPrefix(value, "-")
	}
	mantissa := value
	exponent := new(big.Int)
	if exponentIndex := strings.IndexAny(value, "eE"); exponentIndex >= 0 {
		mantissa = value[:exponentIndex]
		exponentText := strings.TrimPrefix(value[exponentIndex+1:], "+")
		if _, valid := exponent.SetString(exponentText, 10); !valid {
			return "", errors.New("invalid JSON number exponent")
		}
	}

	integer := mantissa
	fraction := ""
	if pointIndex := strings.IndexByte(mantissa, '.'); pointIndex >= 0 {
		integer = mantissa[:pointIndex]
		fraction = mantissa[pointIndex+1:]
	}
	digits := integer + fraction
	firstSignificant := strings.IndexFunc(digits, func(character rune) bool {
		return character != '0'
	})
	if firstSignificant < 0 {
		return json.Number("0"), nil
	}
	adjustment := int64(len(integer) - firstSignificant - 1)
	exponent.Add(exponent, big.NewInt(adjustment))
	significant := strings.TrimRight(digits[firstSignificant:], "0")

	var canonical strings.Builder
	if negative {
		canonical.WriteByte('-')
	}
	canonical.WriteByte(significant[0])
	if len(significant) > 1 {
		canonical.WriteByte('.')
		canonical.WriteString(significant[1:])
	}
	if exponent.Sign() != 0 {
		canonical.WriteByte('e')
		canonical.WriteString(exponent.String())
	}

	return json.Number(canonical.String()), nil
}

func digest(canonical []byte) string {
	sum := sha256.Sum256(canonical)

	return "sha256:" + hex.EncodeToString(sum[:])
}
