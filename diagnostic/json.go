package diagnostic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// canonicalJSON rejects duplicate keys, excessive nesting, trailing values,
// and invalid numbers before returning encoding/json's stable representation.
func canonicalJSON(encoded []byte, maximumDepth int) ([]byte, error) {
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
