package artifact

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ryancswallace/jobman/protocol"
)

// StageInputs materializes declared immutable file inputs beneath the private
// execution workspace.
func StageInputs(
	ctx context.Context,
	store Store,
	workspace string,
	inputs []protocol.InputArtifact,
	maximumBytes int64,
) error {
	if len(inputs) == 0 {
		return nil
	}
	if store == nil {
		return errors.New("artifact store is not configured")
	}
	remaining := maximumBytes
	for _, input := range inputs {
		storeName, key, err := parseArtifactURI(input.Source)
		if err != nil {
			return fmt.Errorf("stage input %q: %w", input.Name, err)
		}
		if storeName != store.Name() {
			return fmt.Errorf("stage input %q: artifact store %q is not configured", input.Name, storeName)
		}
		destination, err := mapSandboxPath(workspace, input.Target, "inputs")
		if err != nil {
			return fmt.Errorf("stage input %q: %w", input.Name, err)
		}
		if err = makePrivateDirectories(workspace, filepath.Dir(destination)); err != nil {
			return fmt.Errorf("stage input %q: %w", input.Name, err)
		}
		object, err := store.Materialize(ctx, key, destination, remaining, input.Checksum)
		if err != nil {
			return fmt.Errorf("stage input %q: %w", input.Name, err)
		}
		remaining -= object.ByteLength
	}

	return nil
}

// PublishOutputs publishes declared regular-file outputs and returns canonical
// metadata suitable for a terminal execution event.
//
//nolint:gocognit // Publication preserves partial results while classifying every file/store failure.
func PublishOutputs(
	ctx context.Context,
	store Store,
	workspace string,
	outputs []protocol.OutputArtifact,
	maximumBytes int64,
) ([]protocol.PublishedArtifact, error) {
	if len(outputs) == 0 {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("artifact store is not configured")
	}
	remaining := maximumBytes
	result := make([]protocol.PublishedArtifact, 0, len(outputs))
	for _, output := range outputs {
		source, err := mapSandboxPath(workspace, output.Source, "outputs")
		if err != nil {
			return result, fmt.Errorf("publish output %q: %w", output.Name, err)
		}
		information, statErr := os.Lstat(source)
		if errors.Is(statErr, fs.ErrNotExist) && !output.Required {
			continue
		}
		if statErr != nil {
			return result, fmt.Errorf("publish output %q: %w", output.Name, statErr)
		}
		if err = rejectSymlinksBelow(workspace, source); err != nil {
			return result, fmt.Errorf("publish output %q: %w", output.Name, err)
		}
		if !information.Mode().IsRegular() || information.Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("publish output %q: output is not a regular file", output.Name)
		}
		storeName, key, err := parseArtifactURI(output.Destination)
		if err != nil {
			return result, fmt.Errorf("publish output %q: %w", output.Name, err)
		}
		if storeName != store.Name() {
			return result, fmt.Errorf("publish output %q: artifact store %q is not configured", output.Name, storeName)
		}
		object, err := store.Publish(ctx, key, source, remaining)
		if err != nil {
			return result, fmt.Errorf("publish output %q: %w", output.Name, err)
		}
		remaining -= object.ByteLength
		result = append(result, protocol.PublishedArtifact{
			Name: output.Name, StoreName: store.Name(), StoreVersion: store.Version(),
			ObjectKey: object.Key, ByteLength: object.ByteLength, Checksum: object.Checksum,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })

	return result, nil
}

func parseArtifactURI(value string) (storeName, key string, err error) {
	if strings.Contains(value, "%") {
		return "", "", errors.New("percent-encoded artifact paths are not supported")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "artifact" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", errors.New("invalid artifact URI")
	}
	key = strings.TrimPrefix(parsed.Path, "/")
	if key == "" {
		return "", "", errors.New("artifact URI has no object key")
	}

	return parsed.Host, key, nil
}

func mapSandboxPath(workspace, logical, root string) (string, error) {
	prefix := root + ":/"
	if !strings.HasPrefix(logical, prefix) {
		return "", fmt.Errorf("path does not use the %s logical root", root)
	}
	relative := strings.TrimPrefix(logical, prefix)
	relative = filepath.FromSlash(relative)
	if relative == "" || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) ||
		filepath.Clean(relative) != relative || strings.Contains(logical, `\`) {
		return "", errors.New("artifact path is empty or not normalized")
	}
	result := filepath.Join(workspace, root, relative)
	relativeCheck, err := filepath.Rel(workspace, result)
	if err != nil || relativeCheck == ".." || strings.HasPrefix(relativeCheck, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact path escapes workspace")
	}

	return result, nil
}
