package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	logChunkBytes   int64 = 256 * 1024
	logStreamStdout       = "stdout"
	logStreamStderr       = "stderr"
)

// agentLogChunk is the control-plane metadata for one immutable filesystem
// object. The bytes are written before this record enters the durable outbox.
type agentLogChunk struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   logChunkMetadata `json:"metadata"`
	Spec       logChunkSpec     `json:"spec"`
}

type logChunkMetadata struct {
	ExecutionID string `json:"executionId"`
	Stream      string `json:"stream"`
	Sequence    int64  `json:"sequence"`
}

type logChunkSpec struct {
	StoreName    string    `json:"storeName"`
	StoreVersion int64     `json:"storeVersion"`
	ObjectKey    string    `json:"objectKey"`
	ByteOffset   int64     `json:"byteOffset"`
	ByteLength   int64     `json:"byteLength"`
	Checksum     string    `json:"checksum"`
	CapturedAt   time.Time `json:"capturedAt"`
	Complete     bool      `json:"complete"`
	Truncated    bool      `json:"truncated"`
}

type logPosition struct {
	ByteOffset   int64
	NextSequence int64
	Complete     bool
}

type logOwner struct {
	namespace   string
	jobID       string
	executionID string
}

type capturedLog struct {
	filePath   string
	length     int64
	capturedAt time.Time
	truncated  bool
	skip       bool
}

func (service *service) publishLogs(
	ctx context.Context,
	execution SpoolExecution,
	directory string,
	completion *CompletionManifest,
) error {
	if service.artifactStore == nil {
		return nil
	}
	metadata := execution.Assignment.Document.Spec.EffectiveExecution.Metadata
	owner := logOwner{
		namespace: metadata.Namespace, jobID: metadata.JobID, executionID: metadata.ExecutionID,
	}
	for _, selected := range []struct {
		name     string
		filename string
	}{
		{name: logStreamStdout, filename: stdoutFilename},
		{name: logStreamStderr, filename: stderrFilename},
	} {
		if err := service.publishLog(
			ctx, owner, directory, selected.name, selected.filename, completion,
		); err != nil {
			return err
		}
	}

	return nil
}

func (service *service) publishLog(
	ctx context.Context,
	owner logOwner,
	directory, stream, filename string,
	completion *CompletionManifest,
) error {
	position, err := service.spool.logPosition(ctx, owner.executionID, stream)
	if err != nil || position.Complete {
		return err
	}
	capture, err := inspectCapturedLog(directory, stream, filename, completion)
	if err != nil || capture.skip {
		return err
	}

	return service.queueCapturedLog(ctx, owner, stream, position, capture, completion != nil)
}

func inspectCapturedLog(
	directory, stream, filename string,
	completion *CompletionManifest,
) (capturedLog, error) {
	result := capturedLog{
		filePath: filepath.Join(directory, filename), capturedAt: completionTime(completion),
	}
	info, err := os.Stat(result.filePath)
	missing := errors.Is(err, os.ErrNotExist)
	if missing && completion == nil {
		result.skip = true

		return result, nil
	}
	if err != nil && !missing {
		return capturedLog{}, fmt.Errorf("publish %s log: inspect capture: %w", stream, err)
	}
	if !missing {
		if !info.Mode().IsRegular() {
			return capturedLog{}, fmt.Errorf("publish %s log: capture is not a regular file", stream)
		}
		result.length = info.Size()
		result.capturedAt = info.ModTime().UTC()
	}
	if completion == nil {
		return result, nil
	}
	if capture, found := completion.Logs[stream]; found {
		result.length = capture.ByteLength
		result.truncated = capture.Truncated
	}
	if (missing && result.length != 0) || (!missing && result.length != info.Size()) {
		return capturedLog{}, fmt.Errorf("publish %s log: completion length does not match capture", stream)
	}

	return result, nil
}

func (service *service) queueCapturedLog(
	ctx context.Context,
	owner logOwner,
	stream string,
	position logPosition,
	capture capturedLog,
	terminal bool,
) error {
	for position.ByteOffset < capture.length ||
		terminal && !position.Complete && position.ByteOffset == capture.length {
		length := min(logChunkBytes, capture.length-position.ByteOffset)
		var contents []byte
		var err error
		if length != 0 {
			contents, err = readLogChunk(capture.filePath, position.ByteOffset, length)
			if err != nil {
				return fmt.Errorf("publish %s log: %w", stream, err)
			}
		}
		sequence := position.NextSequence
		objectKey := logObjectKey(owner.namespace, owner.jobID, owner.executionID, stream, sequence)
		checksum, err := service.artifactStore.Put(ctx, objectKey, contents)
		if err != nil {
			return fmt.Errorf("publish %s log: %w", stream, err)
		}
		complete := terminal && position.ByteOffset+length == capture.length
		chunk := agentLogChunk{
			APIVersion: controlAPIVersion, Kind: "LogChunk",
			Metadata: logChunkMetadata{
				ExecutionID: owner.executionID, Stream: stream, Sequence: sequence,
			},
			Spec: logChunkSpec{
				StoreName: service.artifactStore.Name(), StoreVersion: service.artifactStore.Version(),
				ObjectKey: objectKey, ByteOffset: position.ByteOffset, ByteLength: length,
				Checksum: checksum, CapturedAt: capture.capturedAt,
				Complete: complete, Truncated: complete && capture.truncated,
			},
		}
		if queueErr := service.spool.queueLogChunk(ctx, chunk); queueErr != nil {
			return queueErr
		}
		position.ByteOffset += length
		position.NextSequence++
		position.Complete = complete
		if length == 0 {
			break
		}
	}

	return nil
}

func completionTime(completion *CompletionManifest) time.Time {
	if completion != nil && !completion.ObservedAt.IsZero() {
		return completion.ObservedAt.UTC()
	}

	return time.Now().UTC()
}

func (service *service) flushLogChunks(ctx context.Context) error {
	chunks, err := service.spool.pendingLogChunks(ctx)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		if err := service.client.commitLogChunk(ctx, chunk); err != nil {
			return err
		}
		if err := service.spool.markLogChunkDelivered(
			ctx, chunk.Metadata.ExecutionID, chunk.Metadata.Stream, chunk.Metadata.Sequence,
		); err != nil {
			return err
		}
	}

	return nil
}

func readLogChunk(filePath string, offset, length int64) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open capture: %w", err)
	}
	contents := make([]byte, length)
	_, readErr := file.ReadAt(contents, offset)
	if length == 0 && errors.Is(readErr, io.EOF) {
		readErr = nil
	}
	closeErr := file.Close()
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("read capture: %w", readErr), closeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close capture: %w", closeErr)
	}

	return contents, nil
}

func logObjectKey(namespace, jobID, executionID, stream string, sequence int64) string {
	return filepath.ToSlash(filepath.Join(
		"namespaces", namespace, "jobs", jobID, "executions", executionID,
		"logs", stream, fmt.Sprintf("%08d.chunk", sequence),
	))
}
