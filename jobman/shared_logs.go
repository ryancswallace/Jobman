package jobman

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/controlclient"
)

type sharedLogsOptions struct {
	stream       string
	follow       bool
	lines        int64
	pollInterval time.Duration
	jsonOutput   bool
}

const (
	sharedLogStdout = "stdout"
	sharedLogStderr = "stderr"
	sharedLogBoth   = "both"
)

func newSharedLogsCommand(
	dependencies dependencies,
	root *rootOptions,
	shared *sharedOptions,
) *cobra.Command {
	options := &sharedLogsOptions{stream: sharedLogBoth, lines: -1, pollInterval: time.Second}
	command := &cobra.Command{
		Use:   "logs JOB",
		Short: "Read filesystem-backed logs for a shared job",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, arguments []string) error {
			if options.stream != sharedLogStdout && options.stream != sharedLogStderr &&
				options.stream != sharedLogBoth {
				return usageError(errors.New("--stream must be stdout, stderr, or both"))
			}
			if options.lines < -1 {
				return usageError(errors.New("--lines must be -1 or a nonnegative integer"))
			}
			if options.pollInterval <= 0 {
				return usageError(errors.New("--poll-interval must be positive"))
			}
			if options.follow && (options.jsonOutput || options.lines != -1) {
				return usageError(errors.New("--follow cannot be combined with --json or --lines"))
			}
			if options.jsonOutput && options.lines != -1 {
				return usageError(errors.New("--json cannot be combined with --lines"))
			}
			return withSharedContext(command, dependencies, root, shared, func(sharedContext sharedContext) error {
				return runSharedLogs(command, sharedContext, arguments[0], options)
			})
		},
	}
	command.Flags().StringVar(&options.stream, "stream", options.stream, "select stdout, stderr, or both")
	command.Flags().BoolVarP(&options.follow, "follow", "f", false, "follow output until publication completes")
	command.Flags().Int64VarP(&options.lines, "lines", "n", options.lines, "show the last N lines (-1 means all)")
	command.Flags().DurationVar(&options.pollInterval, "poll-interval", options.pollInterval, "manifest polling interval")
	command.Flags().BoolVar(&options.jsonOutput, "json", false, "emit the versioned logical manifest")

	return command
}

//nolint:gocognit // The follow loop keeps validation, verified reads, terminal detection, and output ordered.
func runSharedLogs(
	command *cobra.Command,
	shared sharedContext,
	jobID string,
	options *sharedLogsOptions,
) error {
	seen := make(map[string]int64)
	for {
		manifest, err := shared.client.GetJobLogs(command.Context(), jobID)
		if err != nil {
			return err
		}
		if options.jsonOutput {
			return writeJSON(command, filterSharedLogManifest(manifest, options.stream))
		}
		chunks, complete, truncated := selectSharedLogChunks(manifest, options.stream, seen)
		contents, err := readSharedLogChunks(shared.artifactRoots, chunks)
		if err != nil {
			return err
		}
		if !options.follow && options.lines >= 0 {
			contents = lastLines(contents, uint64(options.lines))
		}
		if len(contents) != 0 {
			if _, err = command.OutOrStdout().Write(contents); err != nil {
				return fmt.Errorf("write shared job logs: %w", err)
			}
		}
		if !options.follow || complete {
			if truncated {
				_, _ = fmt.Fprintln(command.ErrOrStderr(), "warning: shared job log capture was truncated")
			}

			return nil
		}
		job, err := shared.client.GetJob(command.Context(), jobID)
		if err != nil {
			return err
		}
		if job.Status.Phase == "terminal" {
			return errors.New("shared job reached terminal state before log publication completed")
		}
		if err := waitForSharedLogPoll(command.Context(), options.pollInterval); err != nil {
			return err
		}
	}
}

type selectedLogChunk struct {
	executionID string
	stream      string
	chunk       controlclient.LogChunk
}

func selectSharedLogChunks(
	manifest controlclient.LogManifest,
	selection string,
	seen map[string]int64,
) (selected []selectedLogChunk, complete, truncated bool) {
	latestRun := 0
	for _, stream := range manifest.Items {
		if stream.RunNumber > latestRun {
			latestRun = stream.RunNumber
		}
	}
	wanted := map[string]bool{
		sharedLogStdout: selection != sharedLogStderr,
		sharedLogStderr: selection != sharedLogStdout,
	}
	completeByStream := map[string]bool{}
	for _, stream := range manifest.Items {
		if stream.RunNumber != latestRun || !wanted[stream.Stream] {
			continue
		}
		key := stream.ExecutionID + "\x00" + stream.Stream
		completeByStream[stream.Stream] = stream.State == "complete"
		truncated = truncated || stream.Truncated
		for _, chunk := range stream.Chunks {
			if chunk.Sequence <= seen[key] {
				continue
			}
			selected = append(selected, selectedLogChunk{
				executionID: stream.ExecutionID, stream: stream.Stream, chunk: chunk,
			})
			seen[key] = chunk.Sequence
		}
	}
	selected = mergeSharedLogChunks(selected)
	complete = latestRun != 0
	for stream := range wanted {
		if wanted[stream] && !completeByStream[stream] {
			complete = false
		}
	}

	return selected, complete, truncated
}

// mergeSharedLogChunks approximates cross-stream ordering using capture time
// while always preserving the authoritative sequence within each source.
func mergeSharedLogChunks(chunks []selectedLogChunk) []selectedLogChunk {
	type source struct {
		chunks []selectedLogChunk
		next   int
	}
	byKey := make(map[string]*source)
	var sources []*source
	for _, chunk := range chunks {
		key := chunk.executionID + "\x00" + chunk.stream
		current := byKey[key]
		if current == nil {
			current = &source{}
			byKey[key] = current
			sources = append(sources, current)
		}
		current.chunks = append(current.chunks, chunk)
	}
	for _, current := range sources {
		sort.Slice(current.chunks, func(first, second int) bool {
			return current.chunks[first].chunk.Sequence < current.chunks[second].chunk.Sequence
		})
	}
	result := make([]selectedLogChunk, 0, len(chunks))
	for len(result) != len(chunks) {
		selected := -1
		for index, current := range sources {
			if current.next == len(current.chunks) {
				continue
			}
			if selected == -1 || sharedLogChunkBefore(
				current.chunks[current.next], sources[selected].chunks[sources[selected].next],
			) {
				selected = index
			}
		}
		result = append(result, sources[selected].chunks[sources[selected].next])
		sources[selected].next++
	}

	return result
}

func sharedLogChunkBefore(left, right selectedLogChunk) bool {
	if !left.chunk.CapturedAt.Equal(right.chunk.CapturedAt) {
		return left.chunk.CapturedAt.Before(right.chunk.CapturedAt)
	}
	if left.chunk.Sequence != right.chunk.Sequence {
		return left.chunk.Sequence < right.chunk.Sequence
	}

	return left.stream < right.stream
}

func filterSharedLogManifest(
	manifest controlclient.LogManifest,
	selection string,
) controlclient.LogManifest {
	if selection == sharedLogBoth {
		return manifest
	}
	filtered := manifest
	filtered.Items = make([]controlclient.LogStream, 0, len(manifest.Items))
	for _, stream := range manifest.Items {
		if stream.Stream == selection {
			filtered.Items = append(filtered.Items, stream)
		}
	}

	return filtered
}

func readSharedLogChunks(
	mappings map[string]config.SharedArtifactRoot,
	chunks []selectedLogChunk,
) ([]byte, error) {
	stores := make(map[string]*artifact.FilesystemStore)
	var result []byte
	for _, selected := range chunks {
		mapping, found := mappings[selected.chunk.StoreName]
		if !found || mapping.Version != selected.chunk.StoreVersion {
			return nil, fmt.Errorf(
				"shared artifact store %q version %d is not mapped in the selected profile",
				selected.chunk.StoreName, selected.chunk.StoreVersion,
			)
		}
		key := fmt.Sprintf("%s\x00%d", selected.chunk.StoreName, selected.chunk.StoreVersion)
		store := stores[key]
		if store == nil {
			var err error
			store, err = artifact.NewFilesystemStore(
				selected.chunk.StoreName, selected.chunk.StoreVersion, mapping.Path,
			)
			if err != nil {
				return nil, fmt.Errorf("open shared artifact store %q: %w", selected.chunk.StoreName, err)
			}
			stores[key] = store
		}
		contents, err := store.ReadVerified(
			selected.chunk.ObjectKey, selected.chunk.ByteLength, selected.chunk.Checksum,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"read shared %s log chunk %d: %w", selected.stream, selected.chunk.Sequence, err,
			)
		}
		result = append(result, contents...)
	}

	return result, nil
}

func waitForSharedLogPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("follow shared job logs: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
