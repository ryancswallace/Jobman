package logstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// TailSnapshot is a bounded point-in-time selection from one logical raw
// stream across all retained rotation segments.
type TailSnapshot struct {
	Data          []byte
	OriginalBytes uint64
	ByteStart     uint64
	ByteEnd       uint64
}

// Reader opens raw streams and the chunk index for one existing run.
type Reader struct {
	paths Paths
}

// OpenRun validates and opens the path metadata for an existing run.
func OpenRun(stateDir, jobID string, runNumber uint64) (*Reader, error) {
	paths, parentDirs, err := pathsForRun(stateDir, jobID, runNumber)
	if err != nil {
		return nil, err
	}
	for _, dir := range append(parentDirs, paths.Directory) {
		if err := inspectPrivateDirectory(dir); err != nil {
			return nil, err
		}
	}

	for _, path := range []string{paths.Stdout, paths.Stderr, paths.Index} {
		file, err := openPrivateRegularFile(path)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close validated log file %q: %w", path, err)
		}
	}

	return &Reader{paths: paths}, nil
}

// Paths returns the canonical path set for this reader.
func (reader *Reader) Paths() Paths {
	return reader.paths
}

// OpenStream opens one authoritative raw stream at its beginning.
func (reader *Reader) OpenStream(stream Stream) (io.ReadCloser, error) {
	path, err := reader.streamPath(stream)
	if err != nil {
		return nil, err
	}

	return openPrivateRegularFile(path)
}

// CopyStream copies one authoritative raw stream without text conversion.
func (reader *Reader) CopyStream(destination io.Writer, stream Stream) (int64, error) {
	segments, err := reader.streamSegments(stream)
	if err != nil {
		return 0, err
	}

	var written int64
	for _, segment := range segments {
		source, openErr := openPrivateRegularFile(segment.path)
		if openErr != nil {
			return written, openErr
		}
		count, copyErr := io.Copy(destination, source)
		written += count
		closeErr := source.Close()
		if copyErr != nil {
			copyErr = fmt.Errorf("copy %s segment %d: %w", stream, segment.number, copyErr)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("close %s segment %d: %w", stream, segment.number, closeErr)
		}
		if err := errors.Join(copyErr, closeErr); err != nil {
			return written, err
		}
	}

	return written, nil
}

// ScanIndex validates every complete index record and visits its valid prefix.
// A partial final record is reported through IndexStatus and is not an error.
func (reader *Reader) ScanIndex(visit func(Chunk) error) (IndexStatus, error) {
	index, err := openPrivateRegularFile(reader.paths.Index)
	if err != nil {
		return IndexStatus{}, err
	}

	status, scanErr := scanIndex(index, visit)
	closeErr := index.Close()
	if scanErr != nil {
		return status, errors.Join(scanErr, closeErr)
	}
	if closeErr != nil {
		return status, fmt.Errorf("close log chunk index: %w", closeErr)
	}

	if err := reader.addRawTailStatus(&status); err != nil {
		return status, err
	}

	return status, nil
}

// CopyCombined validates the complete index snapshot before copying chunks in
// their observed order. Raw bytes beyond the valid index prefix are reported
// as unindexed and are intentionally omitted because their ordering is unknown.
func (reader *Reader) CopyCombined(destination io.Writer) (int64, IndexStatus, error) {
	status, err := reader.ScanIndex(nil)
	if err != nil {
		return 0, status, err
	}

	index, err := openPrivateRegularFile(reader.paths.Index)
	if err != nil {
		return 0, status, err
	}

	written, copyErr := reader.copyIndexedPrefix(destination, index, status.Records, 0)
	closeErr := index.Close()
	if copyErr != nil {
		copyErr = fmt.Errorf("copy combined log: %w", copyErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close combined log files: %w", closeErr)
	}

	return written, status, errors.Join(copyErr, closeErr)
}

func (reader *Reader) copyIndexedPrefix(
	destination io.Writer,
	index *os.File,
	records uint64,
	skip uint64,
) (written int64, resultErr error) {
	sources := make(map[segmentKey]*os.File)
	defer func() {
		resultErr = errors.Join(resultErr, closeIndexedSources(sources))
	}()

	for recordNumber := range records {
		var encoded [recordSize]byte
		if _, err := io.ReadFull(index, encoded[:]); err != nil {
			return written, fmt.Errorf("reread validated log chunk index: %w", err)
		}
		chunk, err := decodeRecord(encoded)
		if err != nil {
			return written, err
		}
		if recordNumber < skip {
			continue
		}

		source, err := reader.indexedSource(sources, chunk)
		if err != nil {
			return written, err
		}
		count, err := copyChunk(destination, source, chunk)
		written += count
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func closeIndexedSources(sources map[segmentKey]*os.File) error {
	keys := make([]segmentKey, 0, len(sources))
	for key := range sources {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].stream != keys[right].stream {
			return keys[left].stream < keys[right].stream
		}

		return keys[left].segment < keys[right].segment
	})

	var closeErrors []error
	for _, key := range keys {
		source := sources[key]
		if err := source.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf(
				"close %s segment %d: %w",
				key.stream,
				max(key.segment, 1),
				err,
			))
		}
	}

	return errors.Join(closeErrors...)
}

func (reader *Reader) indexedSource(sources map[segmentKey]*os.File, chunk Chunk) (*os.File, error) {
	key := segmentKey{stream: chunk.Stream, segment: chunk.Segment}
	if source := sources[key]; source != nil {
		return source, nil
	}

	path := segmentPath(reader.paths, chunk.Stream, max(chunk.Segment, 1))
	source, err := openPrivateRegularFile(path)
	if err != nil {
		return nil, err
	}
	sources[key] = source

	return source, nil
}

func copyChunk(destination io.Writer, source *os.File, chunk Chunk) (int64, error) {
	offset, err := uint64ToInt64(chunk.Offset)
	if err != nil {
		return 0, fmt.Errorf("%w: chunk %d offset: %w", ErrCorruptIndex, chunk.Sequence, err)
	}
	length, err := uint64ToInt64(chunk.Length)
	if err != nil {
		return 0, fmt.Errorf("%w: chunk %d length: %w", ErrCorruptIndex, chunk.Sequence, err)
	}
	count, err := io.Copy(destination, io.NewSectionReader(source, offset, length))
	if err != nil {
		return count, fmt.Errorf("copy %s chunk %d: %w", chunk.Stream, chunk.Sequence, err)
	}
	if count != length {
		return count, fmt.Errorf(
			"%w: %s chunk %d refers to %d bytes but only %d exist",
			ErrCorruptIndex,
			chunk.Stream,
			chunk.Sequence,
			chunk.Length,
			count,
		)
	}

	return count, nil
}

func (reader *Reader) addRawTailStatus(status *IndexStatus) error {
	stdoutSize, err := reader.StreamSize(Stdout)
	if err != nil {
		return err
	}
	stderrSize, err := reader.StreamSize(Stderr)
	if err != nil {
		return err
	}

	if stdoutSize < status.IndexedStdoutBytes {
		return fmt.Errorf(
			"%w: stdout index covers %d bytes but file has %d",
			ErrCorruptIndex,
			status.IndexedStdoutBytes,
			stdoutSize,
		)
	}
	if stderrSize < status.IndexedStderrBytes {
		return fmt.Errorf(
			"%w: stderr index covers %d bytes but file has %d",
			ErrCorruptIndex,
			status.IndexedStderrBytes,
			stderrSize,
		)
	}

	status.UnindexedStdoutBytes = stdoutSize - status.IndexedStdoutBytes
	status.UnindexedStderrBytes = stderrSize - status.IndexedStderrBytes

	return nil
}

type segmentKey struct {
	stream  Stream
	segment uint16
}

type streamSegment struct {
	path   string
	number uint16
}

func (reader *Reader) streamSegments(stream Stream) ([]streamSegment, error) {
	if err := validateStream(stream); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(reader.paths.Directory)
	if err != nil {
		return nil, fmt.Errorf("read run log directory %q: %w", reader.paths.Directory, err)
	}
	base := stream.String()
	segments := make([]streamSegment, 0, 1)
	for _, entry := range entries {
		name := entry.Name()
		number, matched := parseSegmentFilename(base, name)
		if !matched {
			continue
		}
		segments = append(segments, streamSegment{
			path:   filepath.Join(reader.paths.Directory, name),
			number: number,
		})
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("inspect private log file %q: %w", reader.streamBasePath(stream), os.ErrNotExist)
	}

	sort.Slice(segments, func(left, right int) bool {
		return segments[left].number < segments[right].number
	})
	for index, segment := range segments {
		want := uint16(index + 1) // #nosec G115 -- segment count is bounded by uint16 filenames.
		if segment.number != want {
			return nil, fmt.Errorf(
				"%w: %s segment %d follows %d",
				ErrUnsafePath,
				stream,
				segment.number,
				index,
			)
		}
	}

	return segments, nil
}

func parseSegmentFilename(stream, name string) (uint16, bool) {
	if name == stream+".log" {
		return 1, true
	}
	prefix := stream + "."
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
	if len(digits) != 6 {
		return 0, false
	}
	value, err := strconv.ParseUint(digits, 10, 16)
	if err != nil || value < 2 || fmt.Sprintf("%06d", value) != digits {
		return 0, false
	}

	return uint16(value), true
}

// StreamSize returns the total authoritative byte count across every segment
// of one raw stream.
func (reader *Reader) StreamSize(stream Stream) (uint64, error) {
	segments, err := reader.streamSegments(stream)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, segment := range segments {
		size, sizeErr := privateRegularFileSize(segment.path)
		if sizeErr != nil {
			return 0, sizeErr
		}
		if total > ^uint64(0)-size {
			return 0, fmt.Errorf("%w: %s stream size overflows", ErrUnsafePath, stream)
		}
		total += size
	}

	return total, nil
}

// ReadTail reads at most maximumBytes from the end of one logical stream
// without buffering the complete log. A zero maximum selects no bytes while
// still returning the observed stream size.
//
//nolint:gocognit,cyclop // Bounded reverse selection validates every segment and close operation explicitly.
func (reader *Reader) ReadTail(
	ctx context.Context,
	stream Stream,
	maximumBytes uint64,
) (TailSnapshot, error) {
	if ctx == nil {
		return TailSnapshot{}, errors.New("read log tail: nil context")
	}
	maximumAllocation := uint64(^uint(0) >> 1)
	if maximumBytes > maximumAllocation {
		return TailSnapshot{}, errors.New("read log tail: maximum bytes exceeds addressable memory")
	}
	segments, err := reader.streamSegments(stream)
	if err != nil {
		return TailSnapshot{}, err
	}
	sizes := make([]uint64, len(segments))
	var total uint64
	for index, segment := range segments {
		if err := ctx.Err(); err != nil {
			return TailSnapshot{}, fmt.Errorf("read log tail: %w", err)
		}
		size, err := privateRegularFileSize(segment.path)
		if err != nil {
			return TailSnapshot{}, err
		}
		if total > ^uint64(0)-size {
			return TailSnapshot{}, fmt.Errorf("%w: %s stream size overflows", ErrUnsafePath, stream)
		}
		sizes[index] = size
		total += size
	}
	selected := min(total, maximumBytes)
	start := total - selected
	result := TailSnapshot{
		Data: make([]byte, 0, selected), OriginalBytes: total,
		ByteStart: start, ByteEnd: total,
	}
	if selected == 0 {
		return result, nil
	}
	logicalOffset := uint64(0)
	for index, segment := range segments {
		if err := ctx.Err(); err != nil {
			return TailSnapshot{}, fmt.Errorf("read log tail: %w", err)
		}
		segmentEnd := logicalOffset + sizes[index]
		if segmentEnd <= start {
			logicalOffset = segmentEnd
			continue
		}
		localStart := uint64(0)
		if start > logicalOffset {
			localStart = start - logicalOffset
		}
		length := sizes[index] - localStart
		file, err := openPrivateRegularFile(segment.path)
		if err != nil {
			return TailSnapshot{}, err
		}
		startOffset, err := uint64ToInt64(localStart)
		if err != nil {
			_ = file.Close()
			return TailSnapshot{}, fmt.Errorf("read log tail offset: %w", err)
		}
		lengthOffset, err := uint64ToInt64(length)
		if err != nil {
			_ = file.Close()
			return TailSnapshot{}, fmt.Errorf("read log tail length: %w", err)
		}
		before := len(result.Data)
		result.Data = append(result.Data, make([]byte, length)...)
		_, readErr := io.ReadFull(io.NewSectionReader(file, startOffset, lengthOffset), result.Data[before:])
		closeErr := file.Close()
		if readErr != nil {
			readErr = fmt.Errorf("read %s log tail segment %d: %w", stream, segment.number, readErr)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("close %s log tail segment %d: %w", stream, segment.number, closeErr)
		}
		if err := errors.Join(readErr, closeErr); err != nil {
			return TailSnapshot{}, err
		}
		logicalOffset = segmentEnd
	}
	if uint64(len(result.Data)) != selected {
		return TailSnapshot{}, io.ErrUnexpectedEOF
	}

	return result, nil
}

func (reader *Reader) streamBasePath(stream Stream) string {
	if stream == Stdout {
		return reader.paths.Stdout
	}

	return reader.paths.Stderr
}

func (reader *Reader) streamPath(stream Stream) (string, error) {
	if err := validateStream(stream); err != nil {
		return "", err
	}
	if stream == Stdout {
		return reader.paths.Stdout, nil
	}

	return reader.paths.Stderr, nil
}

func inspectPrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect private directory %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: %q is not a real directory", ErrUnsafePath, path)
	}

	return validatePrivateMode(path, info, directoryMode)
}

func openPrivateRegularFile(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect private log file %q: %w", path, err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q is not a regular file", ErrUnsafePath, path)
	}
	if modeErr := validatePrivateMode(path, before, fileMode); modeErr != nil {
		return nil, modeErr
	}
	if linkErr := validateSingleLink(path, before); linkErr != nil {
		return nil, linkErr
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open private log file %q: %w", path, err)
	}
	after, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("inspect opened log file %q: %w", path, err)
	}
	if !os.SameFile(before, after) || !after.Mode().IsRegular() {
		_ = file.Close()

		return nil, fmt.Errorf("%w: log file %q changed while opening", ErrUnsafePath, path)
	}
	if linkErr := validateSingleLink(path, after); linkErr != nil {
		_ = file.Close()

		return nil, linkErr
	}

	return file, nil
}

func privateRegularFileSize(path string) (uint64, error) {
	file, err := openPrivateRegularFile(path)
	if err != nil {
		return 0, err
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return 0, errors.Join(fmt.Errorf("stat private log file %q: %w", path, statErr), closeErr)
	}
	size, conversionErr := nonnegativeInt64ToUint64(info.Size())
	if conversionErr != nil {
		return 0, errors.Join(
			fmt.Errorf("%w: invalid file size for %q: %w", ErrUnsafePath, path, conversionErr),
			closeErr,
		)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close private log file %q: %w", path, closeErr)
	}

	return size, nil
}
