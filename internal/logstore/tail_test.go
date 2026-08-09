package logstore

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadTailAcrossRotatedSegments(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	run, err := CreateRunWithOptions(stateDir, "job", 1, RunOptions{
		Rotation: RotationPolicy{SegmentBytes: 4, MaxSegmentsPerStream: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, appendErr := run.Append(Stdout, []byte("abcdefghij"), time.Now().UTC()); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := run.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reader, err := OpenRun(stateDir, "job", 1)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := reader.ReadTail(t.Context(), Stdout, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tail.Data, []byte("defghij")) || tail.OriginalBytes != 10 || tail.ByteStart != 3 || tail.ByteEnd != 10 {
		t.Fatalf("ReadTail() = %#v", tail)
	}
}

func TestReadTailZeroAndCancellation(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	run, err := CreateRun(stateDir, "job", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, appendErr := run.Append(Stderr, []byte("failure"), time.Now().UTC()); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := run.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reader, err := OpenRun(stateDir, "job", 1)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := reader.ReadTail(t.Context(), Stderr, 0)
	if err != nil || len(tail.Data) != 0 || tail.OriginalBytes != 7 {
		t.Fatalf("ReadTail(zero) = %#v, %v", tail, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := reader.ReadTail(ctx, Stderr, 4); err == nil {
		t.Fatal("ReadTail(canceled) error = nil")
	}
	if _, err := reader.ReadTail(t.Context(), Stderr, math.MaxUint64); err == nil {
		t.Fatal("ReadTail(unaddressable maximum) error = nil")
	}
}

func TestReadTailRejectsInvalidContextStreamAndMissingSegment(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	run, err := CreateRun(stateDir, "job", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, appendErr := run.Append(Stdout, []byte("output"), time.Now().UTC()); appendErr != nil {
		t.Fatal(appendErr)
	}
	paths := run.Paths()
	if closeErr := run.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reader, err := OpenRun(stateDir, "job", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadTail(nil, Stdout, 1); err == nil { //nolint:staticcheck // Explicitly verifies nil-context rejection.
		t.Fatal("ReadTail(nil context) error = nil")
	}
	if _, err := reader.ReadTail(t.Context(), Stream(99), 1); err == nil {
		t.Fatal("ReadTail(invalid stream) error = nil")
	}
	if err := os.Remove(paths.Stdout); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadTail(t.Context(), Stdout, 1); err == nil {
		t.Fatal("ReadTail(missing segment) error = nil")
	}
}
