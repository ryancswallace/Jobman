package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFilesystemStoreInterfaceOperationsAndCancellation(t *testing.T) {
	t.Parallel()
	store, err := NewFilesystemStore("store", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	contents := []byte("contents")
	digest, err := store.Put(t.Context(), "input", contents)
	if err != nil || digest != Digest(contents) {
		t.Fatalf("Put() = %q, %v", digest, err)
	}
	read, err := store.Read(t.Context(), "input", int64(len(contents)), digest)
	if err != nil || !reflect.DeepEqual(read, contents) {
		t.Fatalf("Read() = %q, %v", read, err)
	}
	destination := filepath.Join(t.TempDir(), "materialized")
	if _, err = store.Materialize(t.Context(), "input", destination, 100, digest); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	source := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Publish(t.Context(), "output", source, 100); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	operations := []func() error{
		func() error { _, operationErr := store.Put(canceled, "other", nil); return operationErr },
		func() error {
			_, operationErr := store.Read(canceled, "input", int64(len(contents)), digest)
			return operationErr
		},
		func() error {
			_, operationErr := store.Materialize(canceled, "input", filepath.Join(t.TempDir(), "x"), 100, digest)
			return operationErr
		},
		func() error { _, operationErr := store.Publish(canceled, "other", source, 100); return operationErr },
	}
	for index, operation := range operations {
		if operationErr := operation(); !errors.Is(operationErr, context.Canceled) {
			t.Errorf("operation %d error = %v", index, operationErr)
		}
	}
}

func TestPutLogPreservesExistingStoreImplementationsAndCancellation(t *testing.T) {
	store, err := NewFilesystemStore("logs", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// An embedded Store intentionally exposes only the existing interface,
	// matching third-party implementations that do not provide log ACLs.
	legacy := struct{ Store }{store}
	for _, implementation := range []Store{store, legacy} {
		if digest, err := PutLog(t.Context(), implementation, readerTestKey, []byte("log")); err != nil || digest != Digest([]byte("log")) {
			t.Fatalf("publish log: %s %v", digest, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := PutLog(ctx, implementation, readerTestKey, []byte("log")); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled publication: %v", err)
		}
	}
}
