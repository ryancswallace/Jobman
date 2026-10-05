package artifact

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemReaderDoesNotRequireProducerPolicy(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("logs", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := store.PutImmutable(readerTestKey, []byte("log"))
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately unusable producer configuration must not gate reads that
	// the OS permits and the caller's manifest already identifies.
	if err = os.WriteFile(filepath.Join(root, logReaderPolicyFilename), []byte("not a producer policy"), 0o000); err != nil {
		t.Fatal(err)
	}
	reader, err := NewFilesystemReader("logs", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	data, err := reader.ReadVerified(readerTestKey, 3, digest)
	if err != nil || string(data) != "log" {
		t.Fatalf("read published log: %q %v", data, err)
	}
	if _, err := NewFilesystemStore("logs", 1, root); err == nil {
		t.Fatal("writable constructor ignored producer policy")
	}
	if _, err := NewFilesystemReader("invalid/name", 1, root); err == nil {
		t.Fatal("reader accepted invalid mapping")
	}
}

func TestLogPublicationRequiresCanonicalBoundedChunks(t *testing.T) {
	store, err := NewFilesystemStore("logs", 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []struct {
		key  string
		data []byte
	}{
		{"artifact/output", []byte("log")},
		{readerTestKey, make([]byte, maximumSharedLogChunkBytes+1)},
	} {
		if _, err := store.PutLogImmutable(value.key, value.data); err == nil {
			t.Fatal("accepted noncanonical or unbounded log publication")
		}
	}
}
