//go:build linux && integration

package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLogReaderProvisionedFilesystem is opt-in because its root must be an
// operator-provisioned disposable local/NFS directory. It deliberately leaves
// synthetic objects for an external named-reader/nonreader permission probe.
func TestLogReaderProvisionedFilesystem(t *testing.T) {
	root := os.Getenv("JOBMAN_TEST_LOG_READER_ROOT")
	if root == "" {
		t.Skip("set JOBMAN_TEST_LOG_READER_ROOT to a provisioned disposable directory")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		t.Fatal("integration root must be clean and absolute")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("integration root must be empty: %v %v", entries, err)
	}
	file, err := os.OpenFile(filepath.Join(root, logReaderPolicyFilename), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString(readerTestPolicy)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	store := readerTestStore(t, root)
	private := strings.Replace(readerTestKey, "logs/stdout/00000001.chunk", "artifacts/result", 1)
	if _, err := store.PutImmutable(private, []byte("private synthetic artifact\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutLogImmutable(readerTestKey, []byte("shared synthetic log\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutLogImmutable(readerTestKey, []byte("shared synthetic log\n")); err != nil {
		t.Fatal(err)
	}
	assertReaderMode(t, filepath.Join(root, private), 0o600)
	assertReaderMode(t, filepath.Dir(filepath.Join(root, private)), 0o700)
	assertReaderMode(t, filepath.Join(root, readerTestKey), 0o640)
	assertReaderMode(t, filepath.Dir(filepath.Join(root, readerTestKey)), 0o750)
	t.Logf("shared=%s private=%s", filepath.Join(root, readerTestKey), filepath.Join(root, private))
}

// TestLogReaderProvisionedRead runs separately as the configured named reader
// after the producer probe. It must not have access to the private policy.
func TestLogReaderProvisionedRead(t *testing.T) {
	root := os.Getenv("JOBMAN_TEST_LOG_READER_READ_ROOT")
	if root == "" {
		t.Skip("set JOBMAN_TEST_LOG_READER_READ_ROOT as the provisioned named reader")
	}
	reader, err := NewFilesystemReader("logs", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	contents := []byte("shared synthetic log\n")
	data, err := reader.ReadVerified(readerTestKey, int64(len(contents)), Digest(contents))
	if err != nil || string(data) != string(contents) {
		t.Fatalf("named reader cannot read verified chunk: %v", err)
	}
	if _, err := NewFilesystemStore("logs", 1, root); err == nil {
		t.Fatal("named reader unexpectedly initialized the producer policy")
	}
	private := strings.Replace(readerTestKey, "logs/stdout/00000001.chunk", "artifacts/result", 1)
	if _, err := reader.ReadVerified(private, 27, Digest([]byte("private synthetic artifact\n"))); err == nil {
		t.Fatal("named reader accessed private artifact")
	}
}
