package artifact

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFilesystemStoreImmutableRoundTrip(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("department-nfs", 1, root)
	if err != nil {
		t.Fatalf("NewFilesystemStore() error = %v", err)
	}
	if store.Name() != "department-nfs" || store.Version() != 1 {
		t.Fatalf("store identity = %q version %d", store.Name(), store.Version())
	}
	key := "namespaces/research/executions/33333333/logs/stdout/00000001.chunk"
	digest, err := store.PutImmutable(key, []byte("hello\n"))
	if err != nil {
		t.Fatalf("PutImmutable() error = %v", err)
	}
	if _, err = store.PutImmutable(key, []byte("hello\n")); err != nil {
		t.Fatalf("PutImmutable(replay) error = %v", err)
	}
	if _, err = store.PutImmutable(key, []byte("changed\n")); err == nil {
		t.Fatal("PutImmutable(conflict) error = nil")
	}
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(root, filepath.FromSlash(key))))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "00000001.chunk" {
		t.Fatalf("artifact directory entries = %#v", entries)
	}
	contents, err := store.ReadVerified(key, 6, digest)
	if err != nil || string(contents) != "hello\n" {
		t.Fatalf("ReadVerified() = %q, %v", contents, err)
	}
	if _, err = store.ReadVerified(key, 5, digest); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("ReadVerified(wrong length) error = %v", err)
	}
	if _, err = store.ReadVerified("missing", 0, Digest(nil)); err == nil {
		t.Fatalf("ReadVerified(missing) error = %v", err)
	}
}

func TestFilesystemStoreRejectsUnsafeMappings(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name    string
		version int64
		root    string
	}{
		{name: "Bad Name", version: 1, root: root},
		{name: "safe", version: 0, root: root},
		{name: "safe", version: 1, root: "relative"},
		{name: "safe", version: 1, root: filepath.Join(root, "missing")},
	} {
		if _, err := NewFilesystemStore(test.name, test.version, test.root); err == nil {
			t.Fatalf("NewFilesystemStore(%#v) error = nil", test)
		}
	}
	store, err := NewFilesystemStore("safe", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "/absolute", "../escape", "a/../b", `drive:C`, `a\b`} {
		if _, err = store.PutImmutable(key, nil); err == nil {
			t.Errorf("PutImmutable(%q) error = nil", key)
		}
	}
	fileRoot := filepath.Join(root, "file-root")
	if err = os.WriteFile(fileRoot, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewFilesystemStore("safe", 1, fileRoot); err == nil {
		t.Fatal("NewFilesystemStore(file root) error = nil")
	}
	if _, err = store.PutImmutable("file-root/object", nil); err == nil || !strings.Contains(err.Error(), "non-directory") {
		t.Fatalf("PutImmutable(file parent) error = %v", err)
	}
	destinationDirectory := filepath.Join(root, "destination-directory")
	if err = os.Mkdir(destinationDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutImmutable("destination-directory", nil); err == nil ||
		!strings.Contains(err.Error(), "regular file") {
		t.Fatalf("PutImmutable(directory destination) error = %v", err)
	}
	for _, digest := range []string{"", "md5:" + strings.Repeat("0", 32), "sha256:" + strings.Repeat("z", 64)} {
		if ValidDigest(digest) {
			t.Errorf("ValidDigest(%q) = true", digest)
		}
	}
	if _, err = store.ReadVerified("missing", -1, Digest(nil)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("ReadVerified(negative) error = %v", err)
	}
	if _, err = store.ReadVerified("missing", 0, "invalid"); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("ReadVerified(invalid digest) error = %v", err)
	}
	if runtime.GOOS != "windows" {
		linkedRoot := filepath.Join(t.TempDir(), "linked")
		if err = os.Symlink(root, linkedRoot); err != nil {
			t.Fatal(err)
		}
		if _, err = NewFilesystemStore("safe", 1, linkedRoot); err == nil {
			t.Fatal("NewFilesystemStore(symlink) error = nil")
		}
		intermediateLink := filepath.Join(root, "intermediate-link")
		if err = os.Symlink(t.TempDir(), intermediateLink); err != nil {
			t.Fatal(err)
		}
		if _, err = store.PutImmutable("intermediate-link/object", nil); err == nil ||
			!strings.Contains(err.Error(), "symbolic link") {
			t.Fatalf("PutImmutable(symlink parent) error = %v", err)
		}
	}
}

func TestFilesystemStoreDetectsTamperingAndSymlinks(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("safe", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	key := "logs/stdout/one.chunk"
	digest, err := store.PutImmutable(key, []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, filepath.FromSlash(key))
	if err = os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReadVerified(key, int64(len("original")), digest); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("ReadVerified(tampered) error = %v", err)
	}
	if runtime.GOOS != "windows" {
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err = os.Symlink(filepath.Join(root, "missing"), path); err != nil {
			t.Fatal(err)
		}
		if _, err = store.ReadVerified(key, 0, Digest(nil)); err == nil || !strings.Contains(err.Error(), "symbolic") {
			t.Fatalf("ReadVerified(symlink) error = %v", err)
		}
		if _, err = store.PutImmutable(key, nil); err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("PutImmutable(symlink) error = %v", err)
		}
	}
}

func TestFilesystemStoreFailureSurfaces(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("safe", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReadVerified("../escape", 0, Digest(nil)); err == nil {
		t.Fatal("ReadVerified(escaping key) error = nil")
	}
	directoryObject := filepath.Join(root, "directory-object")
	if err = os.Mkdir(directoryObject, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReadVerified("directory-object", 0, Digest(nil)); err == nil {
		t.Fatal("ReadVerified(directory) error = nil")
	}
	fileRoot := filepath.Join(t.TempDir(), "root-file")
	if err = os.WriteFile(fileRoot, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = makePrivateDirectories(fileRoot, filepath.Join(fileRoot, "child")); err == nil {
		t.Fatal("makePrivateDirectories(file root) error = nil")
	}
	if _, err = digestRegularFile(filepath.Join(root, "missing")); err == nil {
		t.Fatal("digestRegularFile(missing) error = nil")
	}
	if runtime.GOOS != "windows" {
		if err = os.Chmod(root, 0o500); err != nil { // #nosec G302 -- directory needs owner traversal.
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if chmodErr := os.Chmod(root, 0o700); chmodErr != nil { // #nosec G302 -- restore private directory traversal.
				t.Errorf("restore artifact root permissions: %v", chmodErr)
			}
		})
		if _, err = store.PutImmutable("cannot-stage", nil); err == nil {
			t.Fatal("PutImmutable(read-only root) error = nil")
		}
	}
}

func TestFilesystemFileTransferBoundaries(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("safe", 2, root)
	if err != nil || store.Root() != root {
		t.Fatalf("NewFilesystemStore() root = %q, %v", store.Root(), err)
	}
	if _, err = store.MaterializeFile("input", filepath.Join(t.TempDir(), "input"), 0, ""); err == nil {
		t.Fatal("MaterializeFile() accepted zero size limit")
	}
	if _, err = store.MaterializeFile("input", filepath.Join(t.TempDir(), "input"), 1, "bad"); err == nil {
		t.Fatal("MaterializeFile() accepted invalid digest")
	}
	digest, err := store.PutImmutable("input", []byte("input"))
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "nested", "input")
	object, err := store.MaterializeFile("input", destination, 10, digest)
	if err != nil || object.ByteLength != 5 {
		t.Fatalf("MaterializeFile() = %#v, %v", object, err)
	}
	if _, err = store.MaterializeFile("input", destination, 10, digest); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("MaterializeFile(existing) error = %v", err)
	}
	if _, err = store.MaterializeFile("input", filepath.Join(t.TempDir(), "input"), 4, digest); err == nil ||
		!strings.Contains(err.Error(), "bounded") {
		t.Fatalf("MaterializeFile(limit) error = %v", err)
	}
	if _, err = store.MaterializeFile("missing", filepath.Join(t.TempDir(), "input"), 10, ""); err == nil {
		t.Fatal("MaterializeFile() accepted missing source")
	}
	if _, err = store.MaterializeFile("input", filepath.Join(t.TempDir(), "input"), 10, Digest([]byte("other"))); err == nil {
		t.Fatal("MaterializeFile() accepted a mismatched digest")
	}
	blockedParent := filepath.Join(t.TempDir(), "parent")
	if err = os.WriteFile(blockedParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.MaterializeFile("input", filepath.Join(blockedParent, "input"), 10, digest); err == nil {
		t.Fatal("MaterializeFile() accepted a file as destination parent")
	}

	source := filepath.Join(t.TempDir(), "output")
	if err = os.WriteFile(source, []byte("result"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutFileImmutable("output", source, 0); err == nil {
		t.Fatal("PutFileImmutable() accepted zero size limit")
	}
	published, err := store.PutFileImmutable("output", source, 10)
	if err != nil || published.Checksum != Digest([]byte("result")) {
		t.Fatalf("PutFileImmutable() = %#v, %v", published, err)
	}
	if _, err = store.PutFileImmutable("output", source, 10); err != nil {
		t.Fatalf("PutFileImmutable(replay) error = %v", err)
	}
	if err = os.WriteFile(source, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutFileImmutable("output", source, 10); err == nil ||
		!strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("PutFileImmutable(conflict) error = %v", err)
	}
	if _, err = store.PutFileImmutable("large", source, 2); err == nil ||
		!strings.Contains(err.Error(), "bounded") {
		t.Fatalf("PutFileImmutable(limit) error = %v", err)
	}
	if _, err = store.PutFileImmutable("directory", t.TempDir(), 10); err == nil ||
		!strings.Contains(err.Error(), "regular") {
		t.Fatalf("PutFileImmutable(directory) error = %v", err)
	}
	if _, err = store.PutFileImmutable("missing", filepath.Join(t.TempDir(), "missing"), 10); err == nil {
		t.Fatal("PutFileImmutable() accepted missing source")
	}
	if _, err = store.PutFileImmutable("../escape", source, 10); err == nil {
		t.Fatal("PutFileImmutable() accepted an unsafe key")
	}
	blockedObjectParent := filepath.Join(root, "blocked")
	if err = os.WriteFile(blockedObjectParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutFileImmutable("blocked/output", source, 10); err == nil {
		t.Fatal("PutFileImmutable() accepted a file as object parent")
	}
	directoryObject := filepath.Join(root, "directory-output")
	if err = os.Mkdir(directoryObject, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutFileImmutable("directory-output", source, 10); err == nil {
		t.Fatal("PutFileImmutable() accepted a directory object")
	}
	if runtime.GOOS != "windows" {
		linkedSource := filepath.Join(t.TempDir(), "linked-output")
		if err = os.Symlink(source, linkedSource); err != nil {
			t.Fatal(err)
		}
		if _, err = store.PutFileImmutable("linked-output", linkedSource, 10); err == nil {
			t.Fatal("PutFileImmutable() accepted a symbolic-link source")
		}
	}
}
