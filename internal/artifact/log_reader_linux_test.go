//go:build linux

package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func readerTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default"} {
		if err := unix.Setxattr(root, name, testPOSIXACL(21901, 7, 5), 0); err != nil {
			if errors.Is(err, unix.EOPNOTSUPP) {
				t.Skip("test filesystem does not support POSIX ACLs")
			}
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, logReaderPolicyFilename), []byte(readerTestPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

func readerTestStore(t *testing.T, root string) *FilesystemStore {
	t.Helper()
	store, err := NewFilesystemStore("logs", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func assertReaderMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Fatalf("mode %o, want %o", info.Mode().Perm(), want)
	}
}

func TestPolicyProducerPreservesPrivateArtifactsAndSharesOnlyLogChunks(t *testing.T) {
	root := readerTestRoot(t)
	store := readerTestStore(t, root)
	private := strings.Replace(readerTestKey, "logs/stdout/00000001.chunk", "artifacts/result", 1)
	// Output publication may create shared ancestors before any log arrives.
	if _, err := store.PutImmutable(private, []byte("private output")); err != nil {
		t.Fatal(err)
	}
	assertReaderMode(t, filepath.Join(root, private), 0o600)
	assertReaderMode(t, filepath.Dir(filepath.Join(root, private)), 0o700)
	digest, err := store.PutImmutable(readerTestKey, []byte("synthetic log\n"))
	if err != nil {
		t.Fatal(err)
	}
	assertReaderMode(t, filepath.Join(root, readerTestKey), 0o640)
	assertReaderMode(t, filepath.Dir(filepath.Join(root, readerTestKey)), 0o750)
	if _, err = store.PutImmutable(readerTestKey, []byte("synthetic log\n")); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutImmutable(readerTestKey, []byte("changed")); err == nil {
		t.Fatal("accepted immutable conflict")
	}
	contents, err := store.ReadVerified(readerTestKey, 14, digest)
	if err != nil || string(contents) != "synthetic log\n" {
		t.Fatalf("verified read: %q %v", contents, err)
	}
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(root, readerTestKey)))
	if err != nil || len(entries) != 1 {
		t.Fatalf("left staging entries: %v %v", entries, err)
	}
	source := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(source, []byte("file output"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutFileImmutable(private+"-file", source, 1024); err != nil {
		t.Fatal(err)
	}
	assertReaderMode(t, filepath.Join(root, private+"-file"), 0o600)
}

func TestPolicyProducerRejectsMaskedExistingParentsWithoutChangingThem(t *testing.T) {
	root := readerTestRoot(t)
	store := readerTestStore(t, root)
	parent := filepath.Join(root, "namespaces")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(readerTestKey, []byte("must not publish")); err == nil {
		t.Fatal("accepted masked parent")
	}
	assertReaderMode(t, parent, 0o700)
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatal("publication continued after invalid parent")
	}
}

func TestPolicyProducerRejectsMissingBroaderAndRevokedACLs(t *testing.T) {
	for _, name := range []string{"missing", "other", "group", "changed policy", "removed policy"} {
		t.Run(name, func(t *testing.T) {
			root := readerTestRoot(t)
			store := readerTestStore(t, root)
			acl := testPOSIXACL(21901, 7, 5)
			switch name {
			case "missing":
				if err := unix.Removexattr(root, "system.posix_acl_default"); err != nil {
					t.Fatal(err)
				}
			case "group":
				acl[22] = 4
				if err := unix.Setxattr(root, "system.posix_acl_access", acl, 0); err != nil {
					t.Fatal(err)
				}
			case "other":
				acl[38] = 4
				if err := unix.Setxattr(root, "system.posix_acl_access", acl, 0); err != nil {
					t.Fatal(err)
				}
			case "changed policy":
				if err := os.WriteFile(filepath.Join(root, logReaderPolicyFilename), []byte(strings.Replace(readerTestPolicy, "21901", "21902", 1)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "removed policy":
				if err := os.Remove(filepath.Join(root, logReaderPolicyFilename)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.PutImmutable(readerTestKey, []byte("must not publish")); err == nil {
				t.Fatal("accepted changed access policy")
			}
			if _, err := os.Stat(filepath.Join(root, readerTestKey)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid policy published a log")
			}
		})
	}
}

func TestPolicyProducerRejectsSymlinksAndUnapprovedReplay(t *testing.T) {
	root := readerTestRoot(t)
	store := readerTestStore(t, root)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "namespaces")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(readerTestKey, []byte("blocked")); err == nil {
		t.Fatal("followed directory symlink")
	}
	if err := os.Remove(filepath.Join(root, "namespaces")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(readerTestKey, []byte("log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, readerTestKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(readerTestKey, []byte("log")); err == nil {
		t.Fatal("accepted unreadable existing replay")
	}
	assertReaderMode(t, filepath.Join(root, readerTestKey), 0o600)
	if err := os.Remove(filepath.Join(root, readerTestKey)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "object"), filepath.Join(root, readerTestKey)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(readerTestKey, []byte("log")); err == nil {
		t.Fatal("followed object symlink")
	}
}

func TestLogReaderPolicyFileMustBePrivateRegularAndNoFollow(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "directory", "public", "large", "foreign mapping"} {
		t.Run(kind, func(t *testing.T) {
			root := readerTestRoot(t)
			filename := filepath.Join(root, logReaderPolicyFilename)
			if err := os.Remove(filename); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), filename); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(filename, 0o600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(filename, 0o700); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.WriteFile(filename, []byte(readerTestPolicy), 0o644); err != nil { //nolint:gosec // Deliberately unsafe policy fixture must be rejected.
					t.Fatal(err)
				}
			case "large":
				if err := os.WriteFile(filename, []byte(strings.Repeat(" ", maximumLogReaderPolicyBytes+1)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "foreign mapping":
				if err := os.WriteFile(filename, []byte(strings.Replace(readerTestPolicy, `"logs"`, `"foreign"`, 1)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := NewFilesystemStore("logs", 1, root); err == nil {
				t.Fatal("accepted unsafe policy file")
			}
		})
	}
}

func TestPrivateStoreStillCreatesPrivateFilesWithoutPolicy(t *testing.T) {
	root := t.TempDir()
	store := readerTestStore(t, root)
	if _, err := store.PutImmutable(readerTestKey, []byte("private log")); err != nil {
		t.Fatal(err)
	}
	assertReaderMode(t, filepath.Join(root, readerTestKey), 0o600)
	assertReaderMode(t, filepath.Dir(filepath.Join(root, readerTestKey)), 0o700)
}
