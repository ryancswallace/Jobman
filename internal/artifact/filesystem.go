// Package artifact implements target-side and client-side artifact storage.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const digestPrefix = "sha256:"

var storeNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,62}[a-z0-9]$|^[a-z]$`)

// FilesystemStore maps logical immutable object keys beneath one local or NFS
// filesystem root. Physical roots are deployment configuration and never part
// of portable workload documents.
type FilesystemStore struct {
	name      string
	version   int64
	root      string
	logReader *logReaderPolicy
}

// NewFilesystemStore validates one logical store mapping. The root must
// already exist and must not itself be a symbolic link.
func NewFilesystemStore(name string, version int64, root string) (*FilesystemStore, error) {
	if !storeNamePattern.MatchString(name) {
		return nil, errors.New("artifact store name is invalid")
	}
	if version < 1 {
		return nil, errors.New("artifact store version must be positive")
	}
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("artifact store root must be a clean absolute path")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect artifact store root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("artifact store root must be a non-symlink directory")
	}

	policy, err := loadLogReaderPolicy(root, name, version)
	if err != nil {
		return nil, err
	}
	return &FilesystemStore{name: name, version: version, root: root, logReader: policy}, nil
}

// Name is the stable logical store identity.
func (store *FilesystemStore) Name() string { return store.name }

// Version identifies the immutable mapping generation.
func (store *FilesystemStore) Version() int64 { return store.version }

// Root is the deployment-local physical path. It is used only when spawning an
// isolated runner on the same host or shared NFS namespace.
func (store *FilesystemStore) Root() string { return store.root }

// PutImmutable writes bytes exactly once at key. An identical existing object
// is an idempotent replay; different bytes are a conflict. The completed file
// is installed with a same-directory hard link so a crash cannot leave a
// partial object at its published key.
func (store *FilesystemStore) PutImmutable(key string, contents []byte) (string, error) {
	destination, err := store.resolve(key)
	if err != nil {
		return "", err
	}
	if store.logReader != nil {
		object, writeErr := store.putReaderObject(key, bytes.NewReader(contents), int64(len(contents)), int64(len(contents)))
		return object.Checksum, writeErr
	}
	directory := filepath.Dir(destination)
	if err = makePrivateDirectories(store.root, directory); err != nil {
		return "", fmt.Errorf("prepare artifact object directory: %w", err)
	}
	digest := Digest(contents)
	file, err := os.CreateTemp(directory, ".jobman-object-*")
	if err != nil {
		return "", fmt.Errorf("create artifact object staging file: %w", err)
	}
	stagingPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(stagingPath)
	}()
	if _, err = file.Write(contents); err != nil {
		return "", fmt.Errorf("write artifact object: %w", err)
	}
	if err = file.Sync(); err != nil {
		return "", fmt.Errorf("sync artifact object: %w", err)
	}
	if err = file.Close(); err != nil {
		return "", fmt.Errorf("close artifact object: %w", err)
	}
	if err = os.Link(stagingPath, destination); errors.Is(err, fs.ErrExist) {
		existingDigest, digestErr := digestRegularFile(destination)
		if digestErr != nil {
			return "", fmt.Errorf("verify existing artifact object: %w", digestErr)
		}
		if existingDigest != digest {
			return "", errors.New("artifact object conflicts with existing content")
		}

		return digest, nil
	}
	if err != nil {
		return "", fmt.Errorf("publish artifact object: %w", err)
	}
	if err = os.Remove(stagingPath); err != nil {
		return "", fmt.Errorf("remove artifact object staging file: %w", err)
	}

	return digest, nil
}

// ReadVerified returns one bounded object after checking its expected length
// and digest. It refuses symbolic links anywhere below the configured root.
func (store *FilesystemStore) ReadVerified(key string, length int64, expectedDigest string) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("artifact object length is negative")
	}
	if !ValidDigest(expectedDigest) {
		return nil, errors.New("artifact object digest is invalid")
	}
	objectPath, err := store.resolve(key)
	if err != nil {
		return nil, err
	}
	if symlinkErr := rejectSymlinksBelow(store.root, objectPath); symlinkErr != nil {
		return nil, symlinkErr
	}
	file, err := os.Open(objectPath)
	if err != nil {
		return nil, fmt.Errorf("open artifact object: %w", err)
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, length+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read artifact object: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close artifact object: %w", closeErr)
	}
	if int64(len(contents)) != length {
		return nil, errors.New("artifact object length does not match manifest")
	}
	if Digest(contents) != expectedDigest {
		return nil, errors.New("artifact object checksum does not match manifest")
	}

	return contents, nil
}

// MaterializeFile verifies and copies one immutable object to a new private
// regular file. It never follows symlinks or overwrites an existing path.
//
//nolint:cyclop,gocognit // The streaming durability path validates each filesystem boundary explicitly.
func (store *FilesystemStore) MaterializeFile(
	key, destination string,
	maximumBytes int64,
	expectedDigest string,
) (Object, error) {
	if maximumBytes < 1 {
		return Object{}, errors.New("artifact size limit must be positive")
	}
	if expectedDigest != "" && !ValidDigest(expectedDigest) {
		return Object{}, errors.New("artifact object digest is invalid")
	}
	objectPath, err := store.resolve(key)
	if err != nil {
		return Object{}, err
	}
	if symlinkErr := rejectSymlinksBelow(store.root, objectPath); symlinkErr != nil {
		return Object{}, symlinkErr
	}
	source, err := os.Open(objectPath)
	if err != nil {
		return Object{}, fmt.Errorf("open artifact input: %w", err)
	}
	defer source.Close()
	information, err := source.Stat()
	if err != nil {
		return Object{}, fmt.Errorf("inspect artifact input: %w", err)
	}
	if !information.Mode().IsRegular() || information.Size() > maximumBytes {
		return Object{}, errors.New("artifact input is not a bounded regular file")
	}
	directory := filepath.Dir(destination)
	if err = os.MkdirAll(directory, 0o700); err != nil {
		return Object{}, fmt.Errorf("prepare artifact input destination: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".jobman-input-*")
	if err != nil {
		return Object{}, fmt.Errorf("create artifact input staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return Object{}, fmt.Errorf("protect artifact input: %w", err)
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(source, maximumBytes+1))
	if err != nil {
		return Object{}, fmt.Errorf("copy artifact input: %w", err)
	}
	if written != information.Size() || written > maximumBytes {
		return Object{}, errors.New("artifact input changed or exceeded its size limit while reading")
	}
	digest := digestPrefix + hex.EncodeToString(hash.Sum(nil))
	if expectedDigest != "" && digest != expectedDigest {
		return Object{}, errors.New("artifact input checksum does not match workload")
	}
	if err = temporary.Sync(); err != nil {
		return Object{}, fmt.Errorf("sync artifact input: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return Object{}, fmt.Errorf("close artifact input: %w", err)
	}
	if err = os.Link(temporaryPath, destination); errors.Is(err, fs.ErrExist) {
		return Object{}, errors.New("artifact input destination already exists")
	}
	if err != nil {
		return Object{}, fmt.Errorf("install artifact input: %w", err)
	}

	return Object{Key: key, ByteLength: written, Checksum: digest}, nil
}

// PutFileImmutable streams one bounded regular file into the store. The
// destination is immutable and an identical replay is accepted.
//
//nolint:cyclop,gocognit // The streaming immutable-write path handles every durability failure explicitly.
func (store *FilesystemStore) PutFileImmutable(key, sourcePath string, maximumBytes int64) (Object, error) {
	if maximumBytes < 1 {
		return Object{}, errors.New("artifact size limit must be positive")
	}
	destination, err := store.resolve(key)
	if err != nil {
		return Object{}, err
	}
	if symlinkErr := rejectSymlinksInExistingPath(filepath.Dir(sourcePath), sourcePath); symlinkErr != nil {
		return Object{}, symlinkErr
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return Object{}, fmt.Errorf("open artifact output: %w", err)
	}
	defer source.Close()
	information, err := source.Stat()
	if err != nil {
		return Object{}, fmt.Errorf("inspect artifact output: %w", err)
	}
	if !information.Mode().IsRegular() || information.Size() > maximumBytes {
		return Object{}, errors.New("artifact output is not a bounded regular file")
	}
	if store.logReader != nil {
		return store.putReaderObject(key, source, information.Size(), maximumBytes)
	}
	directory := filepath.Dir(destination)
	if err = makePrivateDirectories(store.root, directory); err != nil {
		return Object{}, fmt.Errorf("prepare artifact output directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".jobman-object-*")
	if err != nil {
		return Object{}, fmt.Errorf("create artifact output staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(source, maximumBytes+1))
	if err != nil {
		return Object{}, fmt.Errorf("copy artifact output: %w", err)
	}
	if written != information.Size() || written > maximumBytes {
		return Object{}, errors.New("artifact output changed or exceeded its size limit while reading")
	}
	if err = temporary.Sync(); err != nil {
		return Object{}, fmt.Errorf("sync artifact output: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return Object{}, fmt.Errorf("close artifact output: %w", err)
	}
	digest := digestPrefix + hex.EncodeToString(hash.Sum(nil))
	if err = os.Link(temporaryPath, destination); errors.Is(err, fs.ErrExist) {
		existingDigest, digestErr := digestRegularFile(destination)
		if digestErr != nil {
			return Object{}, fmt.Errorf("verify existing artifact output: %w", digestErr)
		}
		if existingDigest != digest {
			return Object{}, errors.New("artifact output conflicts with existing content")
		}
	} else if err != nil {
		return Object{}, fmt.Errorf("publish artifact output: %w", err)
	}

	return Object{Key: key, ByteLength: written, Checksum: digest}, nil
}

// Object is verified immutable object metadata.
type Object struct {
	Key        string
	ByteLength int64
	Checksum   string
}

// Digest returns the canonical checksum representation for bytes.
func Digest(contents []byte) string {
	sum := sha256.Sum256(contents)

	return digestPrefix + hex.EncodeToString(sum[:])
}

// ValidDigest reports whether value is a canonical SHA-256 checksum.
func ValidDigest(value string) bool {
	if len(value) != len(digestPrefix)+sha256.Size*2 || !strings.HasPrefix(value, digestPrefix) {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, digestPrefix))

	return err == nil
}

func (store *FilesystemStore) resolve(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, `\`) || strings.Contains(key, ":") {
		return "", errors.New("artifact object key is invalid")
	}
	clean := filepath.FromSlash(key)
	if filepath.Clean(clean) != clean {
		return "", errors.New("artifact object key is not normalized")
	}
	result := filepath.Join(store.root, clean)
	relative, err := filepath.Rel(store.root, result)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact object key escapes store root")
	}

	return result, nil
}

func makePrivateDirectories(root, destination string) error {
	relative, err := filepath.Rel(root, destination)
	if err != nil {
		return err
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "." || component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, fs.ErrNotExist) {
			if mkdirErr := os.Mkdir(current, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, fs.ErrExist) {
				return mkdirErr
			}
			info, statErr = os.Lstat(current)
		}
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("artifact object path contains a non-directory or symbolic link")
		}
	}

	return nil
}

func rejectSymlinksBelow(root, objectPath string) error {
	relative, err := filepath.Rel(root, objectPath)
	if err != nil {
		return err
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return fmt.Errorf("inspect artifact object path: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("artifact object path contains a symbolic link")
		}
	}

	return nil
}

func rejectSymlinksInExistingPath(root, objectPath string) error {
	root = filepath.Clean(root)
	return rejectSymlinksBelow(root, objectPath)
}

func digestRegularFile(filePath string) (string, error) {
	info, err := os.Lstat(filePath)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("artifact object is not a regular file")
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return "", errors.Join(copyErr, closeErr)
	}

	return digestPrefix + hex.EncodeToString(hash.Sum(nil)), nil
}
