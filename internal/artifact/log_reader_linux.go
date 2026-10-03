//go:build linux

package artifact

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const logDirectoryFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW

func loadLogReaderPolicy(root, name string, version int64) (*logReaderPolicy, error) {
	fd, err := unix.Open(root, logDirectoryFlags, 0)
	if err != nil {
		return nil, fmt.Errorf("open log reader policy root: %w", err)
	}
	defer unix.Close(fd)
	policy, err := readLogReaderPolicyAt(fd, name, version)
	if err != nil || policy == nil {
		return policy, err
	}
	if err := validateLogReaderNode(fd, policy, true, true); err != nil {
		return nil, err
	}
	return policy, nil
}

//nolint:nilnil // An absent opt-in policy preserves private defaults.
func readLogReaderPolicyAt(parent int, name string, version int64) (*logReaderPolicy, error) {
	fd, err := unix.Openat(parent, logReaderPolicyFilename, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open log reader policy: %w", err)
	}
	file := os.NewFile(uintptr(fd), logReaderPolicyFilename)
	defer file.Close()
	var info, root unix.Stat_t
	if statErr := unix.Fstat(fd, &info); statErr != nil {
		return nil, statErr
	}
	if statErr := unix.Fstat(parent, &root); statErr != nil {
		return nil, statErr
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Mode&0o7777 != 0o600 ||
		(info.Uid != root.Uid && info.Uid != 0) || info.Size < 1 || info.Size > maximumLogReaderPolicyBytes {
		return nil, errors.New("log reader policy must be a private bounded regular file owned by the store owner or root")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumLogReaderPolicyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read log reader policy: %w", err)
	}
	policy, err := decodeLogReaderPolicy(data, name, version)
	if err != nil {
		return nil, err
	}
	if err := validateLogReaderNode(fd, policy, false, false); err != nil {
		return nil, fmt.Errorf("validate private log reader policy ACL: %w", err)
	}
	return policy, nil
}

func validateLogReaderNode(fd int, policy *logReaderPolicy, directory, shared bool) error {
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		return err
	}
	wantType, owner, mask := uint32(unix.S_IFREG), uint16(6), uint16(0)
	if directory {
		wantType, owner = unix.S_IFDIR, 7
	}
	if shared {
		mask = 4
		if directory {
			mask = 5
		}
	}
	wantMode := uint32(owner)<<6 | uint32(mask)<<3
	if info.Mode&unix.S_IFMT != wantType || info.Mode&0o7777 != wantMode || info.Uid == policy.ReaderUID {
		return errors.New("log reader object type, owner or mode does not match the configured policy")
	}
	access, err := readLogACL(fd, "system.posix_acl_access")
	if err == nil {
		if aclErr := validatePOSIXLogACL(access, policy.ReaderUID, owner, mask); aclErr != nil {
			return aclErr
		}
		if directory {
			inherited, readErr := readLogACL(fd, "system.posix_acl_default")
			if readErr != nil {
				return fmt.Errorf("read required default log ACL: %w", readErr)
			}
			return validatePOSIXLogACL(inherited, policy.ReaderUID, 7, 5)
		}
		return nil
	}
	if !errors.Is(err, unix.ENODATA) && !errors.Is(err, unix.EOPNOTSUPP) {
		return fmt.Errorf("read log ACL: %w", err)
	}
	native, err := readLogACL(fd, "system.nfs4_acl")
	if err != nil {
		return fmt.Errorf("required log reader ACL is missing or unsupported: %w", err)
	}
	return validateNFSLogACL(native, policy.ReaderUID, directory, shared)
}

func readLogACL(fd int, name string) ([]byte, error) {
	data := make([]byte, maximumLogACLBytes)
	length, err := unix.Fgetxattr(fd, name, data)
	if err != nil {
		return nil, err
	}
	if length < 0 || length > len(data) {
		return nil, errors.New("log ACL exceeds bounds")
	}
	return data[:length], nil
}

type readerObjectPath struct {
	directories []int
	leaf        string
	shared      bool
	policy      *logReaderPolicy
	sharedDepth int
}

func (path *readerObjectPath) close() error {
	var result error
	for index := len(path.directories) - 1; index >= 0; index-- {
		result = errors.Join(result, unix.Close(path.directories[index]))
	}
	return result
}

func (store *FilesystemStore) openReaderObjectPath(key string) (result *readerObjectPath, resultErr error) {
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 32 {
		return nil, errors.New("log reader object path exceeds depth limit")
	}
	root, err := unix.Open(store.root, logDirectoryFlags, 0)
	if err != nil {
		return nil, fmt.Errorf("open log reader root: %w", err)
	}
	path := &readerObjectPath{directories: []int{root}, leaf: parts[len(parts)-1], shared: isSharedLogKey(key), policy: store.logReader, sharedDepth: sharedLogPrefix(parts)}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, path.close())
		}
	}()
	current, err := readLogReaderPolicyAt(root, store.name, store.version)
	if err != nil {
		return nil, err
	}
	if current == nil || *current != *store.logReader {
		return nil, errors.New("log reader policy changed; restart with verified store configuration")
	}
	if err := validateLogReaderNode(root, current, true, true); err != nil {
		return nil, err
	}
	prefix := path.sharedDepth
	for index, component := range parts[:len(parts)-1] {
		shared := index < prefix
		mode := uint32(0o700)
		if shared {
			mode = 0o750
		}
		parent := path.directories[len(path.directories)-1]
		child, openErr := openReaderDirectory(parent, component, mode)
		if openErr != nil {
			return nil, fmt.Errorf("open log reader directory: %w", openErr)
		}
		path.directories = append(path.directories, child)
		if err := validateLogReaderNode(child, current, true, shared); err != nil {
			return nil, err
		}
	}
	return path, nil
}

func openReaderDirectory(parent int, component string, mode uint32) (int, error) {
	child, err := unix.Openat(parent, component, logDirectoryFlags, 0)
	if !errors.Is(err, unix.ENOENT) {
		return child, err
	}
	if err := unix.Mkdirat(parent, component, mode); err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, err
	}
	return unix.Openat(parent, component, logDirectoryFlags, 0)
}

// Policy-enabled publication uses descriptor-relative opens and hard links.
// Every inherited ACL is validated before any payload is written. No ACL or
// existing mode is changed; preexisting masked paths require operator repair.
func (store *FilesystemStore) putReaderObject(key string, source io.Reader, expected, maximum int64) (result Object, resultErr error) {
	if expected < 0 || expected > maximum || expected == math.MaxInt64 {
		return Object{}, errors.New("invalid policy-protected artifact size limit")
	}
	path, err := store.openReaderObjectPath(key)
	if err != nil {
		return Object{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, path.close()) }()
	if path.shared && (expected > maximumSharedLogChunkBytes || maximum > maximumSharedLogChunkBytes) {
		return Object{}, errors.New("shared log chunk exceeds size limit")
	}
	parent := path.directories[len(path.directories)-1]
	file, temporary, err := createReaderObject(parent, path.shared)
	if err != nil {
		return Object{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, file.Close())
		if temporary != "" {
			resultErr = errors.Join(resultErr, unix.Unlinkat(parent, temporary, 0))
		}
	}()
	digest, err := writeReaderObject(path, file, source, expected)
	if err != nil {
		return Object{}, err
	}
	if err := publishReaderObject(path, file, temporary, expected, digest); err != nil {
		return Object{}, err
	}
	if err := unix.Unlinkat(parent, temporary, 0); err != nil {
		return Object{}, err
	}
	temporary = ""
	return Object{Key: key, ByteLength: expected, Checksum: digest}, nil
}

func writeReaderObject(path *readerObjectPath, file *os.File, source io.Reader, expected int64) (string, error) {
	if err := validateLogReaderNode(int(file.Fd()), path.policy, false, path.shared); err != nil {
		return "", err
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(source, expected+1))
	if err != nil {
		return "", fmt.Errorf("write policy-protected artifact: %w", err)
	}
	if written != expected {
		return "", errors.New("artifact changed or exceeded its size limit")
	}
	if err = file.Sync(); err != nil {
		return "", fmt.Errorf("sync policy-protected artifact: %w", err)
	}
	return digestPrefix + hex.EncodeToString(hash.Sum(nil)), nil
}

func publishReaderObject(path *readerObjectPath, file *os.File, temporary string, expected int64, digest string) error {
	if err := validateLogReaderNode(int(file.Fd()), path.policy, false, path.shared); err != nil {
		return err
	}
	for index, directory := range path.directories {
		if err := validateLogReaderNode(directory, path.policy, true, index <= path.sharedDepth); err != nil {
			return err
		}
	}
	parent := path.directories[len(path.directories)-1]
	if err := unix.Linkat(parent, temporary, parent, path.leaf, 0); errors.Is(err, unix.EEXIST) {
		if replayErr := verifyReaderReplay(parent, path, expected, digest); replayErr != nil {
			return replayErr
		}
	} else if err != nil {
		return fmt.Errorf("publish policy-protected artifact: %w", err)
	}
	if err := unix.Fsync(parent); err != nil {
		return fmt.Errorf("sync policy-protected artifact directory: %w", err)
	}
	return nil
}

func createReaderObject(parent int, shared bool) (*os.File, string, error) {
	mode := uint32(0o600)
	if shared {
		mode = 0o640
	}
	for range 3 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := ".jobman-object-" + hex.EncodeToString(random[:])
		fd, err := unix.Openat(parent, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("create policy-protected artifact: %w", err)
		}
		return os.NewFile(uintptr(fd), name), name, nil
	}
	return nil, "", errors.New("cannot allocate unique policy-protected artifact")
}

func verifyReaderReplay(parent int, path *readerObjectPath, size int64, digest string) error {
	fd, err := unix.Openat(parent, path.leaf, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open existing policy-protected artifact: %w", err)
	}
	file := os.NewFile(uintptr(fd), path.leaf)
	defer file.Close()
	if aclErr := validateLogReaderNode(fd, path.policy, false, path.shared); aclErr != nil {
		return aclErr
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, size+1))
	if err != nil {
		return err
	}
	if written != size || digestPrefix+hex.EncodeToString(hash.Sum(nil)) != digest {
		return errors.New("artifact object conflicts with existing content")
	}
	return nil
}
