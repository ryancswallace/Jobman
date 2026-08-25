package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	s3BucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	s3RegionPattern = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z0-9-]+-\d+$`)
	s3OwnerPattern  = regexp.MustCompile(`^\d{12}$`)
)

const (
	s3BucketFlag = "--bucket"
	s3KeyFlag    = "--key"
)

// CommandRunner is the test seam for exact-argument object-store commands.
type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

// ExecCommandRunner invokes a configured command directly without a shell.
type ExecCommandRunner struct{}

// Run executes one command with context cancellation and bounded arguments.
func (ExecCommandRunner) Run(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...) // #nosec G204 -- executable is administrator configuration.

	return command.CombinedOutput()
}

// S3StoreOptions configures a private S3 mapping. Credentials are deliberately
// absent: the AWS CLI uses its standard role/credential chain on the agent or
// compute node, so workload documents never receive cloud credentials.
type S3StoreOptions struct {
	Name                string
	Version             int64
	Bucket              string
	Prefix              string
	Region              string
	ExpectedBucketOwner string
	AWSExecutable       string
	Runner              CommandRunner
}

// S3Store maps immutable logical keys into a private S3 bucket prefix.
type S3Store struct {
	name                string
	version             int64
	bucket              string
	prefix              string
	region              string
	expectedBucketOwner string
	executable          string
	runner              CommandRunner
}

// NewS3Store validates one immutable target-side S3 mapping.
//
//nolint:cyclop // Validation keeps every externally supplied mapping component explicit.
func NewS3Store(options S3StoreOptions) (*S3Store, error) {
	if !storeNamePattern.MatchString(options.Name) || options.Version < 1 {
		return nil, errors.New("S3 artifact store identity is invalid")
	}
	if !s3BucketPattern.MatchString(options.Bucket) || strings.Contains(options.Bucket, "..") ||
		strings.Contains(options.Bucket, ".-") || strings.Contains(options.Bucket, "-.") {
		return nil, errors.New("S3 artifact bucket is invalid")
	}
	options.Prefix = strings.TrimSuffix(options.Prefix, "/")
	if options.Prefix != "" && !validObjectKey(options.Prefix) {
		return nil, errors.New("S3 artifact prefix is invalid")
	}
	if options.Region != "" && !s3RegionPattern.MatchString(options.Region) {
		return nil, errors.New("S3 artifact region is invalid")
	}
	if options.ExpectedBucketOwner != "" && !s3OwnerPattern.MatchString(options.ExpectedBucketOwner) {
		return nil, errors.New("S3 expected bucket owner is invalid")
	}
	if options.AWSExecutable == "" {
		options.AWSExecutable = "aws"
	}
	if strings.ContainsRune(options.AWSExecutable, 0) {
		return nil, errors.New("AWS CLI executable is invalid")
	}
	if options.Runner == nil {
		options.Runner = ExecCommandRunner{}
	}

	return &S3Store{
		name: options.Name, version: options.Version, bucket: options.Bucket,
		prefix: options.Prefix, region: options.Region,
		expectedBucketOwner: options.ExpectedBucketOwner,
		executable:          options.AWSExecutable, runner: options.Runner,
	}, nil
}

// Name is the stable logical store identity.
func (store *S3Store) Name() string { return store.name }

// Version identifies the immutable mapping generation.
func (store *S3Store) Version() int64 { return store.version }

// Bucket returns the deployment-local physical bucket name.
func (store *S3Store) Bucket() string { return store.bucket }

// Prefix returns the deployment-local physical key prefix.
func (store *S3Store) Prefix() string { return store.prefix }

// Region returns the explicitly selected AWS region, if any.
func (store *S3Store) Region() string { return store.region }

// ExpectedBucketOwner returns the confused-deputy protection configured for
// this mapping.
func (store *S3Store) ExpectedBucketOwner() string { return store.expectedBucketOwner }

// AWSExecutable returns the target-side AWS CLI executable.
func (store *S3Store) AWSExecutable() string { return store.executable }

// Probe verifies that the configured AWS CLI is callable. IAM and bucket
// permissions are exercised by the first transfer so Probe needs no broader
// AWS permissions than the actual execution path.
func (store *S3Store) Probe(ctx context.Context) error {
	if _, err := store.runner.Run(ctx, store.executable, "--version"); err != nil {
		return fmt.Errorf("probe AWS CLI: %w", err)
	}

	return nil
}

// Put uploads immutable bytes and accepts only byte-identical replays.
func (store *S3Store) Put(ctx context.Context, key string, contents []byte) (string, error) {
	file, err := os.CreateTemp("", ".jobman-s3-put-*")
	if err != nil {
		return "", fmt.Errorf("create S3 upload staging file: %w", err)
	}
	filename := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(filename)
	}()
	if err = file.Chmod(0o600); err != nil {
		return "", fmt.Errorf("protect S3 upload staging file: %w", err)
	}
	if _, err = file.Write(contents); err != nil {
		return "", fmt.Errorf("write S3 upload staging file: %w", err)
	}
	if err = file.Sync(); err != nil {
		return "", fmt.Errorf("sync S3 upload staging file: %w", err)
	}
	if err = file.Close(); err != nil {
		return "", fmt.Errorf("close S3 upload staging file: %w", err)
	}
	object, err := store.publishFile(ctx, key, filename, int64(len(contents)), Digest(contents))
	if err != nil {
		return "", err
	}

	return object.Checksum, nil
}

// Read downloads and verifies one bounded object into memory.
func (store *S3Store) Read(
	ctx context.Context,
	key string,
	length int64,
	expectedDigest string,
) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("artifact object length is negative")
	}
	directory, err := os.MkdirTemp("", ".jobman-s3-read-*")
	if err != nil {
		return nil, fmt.Errorf("create S3 download directory: %w", err)
	}
	defer os.RemoveAll(directory)
	destination := filepath.Join(directory, "object")
	if _, err = store.Materialize(ctx, key, destination, max(1, length), expectedDigest); err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(destination)
	if err != nil {
		return nil, fmt.Errorf("read S3 object: %w", err)
	}
	if int64(len(contents)) != length {
		return nil, errors.New("artifact object length does not match manifest")
	}

	return contents, nil
}

// Materialize downloads, checksums, and atomically installs one object as a
// private regular file without overwriting an existing destination.
//
//nolint:cyclop,gocognit // The download path deliberately checks each filesystem and checksum boundary.
func (store *S3Store) Materialize(
	ctx context.Context,
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
	physicalKey, err := store.resolve(key)
	if err != nil {
		return Object{}, err
	}
	if err = os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return Object{}, fmt.Errorf("prepare artifact input destination: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".jobman-s3-input-*")
	if err != nil {
		return Object{}, fmt.Errorf("create S3 input staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return Object{}, fmt.Errorf("protect S3 input staging file: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return Object{}, fmt.Errorf("close S3 input staging file: %w", err)
	}
	output, err := store.runner.Run(ctx, store.executable, store.getArguments(physicalKey, temporaryPath)...)
	if err != nil {
		return Object{}, fmt.Errorf("download S3 artifact: %w", err)
	}
	var response struct {
		ChecksumSHA256 string `json:"ChecksumSHA256"`
	}
	if len(output) != 0 && json.Unmarshal(output, &response) != nil {
		return Object{}, errors.New("download S3 artifact: malformed AWS CLI response")
	}
	information, err := os.Lstat(temporaryPath)
	if err != nil {
		return Object{}, fmt.Errorf("inspect S3 artifact input: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode()&os.ModeSymlink != 0 || information.Size() > maximumBytes {
		return Object{}, errors.New("S3 artifact input is not a bounded regular file")
	}
	digest, err := digestRegularFile(temporaryPath)
	if err != nil {
		return Object{}, fmt.Errorf("checksum S3 artifact input: %w", err)
	}
	if expectedDigest != "" && digest != expectedDigest {
		return Object{}, errors.New("artifact input checksum does not match workload")
	}
	if response.ChecksumSHA256 != "" && response.ChecksumSHA256 != digestBase64(digest) {
		return Object{}, errors.New("S3 response checksum does not match downloaded object")
	}
	if err = os.Link(temporaryPath, destination); errors.Is(err, fs.ErrExist) {
		return Object{}, errors.New("artifact input destination already exists")
	}
	if err != nil {
		return Object{}, fmt.Errorf("install S3 artifact input: %w", err)
	}

	return Object{Key: key, ByteLength: information.Size(), Checksum: digest}, nil
}

// Publish streams one bounded regular file into S3 with create-only
// conditional semantics.
func (store *S3Store) Publish(
	ctx context.Context,
	key, sourcePath string,
	maximumBytes int64,
) (Object, error) {
	if maximumBytes < 1 {
		return Object{}, errors.New("artifact size limit must be positive")
	}
	information, err := os.Lstat(sourcePath)
	if err != nil {
		return Object{}, fmt.Errorf("inspect artifact output: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode()&os.ModeSymlink != 0 || information.Size() > maximumBytes {
		return Object{}, errors.New("artifact output is not a bounded regular file")
	}
	digest, err := digestRegularFile(sourcePath)
	if err != nil {
		return Object{}, fmt.Errorf("checksum artifact output: %w", err)
	}

	return store.publishFile(ctx, key, sourcePath, information.Size(), digest)
}

func (store *S3Store) publishFile(
	ctx context.Context,
	key, sourcePath string,
	length int64,
	digest string,
) (Object, error) {
	physicalKey, err := store.resolve(key)
	if err != nil {
		return Object{}, err
	}
	arguments := make([]string, 0, 15+len(store.commonArguments()))
	arguments = append(arguments,
		"--no-cli-pager", "s3api", "put-object", s3BucketFlag, store.bucket,
		s3KeyFlag, physicalKey, "--body", sourcePath, "--if-none-match", "*",
		"--checksum-algorithm", "SHA256", "--checksum-sha256", digestBase64(digest),
	)
	arguments = append(arguments, store.commonArguments()...)
	if _, err = store.runner.Run(ctx, store.executable, arguments...); err == nil {
		return Object{Key: key, ByteLength: length, Checksum: digest}, nil
	}
	originalErr := err
	metadata, headErr := store.head(ctx, physicalKey)
	if headErr == nil && metadata.ContentLength == length && metadata.ChecksumSHA256 == digestBase64(digest) {
		return Object{Key: key, ByteLength: length, Checksum: digest}, nil
	}
	if headErr == nil {
		return Object{}, errors.New("artifact output conflicts with existing S3 object")
	}

	return Object{}, fmt.Errorf("publish S3 artifact: %w", originalErr)
}

type s3Metadata struct {
	ContentLength  int64  `json:"ContentLength"`
	ChecksumSHA256 string `json:"ChecksumSHA256"`
}

func (store *S3Store) head(ctx context.Context, physicalKey string) (s3Metadata, error) {
	arguments := make([]string, 0, 9+len(store.commonArguments()))
	arguments = append(arguments,
		"--no-cli-pager", "s3api", "head-object", s3BucketFlag, store.bucket,
		s3KeyFlag, physicalKey, "--checksum-mode", "ENABLED",
	)
	arguments = append(arguments, store.commonArguments()...)
	output, err := store.runner.Run(ctx, store.executable, arguments...)
	if err != nil {
		return s3Metadata{}, err
	}
	var result s3Metadata
	if err = json.Unmarshal(output, &result); err != nil || result.ContentLength < 0 {
		return s3Metadata{}, errors.New("malformed S3 metadata response")
	}

	return result, nil
}

func (store *S3Store) getArguments(physicalKey, destination string) []string {
	arguments := []string{
		"--no-cli-pager", "s3api", "get-object", s3BucketFlag, store.bucket,
		s3KeyFlag, physicalKey, "--checksum-mode", "ENABLED",
	}
	arguments = append(arguments, store.commonArguments()...)

	return append(arguments, destination)
}

func (store *S3Store) commonArguments() []string {
	var result []string
	if store.region != "" {
		result = append(result, "--region", store.region)
	}
	if store.expectedBucketOwner != "" {
		result = append(result, "--expected-bucket-owner", store.expectedBucketOwner)
	}

	return result
}

func (store *S3Store) resolve(key string) (string, error) {
	if !validObjectKey(key) {
		return "", errors.New("artifact object key is invalid or not normalized")
	}
	if store.prefix == "" {
		return key, nil
	}

	return store.prefix + "/" + key, nil
}

func validObjectKey(key string) bool {
	return key != "" && !strings.HasPrefix(key, "/") && !strings.ContainsAny(key, "\\:\x00\r\n") &&
		path.Clean(key) == key && key != "." && key != ".." && !strings.HasPrefix(key, "../")
}

func digestBase64(digest string) string {
	decoded, err := hex.DecodeString(strings.TrimPrefix(digest, digestPrefix))
	if err != nil || len(decoded) != sha256.Size {
		return ""
	}

	return base64.StdEncoding.EncodeToString(decoded)
}
