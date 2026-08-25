package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type s3Call struct {
	name string
	args []string
}

type s3Response struct {
	output string
	err    error
	write  []byte
}

type fakeS3Runner struct {
	calls     []s3Call
	responses []s3Response
}

func (runner *fakeS3Runner) Run(
	_ context.Context,
	name string,
	arguments ...string,
) ([]byte, error) {
	runner.calls = append(runner.calls, s3Call{name: name, args: append([]string(nil), arguments...)})
	if len(runner.responses) == 0 {
		return nil, errors.New("unexpected AWS CLI call")
	}
	response := runner.responses[0]
	runner.responses = runner.responses[1:]
	if response.write != nil {
		if err := os.WriteFile(arguments[len(arguments)-1], response.write, 0o600); err != nil {
			return nil, err
		}
	}

	return []byte(response.output), response.err
}

func TestS3StoreImmutableRoundTripCommands(t *testing.T) {
	t.Parallel()
	contents := []byte("hello\n")
	encodedChecksum := digestBase64(Digest(contents))
	runner := &fakeS3Runner{responses: []s3Response{
		{output: "aws-cli/2.31"},
		{},
		{write: contents, output: `{"ChecksumSHA256":"` + encodedChecksum + `"}`},
	}}
	store, err := NewS3Store(S3StoreOptions{
		Name: "department-s3", Version: 3, Bucket: "department-artifacts-123",
		Prefix: "jobman/prod", Region: "us-east-1", ExpectedBucketOwner: "123456789012",
		AWSExecutable: "/usr/local/bin/aws", Runner: runner,
	})
	if err != nil {
		t.Fatalf("NewS3Store() error = %v", err)
	}
	if err = store.Probe(t.Context()); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	checksum, err := store.Put(t.Context(), "namespace/job/output", contents)
	if err != nil || checksum != Digest(contents) {
		t.Fatalf("Put() = %q, %v", checksum, err)
	}
	destination := filepath.Join(t.TempDir(), "inputs", "sample")
	object, err := store.Materialize(
		t.Context(), "namespace/job/output", destination, 100, Digest(contents),
	)
	if err != nil || object.ByteLength != int64(len(contents)) {
		t.Fatalf("Materialize() = %#v, %v", object, err)
	}
	read, err := os.ReadFile(destination)
	if err != nil || !reflect.DeepEqual(read, contents) {
		t.Fatalf("materialized contents = %q, %v", read, err)
	}
	if len(runner.calls) != 3 || runner.calls[0].name != "/usr/local/bin/aws" ||
		!containsSequence(runner.calls[1].args, "--if-none-match", "*") ||
		!containsSequence(runner.calls[1].args, "--expected-bucket-owner", "123456789012") ||
		!containsSequence(runner.calls[2].args, "--checksum-mode", "ENABLED") {
		t.Fatalf("AWS CLI calls = %#v", runner.calls)
	}
}

func TestS3StoreAcceptsIdenticalReplayAndRejectsConflict(t *testing.T) {
	t.Parallel()
	contents := []byte("same")
	checksum := digestBase64(Digest(contents))
	runner := &fakeS3Runner{responses: []s3Response{
		{err: errors.New("precondition failed")},
		{output: `{"ContentLength":4,"ChecksumSHA256":"` + checksum + `"}`},
		{err: errors.New("precondition failed")},
		{output: `{"ContentLength":5,"ChecksumSHA256":"` + checksum + `"}`},
	}}
	store, err := NewS3Store(S3StoreOptions{
		Name: "safe", Version: 1, Bucket: "safe-artifacts", Runner: runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(t.Context(), "same", contents); err != nil {
		t.Fatalf("Put(replay) error = %v", err)
	}
	if _, err = store.Put(t.Context(), "same", contents); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("Put(conflict) error = %v", err)
	}
}

func TestS3StoreValidationAndFailureSurfaces(t *testing.T) {
	t.Parallel()
	valid := S3StoreOptions{Name: "safe", Version: 1, Bucket: "safe-artifacts"}
	tests := []struct {
		name   string
		mutate func(*S3StoreOptions)
	}{
		{name: "name", mutate: func(value *S3StoreOptions) { value.Name = "Bad Name" }},
		{name: "version", mutate: func(value *S3StoreOptions) { value.Version = 0 }},
		{name: "bucket", mutate: func(value *S3StoreOptions) { value.Bucket = "Bad_Bucket" }},
		{name: "prefix", mutate: func(value *S3StoreOptions) { value.Prefix = "../escape" }},
		{name: "region", mutate: func(value *S3StoreOptions) { value.Region = "not a region" }},
		{name: "owner", mutate: func(value *S3StoreOptions) { value.ExpectedBucketOwner = "123" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.mutate(&candidate)
			if _, err := NewS3Store(candidate); err == nil {
				t.Fatal("NewS3Store() unexpectedly succeeded")
			}
		})
	}
	store, err := NewS3Store(S3StoreOptions{
		Name: "safe", Version: 1, Bucket: "safe-artifacts",
		Runner: &fakeS3Runner{responses: []s3Response{{err: errors.New("missing")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Probe(t.Context()); err == nil {
		t.Fatal("Probe() accepted a missing AWS CLI")
	}
	for _, key := range []string{"", "/absolute", "../escape", "a/../b", `a\b`, "a:b"} {
		if _, err = store.Put(t.Context(), key, nil); err == nil {
			t.Errorf("Put(%q) unexpectedly succeeded", key)
		}
	}
}

func TestS3StoreReadPublishAndIdentity(t *testing.T) {
	t.Parallel()
	contents := []byte("artifact")
	digest := Digest(contents)
	runner := &fakeS3Runner{responses: []s3Response{
		{write: contents, output: `{"ChecksumSHA256":"` + digestBase64(digest) + `"}`},
		{},
	}}
	store, err := NewS3Store(S3StoreOptions{
		Name: "research", Version: 7, Bucket: "research-artifacts", Prefix: "jobman",
		Region: "us-east-2", ExpectedBucketOwner: "123456789012", Runner: runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.Name() != "research" || store.Version() != 7 || store.Bucket() != "research-artifacts" ||
		store.Prefix() != "jobman" || store.Region() != "us-east-2" ||
		store.ExpectedBucketOwner() != "123456789012" || store.AWSExecutable() != "aws" {
		t.Fatalf("store identity = %#v", store)
	}
	read, err := store.Read(t.Context(), "input", int64(len(contents)), digest)
	if err != nil || !reflect.DeepEqual(read, contents) {
		t.Fatalf("Read() = %q, %v", read, err)
	}
	source := filepath.Join(t.TempDir(), "output")
	if err = os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	object, err := store.Publish(t.Context(), "output", source, 100)
	if err != nil || object.Checksum != digest || object.ByteLength != int64(len(contents)) {
		t.Fatalf("Publish() = %#v, %v", object, err)
	}
}

func TestS3StoreTransferFailureBoundaries(t *testing.T) {
	t.Parallel()
	contents := []byte("data")
	digest := Digest(contents)
	newStore := func(responses ...s3Response) *S3Store {
		store, err := NewS3Store(S3StoreOptions{
			Name: "safe", Version: 1, Bucket: "safe-artifacts",
			Runner: &fakeS3Runner{responses: responses},
		})
		if err != nil {
			t.Fatal(err)
		}

		return store
	}
	if _, err := newStore().Read(t.Context(), "input", -1, digest); err == nil {
		t.Fatal("Read() accepted a negative length")
	}
	if _, err := newStore(s3Response{err: errors.New("download failed")}).Read(
		t.Context(), "input", int64(len(contents)), digest,
	); err == nil {
		t.Fatal("Read() ignored a download failure")
	}
	if _, err := newStore().Materialize(t.Context(), "input", filepath.Join(t.TempDir(), "x"), 0, digest); err == nil {
		t.Fatal("Materialize() accepted a zero bound")
	}
	blockedParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blockedParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newStore().Materialize(
		t.Context(), "input", filepath.Join(blockedParent, "x"), 10, digest,
	); err == nil {
		t.Fatal("Materialize() ignored an invalid destination parent")
	}
	if _, err := newStore().Materialize(t.Context(), "input", filepath.Join(t.TempDir(), "x"), 10, "bad"); err == nil {
		t.Fatal("Materialize() accepted an invalid digest")
	}
	if _, err := newStore(s3Response{write: contents, output: `{`}).Materialize(
		t.Context(), "input", filepath.Join(t.TempDir(), "x"), 10, digest,
	); err == nil {
		t.Fatal("Materialize() accepted malformed metadata")
	}
	if _, err := newStore(s3Response{err: errors.New("download failed")}).Materialize(
		t.Context(), "input", filepath.Join(t.TempDir(), "x"), 10, digest,
	); err == nil || !strings.Contains(err.Error(), "download") {
		t.Fatalf("Materialize(download failure) error = %v", err)
	}
	if _, err := newStore(s3Response{write: contents}).Materialize(
		t.Context(), "input", filepath.Join(t.TempDir(), "x"), 3, digest,
	); err == nil {
		t.Fatal("Materialize() accepted an oversized object")
	}
	if _, err := newStore(s3Response{write: contents}).Materialize(
		t.Context(), "input", filepath.Join(t.TempDir(), "x"), 10, Digest([]byte("other")),
	); err == nil {
		t.Fatal("Materialize() accepted a workload checksum mismatch")
	}
	if _, err := newStore(s3Response{
		write: contents, output: `{"ChecksumSHA256":"wrong"}`,
	}).Materialize(t.Context(), "input", filepath.Join(t.TempDir(), "x"), 10, digest); err == nil {
		t.Fatal("Materialize() accepted an S3 checksum mismatch")
	}
	destination := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(destination, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newStore(s3Response{write: contents}).Materialize(
		t.Context(), "input", destination, 10, digest,
	); err == nil {
		t.Fatal("Materialize() overwrote an existing destination")
	}
	if _, err := newStore().Publish(t.Context(), "output", destination, 0); err == nil {
		t.Fatal("Publish() accepted a zero bound")
	}
	if _, err := newStore().Publish(t.Context(), "output", filepath.Join(t.TempDir(), "missing"), 10); err == nil {
		t.Fatal("Publish() accepted a missing source")
	}
	if _, err := newStore().Publish(t.Context(), "output", filepath.Dir(destination), 10); err == nil {
		t.Fatal("Publish() accepted a directory")
	}
	if _, err := newStore().Publish(t.Context(), "output", destination, 1); err == nil {
		t.Fatal("Publish() accepted an oversized file")
	}
	if _, err := newStore(
		s3Response{err: errors.New("put failed")}, s3Response{err: errors.New("head failed")},
	).Publish(t.Context(), "output", destination, 10); err == nil || !strings.Contains(err.Error(), "publish") {
		t.Fatalf("Publish() error = %v", err)
	}
	if _, err := newStore(
		s3Response{err: errors.New("put failed")}, s3Response{output: `{}`},
	).Publish(t.Context(), "output", destination, 10); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("Publish(empty metadata) error = %v", err)
	}
	if _, err := newStore(s3Response{write: contents}).Read(
		t.Context(), "input", int64(len(contents)+1), digest,
	); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("Read(length mismatch) error = %v", err)
	}
	if digestBase64("invalid") != "" {
		t.Fatal("digestBase64() accepted an invalid digest")
	}
}

func TestS3StoreExecutableValidationAndRunner(t *testing.T) {
	t.Parallel()
	if _, err := NewS3Store(S3StoreOptions{
		Name: "safe", Version: 1, Bucket: "safe-artifacts", AWSExecutable: "bad\x00command",
	}); err == nil {
		t.Fatal("NewS3Store() accepted a NUL executable")
	}
	if _, err := (ExecCommandRunner{}).Run(t.Context(), os.Args[0], "-test.run=^$"); err != nil {
		t.Fatalf("ExecCommandRunner.Run() error = %v", err)
	}
}

func containsSequence(values []string, sequence ...string) bool {
	for index := 0; index+len(sequence) <= len(values); index++ {
		if reflect.DeepEqual(values[index:index+len(sequence)], sequence) {
			return true
		}
	}

	return false
}
