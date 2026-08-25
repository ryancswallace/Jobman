package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ryancswallace/jobman/internal/artifact"
)

type artifactStoreOptions struct {
	name            string
	version         int64
	root            string
	s3Bucket        string
	s3Prefix        string
	s3Region        string
	s3ExpectedOwner string
	awsExecutable   string
	s3Runner        artifact.CommandRunner
}

//nolint:cyclop // Store selection validates two mutually exclusive deployment mappings.
func newArtifactStore(
	ctx context.Context,
	options artifactStoreOptions,
) (artifact.Store, string, error) {
	configured := options.name != "" || options.root != "" || options.s3Bucket != "" ||
		options.s3Prefix != "" || options.s3Region != "" || options.s3ExpectedOwner != ""
	if !configured {
		return nil, "", nil
	}
	if options.name == "" || options.version < 1 {
		return nil, "", errors.New("artifact store name and positive version are required")
	}
	if (options.root == "") == (options.s3Bucket == "") {
		return nil, "", errors.New("exactly one filesystem root or S3 bucket is required")
	}
	if options.root != "" {
		if options.s3Prefix != "" || options.s3Region != "" || options.s3ExpectedOwner != "" {
			return nil, "", errors.New("S3 options require an S3 artifact bucket")
		}
		store, err := artifact.NewFilesystemStore(options.name, options.version, options.root)

		return store, "artifact-filesystem", err
	}
	store, err := artifact.NewS3Store(artifact.S3StoreOptions{
		Name: options.name, Version: options.version, Bucket: options.s3Bucket,
		Prefix: options.s3Prefix, Region: options.s3Region,
		ExpectedBucketOwner: options.s3ExpectedOwner,
		AWSExecutable:       options.awsExecutable, Runner: options.s3Runner,
	})
	if err != nil {
		return nil, "", err
	}
	if err = store.Probe(ctx); err != nil {
		return nil, "", fmt.Errorf("probe S3 artifact store: %w", err)
	}

	return store, "artifact-s3", nil
}

func appendArtifactRunnerArguments(
	arguments []string,
	store artifact.Store,
	maximumBytes int64,
) []string {
	if maximumBytes == 0 {
		maximumBytes = defaultMaximumArtifactBytes
	}
	arguments = append(arguments, "--max-artifact-bytes", strconv.FormatInt(maximumBytes, 10))
	if store == nil {
		return arguments
	}
	arguments = append(arguments,
		"--artifact-store", store.Name(),
		"--artifact-store-version", strconv.FormatInt(store.Version(), 10),
	)
	switch selected := store.(type) {
	case *artifact.FilesystemStore:
		arguments = append(arguments, "--artifact-root", selected.Root())
	case *artifact.S3Store:
		arguments = append(arguments, "--artifact-s3-bucket", selected.Bucket())
		if selected.Prefix() != "" {
			arguments = append(arguments, "--artifact-s3-prefix", selected.Prefix())
		}
		if selected.Region() != "" {
			arguments = append(arguments, "--artifact-s3-region", selected.Region())
		}
		if selected.ExpectedBucketOwner() != "" {
			arguments = append(arguments, "--artifact-s3-expected-owner", selected.ExpectedBucketOwner())
		}
		if selected.AWSExecutable() != "aws" {
			arguments = append(arguments, "--aws-cli", selected.AWSExecutable())
		}
	}

	return arguments
}
