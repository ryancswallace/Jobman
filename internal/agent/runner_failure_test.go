package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryancswallace/jobman/internal/artifact"
	"github.com/ryancswallace/jobman/protocol"
)

func TestRunExecutionRejectsInvalidLimitsBeforeClaiming(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		options ExecutionOptions
		want    string
	}{
		{name: "log limit", options: ExecutionOptions{}, want: "maximum log bytes"},
		{
			name: "artifact limit", options: ExecutionOptions{MaximumLogBytes: 1024, MaximumArtifactBytes: -1},
			want: "maximum artifact bytes",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateDirectory := t.TempDir()
			assignment, authorization := testAssignment(t, "unused", nil, nil)
			directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
			if err != nil {
				t.Fatal(err)
			}
			err = RunExecutionWithOptions(t.Context(), stateDirectory, testExecutionID, test.options)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("RunExecutionWithOptions() error = %v, want %q", err, test.want)
			}
			if _, err = os.Stat(filepath.Join(directory, claimFilename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid limits created a launch claim: %v", err)
			}
		})
	}
}

func TestRunExecutionRecordsArtifactFailures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		missing    bool
		input      bool
		wantCode   string
		wantError  bool
		wantLaunch bool
	}{
		{name: "missing mapping", missing: true, wantCode: "artifact_store_unavailable", wantError: true},
		{name: "missing input", input: true, wantCode: "artifact_stage_failed", wantError: true},
		{name: "missing required output", wantCode: "artifact_publish_failed", wantLaunch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := artifact.NewFilesystemStore("department-nfs", 3, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			assignment, authorization := testAssignment(
				t, os.Args[0], []string{"-test.run=^TestAgentRunnerHelper$"},
				map[string]string{"JOBMAN_AGENT_HELPER": "1"},
			)
			document := assignment.Document
			artifacts := &protocol.Artifacts{Outputs: []protocol.OutputArtifact{{
				Name: "result", Source: "outputs:/missing.txt",
				Destination: "artifact://department-nfs/results/result.txt", Required: true,
			}}}
			if test.input {
				artifacts.Inputs = []protocol.InputArtifact{{
					Name: "input", Source: "artifact://department-nfs/inputs/missing.txt",
					Target: "inputs:/sample.txt", Checksum: "sha256:" + strings.Repeat("0", 64),
				}}
			}
			document.Spec.EffectiveExecution.Spec.Workload.Document.Spec.Artifacts = artifacts
			document.Spec.EffectiveExecution.Spec.ArtifactStores = []protocol.ArtifactStoreBinding{{
				Name: "department-nfs", Version: 3,
			}}
			assignment = resealAssignment(t, document)
			authorization.Spec.EffectiveExecutionDigest = assignment.EffectiveExecutionDigest
			stateDirectory := t.TempDir()
			directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
			if err != nil {
				t.Fatal(err)
			}
			options := ExecutionOptions{MaximumLogBytes: 1024, ArtifactStore: store}
			if test.missing {
				options.ArtifactStore = nil
			}
			err = RunExecutionWithOptions(t.Context(), stateDirectory, testExecutionID, options)
			if (err != nil) != test.wantError {
				t.Fatalf("RunExecutionWithOptions() error = %v, want error %t", err, test.wantError)
			}
			completion, err := readCompletionManifest(directory)
			if err != nil || completion.Result.Outcome != "failure" || completion.Result.FailureCode != test.wantCode {
				t.Fatalf("completion = %#v, %v", completion, err)
			}
			if len(completion.Artifacts) != 0 {
				t.Fatalf("failed publication recorded artifacts: %#v", completion.Artifacts)
			}
			_, err = readStartManifest(directory)
			if test.wantLaunch && err != nil || !test.wantLaunch && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("start manifest error = %v, want launch %t", err, test.wantLaunch)
			}
		})
	}
}

func TestRunExecutionRecordsStartManifestWriteFailure(t *testing.T) {
	t.Parallel()
	stateDirectory := t.TempDir()
	assignment, authorization := testAssignment(
		t, os.Args[0], []string{"-test.run=^TestAgentRunnerHelper$"},
		map[string]string{"JOBMAN_AGENT_HELPER": "block"},
	)
	directory, err := prepareExecutionFiles(stateDirectory, assignment, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(directory, startedFilename), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = RunExecution(t.Context(), stateDirectory, testExecutionID); err == nil {
		t.Fatal("RunExecution() accepted an unwritable start manifest")
	}
	completion, err := readCompletionManifest(directory)
	if err != nil || completion.Result.Outcome != "failure" || completion.Result.FailureCode != "start_manifest_failed" {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
}
