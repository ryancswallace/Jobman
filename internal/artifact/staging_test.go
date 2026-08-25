package artifact

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ryancswallace/jobman/protocol"
)

func TestStageInputsAndPublishOutputs(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("department-nfs", 3, root)
	if err != nil {
		t.Fatal(err)
	}
	inputKey := "research/inputs/sample.txt"
	inputDigest, err := store.PutImmutable(inputKey, []byte("input data\n"))
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	err = StageInputs(t.Context(), store, workspace, []protocol.InputArtifact{{
		Name: "sample", Source: "artifact://department-nfs/" + inputKey,
		Target: "inputs:/nested/sample.txt", Checksum: inputDigest,
	}}, 1024)
	if err != nil {
		t.Fatalf("StageInputs() error = %v", err)
	}
	input, err := os.ReadFile(filepath.Join(workspace, "inputs", "nested", "sample.txt"))
	if err != nil || string(input) != "input data\n" {
		t.Fatalf("staged input = %q, %v", input, err)
	}
	outputPath := filepath.Join(workspace, "outputs", "result.txt")
	if err = os.MkdirAll(filepath.Dir(outputPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(outputPath, []byte("result\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	published, err := PublishOutputs(t.Context(), store, workspace, []protocol.OutputArtifact{
		{Name: "optional", Source: "outputs:/missing", Destination: "artifact://department-nfs/research/outputs/missing"},
		{Name: "result", Source: "outputs:/result.txt", Destination: "artifact://department-nfs/research/outputs/result.txt", Required: true},
	}, 1024)
	if err != nil || len(published) != 1 || published[0].Name != "result" ||
		published[0].StoreVersion != 3 || published[0].Checksum != Digest([]byte("result\n")) {
		t.Fatalf("PublishOutputs() = %#v, %v", published, err)
	}
	contents, err := store.ReadVerified(
		published[0].ObjectKey, published[0].ByteLength, published[0].Checksum,
	)
	if err != nil || string(contents) != "result\n" {
		t.Fatalf("published output = %q, %v", contents, err)
	}
}

func TestArtifactStagingRejectsUnsafeOrConflictingFiles(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("safe", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := store.PutImmutable("input", []byte("input"))
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	for name, input := range map[string]protocol.InputArtifact{
		"wrong store":  {Name: "input", Source: "artifact://other/input", Target: "inputs:/input"},
		"wrong digest": {Name: "input", Source: "artifact://safe/input", Target: "inputs:/input", Checksum: Digest([]byte("other"))},
		"encoded path": {Name: "input", Source: "artifact://safe/a%2fb", Target: "inputs:/input"},
	} {
		t.Run(name, func(t *testing.T) {
			if stageErr := StageInputs(t.Context(), store, t.TempDir(), []protocol.InputArtifact{input}, 1024); stageErr == nil {
				t.Fatal("StageInputs() error = nil")
			}
		})
	}
	if err = StageInputs(t.Context(), store, workspace, []protocol.InputArtifact{{
		Name: "input", Source: "artifact://safe/input", Target: "inputs:/input", Checksum: digest,
	}}, 4); err == nil || !strings.Contains(err.Error(), "bounded") {
		t.Fatalf("StageInputs(size limit) error = %v", err)
	}
	if _, err = PublishOutputs(t.Context(), store, workspace, []protocol.OutputArtifact{{
		Name: "required", Source: "outputs:/missing", Destination: "artifact://safe/output", Required: true,
	}}, 1024); err == nil {
		t.Fatal("PublishOutputs() accepted missing required output")
	}
	if runtime.GOOS != "windows" {
		outputDirectory := filepath.Join(workspace, "outputs")
		if err = os.MkdirAll(outputDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err = os.Symlink(filepath.Join(root, "input"), filepath.Join(outputDirectory, "linked")); err != nil {
			t.Fatal(err)
		}
		if _, err = PublishOutputs(t.Context(), store, workspace, []protocol.OutputArtifact{{
			Name: "linked", Source: "outputs:/linked", Destination: "artifact://safe/output", Required: true,
		}}, 1024); err == nil || !strings.Contains(err.Error(), "symbolic") {
			t.Fatalf("PublishOutputs(symlink) error = %v", err)
		}
	}
}

func TestArtifactStagingBoundaryMatrix(t *testing.T) {
	root := t.TempDir()
	store, err := NewFilesystemStore("safe", 1, root)
	if err != nil {
		t.Fatal(err)
	}
	if err = StageInputs(t.Context(), nil, t.TempDir(), nil, 1); err != nil {
		t.Fatalf("StageInputs(empty) error = %v", err)
	}
	if err = StageInputs(t.Context(), nil, t.TempDir(), []protocol.InputArtifact{{Name: "input"}}, 1); err == nil {
		t.Fatal("StageInputs() accepted a missing store")
	}
	if _, err = PublishOutputs(t.Context(), nil, t.TempDir(), nil, 1); err != nil {
		t.Fatalf("PublishOutputs(empty) error = %v", err)
	}
	if _, err = PublishOutputs(t.Context(), nil, t.TempDir(), []protocol.OutputArtifact{{Name: "output"}}, 1); err == nil {
		t.Fatal("PublishOutputs() accepted a missing store")
	}
	if _, _, err = parseArtifactURI("artifact://safe"); err == nil {
		t.Fatal("parseArtifactURI() accepted a missing object key")
	}
	if _, _, err = parseArtifactURI("https://safe/object"); err == nil {
		t.Fatal("parseArtifactURI() accepted an HTTP URI")
	}
	for _, logical := range []string{"wrong:/file", "inputs:/", "inputs:/../escape", `inputs:/bad\path`} {
		if _, pathErr := mapSandboxPath(t.TempDir(), logical, "inputs"); pathErr == nil {
			t.Errorf("mapSandboxPath(%q) error = nil", logical)
		}
	}
	digest, err := store.PutImmutable("input", []byte("input"))
	if err != nil {
		t.Fatal(err)
	}
	if err = StageInputs(t.Context(), store, t.TempDir(), []protocol.InputArtifact{{
		Name: "input", Source: "artifact://safe/input", Target: "wrong:/input", Checksum: digest,
	}}, 100); err == nil {
		t.Fatal("StageInputs() accepted a wrong logical root")
	}
	blockedWorkspace := t.TempDir()
	if err = os.WriteFile(filepath.Join(blockedWorkspace, "inputs"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = StageInputs(t.Context(), store, blockedWorkspace, []protocol.InputArtifact{{
		Name: "input", Source: "artifact://safe/input", Target: "inputs:/nested/input", Checksum: digest,
	}}, 100); err == nil {
		t.Fatal("StageInputs() accepted a file as the input root")
	}
	workspace := t.TempDir()
	outputDirectory := filepath.Join(workspace, "outputs", "directory")
	if err = os.MkdirAll(outputDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeOutput := func(name, contents string) {
		t.Helper()
		filename := filepath.Join(workspace, "outputs", name)
		if writeErr := os.WriteFile(filename, []byte(contents), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	writeOutput("result", "result")
	for name, output := range map[string]protocol.OutputArtifact{
		"wrong logical root": {Name: "result", Source: "wrong:/result", Destination: "artifact://safe/result", Required: true},
		"directory":          {Name: "directory", Source: "outputs:/directory", Destination: "artifact://safe/directory", Required: true},
		"invalid URI":        {Name: "result", Source: "outputs:/result", Destination: "https://safe/result", Required: true},
		"wrong store":        {Name: "result", Source: "outputs:/result", Destination: "artifact://other/result", Required: true},
		"size limit":         {Name: "result", Source: "outputs:/result", Destination: "artifact://safe/result", Required: true},
	} {
		t.Run(name, func(t *testing.T) {
			limit := int64(100)
			if name == "size limit" {
				limit = 1
			}
			if _, publishErr := PublishOutputs(t.Context(), store, workspace, []protocol.OutputArtifact{output}, limit); publishErr == nil {
				t.Fatal("PublishOutputs() error = nil")
			}
		})
	}
	writeOutput("alpha", "a")
	published, err := PublishOutputs(t.Context(), store, workspace, []protocol.OutputArtifact{
		{Name: "zulu", Source: "outputs:/result", Destination: "artifact://safe/zulu", Required: true},
		{Name: "alpha", Source: "outputs:/alpha", Destination: "artifact://safe/alpha", Required: true},
	}, 100)
	if err != nil || len(published) != 2 || published[0].Name != "alpha" {
		t.Fatalf("PublishOutputs(ordering) = %#v, %v", published, err)
	}
}
