package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	checksums := filepath.Join(root, "jobman_1.2.3_checksums.txt")
	const amd64Digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const arm64Digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	contents := amd64Digest + "  jobman_1.2.3_darwin_amd64.tar.gz\n" +
		arm64Digest + "  jobman_1.2.3_darwin_arm64.tar.gz\n"
	if err := os.WriteFile(checksums, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	output := filepath.Join(root, "Formula", "jobman.rb")
	if err := run(checksums, output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	formula, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`# typed: strict`,
		`# frozen_string_literal: true`,
		"/releases/download/v1.2.3/jobman_1.2.3_darwin_amd64.tar.gz",
		`sha256 "` + amd64Digest + `"`,
		"/releases/download/v1.2.3/jobman_1.2.3_darwin_arm64.tar.gz",
		`sha256 "` + arm64Digest + `"`,
		`bash_completion.install "docs/completions/bash/jobman"`,
		`zsh_completion.install "docs/completions/zsh/_jobman"`,
		`(etc/"jobman").install "etc/jobman/jobman.yml"`,
		`assert_match "jobman 1.2.3", shell_output("#{bin}/jobman --version")`,
	} {
		if !strings.Contains(string(formula), want) {
			t.Errorf("formula does not contain %q", want)
		}
	}
	if strings.Contains(string(formula), `version "1.2.3"`) {
		t.Error("formula contains a redundant explicit version")
	}
}

func TestRunRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	tests := []struct {
		name     string
		filename string
		contents string
	}{
		{name: "invalid name", filename: "checksums.txt"},
		{
			name:     "missing architecture",
			filename: "jobman_1.2.3_checksums.txt",
			contents: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
				"  jobman_1.2.3_darwin_amd64.tar.gz\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checksums := filepath.Join(root, test.name, test.filename)
			if err := os.MkdirAll(filepath.Dir(checksums), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(checksums, []byte(test.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := run(checksums, filepath.Join(root, test.name, "jobman.rb")); err == nil {
				t.Fatal("run() error = nil")
			}
		})
	}
}

func TestRunReportsFilesystemFailures(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	valid := filepath.Join(root, "jobman_1.2.3_checksums.txt")
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	contents := digest + "  jobman_1.2.3_darwin_amd64.tar.gz\n" +
		digest + "  jobman_1.2.3_darwin_arm64.tar.gz\n"
	if err := os.WriteFile(valid, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := run("", filepath.Join(root, "jobman.rb")); err == nil {
		t.Fatal("run(empty checksums) error = nil")
	}
	missing := filepath.Join(root, "missing", "jobman_1.2.3_checksums.txt")
	if err := run(missing, filepath.Join(root, "jobman.rb")); err == nil {
		t.Fatal("run(missing checksums) error = nil")
	}

	blocked := filepath.Join(root, "blocked")
	if err := os.WriteFile(blocked, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(valid, filepath.Join(blocked, "jobman.rb")); err == nil {
		t.Fatal("run(blocked output parent) error = nil")
	}

	outputDirectory := filepath.Join(root, "output")
	if err := os.Mkdir(outputDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := run(valid, outputDirectory); err == nil {
		t.Fatal("run(directory output) error = nil")
	}
}

func TestReadFormulaDataReportsScannerFailure(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "jobman_1.2.3_checksums.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 128*1024)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readFormulaData(path); err == nil {
		t.Fatal("readFormulaData(oversized line) error = nil")
	}
}

func TestExecute(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	checksums := filepath.Join(root, "jobman_1.2.3_checksums.txt")
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	contents := digest + "  jobman_1.2.3_darwin_amd64.tar.gz\n" +
		digest + "  jobman_1.2.3_darwin_arm64.tar.gz\n"
	if err := os.WriteFile(checksums, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	if status := execute([]string{
		"-checksums", checksums,
		"-output", filepath.Join(root, "jobman.rb"),
	}, &stderr); status != 0 {
		t.Fatalf("execute(valid) status = %d, stderr = %q", status, stderr.String())
	}
	if status := execute([]string{"-unknown"}, &stderr); status != 1 {
		t.Errorf("execute(invalid flag) status = %d", status)
	}
	if status := execute(nil, &stderr); status != 1 {
		t.Errorf("execute(missing arguments) status = %d", status)
	}
}

func TestWriteFormulaReportsFailures(t *testing.T) {
	t.Parallel()

	want := errors.New("failed")
	if err := writeFormula(&failingWriteCloser{writeErr: want}, formulaData{}); !errors.Is(err, want) {
		t.Errorf("writeFormula(write failure) error = %v", err)
	}
	if err := writeFormula(&failingWriteCloser{closeErr: want}, formulaData{}); !errors.Is(err, want) {
		t.Errorf("writeFormula(close failure) error = %v", err)
	}
}

type failingWriteCloser struct {
	writeErr error
	closeErr error
}

func (writer *failingWriteCloser) Write(data []byte) (int, error) {
	if writer.writeErr != nil {
		return 0, writer.writeErr
	}
	return len(data), nil
}

func (writer *failingWriteCloser) Close() error {
	return writer.closeErr
}
