// Command homebrewformula generates Jobman's Homebrew formula from a release
// checksum manifest.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

var (
	versionRE = regexp.MustCompile(`^jobman_(\d+\.\d+\.\d+)_checksums\.txt$`)
	digestRE  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type formulaData struct {
	Version      string
	AMD64Archive string
	AMD64Digest  string
	ARM64Archive string
	ARM64Digest  string
}

func main() {
	os.Exit(execute(os.Args[1:], os.Stderr))
}

func execute(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("homebrewformula", flag.ContinueOnError)
	flags.SetOutput(stderr)
	checksums := flags.String("checksums", "", "release checksum manifest")
	output := flags.String("output", "", "formula output path")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if err := run(*checksums, *output); err != nil {
		fmt.Fprintf(stderr, "generate Homebrew formula: %v\n", err)
		return 1
	}
	return 0
}

func run(checksums, output string) error {
	if checksums == "" || output == "" {
		return errors.New("-checksums and -output are required")
	}

	data, err := readFormulaData(checksums)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(output), 0o750)
	if err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	file, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	return writeFormula(file, data)
}

func writeFormula(file io.WriteCloser, data formulaData) error {
	if err := formulaTemplate.Execute(file, data); err != nil {
		_ = file.Close()
		return fmt.Errorf("render formula: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	return nil
}

func readFormulaData(path string) (formulaData, error) {
	match := versionRE.FindStringSubmatch(filepath.Base(path))
	if match == nil {
		return formulaData{}, fmt.Errorf("invalid checksum manifest name %q", filepath.Base(path))
	}

	data := formulaData{
		Version:      match[1],
		AMD64Archive: "jobman_" + match[1] + "_darwin_amd64.tar.gz",
		ARM64Archive: "jobman_" + match[1] + "_darwin_arm64.tar.gz",
	}
	file, err := os.Open(path)
	if err != nil {
		return formulaData{}, fmt.Errorf("open checksums: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || !digestRE.MatchString(fields[0]) {
			continue
		}
		switch fields[1] {
		case data.AMD64Archive:
			data.AMD64Digest = fields[0]
		case data.ARM64Archive:
			data.ARM64Digest = fields[0]
		}
	}
	if err := scanner.Err(); err != nil {
		return formulaData{}, fmt.Errorf("read checksums: %w", err)
	}
	if data.AMD64Digest == "" || data.ARM64Digest == "" {
		return formulaData{}, errors.New("checksum manifest is missing a macOS release archive")
	}
	return data, nil
}

var formulaTemplate = template.Must(template.New("formula").Parse(`# typed: strict
# frozen_string_literal: true

# Installs Jobman from its verified release archive.
class Jobman < Formula
  desc "Daemonless command-line job manager with retries, timeouts, and logs"
  homepage "https://github.com/ryancswallace/jobman"
  license "MIT"

  on_macos do
    on_intel do
      url "https://github.com/ryancswallace/jobman/releases/download/v{{ .Version }}/{{ .AMD64Archive }}"
      sha256 "{{ .AMD64Digest }}"
    end
    on_arm do
      url "https://github.com/ryancswallace/jobman/releases/download/v{{ .Version }}/{{ .ARM64Archive }}"
      sha256 "{{ .ARM64Digest }}"
    end
  end

  def install
    bin.install "jobman"
    bash_completion.install "docs/completions/bash/jobman"
    zsh_completion.install "docs/completions/zsh/_jobman"
    man1.install Dir["docs/manpage/jobman*.1"]
    (etc/"jobman").install "etc/jobman/jobman.yml"
  end

  test do
    assert_match "jobman {{ .Version }}", shell_output("#{bin}/jobman --version")
  end
end
`))
