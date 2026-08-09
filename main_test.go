package main

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

type testSilentError struct{}

func (testSilentError) Error() string { return "already printed" }
func (testSilentError) Silent() bool  { return true }

func TestMainWithSuccessAndFailure(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	exitCode := -1
	mainWith(func() error { return nil }, &stderr, func(code int) { exitCode = code })
	if stderr.Len() != 0 || exitCode != -1 {
		t.Fatalf("successful mainWith() stderr/code = %q/%d", stderr.String(), exitCode)
	}

	want := errors.New("failed")
	mainWith(func() error { return want }, &stderr, func(code int) { exitCode = code })
	if stderr.String() != "failed\n" || exitCode != 1 {
		t.Fatalf("failed mainWith() stderr/code = %q/%d", stderr.String(), exitCode)
	}
}

func TestMainWithSilentFailure(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	exitCode := -1
	mainWith(func() error { return testSilentError{} }, &stderr, func(code int) { exitCode = code })
	if stderr.Len() != 0 || exitCode != 1 {
		t.Fatalf("silent mainWith() stderr/code = %q/%d", stderr.String(), exitCode)
	}
}

func TestMainSuccess(t *testing.T) {
	arguments := os.Args
	os.Args = []string{"jobman", "--version"}
	t.Cleanup(func() { os.Args = arguments })

	main()
}
