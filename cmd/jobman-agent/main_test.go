package main

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestMainWith(t *testing.T) {
	var stderr bytes.Buffer
	exitCode := 0
	mainWith(func() error { return nil }, &stderr, func(code int) { exitCode = code })
	if stderr.Len() != 0 || exitCode != 0 {
		t.Fatalf("successful mainWith() stderr/code = %q/%d", stderr.String(), exitCode)
	}

	want := errors.New("failed")
	mainWith(func() error { return want }, &stderr, func(code int) { exitCode = code })
	if stderr.String() != "failed\n" || exitCode != 1 {
		t.Fatalf("failed mainWith() stderr/code = %q/%d", stderr.String(), exitCode)
	}
}

func TestMainHelp(t *testing.T) {
	originalArgs := os.Args
	t.Cleanup(func() { os.Args = originalArgs })
	os.Args = []string{"jobman-agent", "help"}

	main()
}
