package jobman

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/model"
)

const (
	matchingJobID   = "01980f4c-7b2a-7a6f-8c10-0123456789ac"
	matchingJobName = "nightly-report"
)

func TestJobSelectorCompletionReturnsIDAndNamePrefixMatches(t *testing.T) {
	t.Parallel()
	backend := completionBackend(t)
	complete := jobSelectorCompletion(dependenciesFor(backend), &rootOptions{})
	command := &cobra.Command{}

	completions, directive := complete(command, nil, "01980")
	want := []cobra.Completion{testJobID, matchingJobID}
	if !slices.Equal(completions, want) {
		t.Fatalf("completions = %q, want %q", completions, want)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("directive = %v, want no file completion", directive)
	}
	if !backend.closed {
		t.Fatal("completion backend was not closed")
	}

	backend.closed = false
	completions, _ = complete(command, nil, matchingJobID)
	if !slices.Equal(completions, []cobra.Completion{matchingJobID}) {
		t.Fatalf("unique completions = %q, want %q", completions, matchingJobID)
	}

	backend.closed = false
	completions, _ = complete(command, nil, "missing")
	if len(completions) != 0 {
		t.Fatalf("missing completions = %q, want none", completions)
	}

	backend.closed = false
	completions, _ = complete(command, nil, "nightly-")
	want = []cobra.Completion{"nightly-primary", matchingJobName}
	if !slices.Equal(completions, want) {
		t.Fatalf("name completions = %q, want %q", completions, want)
	}

	backend.jobs[0].Spec = mustTestSpec(t, matchingJobID)
	backend.closed = false
	completions, _ = complete(command, nil, "01980")
	want = []cobra.Completion{testJobID, matchingJobID}
	if !slices.Equal(completions, want) {
		t.Fatalf("deduplicated completions = %q, want %q", completions, want)
	}
}

func TestJobSelectorCompletionSuppressesAmbiguousNames(t *testing.T) {
	t.Parallel()
	backend := completionBackend(t)
	duplicate := backend.jobs[1]
	duplicate.ID = model.JobID("03980f4c-7b2a-7a6f-8c10-0123456789ae")
	backend.jobs = append(backend.jobs, duplicate)
	complete := jobSelectorCompletion(dependenciesFor(backend), &rootOptions{})

	completions, directive := complete(&cobra.Command{}, nil, matchingJobName)
	if len(completions) != 0 {
		t.Fatalf("ambiguous name completions = %q, want none", completions)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("directive = %v, want no file completion", directive)
	}
}

func TestCobraCompletionReturnsMatchingJobSelectors(t *testing.T) {
	t.Parallel()
	output, err := executeCommand(t, dependenciesFor(completionBackend(t)), []string{
		"__complete", "rerun", "01980",
	})
	if err != nil {
		t.Fatalf("completion command error = %v", err)
	}
	for _, id := range []string{testJobID, matchingJobID} {
		if !strings.Contains(output, id+"\n") {
			t.Errorf("completion output = %q, want %s", output, id)
		}
	}

	output, err = executeCommand(t, dependenciesFor(completionBackend(t)), []string{
		"__complete", "status", "nightly-",
	})
	if err != nil {
		t.Fatalf("name completion command error = %v", err)
	}
	for _, name := range []string{"nightly-primary", matchingJobName} {
		if !strings.Contains(output, name+"\n") {
			t.Errorf("completion output = %q, want %s", output, name)
		}
	}
}

func TestJobSelectorCompletionFailuresAreSilent(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("list failed")
	backend := completionBackend(t)
	backend.operationErr = wantErr
	complete := jobSelectorCompletion(dependenciesFor(backend), &rootOptions{})

	completions, directive := complete(&cobra.Command{}, nil, "019")
	if len(completions) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("failed completion = (%q, %v), want no candidates and no file completion", completions, directive)
	}
	if !backend.closed {
		t.Fatal("failed completion backend was not closed")
	}

	openFailure := jobSelectorCompletion(dependencies{OpenBackend: func(
		context.Context,
		string,
	) (app.Backend, error) {
		return nil, wantErr
	}}, &rootOptions{})
	completions, directive = openFailure(&cobra.Command{}, nil, "019")
	if len(completions) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("open failure completion = (%q, %v), want no candidates and no file completion", completions, directive)
	}
}

func TestRootCompletionOffersDiagnoseAndItsJobSelectors(t *testing.T) {
	t.Parallel()
	output, err := executeCommand(t, dependenciesFor(completionBackend(t)), []string{
		"__complete", "dia",
	})
	if err != nil {
		t.Fatalf("command-name completion error = %v", err)
	}
	if !strings.Contains(output, "diagnose\tDiagnose a failed job with the optional companion\n") {
		t.Fatalf("command-name completion output = %q, want diagnose", output)
	}

	output, err = executeCommand(t, dependenciesFor(completionBackend(t)), []string{
		"__complete", diagnoseExtension, "nightly-",
	})
	if err != nil {
		t.Fatalf("diagnose selector completion error = %v", err)
	}
	for _, name := range []string{"nightly-primary", matchingJobName} {
		if !strings.Contains(output, name+"\n") {
			t.Errorf("diagnose completion output = %q, want %s", output, name)
		}
	}

	output, err = executeCommand(t, dependenciesFor(completionBackend(t)), []string{
		"__complete", diagnoseExtension, "01980",
	})
	if err != nil {
		t.Fatalf("diagnose ID completion error = %v", err)
	}
	for _, id := range []string{testJobID, matchingJobID} {
		if !strings.Contains(output, id+"\n") {
			t.Errorf("diagnose completion output = %q, want %s", output, id)
		}
	}
}

func TestRootCompletionHonorsExtensionDisableControls(t *testing.T) {
	t.Parallel()
	tests := map[string]dependencies{
		"environment": {
			Getenv: func(string) string { return "1" },
		},
		"option": {},
	}
	for name, dependencies := range tests {
		root := &rootOptions{noExtensions: name == "option"}
		complete := rootArgumentCompletion(dependencies, root)
		completions, directive := complete(&cobra.Command{}, nil, "dia")
		if len(completions) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
			t.Errorf("%s disabled completion = (%q, %v), want none", name, completions, directive)
		}
	}
}

func TestEveryJobArgumentRegistersCompletion(t *testing.T) {
	t.Parallel()
	backend := completionBackend(t)
	root := newRootCommand(dependenciesFor(backend))
	paths := [][]string{
		{"status"},
		{"show"},
		{"show", "job"},
		{"show", "run"},
		{"logs"},
		{"cancel"},
		{"cancel", "job"},
		{"cancel", "run"},
		{"pause"},
		{"resume"},
		{"wait"},
		{"input"},
		{"rerun"},
		{"clean"},
	}
	for _, path := range paths {
		command := completionCommand(t, root, path...)
		if command.ValidArgsFunction == nil {
			t.Errorf("%v has no argument completion", path)

			continue
		}
		completions, directive := command.ValidArgsFunction(command, nil, matchingJobID)
		if !slices.Equal(completions, []cobra.Completion{matchingJobID}) {
			t.Errorf("%v completions = %q, want %q", path, completions, matchingJobID)
		}
		if directive != cobra.ShellCompDirectiveNoFileComp {
			t.Errorf("%v directive = %v, want no file completion", path, directive)
		}
		backend.closed = false
		completions, directive = command.ValidArgsFunction(command, nil, matchingJobName)
		if !slices.Equal(completions, []cobra.Completion{matchingJobName}) {
			t.Errorf("%v name completions = %q, want %q", path, completions, matchingJobName)
		}
		if directive != cobra.ShellCompDirectiveNoFileComp {
			t.Errorf("%v name directive = %v, want no file completion", path, directive)
		}
		backend.closed = false
	}

	runCommand := completionCommand(t, root, "run")
	if runCommand.ValidArgsFunction != nil {
		t.Fatal("run positional arguments unexpectedly gained job ID completion")
	}
}

func TestRunJobSelectorFlagsRegisterCompletion(t *testing.T) {
	t.Parallel()
	backend := completionBackend(t)
	runCommand := completionCommand(t, newRootCommand(dependenciesFor(backend)), "run")
	for _, flagName := range []string{"rerun", "after-success", "after-finish", "after-failed"} {
		complete, ok := runCommand.GetFlagCompletionFunc(flagName)
		if !ok {
			t.Errorf("--%s has no completion", flagName)

			continue
		}
		completions, directive := complete(runCommand, nil, matchingJobID)
		if !slices.Equal(completions, []cobra.Completion{matchingJobID}) {
			t.Errorf("--%s completions = %q, want %q", flagName, completions, matchingJobID)
		}
		if directive != cobra.ShellCompDirectiveNoFileComp {
			t.Errorf("--%s directive = %v, want no file completion", flagName, directive)
		}
		backend.closed = false
	}

	complete, ok := runCommand.GetFlagCompletionFunc("after-outcome")
	if !ok {
		t.Fatal("--after-outcome has no completion")
	}
	completions, directive := complete(runCommand, nil, matchingJobID)
	if !slices.Equal(completions, []cobra.Completion{matchingJobID + "="}) {
		t.Fatalf("--after-outcome completions = %q, want job ID followed by =", completions)
	}
	wantDirective := cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	if directive != wantDirective {
		t.Fatalf("--after-outcome directive = %v, want %v", directive, wantDirective)
	}
	completions, directive = complete(runCommand, nil, matchingJobID+"=")
	if len(completions) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("--after-outcome value completions = (%q, %v), want none", completions, directive)
	}
	completions, directive = complete(runCommand, nil, matchingJobName)
	if !slices.Equal(completions, []cobra.Completion{matchingJobName + "="}) {
		t.Fatalf("--after-outcome name completions = %q, want job name followed by =", completions)
	}
	if directive != wantDirective {
		t.Fatalf("--after-outcome name directive = %v, want %v", directive, wantDirective)
	}
}

func TestJobSelectorArgumentCompletionStopsAfterSelector(t *testing.T) {
	t.Parallel()
	opened := false
	complete := jobSelectorArgumentCompletion(dependencies{OpenBackend: func(
		context.Context,
		string,
	) (app.Backend, error) {
		opened = true

		return nil, errors.New("unexpected open")
	}}, &rootOptions{})

	completions, directive := complete(&cobra.Command{}, []string{testJobID}, "")
	if len(completions) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("second argument completion = (%q, %v), want none", completions, directive)
	}
	if opened {
		t.Fatal("second argument completion opened the backend")
	}
}

func completionBackend(t *testing.T) *fakeBackend {
	t.Helper()
	backend := newFakeBackend(t)
	backend.jobs[0].Spec = mustTestSpec(t, "nightly-primary")
	matching := backend.jobs[0]
	matching.ID = model.JobID(matchingJobID)
	matching.Spec = mustTestSpec(t, matchingJobName)
	unrelated := backend.jobs[0]
	unrelated.ID = model.JobID("02980f4c-7b2a-7a6f-8c10-0123456789ad")
	unrelated.Spec = mustTestSpec(t, "other-job")
	backend.jobs = append(backend.jobs, matching, unrelated)

	return backend
}

func completionCommand(t *testing.T, command *cobra.Command, path ...string) *cobra.Command {
	t.Helper()
	for _, name := range path {
		var found *cobra.Command
		for _, child := range command.Commands() {
			if child.Name() == name {
				found = child

				break
			}
		}
		if found == nil {
			t.Fatalf("command %v not found", path)
		}
		command = found
	}

	return command
}
