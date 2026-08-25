package slurm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ryancswallace/jobman/protocol"
)

type fakeCall struct {
	name string
	args []string
}

type fakeResponse struct {
	output string
	err    error
}

type fakeRunner struct {
	calls     []fakeCall
	responses []fakeResponse
}

func (runner *fakeRunner) Run(_ context.Context, name string, arguments ...string) ([]byte, error) {
	runner.calls = append(runner.calls, fakeCall{name: name, args: append([]string(nil), arguments...)})
	if len(runner.responses) == 0 {
		return nil, errors.New("unexpected call")
	}
	response := runner.responses[0]
	runner.responses = runner.responses[1:]

	return []byte(response.output), response.err
}

func TestSubmitUsesExactArgumentsAndTranslatesResources(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{responses: []fakeResponse{{output: "12345;alpha\n"}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	submission, err := adapter.Submit(t.Context(), SubmitRequest{
		JobName: "jobman-abc", Partition: "gpu", ScriptPath: "/nfs/run.sh",
		StdoutPath: "/nfs/slurm.out", StderrPath: "/nfs/slurm.err",
		RunnerPath: "/opt/jobman-agent", RunnerArgs: []string{"run-execution", "--execution-id", "abc"},
		Resources: &protocol.Resources{
			CPU: 4, Memory: "1500MB", GPU: 2, Nodes: 1, Tasks: 8, WallTime: "61s",
		},
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if submission != (Submission{JobID: "12345", Cluster: "alpha"}) {
		t.Fatalf("Submit() = %#v", submission)
	}
	want := fakeCall{name: "sbatch", args: []string{
		"--parsable", "--job-name=jobman-abc", "--output=/nfs/slurm.out", "--error=/nfs/slurm.err",
		"--partition=gpu", "--cpus-per-task=4", "--mem=1431M", "--gpus=2", "--nodes=1",
		"--ntasks=8", "--time=2", "/nfs/run.sh", "/opt/jobman-agent",
		"run-execution", "--execution-id", "abc",
	}}
	if !reflect.DeepEqual(runner.calls, []fakeCall{want}) {
		t.Fatalf("calls = %#v, want %#v", runner.calls, []fakeCall{want})
	}
}

func TestSubmitArrayUsesBoundedContiguousTaskRange(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{responses: []fakeResponse{{output: "12345;alpha\n"}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := adapter.SubmitArray(t.Context(), ArraySubmitRequest{
		SubmitRequest: SubmitRequest{
			JobName: "jobman-array", Partition: "cpu", ScriptPath: "/nfs/run.sh",
			StdoutPath: "/nfs/%A_%a.out", StderrPath: "/nfs/%A_%a.err",
			RunnerPath: "/nfs/jobman-agent",
			RunnerArgs: []string{"run-array-task", "--manifest", "/nfs/array.json"},
		},
		TaskCount: 3, MaxParallel: 2,
	})
	if err != nil || submission.JobID != "12345" {
		t.Fatalf("SubmitArray() = %#v, %v", submission, err)
	}
	if len(runner.calls) != 1 || !reflect.DeepEqual(runner.calls[0].args, []string{
		"--parsable", "--job-name=jobman-array", "--output=/nfs/%A_%a.out",
		"--error=/nfs/%A_%a.err", "--array=0-2%2", "--partition=cpu",
		"/nfs/run.sh", "/nfs/jobman-agent", "run-array-task", "--manifest", "/nfs/array.json",
	}) {
		t.Fatalf("SubmitArray() calls = %#v", runner.calls)
	}
	for _, invalid := range []ArraySubmitRequest{
		{SubmitRequest: SubmitRequest{}, TaskCount: 1, MaxParallel: 1},
		{SubmitRequest: validSubmitRequest(), TaskCount: 0, MaxParallel: 1},
		{SubmitRequest: validSubmitRequest(), TaskCount: 2, MaxParallel: 3},
	} {
		if _, err = adapter.SubmitArray(t.Context(), invalid); err == nil {
			t.Fatalf("SubmitArray(%#v) unexpectedly succeeded", invalid)
		}
	}
}

func TestCompileAndAddressArrayTasks(t *testing.T) {
	t.Parallel()
	plan, err := CompileArray([]ArrayTask{
		{Index: 1, ExecutionID: "22222222-2222-4222-8222-222222222222"},
		{Index: 0, ExecutionID: "11111111-1111-4111-8111-111111111111"},
	}, 1)
	if err != nil || plan.Tasks[0].Index != 0 {
		t.Fatalf("CompileArray() = %#v, %v", plan, err)
	}
	if taskID, taskErr := ArrayTaskID("12345", 1); taskErr != nil || taskID != "12345_1" {
		t.Fatalf("ArrayTaskID() = %q, %v", taskID, taskErr)
	}
	runner := &fakeRunner{responses: []fakeResponse{{}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.CancelArrayTask(t.Context(), "12345", 1); err != nil ||
		len(runner.calls) != 1 || !reflect.DeepEqual(runner.calls[0].args, []string{"12345_1"}) {
		t.Fatalf("CancelArrayTask() error = %v, calls = %#v", err, runner.calls)
	}
	for _, tasks := range [][]ArrayTask{
		{{Index: 1, ExecutionID: "11111111-1111-4111-8111-111111111111"}},
		{{Index: 0, ExecutionID: "invalid"}},
		{{Index: 0, ExecutionID: "11111111-1111-4111-8111-111111111111"}, {Index: 1, ExecutionID: "11111111-1111-4111-8111-111111111111"}},
	} {
		if _, err = CompileArray(tasks, 1); err == nil {
			t.Fatalf("CompileArray(%#v) unexpectedly succeeded", tasks)
		}
	}
}

func validSubmitRequest() SubmitRequest {
	return SubmitRequest{
		JobName: "jobman", ScriptPath: "/nfs/run.sh", StdoutPath: "/nfs/out",
		StderrPath: "/nfs/err", RunnerPath: "/nfs/jobman-agent",
	}
}

func TestObserveFallsBackToAccounting(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{responses: []fakeResponse{
		{output: ""}, {output: "12345|COMPLETED|0:0|None|alpha\n"},
	}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	observation, err := adapter.Observe(t.Context(), "12345")
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !observation.Terminal || observation.State != "completed" ||
		observation.Result == nil || observation.Result.Outcome != "success" {
		t.Fatalf("Observe() = %#v", observation)
	}
}

func TestObserveNormalizesQueueState(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{responses: []fakeResponse{{output: "12345|PENDING|Resources\n"}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	observation, err := adapter.Observe(t.Context(), "12345")
	if err != nil || observation.State != "queued" || observation.Reason != "Resources" {
		t.Fatalf("Observe() = %#v, %v", observation, err)
	}
}

func TestFindByNameFailsClosedOnAmbiguity(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{responses: []fakeResponse{{output: "1|alpha\n2|alpha\n"}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := adapter.FindByName(t.Context(), "jobman-abc", "alice", time.Now()); err == nil {
		t.Fatal("FindByName() accepted ambiguous result")
	}
}

func TestResourceArgumentsRejectUnsupportedTemporaryStorage(t *testing.T) {
	t.Parallel()
	if _, err := resourceArguments(&protocol.Resources{TemporaryStorage: "1GiB"}); err == nil {
		t.Fatal("resourceArguments() accepted temporary storage")
	}
}

func TestNewAndProbe(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); err == nil {
		t.Fatal("New() accepted a nil runner")
	}
	runner := &fakeRunner{responses: []fakeResponse{{}, {}, {}, {}}}
	adapter, err := New(runner)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err = adapter.Probe(t.Context()); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if len(runner.calls) != 4 || runner.calls[0].name != "sbatch" ||
		runner.calls[3].name != "scancel" {
		t.Fatalf("Probe() calls = %#v", runner.calls)
	}
	failing, err := New(&fakeRunner{responses: []fakeResponse{{err: errors.New("missing")}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err = failing.Probe(t.Context()); err == nil {
		t.Fatal("Probe() accepted a failed command")
	}
}

func TestSubmitRejectsInvalidIntentAndNativeFailures(t *testing.T) {
	t.Parallel()
	valid := SubmitRequest{
		JobName: "jobman-abc", ScriptPath: "/nfs/run.sh", StdoutPath: "/nfs/out",
		StderrPath: "/nfs/err", RunnerPath: "/nfs/jobman-agent",
	}
	tests := []struct {
		name   string
		mutate func(*SubmitRequest)
	}{
		{name: "missing job name", mutate: func(value *SubmitRequest) { value.JobName = "" }},
		{name: "invalid partition", mutate: func(value *SubmitRequest) { value.Partition = "bad|partition" }},
		{name: "invalid runner argument", mutate: func(value *SubmitRequest) { value.RunnerArgs = []string{"bad\x00arg"} }},
		{name: "temporary storage", mutate: func(value *SubmitRequest) {
			value.Resources = &protocol.Resources{TemporaryStorage: "1GiB"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			test.mutate(&request)
			adapter, err := New(&fakeRunner{})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if _, err = adapter.Submit(t.Context(), request); err == nil {
				t.Fatal("Submit() accepted invalid intent")
			}
		})
	}
	for _, response := range []fakeResponse{
		{err: errors.New("connection lost")},
		{output: "bad|job\n"},
		{output: "123;alpha;extra\n"},
	} {
		runner := &fakeRunner{responses: []fakeResponse{response}}
		adapter, err := New(runner)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if _, err = adapter.Submit(t.Context(), valid); err == nil {
			t.Fatalf("Submit() accepted response %#v", response)
		}
	}
}

func TestObserveRejectsQueryAndResponseFailures(t *testing.T) {
	t.Parallel()
	adapter, err := New(&fakeRunner{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err = adapter.Observe(t.Context(), "bad|id"); err == nil {
		t.Fatal("Observe() accepted an invalid job ID")
	}
	tests := []struct {
		name      string
		responses []fakeResponse
	}{
		{name: "queue failure", responses: []fakeResponse{{err: errors.New("queue unavailable")}}},
		{name: "ambiguous queue", responses: []fakeResponse{{output: "1|RUNNING|None\n1|RUNNING|None\n"}}},
		{name: "terminal queue state", responses: []fakeResponse{{output: "123|COMPLETED|None\n"}}},
		{name: "accounting failure", responses: []fakeResponse{{}, {err: errors.New("accounting unavailable")}}},
		{name: "missing accounting row", responses: []fakeResponse{{}, {}}},
		{name: "wrong accounting ID", responses: []fakeResponse{{}, {output: "999|FAILED|1:0|None|alpha\n"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter, newErr := New(&fakeRunner{responses: test.responses})
			if newErr != nil {
				t.Fatalf("New() error = %v", newErr)
			}
			if _, observeErr := adapter.Observe(t.Context(), "123"); observeErr == nil {
				t.Fatal("Observe() accepted an invalid response")
			}
		})
	}
}

func TestFindByNameSuccessAndFailures(t *testing.T) {
	t.Parallel()
	adapter, err := New(&fakeRunner{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err = adapter.FindByName(t.Context(), "bad|name", "alice", time.Now()); err == nil {
		t.Fatal("FindByName() accepted an invalid lookup")
	}
	tests := []struct {
		name     string
		response fakeResponse
		want     Submission
		wantErr  bool
	}{
		{name: "success", response: fakeResponse{output: "123|alpha\n"}, want: Submission{JobID: "123", Cluster: "alpha"}},
		{name: "command failure", response: fakeResponse{err: errors.New("unavailable")}, wantErr: true},
		{name: "malformed response", response: fakeResponse{output: "123|alpha\nmalformed\n"}, wantErr: true},
		{name: "no match", response: fakeResponse{}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter, newErr := New(&fakeRunner{responses: []fakeResponse{test.response}})
			if newErr != nil {
				t.Fatalf("New() error = %v", newErr)
			}
			got, findErr := adapter.FindByName(
				t.Context(), "jobman-abc", "alice", time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
			)
			if test.wantErr && findErr == nil {
				t.Fatal("FindByName() unexpectedly succeeded")
			}
			if !test.wantErr && (findErr != nil || got != test.want) {
				t.Fatalf("FindByName() = %#v, %v", got, findErr)
			}
		})
	}
}

func TestCancelValidatesAndInvokesScancel(t *testing.T) {
	t.Parallel()
	adapter, err := New(&fakeRunner{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err = adapter.Cancel(t.Context(), "bad\nid"); err == nil {
		t.Fatal("Cancel() accepted an invalid job ID")
	}
	for _, response := range []fakeResponse{{}, {err: errors.New("denied")}} {
		runner := &fakeRunner{responses: []fakeResponse{response}}
		adapter, err = New(runner)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		cancelErr := adapter.Cancel(t.Context(), "123")
		if (response.err == nil) != (cancelErr == nil) {
			t.Fatalf("Cancel() error = %v", cancelErr)
		}
		if len(runner.calls) != 1 || runner.calls[0].name != "scancel" {
			t.Fatalf("Cancel() calls = %#v", runner.calls)
		}
	}
}

func TestResourceTranslationValidation(t *testing.T) {
	t.Parallel()
	if err := ValidateResources(nil); err != nil {
		t.Fatalf("ValidateResources(nil) error = %v", err)
	}
	for _, resources := range []*protocol.Resources{
		{Memory: "0GiB"},
		{Memory: strings.Repeat("9", 32) + "TiB"},
		{WallTime: "invalid"},
		{WallTime: "0s"},
	} {
		if err := ValidateResources(resources); err == nil {
			t.Fatalf("ValidateResources(%#v) unexpectedly succeeded", resources)
		}
	}
}

func TestNormalizeSchedulerStatesAndResults(t *testing.T) {
	t.Parallel()
	states := map[string]struct {
		state    string
		terminal bool
	}{
		"PENDING": {"queued", false}, "CONFIGURING": {"queued", false},
		"RUNNING": {"running", false}, "SUSPENDED": {"suspended", false},
		"COMPLETING": {"completing", false}, "COMPLETED": {"completed", true},
		slurmStateCancelled + " by 42": {stateCancelled, true}, "TIMEOUT": {stateTimedOut, true},
		"PREEMPTED": {"preempted", true}, "NODE_FAIL": {"node_failed", true},
		"OUT_OF_MEMORY": {"out_of_memory", true}, "BOOT_FAIL": {"boot_failed", true},
		"DEADLINE": {"deadline", true}, "REVOKED+": {"failed", true},
		"FUTURE_STATE": {"unknown", false},
	}
	for input, want := range states {
		state, terminal := normalizeState(input)
		if state != want.state || terminal != want.terminal {
			t.Errorf("normalizeState(%q) = %q, %v", input, state, terminal)
		}
	}
	results := []struct {
		state, status, outcome string
	}{
		{"completed", "0:0", "success"},
		{stateCancelled, "0:15", stateCancelled},
		{stateTimedOut, "1:0", stateTimedOut},
		{"lost", "1:0", "lost"},
		{"failed", "invalid", "failure"},
	}
	for _, test := range results {
		if result := schedulerResult(test.state, test.status); result.Outcome != test.outcome {
			t.Errorf("schedulerResult(%q, %q) = %#v", test.state, test.status, result)
		}
	}
	for _, value := range []string{"1", "bad:0", "-1:0", "0:256"} {
		if _, _, valid := parseExitStatus(value); valid {
			t.Errorf("parseExitStatus(%q) unexpectedly succeeded", value)
		}
	}
}
