package slurm

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestObserveCompletedArrayTaskAfterQueueFailure(t *testing.T) {
	t.Parallel()
	for _, nativeID := range []string{"45_0", "45_2", "45_4", "4294967295_9999"} {
		t.Run(nativeID, func(t *testing.T) {
			t.Parallel()
			runner := &fakeRunner{responses: []fakeResponse{
				// Even seemingly valid output from a failed command is untrusted.
				{output: nativeID + "|RUNNING|None\n", err: errors.New("exit status 1")},
				{output: nativeID + "|COMPLETED|0:0|None|jobman-lab\n"},
			}}
			adapter, err := New(runner)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := adapter.Observe(t.Context(), nativeID)
			if err != nil || observed.JobID != nativeID || !observed.Terminal ||
				observed.Result == nil || observed.Result.Outcome != "success" {
				t.Fatalf("Observe() = %#v, %v", observed, err)
			}
			// Actual Slurm JobIDRaw values 46/47/45 are allocation identities,
			// while JobID exposes the requested 45_0/45_2/45_4 task identities.
			want := fakeCall{name: "sacct", args: []string{
				"--jobs=" + nativeID, "--noheader", "--parsable2", "--allocations", "--array",
				"--format=JobID%64,State,ExitCode,Reason,Cluster",
			}}
			if len(runner.calls) != 2 || !reflect.DeepEqual(runner.calls[1], want) {
				t.Fatalf("accounting calls = %#v", runner.calls)
			}
		})
	}
}

func TestObserveQueueFailureDoesNotInventAccountingFacts(t *testing.T) {
	t.Parallel()
	queueErr := errors.New("queue unavailable")
	accountingErr := errors.New("accounting unavailable")
	for _, test := range []struct {
		name     string
		response fakeResponse
	}{
		{name: "no allocation", response: fakeResponse{}},
		{name: "native allocation ID", response: fakeResponse{output: "46|COMPLETED|0:0|None|jobman-lab\n"}},
		{name: "different task", response: fakeResponse{output: "45_2|COMPLETED|0:0|None|jobman-lab\n"}},
		{name: "compressed task set", response: fakeResponse{output: "45_[0-4]|COMPLETED|0:0|None|jobman-lab\n"}},
		{name: "duplicate task", response: fakeResponse{output: "45_0|COMPLETED|0:0|None|jobman-lab\n45_0|COMPLETED|0:0|None|jobman-lab\n"}},
		{name: "failed accounting", response: fakeResponse{output: "45_0|COMPLETED|0:0|None|jobman-lab\n", err: accountingErr}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(&fakeRunner{responses: []fakeResponse{{err: queueErr}, test.response}})
			if err != nil {
				t.Fatal(err)
			}
			observed, err := adapter.Observe(t.Context(), "45_0")
			if !errors.Is(err, queueErr) || observed.JobID != "" || observed.Terminal || observed.Result != nil {
				t.Fatalf("unverified observation = %#v, %v", observed, err)
			}
			if test.response.err != nil && !errors.Is(err, accountingErr) {
				t.Fatalf("accounting error lost: %v", err)
			}
		})
	}
}

type cancellingObservationRunner struct {
	cancel context.CancelFunc
	calls  int
	stage  int
}

func (runner *cancellingObservationRunner) Run(context.Context, string, ...string) ([]byte, error) {
	runner.calls++
	if runner.calls == runner.stage {
		runner.cancel()
	}
	if runner.calls == 1 {
		return nil, errors.New("queue unavailable")
	}

	return []byte("45_0|COMPLETED|0:0|None|jobman-lab\n"), nil
}

func TestObserveCancellationPreventsFallbackOrPublication(t *testing.T) {
	t.Parallel()
	for _, stage := range []int{1, 2} {
		ctx, cancel := context.WithCancel(t.Context())
		runner := &cancellingObservationRunner{cancel: cancel, stage: stage}
		adapter, err := New(runner)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := adapter.Observe(ctx, "45_0")
		cancel()
		if !errors.Is(err, context.Canceled) || runner.calls != stage || observed.Terminal || observed.Result != nil {
			t.Fatalf("canceled stage %d = %#v, %v; calls %d", stage, observed, err, runner.calls)
		}
	}
}
