package agent

import (
	"context"
	"errors"

	"github.com/ryancswallace/jobman/internal/runtimeenv"
)

//nolint:nilnil // A nil adapter with no error represents valid native-only target policy.
func newContainerRuntime(
	ctx context.Context,
	engine, executable string,
	allowHostNetwork bool,
	runner runtimeenv.CommandRunner,
) (*runtimeenv.Adapter, error) {
	if engine == "" && executable == "" && !allowHostNetwork {
		return nil, nil
	}
	if engine == "" {
		return nil, errors.New("container engine is required")
	}
	adapter, err := runtimeenv.New(runtimeenv.Options{
		Engine: engine, Executable: executable,
		AllowHostNetwork: allowHostNetwork, Runner: runner,
	})
	if err != nil {
		return nil, err
	}
	if probeErr := adapter.Probe(ctx); probeErr != nil {
		return nil, probeErr
	}

	return adapter, nil
}

func appendContainerRunnerArguments(arguments []string, adapter *runtimeenv.Adapter) []string {
	if adapter == nil {
		return arguments
	}
	arguments = append(arguments, "--container-engine", adapter.Engine())
	if adapter.Executable() != adapter.Engine() {
		arguments = append(arguments, "--container-runtime", adapter.Executable())
	}
	if adapter.AllowHostNetwork() {
		arguments = append(arguments, "--container-host-network")
	}

	return arguments
}
