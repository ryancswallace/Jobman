package jobman

import (
	"os"
	"os/exec"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/supervisor"
)

// defaultDependencies wires the production application while keeping command
// construction free of package-global Cobra or Viper state.
func defaultDependencies() dependencies {
	return dependencies{
		OpenBackend:  app.Open,
		Supervise:    supervisor.Run,
		LookPath:     exec.LookPath,
		Executable:   os.Executable,
		Environment:  os.Environ,
		Getenv:       os.Getenv,
		RunExtension: runExtension,
	}
}
