package jobman

import (
	"context"
	"errors"
	"os/exec"
)

func runExtension(ctx context.Context, invocation extensionInvocation) (int, error) {
	command := exec.CommandContext(ctx, invocation.Path, invocation.Args...) // #nosec G204 -- path is exact jobman-NAME lookup and no shell is involved.
	command.Env = invocation.Env
	command.Stdin = invocation.Stdin
	command.Stdout = invocation.Stdout
	command.Stderr = invocation.Stderr
	err := command.Run()
	if err == nil {
		return 0, nil
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return extensionProcessExitCode(exitError.ProcessState), nil
	}

	return 0, err
}
