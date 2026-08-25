// Command jobman-agent runs the named-host Jobman execution agent.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryancswallace/jobman/internal/agent"
)

func main() {
	mainWith(executeAgentCLI, os.Stderr, os.Exit)
}

func executeAgentCLI() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return agent.ExecuteCLI(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}

func mainWith(execute func() error, stderr io.Writer, exit func(int)) {
	if err := execute(); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		exit(1)
	}
}
