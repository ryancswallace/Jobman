// Package jobman implements the Jobman command-line interface.
package jobman

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/app"
	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/config"
	"github.com/ryancswallace/jobman/internal/model"
)

type openBackendFunc func(context.Context, string) (app.Backend, error)

type superviseFunc func(context.Context, string, string, io.Reader, io.Writer) error

type extensionInvocation struct {
	Path   string
	Args   []string
	Env    []string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type runExtensionFunc func(context.Context, extensionInvocation) (int, error)

type dependencies struct {
	OpenBackend  openBackendFunc
	Supervise    superviseFunc
	LookPath     func(string) (string, error)
	Executable   func() (string, error)
	Environment  func() []string
	Getenv       func(string) string
	RunExtension runExtensionFunc
}

type rootOptions struct {
	stateDir     string
	configPath   string
	noExtensions bool
}

// NewCommand constructs an independent production Jobman command tree. The
// returned command has no process-global Cobra state and is suitable for help,
// completion, and embedding in another command runner.
func NewCommand() *cobra.Command { return newRootCommand(defaultDependencies()) }

func newRootCommand(dependencies dependencies) *cobra.Command {
	options := &rootOptions{}
	command := &cobra.Command{
		Use:           "jobman",
		Short:         "Run and manage background jobs without a shared daemon",
		Long:          "Jobman starts and manages durable per-user command-line jobs without a continuously running shared daemon.",
		Version:       buildinfo.Display(),
		Args:          usageArgs(func(_ *cobra.Command, _ []string) error { return nil }),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(command *cobra.Command, arguments []string) error {
			if len(arguments) == 0 {
				return command.Help()
			}

			return dispatchExternal(command, dependencies, options, arguments)
		},
	}
	command.SetVersionTemplate("{{.Name}} {{.Version}}\n")
	command.PersistentFlags().StringVar(
		&options.stateDir,
		"state-dir",
		"",
		"override the per-user state directory",
	)
	command.PersistentFlags().BoolVar(
		&options.noExtensions,
		"no-extensions",
		false,
		"disable external command discovery",
	)
	command.Flags().SetInterspersed(false)
	command.PersistentFlags().StringVar(
		&options.configPath,
		"config",
		"",
		"use an explicit YAML configuration file",
	)
	command.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError(err)
	})

	command.AddCommand(
		newRunCommand(dependencies, options),
		newListCommand(dependencies, options),
		newStatusCommand(dependencies, options),
		newShowCommand(dependencies, options),
		newLogsCommand(dependencies, options),
		newCancelCommand(dependencies, options),
		newPauseCommand(dependencies, options),
		newResumeCommand(dependencies, options),
		newWaitCommand(dependencies, options),
		newInputCommand(dependencies, options),
		newRerunCommand(dependencies, options),
		newCleanCommand(dependencies, options),
		newDoctorCommand(dependencies, options),
		newConfigCommand(dependencies, options),
		newSupervisorCommand(dependencies, options),
	)

	return command
}

// Execute runs a production command tree using process-global CLI streams.
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	command := newRootCommand(defaultDependencies())
	command.SetArgs(os.Args[1:])
	command.SetIn(os.Stdin)
	command.SetOut(os.Stdout)
	command.SetErr(os.Stderr)

	return command.ExecuteContext(ctx)
}

// ExitCode maps a returned command error to Jobman's stable process status.
func ExitCode(err error) int {
	var validationError *model.ValidationError
	var extensionError *extensionExitError

	switch {
	case err == nil:
		return 0
	case errors.As(err, &extensionError):
		return extensionError.code
	case errors.Is(err, errUsage):
		return 2
	case errors.Is(err, config.ErrInvalid):
		return 2
	case errors.As(err, &validationError):
		return 2
	case errors.Is(err, app.ErrNotFound):
		return 3
	case errors.Is(err, app.ErrAmbiguous):
		return 4
	case errors.Is(err, app.ErrConflict):
		return 5
	case errors.Is(err, io.ErrShortWrite):
		return 6
	default:
		return 1
	}
}

// ShouldPrintError reports whether the executable boundary should render an
// error. External children own their own diagnostics and only return status.
func ShouldPrintError(err error) bool {
	var silent interface{ Silent() bool }

	return err != nil && (!errors.As(err, &silent) || !silent.Silent())
}

func openBackend(
	ctx context.Context,
	dependencies dependencies,
	options *rootOptions,
) (app.Backend, error) {
	if dependencies.OpenBackend == nil {
		return nil, errors.New("application backend is unavailable")
	}

	stateDir, err := config.StateDir(options.stateDir)
	if err != nil {
		return nil, err
	}
	backend, err := dependencies.OpenBackend(ctx, stateDir)
	if err != nil {
		return nil, fmt.Errorf("open job manager: %w", err)
	}

	return backend, nil
}

func withBackend(
	command *cobra.Command,
	dependencies dependencies,
	options *rootOptions,
	operation func(app.Backend) error,
) (returned error) {
	configureBestEffortRedactor(command, options)
	backend, err := openBackend(command.Context(), dependencies, options)
	if err != nil {
		return redactCommandError(command, err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			returned = redactCommandError(
				command,
				errors.Join(returned, fmt.Errorf("close job manager: %w", closeErr)),
			)
		}
	}()

	return redactCommandError(command, operation(backend))
}

func withConfiguredBackend(
	command *cobra.Command,
	dependencies dependencies,
	options *rootOptions,
	operation func(app.Backend, config.Loaded) error,
) (returned error) {
	return withLoadedBackend(command, dependencies, options, func(backend app.Backend, loaded config.Loaded) error {
		if err := applyBackendConfiguration(command.Context(), backend, loaded.Config); err != nil {
			return err
		}

		return operation(backend, loaded)
	})
}

func applyBackendConfiguration(ctx context.Context, backend app.Backend, configuration config.Config) error {
	configurable, ok := backend.(app.ConfigurableBackend)
	if !ok {
		return errors.New("application backend does not support durable configuration")
	}
	if err := configurable.ApplyConfig(ctx, configuration); err != nil {
		return fmt.Errorf("apply configuration: %w", err)
	}

	return nil
}

func withLoadedBackend(
	command *cobra.Command,
	dependencies dependencies,
	options *rootOptions,
	operation func(app.Backend, config.Loaded) error,
) (returned error) {
	configureBestEffortRedactor(command, options)
	backend, err := openBackend(command.Context(), dependencies, options)
	if err != nil {
		return redactCommandError(command, err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			returned = redactCommandError(
				command,
				errors.Join(returned, fmt.Errorf("close job manager: %w", closeErr)),
			)
		}
	}()
	loaded, err := loadConfiguration(options)
	if err != nil {
		return redactCommandError(command, err)
	}
	if err := configureRedactor(command, loaded.Config); err != nil {
		return redactCommandError(command, err)
	}
	if configurable, ok := backend.(app.ConfigurationBackend); ok {
		configurable.ConfigureInvocation(loaded.Config)
	}

	return redactCommandError(command, operation(backend, loaded))
}
