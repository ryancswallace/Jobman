package jobman

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ryancswallace/jobman/internal/buildinfo"
	"github.com/ryancswallace/jobman/internal/config"
)

const extensionProtocolVersion = "1"

var reservedExtensionEnvironment = []string{
	"JOBMAN_CONFIG",
	"JOBMAN_EXECUTABLE",
	"JOBMAN_EXTENSION_PROTOCOL",
	"JOBMAN_NO_EXTENSIONS",
	"JOBMAN_STATE_DIR",
	"JOBMAN_VERSION",
}

type extensionExitError struct{ code int }

func (err *extensionExitError) Error() string {
	return fmt.Sprintf("external command exited with status %d", err.code)
}
func (*extensionExitError) Silent() bool { return true }

//nolint:cyclop // Dispatch validates each protocol boundary before executing trusted native code.
func dispatchExternal(
	command *cobra.Command,
	dependencies dependencies,
	root *rootOptions,
	arguments []string,
) error {
	name := arguments[0]
	if !validExtensionName(name) {
		return unknownCommandError(command, name)
	}
	parsed, err := parseExtensionArguments(arguments[1:], root)
	if err != nil {
		return usageError(err)
	}
	if parsed.disabled || extensionDisabled(dependencies) {
		return usageError(fmt.Errorf("external commands are disabled; unknown command %q", name))
	}
	if dependencies.LookPath == nil || dependencies.Executable == nil || dependencies.Environment == nil ||
		dependencies.RunExtension == nil {
		return unknownCommandError(command, name)
	}
	path, err := dependencies.LookPath("jobman-" + name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return unknownCommandError(command, name)
		}

		return fmt.Errorf("resolve external command %q: %w", name, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve external command %q: %w", name, err)
	}
	coreExecutable, err := dependencies.Executable()
	if err != nil {
		return fmt.Errorf("resolve Jobman executable for extension: %w", err)
	}
	coreExecutable, err = filepath.Abs(coreExecutable)
	if err != nil {
		return fmt.Errorf("resolve Jobman executable for extension: %w", err)
	}
	stateDir, err := config.StateDir(parsed.stateDir)
	if err != nil {
		return err
	}
	configPath := ""
	if parsed.configPath != "" {
		configPath, err = filepath.Abs(parsed.configPath)
		if err != nil {
			return fmt.Errorf("resolve extension configuration path: %w", err)
		}
		configPath = filepath.Clean(configPath)
	}
	environment := extensionEnvironment(
		dependencies.Environment(), filepath.Clean(coreExecutable), stateDir, configPath,
	)
	status, err := dependencies.RunExtension(command.Context(), extensionInvocation{
		Path: filepath.Clean(path), Args: parsed.childArgs, Env: environment,
		Stdin: command.InOrStdin(), Stdout: command.OutOrStdout(), Stderr: command.ErrOrStderr(),
	})
	if err != nil {
		return fmt.Errorf("run external command %q: %w", name, err)
	}
	if status < 0 {
		return fmt.Errorf("run external command %q: invalid negative exit status %d", name, status)
	}
	if status != 0 {
		return &extensionExitError{code: status}
	}

	return nil
}

type parsedExtensionArguments struct {
	childArgs  []string
	stateDir   string
	configPath string
	disabled   bool
}

//nolint:gocognit,cyclop // Reserved flag forms are enumerated to preserve every nonreserved argument exactly.
func parseExtensionArguments(arguments []string, root *rootOptions) (parsedExtensionArguments, error) {
	parsed := parsedExtensionArguments{
		childArgs: make([]string, 0, len(arguments)), stateDir: root.stateDir,
		configPath: root.configPath, disabled: root.noExtensions,
	}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		switch {
		case argument == "--state-dir" || argument == "--config":
			remaining := arguments[index+1:]
			if len(remaining) == 0 {
				return parsedExtensionArguments{}, fmt.Errorf("%s requires a value", argument)
			}
			selected := remaining[0]
			index++
			if selected == "" {
				return parsedExtensionArguments{}, fmt.Errorf("%s requires a value", argument)
			}
			if argument == "--state-dir" {
				parsed.stateDir = selected
			} else {
				parsed.configPath = selected
			}
		case strings.HasPrefix(argument, "--state-dir="):
			parsed.stateDir = strings.TrimPrefix(argument, "--state-dir=")
			if parsed.stateDir == "" {
				return parsedExtensionArguments{}, errors.New("--state-dir requires a value")
			}
		case strings.HasPrefix(argument, "--config="):
			parsed.configPath = strings.TrimPrefix(argument, "--config=")
			if parsed.configPath == "" {
				return parsedExtensionArguments{}, errors.New("--config requires a value")
			}
		case argument == "--no-extensions":
			parsed.disabled = true
		case strings.HasPrefix(argument, "--no-extensions="):
			value, err := strconv.ParseBool(strings.TrimPrefix(argument, "--no-extensions="))
			if err != nil {
				return parsedExtensionArguments{}, errors.New("--no-extensions must be true or false")
			}
			parsed.disabled = value
		default:
			parsed.childArgs = append(parsed.childArgs, argument)
		}
	}
	if parsed.stateDir == "" && root.stateDir != "" || parsed.configPath == "" && root.configPath != "" {
		return parsedExtensionArguments{}, errors.New("reserved extension paths must not be empty")
	}

	return parsed, nil
}

func validExtensionName(name string) bool {
	if name == "" || name[0] < 'a' || name[0] > 'z' || strings.HasSuffix(name, "-") || strings.Contains(name, "--") {
		return false
	}
	for _, character := range name {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
			continue
		}

		return false
	}

	return true
}

func extensionDisabled(dependencies dependencies) bool {
	return dependencies.Getenv != nil && dependencies.Getenv("JOBMAN_NO_EXTENSIONS") == "1"
}

func extensionEnvironment(base []string, executable, stateDir, configPath string) []string {
	result := make([]string, 0, len(base)+6)
	for _, entry := range base {
		name, _, found := strings.Cut(entry, "=")
		if !found || reservedEnvironmentName(name) {
			continue
		}
		result = append(result, entry)
	}
	result = append(result,
		"JOBMAN_EXTENSION_PROTOCOL="+extensionProtocolVersion,
		"JOBMAN_EXECUTABLE="+executable,
		"JOBMAN_VERSION="+buildinfo.Version,
		"JOBMAN_STATE_DIR="+stateDir,
		"JOBMAN_NO_EXTENSIONS=1",
	)
	if configPath != "" {
		result = append(result, "JOBMAN_CONFIG="+configPath)
	}

	return result
}

func reservedEnvironmentName(name string) bool {
	return strings.HasPrefix(name, "JOBMAN_EXTENSION_") || slices.Contains(reservedExtensionEnvironment, name)
}

func unknownCommandError(command *cobra.Command, name string) error {
	return usageError(fmt.Errorf("unknown command %q for %q", name, command.CommandPath()))
}
