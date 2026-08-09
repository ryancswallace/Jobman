package jobman

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestExternalCommandDispatchPreservesBoundariesAndReplacesProtocol(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state with space")
	configPath := filepath.Join(t.TempDir(), "config file.yaml")
	var invocation extensionInvocation
	deps := extensionTestDependencies(t, func(_ context.Context, value extensionInvocation) (int, error) {
		invocation = value
		if _, err := value.Stdout.Write([]byte("extension output\n")); err != nil {
			return 0, err
		}
		return 0, nil
	})
	stdout, err := executeCommand(t, deps, []string{
		"--state-dir", stateDir, "diagnose", "--config", configPath,
		"--json", "job name", "--literal=value",
	})
	if err != nil {
		t.Fatalf("external command error = %v", err)
	}
	if stdout != "extension output\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	wantExtensionPath := cleanAbsoluteTestPath(t, "/extensions/jobman-diagnose")
	wantCoreExecutable := cleanAbsoluteTestPath(t, "/core/jobman")
	if invocation.Path != wantExtensionPath ||
		!reflect.DeepEqual(invocation.Args, []string{"--json", "job name", "--literal=value"}) {
		t.Fatalf("invocation path/args = %q / %#v", invocation.Path, invocation.Args)
	}
	values := environmentMap(invocation.Env)
	if values["SAFE"] != "value" || values["JOBMAN_EXTENSION_PROTOCOL"] != "1" ||
		values["JOBMAN_EXECUTABLE"] != wantCoreExecutable || values["JOBMAN_STATE_DIR"] != stateDir ||
		values["JOBMAN_CONFIG"] != configPath || values["JOBMAN_NO_EXTENSIONS"] != "1" ||
		values["JOBMAN_VERSION"] == "" {
		t.Fatalf("extension environment = %#v", values)
	}
	if strings.Contains(strings.Join(invocation.Env, "\n"), "malicious") {
		t.Fatalf("reserved environment leaked: %#v", invocation.Env)
	}
}

func TestExternalCommandStatusIsPreservedAndSilent(t *testing.T) {
	t.Parallel()

	deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) {
		return 23, nil
	})
	_, err := executeCommand(t, deps, []string{"--state-dir", t.TempDir(), "diagnose", "job"})
	if err == nil || ExitCode(err) != 23 || ShouldPrintError(err) {
		t.Fatalf("external status error/code/print = %v/%d/%t", err, ExitCode(err), ShouldPrintError(err))
	}
}

func TestExternalCommandControlsFailClosed(t *testing.T) {
	t.Parallel()

	for name, arguments := range map[string][]string{
		"flag before": {"--no-extensions", "diagnose"},
		"flag after":  {"diagnose", "--no-extensions"},
	} {
		t.Run(name, func(t *testing.T) {
			deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) {
				t.Fatal("disabled extension was executed")
				return 0, nil
			})
			if _, err := executeCommand(t, deps, arguments); !errors.Is(err, errUsage) {
				t.Fatalf("disabled extension error = %v", err)
			}
		})
	}
	t.Run("environment", func(t *testing.T) {
		deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) {
			t.Fatal("disabled extension was executed")
			return 0, nil
		})
		deps.Getenv = func(name string) string {
			if name == "JOBMAN_NO_EXTENSIONS" {
				return "1"
			}
			return ""
		}
		if _, err := executeCommand(t, deps, []string{"diagnose"}); !errors.Is(err, errUsage) {
			t.Fatalf("disabled extension error = %v", err)
		}
	})
}

func TestExternalCommandValidationAndBuiltInPrecedence(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"Diagnose", "diag/nose", "diag.nose", "-diagnose", "diagnose-", "diag--nose", "_private"} {
		if validExtensionName(name) {
			t.Errorf("validExtensionName(%q) = true", name)
		}
	}
	for _, name := range []string{"diagnose", "diagnose2", "support-bundle"} {
		if !validExtensionName(name) {
			t.Errorf("validExtensionName(%q) = false", name)
		}
	}
	deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) {
		t.Fatal("built-in command dispatched externally")
		return 0, nil
	})
	deps.LookPath = func(string) (string, error) {
		t.Fatal("built-in command performed extension lookup")
		return "", fs.ErrNotExist
	}
	if _, err := executeCommand(t, deps, []string{"show", "--help"}); err != nil {
		t.Fatalf("built-in show help error = %v", err)
	}

	missing := extensionTestDependencies(t, nil)
	missing.LookPath = func(string) (string, error) { return "", fs.ErrNotExist }
	if _, err := executeCommand(t, missing, []string{"missing"}); !errors.Is(err, errUsage) {
		t.Fatalf("missing extension error = %v", err)
	}
}

func TestParseExtensionArgumentsRemovesOnlyReservedFlags(t *testing.T) {
	t.Parallel()

	parsed, err := parseExtensionArguments([]string{
		"--state-dir=/state", "--custom", "value", "--config", "/config", "--no-extensions=false", "--", "tail",
	}, &rootOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.stateDir != "/state" || parsed.configPath != "/config" || parsed.disabled ||
		!reflect.DeepEqual(parsed.childArgs, []string{"--custom", "value", "--", "tail"}) {
		t.Fatalf("parsed arguments = %#v", parsed)
	}
	for _, arguments := range [][]string{{"--state-dir"}, {"--config="}, {"--no-extensions=maybe"}} {
		if _, err := parseExtensionArguments(arguments, &rootOptions{}); err == nil {
			t.Fatalf("parseExtensionArguments(%v) error = nil", arguments)
		}
	}
}

func TestExternalCommandDispatchErrorBoundaries(t *testing.T) {
	t.Parallel()

	if got := (&extensionExitError{code: 7}).Error(); got != "external command exited with status 7" {
		t.Fatalf("extensionExitError.Error() = %q", got)
	}
	stateDir := t.TempDir()
	for name, configure := range map[string]func(*dependencies){
		"lookup": func(value *dependencies) {
			value.LookPath = func(string) (string, error) { return "", errors.New("lookup failed") }
		},
		"executable": func(value *dependencies) {
			value.Executable = func() (string, error) { return "", errors.New("executable failed") }
		},
		"runner": func(value *dependencies) {
			value.RunExtension = func(context.Context, extensionInvocation) (int, error) {
				return 0, errors.New("runner failed")
			}
		},
		"negative status": func(value *dependencies) {
			value.RunExtension = func(context.Context, extensionInvocation) (int, error) { return -1, nil }
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) { return 0, nil })
			configure(&deps)
			if _, err := executeCommand(t, deps, []string{"--state-dir", stateDir, "diagnose"}); err == nil {
				t.Fatal("external command error = nil")
			}
		})
	}

	for name, clear := range map[string]func(*dependencies){
		"lookup":      func(value *dependencies) { value.LookPath = nil },
		"executable":  func(value *dependencies) { value.Executable = nil },
		"environment": func(value *dependencies) { value.Environment = nil },
		"runner":      func(value *dependencies) { value.RunExtension = nil },
	} {
		t.Run("missing "+name, func(t *testing.T) {
			t.Parallel()
			deps := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) { return 0, nil })
			clear(&deps)
			if _, err := executeCommand(t, deps, []string{"diagnose"}); !errors.Is(err, errUsage) {
				t.Fatalf("missing dependency error = %v, want usage", err)
			}
		})
	}

	for _, arguments := range [][]string{
		{"--config"}, {"--state-dir", ""}, {"--config", ""}, {"--state-dir="},
	} {
		if _, err := parseExtensionArguments(arguments, &rootOptions{}); err == nil {
			t.Errorf("parseExtensionArguments(%q) error = nil", arguments)
		}
	}
}

func TestDispatchExternalDirectValidationBoundaries(t *testing.T) {
	t.Parallel()

	command := &cobra.Command{Use: "jobman"}
	command.SetContext(t.Context())
	root := &rootOptions{stateDir: t.TempDir()}
	base := extensionTestDependencies(t, func(context.Context, extensionInvocation) (int, error) { return 0, nil })
	for name, arguments := range map[string][]string{
		"invalid name":  {"Bad"},
		"invalid flags": {"diagnose", "--config"},
		"disabled":      {"diagnose", "--no-extensions"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := dispatchExternal(command, base, root, arguments); err == nil {
				t.Fatal("dispatchExternal() error = nil")
			}
		})
	}
	notFound := base
	notFound.LookPath = func(string) (string, error) { return "", fs.ErrNotExist }
	if err := dispatchExternal(command, notFound, root, []string{"diagnose"}); err == nil {
		t.Fatal("dispatchExternal(not found) error = nil")
	}
	parsed, err := parseExtensionArguments([]string{"--state-dir", "/state"}, &rootOptions{})
	if err != nil || parsed.stateDir != "/state" {
		t.Fatalf("parseExtensionArguments(separate state) = (%#v, %v)", parsed, err)
	}
}

func extensionTestDependencies(t *testing.T, run runExtensionFunc) dependencies {
	t.Helper()
	return dependencies{
		LookPath: func(name string) (string, error) {
			if name != "jobman-diagnose" {
				t.Fatalf("LookPath(%q)", name)
			}
			return "/extensions/jobman-diagnose", nil
		},
		Executable: func() (string, error) { return "/core/jobman", nil },
		Environment: func() []string {
			return []string{"SAFE=value", "JOBMAN_EXECUTABLE=malicious", "JOBMAN_EXTENSION_FAKE=malicious"}
		},
		Getenv:       func(string) string { return "" },
		RunExtension: run,
	}
}

func cleanAbsoluteTestPath(t *testing.T, value string) string {
	t.Helper()
	absolute, err := filepath.Abs(value)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", value, err)
	}
	return filepath.Clean(absolute)
}

func environmentMap(values []string) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		name, content, found := strings.Cut(value, "=")
		if found {
			result[name] = content
		}
	}

	return result
}
